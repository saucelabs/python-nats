# Copyright 2015-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json
import logging
from functools import partial
from random import shuffle
from urlparse import urlparse

from twisted.internet import task, defer, reactor
from twisted.internet.defer import inlineCallbacks, returnValue, DeferredQueue
from twisted.internet.protocol import ReconnectingClientFactory, Protocol, connectionDone

from nats import __lang__, __version__
from nats.io.errors import *
from nats.io.nuid import NUID
from nats.protocol.parser import *

CONNECT_PROTO = b'{0} {1}{2}'
PUB_PROTO     = b'{0} {1} {2} {3} {4}{5}{6}'
SUB_PROTO     = b'{0} {1} {2} {3}{4}'
UNSUB_PROTO   = b'{0} {1} {2}{3}'

INFO_OP       = b'INFO'
CONNECT_OP    = b'CONNECT'
PUB_OP        = b'PUB'
MSG_OP        = b'MSG'
SUB_OP        = b'SUB'
UNSUB_OP      = b'UNSUB'
PING_OP       = b'PING'
PONG_OP       = b'PONG'
OK_OP         = b'+OK'
ERR_OP        = b'-ERR'
_CRLF_        = b'\r\n'
_SPC_         = b' '
_EMPTY_       = b''

PING_PROTO    = b'{0}{1}'.format(PING_OP, _CRLF_)
PONG_PROTO    = b'{0}{1}'.format(PONG_OP, _CRLF_)

# Defaults
DEFAULT_PING_INTERVAL     = 120 # seconds
MAX_OUTSTANDING_PINGS     = 2
MAX_RECONNECT_ATTEMPTS    = 60
RECONNECT_TIME_WAIT       = 2   # seconds
DEFAULT_CONNECT_TIMEOUT   = 2   # seconds

DEFAULT_READ_BUFFER_SIZE  = 1024 * 1024 * 10
DEFAULT_WRITE_BUFFER_SIZE = None
DEFAULT_READ_CHUNK_SIZE   = 32768 * 2
DEFAULT_PENDING_SIZE      = 1024 * 1024
DEFAULT_MAX_PAYLOAD_SIZE  = 1048576

PROTOCOL = 1
INBOX_PREFIX = bytearray(b'_INBOX.')

log = logging.getLogger("python-nats")


class NatsProtocol(Protocol):

    DISCONNECTED = 0
    CONNECTED = 1
    CLOSED = 2
    RECONNECTING = 3
    CONNECTING = 4

    def __repr__(self):
        return "<nats client v{}>".format(__version__)

    def __init__(self, verbose=False, pedantic=False, name=None, dont_randomize=False,
                 ping_interval=DEFAULT_PING_INTERVAL, max_outstanding_pings=MAX_OUTSTANDING_PINGS,
                 max_payload=DEFAULT_MAX_PAYLOAD_SIZE):
        self.options = {}

        # INFO that we get upon connect from the server.
        self._server_info = {}
        self._max_payload_size = max_payload

        # Client connection state and clustering.
        self._socket = None
        self._status = NatsProtocol.DISCONNECTED
        self._server_pool = []
        self._current_server = None
        self._pending = []
        self._pending_size = 0
        self._loop = None
        self.stats = {
            'in_msgs':    0,
            'out_msgs':   0,
            'in_bytes':   0,
            'out_bytes':  0,
            'reconnects': 0,
            'errors_received': 0
        }

        # Storage and monotonically increasing index for subscription callbacks.
        self._subs = {}
        self._ssid = 0

        # Parser with state for processing the wire protocol.
        self._ps = Parser(self)
        self._err = None
        self._flush_queue = DeferredQueue()
        self._flusher_task = None

        self._nuid = NUID()

        # Ping interval to disconnect from unhealthy servers.
        self._ping_timer = None
        self._pings_outstanding = 0
        self._pongs = []

        self._error_cb = None
        self._close_cb = None
        self._disconnected_cb = None
        self._reconnected_cb = None

        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["dont_randomize"] = dont_randomize
        self.options["ping_interval"] = ping_interval
        self.options["max_outstanding_pings"] = max_outstanding_pings

    def dataReceived(self, data):
        self._ps.parse(data)

    def connectionMade(self):
        # We aren't officially connected yet until the INFO server msg has been processed
        self._status = NatsProtocol.CONNECTING

    def connectionLost(self, reason=connectionDone):
        self._status = NatsProtocol.DISCONNECTED
        self._ping_timer.stop()
        self._end_flusher_loop()
        # TODO probably need something like reason.throwExceptionIntoGenerator() for error callbacks

    @inlineCallbacks
    def _send_ping(self, future=None):
        if self._pings_outstanding > self.options["max_outstanding_pings"]:
            yield self._unbind()
        else:
            self.send_command(PING_PROTO)
            self._flush_pending()
            if future is None:
                future = defer.Deferred()
            self._pings_outstanding += 1
            self._pongs.append(future)

    def connect_command(self):
        """
        Generates a JSON string with the params to be used
        when sending CONNECT to the server.

          ->> CONNECT {"verbose": false, "pedantic": false, "lang": "python2" }

        """
        options = {
            "verbose":  self.options["verbose"],
            "pedantic": self.options["pedantic"],
            "lang":     __lang__,
            "version":  __version__,
            "protocol": PROTOCOL
        }
        if "auth_required" in self._server_info:
            if self._server_info["auth_required"]:
                # In case there is no password, then consider handle
                # sending a token instead.
                if self._current_server.uri.password is None:
                    options["auth_token"] = self._current_server.uri.username
                else:
                    options["user"] = self._current_server.uri.username
                    options["pass"] = self._current_server.uri.password
        if self.options["name"] is not None:
            options["name"] = self.options["name"]

        args = json.dumps(options, sort_keys=True)
        return CONNECT_PROTO.format(CONNECT_OP, args, _CRLF_)

    def send_command(self, cmd, priority=False):
        """
        Flushes a command to the server as a bytes payload.
        """
        if priority:
            self._pending.insert(0, cmd)
        else:
            self._pending.append(cmd)
        self._pending_size += len(cmd)

        if self._pending_size > DEFAULT_PENDING_SIZE:
            self._flush_pending()

    def _publish(self, subject, reply, payload, payload_size):
        payload_size_bytes = ("%d" % payload_size).encode()
        pub_cmd = b''.join([PUB_OP, _SPC_, subject.encode(
        ), _SPC_, reply, _SPC_, payload_size_bytes, _CRLF_, payload, _CRLF_])
        self.stats['out_msgs'] += 1
        self.stats['out_bytes'] += payload_size
        self.send_command(pub_cmd)

    def _flush_pending(self, check_connected=True):
        if not self.is_connected and check_connected:
            return
        self._flush_queue.put(None)

    def publish(self, subject, payload):
        """
        Sends a PUB command to the server on the specified subject.

          ->> PUB hello 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 5

        """
        self.publish_request(subject, _EMPTY_, payload)

    def publish_request(self, subject, reply, payload):
        """
        Publishes a message tagging it with a reply subscription
        which can be used by those receiving the message to respond:

          ->> PUB hello   _INBOX.2007314fe0fcb2cdc2a2914c1 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 _INBOX.2007314fe0fcb2cdc2a2914c1 5

        """
        payload_size = len(payload)
        if payload_size > self._max_payload_size:
            raise ErrMaxPayload
        if self.is_closed:
            raise ErrConnectionClosed
        self._publish(subject, reply, payload, payload_size)
        self._flush_pending()

    def flush(self, timeout=60):
        """
        Flush will perform a round trip to the server and return True
        when it receives the internal reply or raise a Timeout error.
        """
        if self.is_closed:
            raise ErrConnectionClosed
        return self._flush_timeout(timeout)

    def _flush_timeout(self, timeout):
        """
        Takes a timeout and sets up a future which will return True
        once the server responds back otherwise raise a TimeoutError.
        """
        future = defer.Deferred()
        future.addTimeout(timeout, reactor)
        self._send_ping(future)
        return future

    def request(self, subject, payload, expected=1, cb=None):
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with customizable limited interest.

          ->> SUB _INBOX.gnKUg9bmAHANjxIsDiQsWO 90
          ->> UNSUB 90 1
          ->> PUB hello _INBOX.gnKUg9bmAHANjxIsDiQsWO 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 _INBOX.gnKUg9bmAHANjxIsDiQsWO 5

        """
        next_inbox = INBOX_PREFIX[:]
        next_inbox.extend(self._nuid.next())
        inbox = str(next_inbox)
        sid = self.subscribe(inbox, _EMPTY_, cb)
        self.auto_unsubscribe(sid, expected)
        self.publish_request(subject, inbox, payload)
        return sid

    @inlineCallbacks
    def timed_request(self, subject, payload, timeout=1):
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with a limited interest of 1 reply returning the response
        or raising a Timeout error.

          ->> SUB _INBOX.E9jM2HTirMXDMXPROSQmSd 90
          ->> UNSUB 90 1
          ->> PUB hello _INBOX.E9jM2HTirMXDMXPROSQmSd 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 _INBOX.E9jM2HTirMXDMXPROSQmSd 5

        """
        next_inbox = INBOX_PREFIX[:]
        next_inbox.extend(self._nuid.next())
        inbox = str(next_inbox)
        future = defer.Deferred()
        future.addTimeout(timeout, reactor)
        sid = self.subscribe(subject=inbox, queue=_EMPTY_, cb=None, future=future, max_msgs=1)
        self.auto_unsubscribe(sid, 1)
        self.publish_request(subject, inbox, payload)
        msg = yield future
        returnValue(msg)

    def subscribe(self, subject="", queue="", cb=None, future=None, max_msgs=0):
        """
        Sends a SUB command to the server. Takes a queue parameter which can be used
        in case of distributed queues or left empty if it is not the case, and a callback
        that will be dispatched message for processing them.
        """
        if self.is_closed:
            raise ErrConnectionClosed

        self._ssid += 1
        sid = self._ssid
        sub = Subscription(
            subject=subject,
            queue=queue,
            cb=cb,
            future=future,
            max_msgs=max_msgs,
        )
        self._subs[sid] = sub
        self._subscribe(sub, sid)
        return sid

    def unsubscribe(self, ssid, max_msgs=0):
        """
        Takes a subscription sequence id and removes the subscription
        from the client, optionally after receiving more than max_msgs,
        and unsubscribes immediatedly.
        """
        if self.is_closed:
            raise ErrConnectionClosed

        sub = None
        try:
            sub = self._subs[ssid]
        except KeyError:
            # Already unsubscribed.
            return

        # In case subscription has already received enough messages
        # then announce to the server that we are unsubscribing and
        # remove the callback locally too.
        if max_msgs == 0 or sub.received >= max_msgs:
            self._subs.pop(ssid, None)

        # We will send these for all subs when we reconnect anyway,
        # so that we can suppress here.
        if not self.is_reconnecting:
            self.auto_unsubscribe(ssid, max_msgs)

    def _subscribe(self, sub, ssid):
        """
        Generates a SUB command given a Subscription and the subject sequence id.
        """
        sub_cmd = b''.join([SUB_OP, _SPC_, sub.subject.encode(
        ), _SPC_, sub.queue.encode(), _SPC_, ("%d" % ssid).encode(), _CRLF_])
        self.send_command(sub_cmd)
        self._flush_pending()

    def auto_unsubscribe(self, sid, limit=1):
        """
        Sends an UNSUB command to the server.  Unsubscribe is one of the basic building
        blocks in order to be able to define request/response semantics via pub/sub
        by announcing the server limited interest a priori.
        """
        b_limit = b''
        if limit > 0:
            b_limit = ("%d" % limit).encode()
        b_sid = ("%d" % sid).encode()
        unsub_cmd = b''.join([UNSUB_OP, _SPC_, b_sid, _SPC_, b_limit, _CRLF_])
        self.send_command(unsub_cmd)
        self._flush_pending()

    def _process_ping(self):
        """
        The server will be periodically sending a PING, and if the the client
        does not reply a PONG back a number of times, it will close the connection
        sending an `-ERR 'Stale Connection'` error.
        """
        yield self.send_command(PONG_PROTO)

    def _process_pong(self):
        """
        The client will send a PING soon after CONNECT and then periodically
        to the server as a failure detector to close connections to unhealthy servers.
        For each PING the client sends, we will add a respective PONG future.
        """
        while len(self._pongs) > 0:
            future = self._pongs.pop(0)
            self._pings_outstanding = 0
            if not future.called:
                future.callback(True)

    @inlineCallbacks
    def _process_msg(self, sid, subject, reply, data):
        """
        Dispatches the received message to the stored subscription.
        It first tries to detect whether the message should be
        dispatched to a passed callback.  In case there was not
        a callback, then it tries to set the message into a future.
        """
        self.stats['in_msgs'] += 1
        self.stats['in_bytes'] += len(data)

        msg = Msg(subject=subject.decode(), reply=reply.decode(), data=data)

        # Don't process the message if the subscription has been removed
        sub = self._subs.get(sid)
        if sub is None:
            returnValue(True)
        sub.received += 1

        if 0 < sub.max_msgs <= sub.received:
            # Enough messages so can throwaway subscription now.
            self._subs.pop(sid, None)

        if sub.cb is not None:
            # Call it and take the possible future in the loop.
            maybe_future = sub.cb(msg)
            if maybe_future is not None and type(maybe_future) is defer.Deferred:
                yield maybe_future
        elif sub.future is not None:
            sub.future.callback(msg)

    def _process_connect_init(self):
        """
        Handles the initial part of the NATS protocol, moving from
        the (RE)CONNECTING to CONNECTED states when establishing
        a connection with the server.
        """
        cmd = self.connect_command()
        self.transport.write(cmd)

        # Only now are we officially connected
        self._status = NatsProtocol.CONNECTED
        self._flusher_loop()

        # Prepare the ping pong interval.
        self._ping_timer = task.LoopingCall(self._send_ping)
        self._ping_timer.start(self.options["ping_interval"])

    def _process_info(self, info_line):
        """
        Process INFO lines sent by the server to reconfigure client
        with latest updates from cluster to enable server discovery.
        """
        info = json.loads(info_line.decode())

        if not self.is_connected:
            self._process_connect_init()

        # TODO pass the URLs back to the factory
        if 'connect_urls' in info:
            if info['connect_urls']:
                connect_urls = []
                for connect_url in info['connect_urls']:
                    uri = urlparse("nats://%s" % connect_url)
                    srv = Srv(uri)
                    srv.discovered = True

                    # Filter for any similar server in the server pool already.
                    should_add = True
                    for s in self._server_pool:
                        if uri.netloc == s.uri.netloc:
                            should_add = False
                    if should_add:
                        connect_urls.append(srv)

                if self.options["dont_randomize"] is not True:
                    shuffle(connect_urls)
                for srv in connect_urls:
                    self._server_pool.append(srv)

    @property
    def is_closed(self):
        return self._status == NatsProtocol.CLOSED

    @property
    def is_reconnecting(self):
        return self._status == NatsProtocol.RECONNECTING

    @property
    def is_connected(self):
        return self._status == NatsProtocol.CONNECTED

    @property
    def is_connecting(self):
        return self._status == NatsProtocol.CONNECTING

    def _unbind(self):
        """
        Unbind handles the disconnection from the server then
        attempts to reconnect if `allow_reconnect' is enabled.
        """
        if self.is_connecting or self.is_closed or self.is_reconnecting:
            return

        if self._disconnected_cb is not None:
            self._disconnected_cb()

        self._process_disconnect()
        self._end_flusher_loop()

    def _process_disconnect(self):
        """
        Does cleanup of the client state and tears down the connection.
        """
        self._status = NatsProtocol.DISCONNECTED
        self.close()

    def close(self):
        """
        Wraps up connection to the NATS cluster and stops reconnecting.
        """
        yield self._close(NatsProtocol.CLOSED)

    def _close(self, status, do_callbacks=True):
        """
        Takes the status on which it should leave the connection
        and an optional boolean parameter to dispatch the disconnected
        and close callbacks if there are any.
        """
        if self.is_closed:
            # If connection already closed, then just set status explicitly.
            self._status = status
            return

        self._status = NatsProtocol.CLOSED
        if self._ping_timer is not None and self._ping_timer.is_running():
            self._ping_timer.stop()

        if do_callbacks:
            if self._disconnected_cb is not None:
                self._disconnected_cb()
            if self._close_cb is not None:
                self._close_cb()

    def _process_err(self, err=None):
        """
        Stores the last received error from the server and dispatches the error callback.
        """
        self.stats['errors_received'] += 1

        if err == "'Authorization Violation'":
            self._err = ErrAuthorization
        elif err == "'Slow Consumer'":
            self._err = ErrSlowConsumer
        elif err == "'Stale Connection'":
            self._err = ErrStaleConnection
        else:
            self._err = Exception(err)

        if self._error_cb is not None:
            self._error_cb(err)

    def last_error(self):
        return self._err

    @property
    def connected_url(self):
        if self.is_connected:
            return self._current_server.uri
        else:
            return None

    @property
    def servers(self):
        servers = []
        for srv in self._server_pool:
            servers.append(srv)
        return servers

    @property
    def discovered_servers(self):
        servers = []
        for srv in self._server_pool:
            if srv.discovered:
                servers.append(srv)
        return servers

    @inlineCallbacks
    def _flusher_loop(self):
        """
        Coroutine which continuously tries to consume pending commands
        and then flushes them to the socket.
        """
        while True:
            pending = []
            pending_size = 0
            try:
                # Block and wait for the flusher to be kicked
                yield self._flush_queue.get()

                # Check whether we should bail first
                if not self.is_connected or self.is_connecting:
                    break

                # Flush only when we actually have something in buffer...
                if self._pending_size > 0:
                    cmds = b''.join(self._pending)

                    # Reset pending queue and store tmp in case write fails
                    self._pending, pending = [], self._pending
                    self._pending_size, pending_size = 0, self._pending_size
                    self.transport.write(cmds)
            except Exception as e:
                self._pending = pending + self._pending
                self._pending_size += pending_size
                self._err = e
                if self._error_cb is not None and not self.is_reconnecting:
                    self._error_cb(e)
                self._unbind()

    def _end_flusher_loop(self):
        """
        Let flusher_loop coroutine quit - useful when disconnecting.
        """
        if not self.is_connected or self.is_connecting:
            if self._flush_queue is not None:
                self._flush_pending(check_connected=False)


class Subscription(object):

    def __init__(self,
                 subject='',
                 queue='',
                 cb=None,
                 future=None,
                 max_msgs=0,
                 ):
        self.subject = subject
        self.queue = queue
        self.cb = cb
        self.future = future
        self.max_msgs = max_msgs
        self.received = 0


class Msg(object):

    def __init__(self,
                 subject='',
                 reply='',
                 data=b'',
                 sid=0,
                 ):
        self.subject = subject
        self.reply = reply
        self.data = data
        self.sid = sid


class Srv(object):
    """
    Srv is a helper data structure to hold state of a server.
    """

    def __init__(self, uri):
        self.uri = uri
        self.reconnects = 0
        self.last_attempt = None
        self.did_connect = False
        self.discovered = False


class NatsClientFactory(ReconnectingClientFactory):

    protocol = NatsProtocol

    def __init__(self, host, port, connect_timeout=DEFAULT_CONNECT_TIMEOUT):
        self.options = {}
        self.host = host
        self.port = port
        self.client = None
        self.options["connect_timeout"] = connect_timeout
        self.deferred = defer.Deferred()

    def buildProtocol(self, addr):
        log.info("building...")
        self.resetDelay()

        self.client = self.protocol()
        self.client.factory = self

        def client_ready(client):
            self.deferred.callback(client)
            self.deferred = defer.Deferred()

        reactor.callLater(0, client_ready, self.client)

        return self.client

    def connect(self):
        self.connector = reactor.connectTCP(self.host, self.port, self, self.options["connect_timeout"])
        return self.deferred

    def clientConnectionLost(self, connector, reason):
        log.error("NATS server connection lost: %r", reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.error("Failed to connect to NATS server: %r", reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector,
                                                         reason)


class NatsClient(object):

    def __init__(self):
        self.ncf = None
        pass

    @inlineCallbacks
    def connect(self, host, port):
        """Establishes initial connectivity."""
        # TODO although not needed for k8s, this method should accept a list of servers
        self.ncf = NatsClientFactory(host, port)
        yield self.ncf.connect()

    def __getattr__(self, name):
        return partial(self._call_method, name)

    def _call_method(self, name, *args, **kwargs):
        method = getattr(self.ncf.client, name)
        return method(*args, **kwargs)
