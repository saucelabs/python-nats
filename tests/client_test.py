import sys
import unittest

import tornado.httpclient
import tornado.concurrent
import tornado.testing
import tornado.gen
import tornado.ioloop
import tornado.iostream
import tornado.tcpserver
import subprocess
import threading
import tempfile
import time
import json
import socket
import ssl
import os

from datetime import timedelta
from collections import defaultdict as Hash
from nats.io import NatsProtocol
from nats.io.errors import *
from nats.io.utils import new_inbox, INBOX_PREFIX
from nats.protocol.parser import *
from nats import __lang__, __version__


class Gnatsd(object):

    def __init__(self,
                 port=4222,
                 user="",
                 password="",
                 timeout=0,
                 http_port=8222,
                 config_file=None,
                 debug=False,
                 conf=None,
                 cluster_port=None,
                 ):
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.http_port = http_port
        self.cluster_port = cluster_port
        self.proc = None
        self.debug = debug
        self.config_file = config_file
        self.conf = conf
        self.thread = None

        env_debug_flag = os.environ.get("DEBUG_NATS_TEST")
        if env_debug_flag == "true":
            self.debug = True

    def __enter__(self):
        """For when NATS client is used in a context manager"""
        config_file = tempfile.NamedTemporaryFile(mode='w', delete=True)
        self.config_file = config_file
        self.config_file.write(self.conf)
        self.config_file.flush()

        t = threading.Thread(target=self.start)
        self.thread = t
        self.thread.start()

        http = tornado.httpclient.HTTPClient()
        while True:
            try:
                response = http.fetch(
                    'http://127.0.0.1:%d/varz' % self.http_port)
                if response.code == 200:
                    break
                continue
            except:
                time.sleep(0.1)
                continue
        return self

    def __exit__(self, *exc_info):
        """Close connection to NATS when used in a context manager"""
        self.finish()
        self.thread.join()

    def start(self):
        cmd = ["gnatsd"]
        cmd.append("-p")
        cmd.append("%d" % self.port)
        cmd.append("-m")
        cmd.append("%d" % self.http_port)

        if self.cluster_port is not None:
            cmd.append("--cluster")
            cmd.append("nats://127.0.0.1:%d" % self.cluster_port)

        if self.config_file is not None:
            cmd.append("-c")
            cmd.append(self.config_file.name)

        if self.user != "":
            cmd.append("--user")
            cmd.append(self.user)
        if self.password != "":
            cmd.append("--pass")
            cmd.append(self.password)

        if self.debug:
            cmd.append("-DV")

        if self.debug:
            self.proc = subprocess.Popen(cmd)
        else:
            # Redirect to dev null all server output
            devnull = open(os.devnull, 'w')
            self.proc = subprocess.Popen(
                cmd, stdout=devnull, stderr=subprocess.STDOUT)

        if self.debug:
            if self.proc is None:
                print(
                    "[\031[0;33mDEBUG\033[0;0m] Failed to start server listening on port %d started." % self.port)
            else:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on port %d started." % self.port)

    def finish(self):
        if self.debug:
            print(
                "[\033[0;33mDEBUG\033[0;0m] Server listening on %d will stop." % self.port)

        if self.debug and self.proc is None:
            print(
                "[\033[0;31mDEBUG\033[0;0m] Failed terminating server listening on port %d" % self.port)
        else:
            try:
                self.proc.terminate()
                self.proc.wait()
            except Exception as e:
                if self.debug:
                    print(
                        "[\033[0;33m WARN\033[0;0m] Could not stop server listening on %d. (%s)" % (self.port, e))

            if self.debug:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on %d was stopped." % self.port)


class Log():
    def __init__(self, debug=False):
        self.records = Hash(list)
        self.debug = debug

    def persist(self, msg):
        if self.debug:
            print("[\033[0;33mDEBUG\033[0;0m] Message received: [{0} {1} {2}].".format(
                msg.subject, msg.reply, msg.data))
        self.records[msg.subject].append(msg)


class ClientUtilsTest(unittest.TestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))

    def test_default_connect_command(self):
        nc = NatsProtocol()
        nc.options["verbose"] = False
        nc.options["pedantic"] = False
        nc.options["auth_required"] = False
        nc.options["name"] = None
        got = nc.connect_command()
        expected = 'CONNECT {"lang": "python2", "pedantic": false, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected, got)

    def test_default_connect_command_with_name(self):
        nc = NatsProtocol()
        nc.options["verbose"] = False
        nc.options["pedantic"] = False
        nc.options["auth_required"] = False
        nc.options["name"] = "secret"
        got = nc.connect_command()
        expected = 'CONNECT {"lang": "python2", "name": "secret", "pedantic": false, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected, got)

    def tests_generate_new_inbox(self):
        inbox = new_inbox()
        self.assertTrue(inbox.startswith(INBOX_PREFIX))
        min_expected_len = len(INBOX_PREFIX)
        self.assertTrue(len(inbox) > min_expected_len)


class ClientTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))
        self.threads = []
        self.server_pool = []

        server = Gnatsd(port=4222)
        self.server_pool.append(server)

        for gnatsd in self.server_pool:
            t = threading.Thread(target=gnatsd.start)
            self.threads.append(t)
            t.start()

        http = tornado.httpclient.HTTPClient()
        while True:
            try:
                response = http.fetch('http://127.0.0.1:8222/varz')
                if response.code == 200:
                    break
                continue
            except:
                time.sleep(0.1)
                continue
        super(ClientTest, self).setUp()

    def tearDown(self):
        for gnatsd in self.server_pool:
            gnatsd.finish()

        for t in self.threads:
            t.join()

        super(ClientTest, self).tearDown()

    @tornado.testing.gen_test
    def test_connect_verbose(self):
        nc = NatsProtocol()
        options = {
            "verbose": True,
            "io_loop": self.io_loop
        }
        yield nc.connect(**options)

        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)

        got = nc.connect_command()
        expected = 'CONNECT {"lang": "python2", "pedantic": false, "protocol": 1, "verbose": true, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected, got)

    @tornado.testing.gen_test
    def test_connect_pedantic(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop, pedantic=True)

        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)

        got = nc.connect_command()
        expected = 'CONNECT {"lang": "python2", "pedantic": true, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected, got)

    @tornado.testing.gen_test
    def test_connect_custom_connect_timeout(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop, connect_timeout=1)
        self.assertEqual(1, nc.options["connect_timeout"])

    @tornado.testing.gen_test
    def test_parse_info(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop)

        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)
        self.assertIn("server_id", info_keys)
        self.assertIn("version", info_keys)
        self.assertIn("go", info_keys)
        self.assertIn("host", info_keys)
        self.assertIn("port", info_keys)
        self.assertIn("auth_required", info_keys)
        self.assertIn("tls_required", info_keys)
        self.assertIn("max_payload", info_keys)

    @tornado.testing.gen_test(timeout=5)
    def test_connect_fails(self):

        class SampleClient():
            def __init__(self):
                self.nc = NatsProtocol()
                self.disconnected_cb_called = False

            def disconnected_cb(self):
                self.disconnected_cb_called = True

        client = SampleClient()
        with self.assertRaises(ErrNoServers):
            options = {
                "servers": ["nats://127.0.0.1:4223"],
                "close_cb": client.disconnected_cb,
                "allow_reconnect": False,
                "io_loop": self.io_loop
            }
            yield client.nc.connect(**options)
        self.assertFalse(client.disconnected_cb_called)

    @tornado.testing.gen_test(timeout=5)
    def test_connect_fails_allow_reconnect(self):

        class SampleClient():
            def __init__(self):
                self.nc = NatsProtocol()
                self.disconnected_cb_called = False
                self.closed_cb_called = False

            def disconnected_cb(self):
                self.disconnected_cb_called = True

            def closed_cb(self):
                self.closed_cb_called = True

        client = SampleClient()
        with self.assertRaises(ErrNoServers):
            options = {
                "servers": ["nats://127.0.0.1:4223"],
                "disconnected_cb": client.disconnected_cb,
                "close_cb": client.closed_cb,
                "allow_reconnect": True,
                "io_loop": self.io_loop,
                "max_reconnect_attempts": 2
            }
            yield client.nc.connect(**options)
        self.assertFalse(client.disconnected_cb_called)
        self.assertFalse(client.closed_cb_called)

    @tornado.testing.gen_test(timeout=5)
    def test_connect_fails_allow_reconnect_forever_until_close(self):

        class SampleClient():
            def __init__(self):
                self.nc = NatsProtocol()
                self.disconnected_cb_called = False
                self.closed_cb_called = False

            def disconnected_cb(self):
                self.disconnected_cb_called = True

            def close_cb(self):
                self.closed_cb_called = True

        client = SampleClient()
        options = {
            "servers": ["nats://127.0.0.1:4223"],
            "close_cb": client.close_cb,
            "disconnected_cb": client.disconnected_cb,
            "allow_reconnect": True,
            "io_loop": self.io_loop,
            "max_reconnect_attempts": -1,
            "reconnect_time_wait": 0.1
        }
        self.io_loop.spawn_callback(client.nc.connect, **options)
        yield tornado.gen.sleep(2)
        yield client.nc.close()
        self.assertTrue(client.nc._server_pool[0].reconnects > 10)
        self.assertTrue(client.disconnected_cb_called)
        self.assertTrue(client.closed_cb_called)

    @tornado.testing.gen_test
    def test_iostream_closed_on_unbind(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop)
        self.assertTrue(nc.is_connected)
        self.assertEqual(nc.stats['reconnects'], 0)
        old_io = nc.io
        # Unbind and reconnect.
        yield nc._unbind()
        self.assertTrue(nc.is_connected)
        self.assertEqual(nc.stats['reconnects'], 1)
        self.assertTrue(old_io.closed())
        self.assertFalse(nc.io.closed())
        # Unbind, but don't reconnect.
        nc.options["allow_reconnect"] = False
        yield nc._unbind()
        self.assertFalse(nc.is_connected)
        self.assertTrue(nc.io.closed())

    @tornado.testing.gen_test
    def test_flusher_exits_on_unbind(self):

        class FlusherClient(NatsProtocol):
            def __init__(self, *args, **kwargs):
                super(FlusherClient, self).__init__(*args, **kwargs)
                self.flushers_running = {}

            @tornado.gen.coroutine
            def _flusher_loop(self):
                flusher_id = len(self.flushers_running)
                self.flushers_running.update({flusher_id: True})
                yield super(FlusherClient, self)._flusher_loop()
                self.flushers_running.update({flusher_id: False})

        nc = FlusherClient()
        yield nc.connect(io_loop=self.io_loop)
        self.assertTrue(nc.is_connected)
        self.assertEqual(len(nc.flushers_running), 1)
        self.assertTrue(nc.flushers_running[0])
        # Unbind and reconnect.
        yield nc._unbind()
        self.assertTrue(nc.is_connected)
        self.assertEqual(len(nc.flushers_running), 2)
        self.assertFalse(nc.flushers_running[0])
        self.assertTrue(nc.flushers_running[1])
        # Unbind, but don't reconnect.
        nc.options["allow_reconnect"] = False
        yield nc._unbind()
        yield tornado.gen.sleep(0.1)
        self.assertFalse(nc.is_connected)
        self.assertTrue(nc.io.closed())
        self.assertEqual(len(nc.flushers_running), 2)
        self.assertFalse(nc.flushers_running[0])
        self.assertFalse(nc.flushers_running[1])

    @tornado.testing.gen_test
    def test_subscribe(self):
        nc = NatsProtocol()
        options = {
            "io_loop": self.io_loop
        }
        yield nc.connect(**options)
        self.assertEqual(NatsProtocol.CONNECTED, nc._status)
        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)

        inbox = new_inbox()
        yield nc.subscribe("help.1")
        yield nc.subscribe("help.2")
        yield tornado.gen.sleep(0.5)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/connz' % self.server_pool[0].http_port)
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(2, connz['subscriptions'])

    @tornado.testing.gen_test
    def test_subscribe_sync(self):
        nc = NatsProtocol()
        msgs = []

        @tornado.gen.coroutine
        def subscription_handler(msg):
            # Futures for subscription are each processed
            # in sequence.
            if msg.subject == "tests.1":
                yield tornado.gen.sleep(1.0)
            if msg.subject == "tests.3":
                yield tornado.gen.sleep(1.0)
            msgs.append(msg)

        yield nc.connect(io_loop=self.io_loop)
        sid = yield nc.subscribe("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield nc.publish("tests.{0}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield tornado.gen.sleep(4.0)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield nc.close()

    @tornado.testing.gen_test
    def test_subscribe_sync_non_coro(self):
        nc = NatsProtocol()
        msgs = []

        def subscription_handler(msg):
            # Callback blocks so dispatched in sequence.
            if msg.subject == "tests.1":
                time.sleep(0.5)
            if msg.subject == "tests.3":
                time.sleep(0.2)
            msgs.append(msg)

        yield nc.connect(io_loop=self.io_loop)
        sid = yield nc.subscribe("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield nc.publish("tests.{0}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield tornado.gen.sleep(4.0)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield nc.close()

    @tornado.testing.gen_test
    def test_subscribe_async(self):
        nc = NatsProtocol()
        msgs = []

        @tornado.gen.coroutine
        def subscription_handler(msg):
            # Callback dispatched asynchronously and a coroutine
            # so it does not block.
            if msg.subject == "tests.1":
                yield tornado.gen.sleep(0.5)
            if msg.subject == "tests.3":
                yield tornado.gen.sleep(0.2)
            msgs.append(msg)

        yield nc.connect(io_loop=self.io_loop)
        sid = yield nc.subscribe_async("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield nc.publish("tests.{0}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield tornado.gen.sleep(4.0)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[4].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield nc.close()

    @tornado.testing.gen_test
    def test_subscribe_async_non_coro(self):
        nc = NatsProtocol()
        msgs = []

        def subscription_handler(msg):
            # Dispatched asynchronously but would be received in sequence...
            msgs.append(msg)

        yield nc.connect(io_loop=self.io_loop)
        sid = yield nc.subscribe_async("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield nc.publish("tests.{0}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield tornado.gen.sleep(4.0)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield nc.close()

    @tornado.testing.gen_test
    def test_publish(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop)
        self.assertEqual(NatsProtocol.CONNECTED, nc._status)
        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)

        log = Log()
        yield nc.subscribe(">", "", log.persist)
        yield nc.publish("one", "hello")
        yield nc.publish("two", "world")
        yield tornado.gen.sleep(1.0)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/varz' % self.server_pool[0].http_port)
        varz = json.loads(response.body)
        self.assertEqual(10, varz['in_bytes'])
        self.assertEqual(10, varz['out_bytes'])
        self.assertEqual(2, varz['in_msgs'])
        self.assertEqual(2, varz['out_msgs'])
        self.assertEqual(2, len(log.records.keys()))
        self.assertEqual("hello", log.records['one'][0].data)
        self.assertEqual("world", log.records['two'][0].data)
        self.assertEqual(10, nc.stats['in_bytes'])
        self.assertEqual(10, nc.stats['out_bytes'])
        self.assertEqual(2, nc.stats['in_msgs'])
        self.assertEqual(2, nc.stats['out_msgs'])

    @tornado.testing.gen_test(timeout=15)
    def test_publish_race_condition(self):
        # This tests a race condition fixed in #23 where a series of
        # large publishes followed by a flush and another publish
        # will cause the last publish to never get written.
        nc = NatsProtocol()

        yield nc.connect(io_loop=self.io_loop)
        self.assertTrue(nc.is_connected)

        @tornado.gen.coroutine
        def sub(msg):
            sub.msgs.append(msg)
            if len(sub.msgs) == 501:
                sub.future.set_result(True)

        sub.msgs = []
        sub.future = tornado.concurrent.Future()
        yield nc.subscribe("help.*", cb=sub)

        # Close to 1MB payload
        payload = "A" * 1000000

        # Publish messages from 0..499
        for i in range(500):
            yield nc.publish("help.%s" % i, payload)

        # Relinquish control often to unblock the flusher
        yield tornado.gen.moment
        yield nc.publish("help.500", "A")

        # Wait for the future to yield after receiving all the messages.
        try:
            yield tornado.gen.with_timeout(timedelta(seconds=10), sub.future)
        except:
            # Skip timeout in case it may occur and let test fail
            # when checking how many messages we received in the end.
            pass

        # We should definitely have all the messages
        self.assertEqual(len(sub.msgs), 501)

        for i in range(501):
            self.assertEqual(sub.msgs[i].subject, u"help.%s" % (i))

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/varz' % self.server_pool[0].http_port)
        varz = json.loads(response.body)

        self.assertEqual(500000001, varz['in_bytes'])
        self.assertEqual(500000001, varz['out_bytes'])
        self.assertEqual(501,  varz['in_msgs'])
        self.assertEqual(501,  varz['out_msgs'])
        self.assertEqual(500000001,  nc.stats['in_bytes'])
        self.assertEqual(500000001,  nc.stats['out_bytes'])
        self.assertEqual(501,  nc.stats['in_msgs'])
        self.assertEqual(501,  nc.stats['out_msgs'])

    @tornado.testing.gen_test(timeout=15)
    def test_publish_flush_race_condition(self):
        # This tests a race condition fixed in #23 where a series of
        # large publishes followed by a flush and another publish
        # will cause the last publish to never get written.
        nc = NatsProtocol()

        yield nc.connect(io_loop=self.io_loop)
        self.assertTrue(nc.is_connected)

        @tornado.gen.coroutine
        def sub(msg):
            sub.msgs.append(msg)
            if len(sub.msgs) == 501:
                sub.future.set_result(True)

        sub.msgs = []
        sub.future = tornado.concurrent.Future()
        yield nc.subscribe("help.*", cb=sub)

        # Close to 1MB payload
        payload = "A" * 1000000

        # Publish messages from 0..499
        for i in range(500):
            yield nc.publish("help.%s" % i, payload)
            if i % 10 == 0:
                # Relinquish control often to unblock the flusher
                yield tornado.gen.moment

        yield nc.publish("help.500", "A")

        # Flushing and doing ping/pong should not cause commands
        # to be dropped either.
        yield nc.flush()

        # Wait for the future to yield after receiving all the messages.
        try:
            yield tornado.gen.with_timeout(timedelta(seconds=10), sub.future)
        except:
            # Skip timeout in case it may occur and let test fail
            # when checking how many messages we received in the end.
            pass

        # We should definitely have all the messages
        self.assertEqual(len(sub.msgs), 501)

        for i in range(501):
            self.assertEqual(sub.msgs[i].subject, u"help.%s" % (i))

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/varz' % self.server_pool[0].http_port)
        varz = json.loads(response.body)

        self.assertEqual(500000001, varz['in_bytes'])
        self.assertEqual(500000001, varz['out_bytes'])
        self.assertEqual(501,  varz['in_msgs'])
        self.assertEqual(501,  varz['out_msgs'])
        self.assertEqual(500000001,  nc.stats['in_bytes'])
        self.assertEqual(500000001,  nc.stats['out_bytes'])
        self.assertEqual(501,  nc.stats['in_msgs'])
        self.assertEqual(501,  nc.stats['out_msgs'])

    @tornado.testing.gen_test
    def test_unsubscribe(self):
        nc = NatsProtocol()
        options = {
            "io_loop": self.io_loop
        }
        yield nc.connect(**options)

        log = Log()
        sid = yield nc.subscribe("foo", cb=log.persist)
        yield nc.publish("foo", b'A')
        yield nc.publish("foo", b'B')
        yield tornado.gen.sleep(1)
        yield nc.unsubscribe(sid)
        yield nc.flush()
        yield nc.publish("foo", b'C')
        yield nc.publish("foo", b'D')
        self.assertEqual(2, len(log.records["foo"]))

        self.assertEqual(b'A', log.records["foo"][0].data)
        self.assertEqual(b'B', log.records["foo"][1].data)

        # Should not exist by now
        with self.assertRaises(KeyError):
            nc._subs[sid].received

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/connz' % self.server_pool[0].http_port)
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(0, connz['subscriptions'])

    @tornado.testing.gen_test
    def test_unsubscribe_only_if_max_reached(self):
        nc = NatsProtocol()
        options = {
            "io_loop": self.io_loop
        }
        yield nc.connect(**options)

        log = Log()
        sid = yield nc.subscribe("foo", cb=log.persist)
        yield nc.publish("foo", b'A')
        yield nc.publish("foo", b'B')
        yield nc.publish("foo", b'C')
        yield tornado.gen.sleep(1)
        self.assertEqual(3, len(log.records["foo"]))
        yield nc.unsubscribe(sid, 3)
        yield nc.publish("foo", b'D')
        yield nc.flush()
        self.assertEqual(3, len(log.records["foo"]))

        self.assertEqual(b'A', log.records["foo"][0].data)
        self.assertEqual(b'B', log.records["foo"][1].data)
        self.assertEqual(b'C', log.records["foo"][2].data)

        # Should not exist by now
        yield tornado.gen.sleep(1)
        with self.assertRaises(KeyError):
            nc._subs[sid].received

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/connz' % self.server_pool[0].http_port)
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(0, connz['subscriptions'])

    @tornado.testing.gen_test
    def test_request(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop)

        class Component:
            def __init__(self, nc):
                self.nc = nc
                self.replies = []

            @tornado.gen.coroutine
            def receive_responses(self, msg=None):
                self.replies.append(msg)

            @tornado.gen.coroutine
            def respond(self, msg=None):
                yield self.nc.publish(msg.reply, "ok:1")
                yield self.nc.publish(msg.reply, "ok:2")
                yield self.nc.publish(msg.reply, "ok:3")

        log = Log()
        c = Component(nc)
        yield nc.subscribe(">", "", log.persist)
        yield nc.subscribe("help", "", c.respond)
        yield nc.request("help", "please", expected=2, cb=c.receive_responses)
        yield tornado.gen.sleep(0.5)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/varz' % self.server_pool[0].http_port)
        varz = json.loads(response.body)
        self.assertEqual(18, varz['in_bytes'])
        self.assertEqual(32, varz['out_bytes'])
        self.assertEqual(4, varz['in_msgs'])
        self.assertEqual(7, varz['out_msgs'])
        self.assertEqual(2, len(log.records.keys()))
        self.assertEqual("please", log.records['help'][0].data)
        self.assertEqual(2, len(c.replies))
        self.assertEqual(32, nc.stats['in_bytes'])
        self.assertEqual(18, nc.stats['out_bytes'])
        self.assertEqual(7, nc.stats['in_msgs'])
        self.assertEqual(4, nc.stats['out_msgs'])

        full_msg = ''
        for msg in log.records['help']:
            full_msg += msg.data

        self.assertEqual('please', full_msg)
        self.assertEqual("ok:1", c.replies[0].data)
        self.assertEqual("ok:2", c.replies[1].data)

    @tornado.testing.gen_test
    def test_timed_request(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop)

        class Component:
            def __init__(self, nc):
                self.nc = nc

            @tornado.gen.coroutine
            def respond(self, msg=None):
                yield self.nc.publish(msg.reply, "ok:1")
                yield self.nc.publish(msg.reply, "ok:2")
                yield self.nc.publish(msg.reply, "ok:3")

        log = Log()
        c = Component(nc)
        yield nc.subscribe(">", "", log.persist)
        yield nc.subscribe("help", "", c.respond)

        reply = yield nc.timed_request("help", "please")
        self.assertEqual("ok:1", reply.data)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/varz' % self.server_pool[0].http_port)
        varz = json.loads(response.body)
        self.assertEqual(18, varz['in_bytes'])
        self.assertEqual(28, varz['out_bytes'])
        self.assertEqual(4, varz['in_msgs'])
        self.assertEqual(6, varz['out_msgs'])
        self.assertEqual(2, len(log.records.keys()))
        self.assertEqual("please", log.records['help'][0].data)
        self.assertEqual(28, nc.stats['in_bytes'])
        self.assertEqual(18, nc.stats['out_bytes'])
        self.assertEqual(6, nc.stats['in_msgs'])
        self.assertEqual(4, nc.stats['out_msgs'])

        full_msg = ''
        for msg in log.records['help']:
            full_msg += msg.data
        self.assertEqual('please', full_msg)

        # There should not be lingering inboxes with requests by default
        self.assertEqual(len(c.nc._subs), 2)

    @tornado.testing.gen_test
    def test_publish_max_payload(self):
        nc = NatsProtocol()
        yield nc.connect(io_loop=self.io_loop)
        self.assertEqual(NatsProtocol.CONNECTED, nc._status)
        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)

        with self.assertRaises(ErrMaxPayload):
            yield nc.publish("large-message", "A" * (nc._server_info["max_payload"] * 2))

    @tornado.testing.gen_test
    def test_publish_request(self):
        nc = NatsProtocol()

        yield nc.connect(io_loop=self.io_loop)
        self.assertEqual(NatsProtocol.CONNECTED, nc._status)
        info_keys = nc._server_info.keys()
        self.assertTrue(len(info_keys) > 0)

        inbox = new_inbox()
        yield nc.publish_request("help.1", inbox, "hello")
        yield nc.publish_request("help.2", inbox, "world")
        yield tornado.gen.sleep(1.0)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:%d/varz' % self.server_pool[0].http_port)
        varz = json.loads(response.body)

        self.assertEqual(10, varz['in_bytes'])
        self.assertEqual(0,  varz['out_bytes'])
        self.assertEqual(2,  varz['in_msgs'])
        self.assertEqual(0,  varz['out_msgs'])
        self.assertEqual(0,  nc.stats['in_bytes'])
        self.assertEqual(10, nc.stats['out_bytes'])
        self.assertEqual(0,  nc.stats['in_msgs'])
        self.assertEqual(2,  nc.stats['out_msgs'])

    @tornado.testing.gen_test
    def test_customize_io_buffers(self):
        class Component():
            def __init__(self):
                self.nc = NatsProtocol()
                self.errors = []
                self.disconnected_cb_called = 0
                self.closed_cb_called = 0

            def error_cb(self, e):
                self.errors.append(e)

            def close_cb(self):
                self.closed_cb_called += 1

            def disconnected_cb(self):
                self.disconnected_cb_called += 1

        c = Component()
        options = {
            "io_loop": self.io_loop,
            "max_read_buffer_size": 1024,
            "max_write_buffer_size": 50,
            "read_chunk_size": 10,
            "error_cb": c.error_cb,
            "close_cb": c.close_cb,
            "disconnected_cb": c.disconnected_cb
        }
        with self.assertRaises(tornado.iostream.StreamBufferFullError):
            yield c.nc.connect(**options)
        self.assertFalse(c.nc.is_connected)
        self.assertEqual(1024, c.nc._max_read_buffer_size)
        self.assertEqual(50, c.nc._max_write_buffer_size)
        self.assertEqual(10, c.nc._read_chunk_size)

    @tornado.testing.gen_test
    def test_default_ping_interval(self):

        class Parser():

            def __init__(self, nc, t):
                self.nc = nc
                self.t = t

            @tornado.gen.coroutine
            def parse(self, data=''):
                self.t.assertEqual(1, len(self.nc._pongs))
                yield self.nc._process_pong()
                self.t.assertEqual(0, len(self.nc._pongs))

        nc = NatsProtocol()
        nc._ps = Parser(nc, self)
        yield nc.connect(io_loop=self.io_loop)
        yield tornado.gen.sleep(1)
        self.assertEqual(0, nc._pings_outstanding)
        self.assertTrue(nc.is_connected)

    @tornado.testing.gen_test
    def test_custom_ping_interval(self):
        pongs = []

        class Parser():
            def __init__(self, nc):
                self.nc = nc

            @tornado.gen.coroutine
            def parse(self, data=''):
                if b'PONG' in data:
                    pongs.append(data)
                    yield self.nc._process_pong()

        nc = NatsProtocol()
        nc._ps = Parser(nc)
        yield nc.connect(io_loop=self.io_loop, ping_interval=0.1)
        yield tornado.gen.sleep(1)

        # Should have processed at least more than 5 pongs already
        self.assertTrue(len(pongs) > 5)
        self.assertTrue(nc.is_connected)
        self.assertFalse(nc.is_reconnecting)

    @tornado.testing.gen_test
    def test_ping_slow_replies(self):
        pongs = []

        class Parser():
            def __init__(self, nc):
                self.nc = nc

            @tornado.gen.coroutine
            def parse(self, data=''):
                pongs.append(data)  # but, don't process now

        nc = NatsProtocol()
        nc._ps = Parser(nc)
        yield nc.connect(io_loop=self.io_loop,
                         ping_interval=0.1,
                         max_outstanding_pings=20)
        yield tornado.gen.sleep(1)

        # Should have received more than 5 pongs, but processed none.
        self.assertTrue(len(pongs) > 5)
        self.assertTrue(len(pongs) <= nc._pings_outstanding)
        self.assertEqual(0, nc._pongs_received)
        self.assertEqual(len(nc._pongs), nc._pings_outstanding)
        # Process all that were sent.
        expected_outstanding = nc._pings_outstanding
        for i in range(nc._pings_outstanding):
            yield nc._process_pong()
            expected_outstanding -= 1
            self.assertEqual(expected_outstanding, nc._pings_outstanding)
            self.assertEqual(expected_outstanding, len(nc._pongs))
            self.assertEqual(i + 1, nc._pongs_received)

    @tornado.testing.gen_test
    def test_flush_timeout(self):

        class Parser():
            def __init__(self, nc, t):
                self.nc = nc
                self.t = t

            @tornado.gen.coroutine
            def parse(self, data=''):
                self.t.assertEqual(1, self.nc._pings_outstanding)
                yield tornado.gen.sleep(2.0)
                yield self.nc._process_pong()

        nc = NatsProtocol()
        nc._ps = Parser(nc, self)
        yield nc.connect(io_loop=self.io_loop)
        with self.assertRaises(tornado.gen.TimeoutError):
            yield nc.flush(timeout=1)
        self.assertEqual(1, len(nc._pongs))
        self.assertEqual(1, nc._pings_outstanding)

    @tornado.testing.gen_test
    def test_flush_timeout_lost_message(self):

        class Parser():
            def __init__(self, nc):
                self.nc = nc
                self.drop_messages = False

            @tornado.gen.coroutine
            def parse(self, data=''):
                if not self.drop_messages:
                    yield self.nc._process_pong()

        nc = NatsProtocol()
        nc._ps = Parser(nc)
        yield nc.connect(io_loop=self.io_loop)
        nc._ps.drop_messages = True
        with self.assertRaises(tornado.gen.TimeoutError):
            yield nc.flush(timeout=1)
        self.assertEqual(1, len(nc._pongs))
        self.assertEqual(1, nc._pings_outstanding)
        self.assertEqual(0, nc._pongs_received)
        # Successful flush must clear timed out pong and the new one.
        nc._ps.drop_messages = False
        yield nc.flush(timeout=1)
        self.assertEqual(0, len(nc._pongs))
        self.assertEqual(0, nc._pings_outstanding)
        self.assertEqual(2, nc._pongs_received)

    @tornado.testing.gen_test
    def test_timed_request_timeout(self):

        class Parser():
            def __init__(self, nc, t):
                self.nc = nc
                self.t = t

            def parse(self, data=''):
                self.nc._process_pong()

        nc = NatsProtocol()
        nc._ps = Parser(nc, self)
        yield nc.connect(io_loop=self.io_loop)
        with self.assertRaises(tornado.gen.TimeoutError):
            yield nc.timed_request("hello", "world", timeout=0.5)

    @tornado.testing.gen_test
    def test_process_message_subscription_not_present(self):
        nc = NatsProtocol()
        yield nc._process_msg(387, 'some-subject', 'some-reply', [0, 1, 2])


class ClientAuthTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))
        self.threads = []
        self.server_pool = []

        server1 = Gnatsd(port=4223, user="foo", password="bar", http_port=8223)
        self.server_pool.append(server1)
        server2 = Gnatsd(port=4224, user="hoge",
                         password="fuga", http_port=8224)
        self.server_pool.append(server2)

        for gnatsd in self.server_pool:
            t = threading.Thread(target=gnatsd.start)
            self.threads.append(t)
            t.start()

        http = tornado.httpclient.HTTPClient()
        while True:
            try:
                response1 = http.fetch('http://127.0.0.1:8223/varz')
                response2 = http.fetch('http://127.0.0.1:8224/varz')
                if response1.code == 200 and response2.code == 200:
                    break
                continue
            except:
                time.sleep(0.1)
                continue
        super(ClientAuthTest, self).setUp()

    def tearDown(self):
        super(ClientAuthTest, self).tearDown()
        for gnatsd in self.server_pool:
            gnatsd.finish()

        for t in self.threads:
            t.join()

    @tornado.testing.gen_test(timeout=10)
    def test_auth_connect(self):
        nc = NatsProtocol()
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            "io_loop": self.io_loop
        }
        yield nc.connect(**options)
        self.assertEqual(True, nc._server_info["auth_required"])

        log = Log()
        sid_1 = yield nc.subscribe("foo",  "", log.persist)
        self.assertEqual(sid_1, 1)
        sid_2 = yield nc.subscribe("bar",  "", log.persist)
        self.assertEqual(sid_2, 2)
        sid_3 = yield nc.subscribe("quux", "", log.persist)
        self.assertEqual(sid_3, 3)
        yield nc.publish("foo", "hello")
        yield tornado.gen.sleep(1.0)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:8223/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(3, connz['subscriptions'])
        self.assertEqual(1, connz['in_msgs'])
        self.assertEqual(5, connz['in_bytes'])

        yield nc.publish("foo", "world")
        yield tornado.gen.sleep(0.5)
        response = yield http.fetch('http://127.0.0.1:8223/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(3, connz['subscriptions'])
        self.assertEqual(2, connz['in_msgs'])
        self.assertEqual(10, connz['in_bytes'])

        orig_gnatsd = self.server_pool.pop(0)
        orig_gnatsd.finish()

        try:
            a = nc._current_server
            # Wait for reconnect logic kick in...
            yield tornado.gen.sleep(5)
        finally:
            b = nc._current_server
            self.assertNotEqual(a.uri, b.uri)

        self.assertTrue(nc.is_connected)
        self.assertFalse(nc.is_reconnecting)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:8224/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(3, connz['subscriptions'])
        self.assertEqual(0, connz['in_msgs'])
        self.assertEqual(0, connz['in_bytes'])

        yield nc.publish("foo", "!!!")
        yield tornado.gen.sleep(0.5)
        response = yield http.fetch('http://127.0.0.1:8224/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(3, connz['subscriptions'])
        self.assertEqual(1, connz['in_msgs'])
        self.assertEqual(3, connz['in_bytes'])

        full_msg = ''
        for msg in log.records['foo']:
            full_msg += msg.data

        self.assertEqual('helloworld!!!', full_msg)

    @tornado.testing.gen_test(timeout=10)
    def test_auth_connect_fails(self):

        class Component:
            def __init__(self, nc):
                self.nc = nc
                self.error = None
                self.error_cb_called = False
                self.errors = []
                self.close_cb_called = False
                self.disconnected_cb_called = False
                self.reconnected_cb_called = False

            def error_cb(self, err):
                self.error = err
                self.errors.append(err)
                self.error_cb_called = True

            def close_cb(self):
                self.close_cb_called = True

            def disconnected_cb(self):
                self.disconnected_cb_called = True

            def reconnected_cb(self):
                self.reconnected_cb_called = True

        nc = NatsProtocol()
        component = Component(nc)
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://foo2:bar2@127.0.0.1:4224"
            ],
            "io_loop": self.io_loop,
            "close_cb": component.close_cb,
            "error_cb": component.error_cb,
            "disconnected_cb": component.disconnected_cb,
            "reconnected_cb": component.reconnected_cb,
            "max_reconnect_attempts": 5,
            "reconnect_time_wait": 0.1
        }
        yield component.nc.connect(**options)
        self.assertEqual(True, component.nc.is_connected)
        self.assertEqual(True, nc._server_info["auth_required"])

        log = Log()
        sid_1 = yield component.nc.subscribe("foo",  "", log.persist)
        self.assertEqual(sid_1, 1)
        sid_2 = yield component.nc.subscribe("bar",  "", log.persist)
        self.assertEqual(sid_2, 2)
        sid_3 = yield component.nc.subscribe("quux", "", log.persist)
        self.assertEqual(sid_3, 3)
        yield nc.publish("foo", "hello")
        yield tornado.gen.sleep(2)
        self.assertEqual("hello", log.records['foo'][0].data)
        yield tornado.gen.sleep(1.0)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:8223/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(3, connz['subscriptions'])
        self.assertEqual(1, connz['in_msgs'])
        self.assertEqual(5, connz['in_bytes'])

        yield component.nc.publish("foo", "world")
        yield tornado.gen.sleep(0.5)
        response = yield http.fetch('http://127.0.0.1:8223/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertEqual(3, connz['subscriptions'])
        self.assertEqual(2, connz['in_msgs'])
        self.assertEqual(10, connz['in_bytes'])

        # Shutdown first server, triggering reconnect...
        orig_gnatsd = self.server_pool.pop(0)
        orig_gnatsd.finish()

        # Wait for reconnect logic kick in and fail due to authorization error.
        yield tornado.gen.sleep(0.5)
        self.assertFalse(component.nc.is_connected)
        self.assertTrue(component.nc.is_reconnecting)
        self.assertTrue(component.disconnected_cb_called)

        # No guarantee in getting the error before the connection is closed,
        # by the server though the behavior should be as below.
        # self.assertEqual(1, component.nc.stats['errors_received'])
        # self.assertEqual(ErrAuthorization, component.nc.last_error())
        # self.assertTrue(component.error_cb_called)

        # Connection is closed at this point after reconnect failed.
        yield tornado.gen.sleep(1)
        self.assertTrue(component.reconnected_cb_called)
        self.assertEqual(6, len(component.errors))
        self.assertTrue(component.close_cb_called)

    # @tornado.testing.gen_test(timeout=15)
    @unittest.skip("FIXME: flapping test")
    def test_auth_pending_bytes_handling(self):
        
        nc = NatsProtocol()

        class Component:

            def __init__(self, nc):
                self.nc = nc
                self.errors = []
                self.written = 0
                self.max_messages = 2000
                self.disconnected_at = 0
                self.pending_bytes_when_closed = 0
                self.pending_bytes_when_reconnected = 0

            def error_cb(self, err):
                self.errors.append(err)

            def disconnected_cb(self):
                self.disconnected_at = self.written
                self.pending_bytes_when_closed = len(self.nc._pending)

            def reconnected_cb(self):
                self.pending_bytes_when_reconnected = len(self.nc._pending)

            @tornado.gen.coroutine
            def publisher(self):
                for i in range(0, self.max_messages):
                    yield self.nc.publish("foo", "{0},".format(i))
                    # yield self.nc.flush()
                    self.written += 1
                    yield tornado.gen.sleep(0.0001)

        c = Component(nc)
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            "io_loop": self.io_loop,
            "reconnected_cb": c.reconnected_cb,
            "disconnected_cb": c.disconnected_cb,
            "error_cb": c.error_cb,
            "reconnect_time_wait": 0.1
        }
        yield c.nc.connect(**options)
        self.assertEqual(True, nc._server_info["auth_required"])

        log = Log()
        yield c.nc.subscribe("foo",  "", log.persist)
        self.io_loop.spawn_callback(c.publisher)

        a = nc._current_server
        yield tornado.gen.sleep(0.001)
        orig_gnatsd = self.server_pool.pop(0)
        orig_gnatsd.finish()
        yield tornado.gen.sleep(0.001)

        # Wait for reconnect logic kick in...
        yield tornado.gen.sleep(3)
        b = nc._current_server
        self.assertNotEqual(a.uri, b.uri)

        # Should have reconnected already
        self.assertTrue(nc.is_connected)
        self.assertFalse(nc.is_reconnecting)

        # Wait a bit until it flushes all...
        yield tornado.gen.sleep(1)

        http = tornado.httpclient.AsyncHTTPClient()
        response = yield http.fetch('http://127.0.0.1:8224/connz')
        result = json.loads(response.body)
        connz = result['connections'][0]
        self.assertTrue(connz['in_msgs'] > 0)
        self.assertTrue(c.pending_bytes_when_reconnected >
                        c.pending_bytes_when_closed)

        # Give a small margin of error in case we didn't grab the
        # self.assertTrue(c.max_messages - c.disconnected_at - 5 < connz['in_msgs'] < c.max_messages - c.disconnected_at + 5)

        # FIXME: Race with subscription not present by the time
        # we flush the buffer again so can't receive messages
        # if sending to our own subscription.
        #
        # full_msg = ''
        # for msg in log.records['foo']:
        #      full_msg += msg.data
        #
        # expected_full_msg = ''
        # for i in range(0, c.max_messages):
        #      expected_full_msg += "%d," % i
        #
        # a = set(full_msg.split(','))
        # b = set(expected_full_msg.split(','))
        # print(sorted(list(b - a)))
        # self.assertEqual(len(expected_full_msg), len(full_msg))

    @tornado.testing.gen_test(timeout=10)
    def test_close_connection(self):
        nc = NatsProtocol()
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            "io_loop": self.io_loop
        }
        yield nc.connect(**options)
        self.assertEqual(True, nc._server_info["auth_required"])

        log = Log()
        sid_1 = yield nc.subscribe("foo",  "", log.persist)
        self.assertEqual(sid_1, 1)
        sid_2 = yield nc.subscribe("bar",  "", log.persist)
        self.assertEqual(sid_2, 2)
        sid_3 = yield nc.subscribe("quux", "", log.persist)
        self.assertEqual(sid_3, 3)
        yield nc.publish("foo", "hello")
        yield tornado.gen.sleep(1.0)

        # Done
        yield nc.close()

        orig_gnatsd = self.server_pool.pop(0)
        orig_gnatsd.finish()

        try:
            a = nc._current_server
            # Wait and assert that we don't reconnect.
            yield tornado.gen.sleep(3)
        finally:
            b = nc._current_server
            self.assertEqual(a.uri, b.uri)

        self.assertFalse(nc.is_connected)
        self.assertFalse(nc.is_reconnecting)
        self.assertTrue(nc.is_closed)

        with(self.assertRaises(ErrConnectionClosed)):
            yield nc.publish("hello", "world")

        with(self.assertRaises(ErrConnectionClosed)):
            yield nc.flush()

        with(self.assertRaises(ErrConnectionClosed)):
            yield nc.subscribe("hello", "worker")

        with(self.assertRaises(ErrConnectionClosed)):
            yield nc.publish_request("hello", "inbox", "world")

        with(self.assertRaises(ErrConnectionClosed)):
            yield nc.request("hello", "world")

        with(self.assertRaises(ErrConnectionClosed)):
            yield nc.timed_request("hello", "world")


class ClientTLSTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))
        self.threads = []
        self.server_pool = []

        conf = """
          # Simple TLS config file
          port: 4444
          net: 127.0.0.1

          http_port: 8222

          """
        config_file = tempfile.NamedTemporaryFile(mode='w', delete=True)
        config_file.write(conf)
        config_file.flush()

        server = Gnatsd(port=4444, http_port=8222, config_file=config_file)
        self.server_pool.append(server)
        server = Gnatsd(port=4445, http_port=8223, config_file=config_file)
        self.server_pool.append(server)

        for gnatsd in self.server_pool:
            t = threading.Thread(target=gnatsd.start)
            self.threads.append(t)
            t.start()

            http = tornado.httpclient.HTTPClient()
            while True:
                try:
                    response = http.fetch(
                        'http://127.0.0.1:%d/varz' % gnatsd.http_port)
                    if response.code == 200:
                        break
                    continue
                except:
                    time.sleep(0.1)
                    continue
        super(ClientTLSTest, self).setUp()

    def tearDown(self):
        for gnatsd in self.server_pool:
            gnatsd.finish()

        for t in self.threads:
            t.join()

        super(ClientTLSTest, self).tearDown()

    @tornado.testing.gen_test(timeout=10)
    def test_tls_connection(self):

        class Component:
            def __init__(self, nc):
                self.nc = nc
                self.error = None
                self.error_cb_called = False
                self.close_cb_called = False
                self.disconnected_cb_called = False
                self.reconnected_cb_called = False
                self.msgs = []

            @tornado.gen.coroutine
            def subscription_handler(self, msg):
                yield self.nc.publish(msg.reply, 'hi')

            def error_cb(self, err):
                self.error = err
                self.error_cb_called = True

            def close_cb(self):
                self.close_cb_called = True

            def disconnected_cb(self):
                self.disconnected_cb_called = True

            def reconnected_cb(self):
                self.reconnected_cb_called = True

        nc = NatsProtocol()
        c = Component(nc)
        options = {
            "servers": [
                "nats://127.0.0.1:4444"
            ],
            "io_loop": self.io_loop,
            "close_cb": c.close_cb,
            "error_cb": c.error_cb,
            "disconnected_cb": c.disconnected_cb,
            "reconnected_cb": c.reconnected_cb
        }

        yield c.nc.connect(**options)
        yield c.nc.subscribe("hello", cb=c.subscription_handler)
        yield c.nc.flush()
        for i in range(0, 10):
            msg = yield c.nc.timed_request("hello", b'world')
            c.msgs.append(msg)
        self.assertEqual(len(c.msgs), 10)
        self.assertFalse(c.disconnected_cb_called)
        self.assertFalse(c.close_cb_called)
        self.assertFalse(c.error_cb_called)
        self.assertFalse(c.reconnected_cb_called)

        # Should be able to close normally
        yield c.nc.close()
        self.assertTrue(c.disconnected_cb_called)
        self.assertTrue(c.close_cb_called)
        self.assertFalse(c.error_cb_called)
        self.assertFalse(c.reconnected_cb_called)

    @tornado.testing.gen_test(timeout=15)
    def test_tls_reconnection(self):      

        class Component:
            def __init__(self, nc):
                self.nc = nc
                self.error = None
                self.error_cb_called = False
                self.close_cb_called = False
                self.disconnected_cb_called = False
                self.reconnected_cb_called = False
                self.msgs = []
                self.reconnected_future = tornado.concurrent.Future()

            @tornado.gen.coroutine
            def subscription_handler(self, msg):
                yield self.nc.publish(msg.reply, 'hi')

            def error_cb(self, err):
                self.error = err
                self.error_cb_called = True

            def close_cb(self):
                self.close_cb_called = True

            def disconnected_cb(self):
                self.disconnected_cb_called = True

            def reconnected_cb(self):
                self.reconnected_cb_called = True
                self.reconnected_future.set_result(True)

        nc = NatsProtocol()
        c = Component(nc)
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://127.0.0.1:4444",
                "nats://127.0.0.1:4445",
            ],
            "io_loop": self.io_loop,
            "close_cb": c.close_cb,
            "error_cb": c.error_cb,
            "disconnected_cb": c.disconnected_cb,
            "reconnected_cb": c.reconnected_cb
        }

        yield c.nc.connect(**options)
        yield c.nc.subscribe("hello", cb=c.subscription_handler)
        yield c.nc.flush()
        for i in range(0, 5):
            msg = yield c.nc.timed_request("hello", b'world')
            c.msgs.append(msg)
        self.assertEqual(len(c.msgs), 5)

        # Trigger disconnect...
        orig_gnatsd = self.server_pool.pop(0)
        orig_gnatsd.finish()
        try:
            a = nc._current_server
            # Wait for reconnect logic kick in...
            yield tornado.gen.with_timeout(timedelta(seconds=10), c.reconnected_future)
        finally:
            b = nc._current_server
            self.assertNotEqual(a.uri, b.uri)

        self.assertTrue(c.disconnected_cb_called)
        self.assertFalse(c.close_cb_called)
        self.assertTrue(c.error_cb_called)
        self.assertTrue(c.reconnected_cb_called)

        for i in range(0, 5):
            msg = yield c.nc.timed_request("hello", b'world')
            c.msgs.append(msg)
        self.assertEqual(len(c.msgs), 10)

        # Should be able to close normally
        yield c.nc.close()
        self.assertTrue(c.disconnected_cb_called)
        self.assertTrue(c.close_cb_called)
        self.assertTrue(c.error_cb_called)
        self.assertTrue(c.reconnected_cb_called)


class ClientTLSCertsTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))
        super(ClientTLSCertsTest, self).setUp()

    class Component:
        def __init__(self, nc):
            self.nc = nc
            self.error = None
            self.error_cb_called = False
            self.close_cb_called = False
            self.disconnected_cb_called = False
            self.reconnected_cb_called = False
            self.msgs = []

        @tornado.gen.coroutine
        def subscription_handler(self, msg):
            yield self.nc.publish(msg.reply, 'hi')

        def error_cb(self, err):
            self.error = err
            self.error_cb_called = True

        def close_cb(self):
            self.close_cb_called = True

        def disconnected_cb(self):
            self.disconnected_cb_called = True

        def reconnected_cb(self):
            self.reconnected_cb_called = True

    @tornado.testing.gen_test(timeout=10)
    def test_tls_verify(self):
        nc = NatsProtocol()
        c = self.Component(nc)
        options = {
            "servers": [
                "nats://127.0.0.1:4446"
            ],
            "allow_reconnect": False,
            "io_loop": self.io_loop,
            "close_cb": c.close_cb,
            "error_cb": c.error_cb,
            "disconnected_cb": c.disconnected_cb,
            "reconnected_cb": c.reconnected_cb,
            "tls": {
                "cert_reqs": ssl.CERT_REQUIRED,
                "ca_certs": "./tests/configs/certs/ca.pem",
                "keyfile":  "./tests/configs/certs/client-key.pem",
                "certfile": "./tests/configs/certs/client-cert.pem"
            }
        }

        conf = """
          port: 4446
          net: 127.0.0.1

          http_port: 8446
          tls {
            cert_file: './tests/configs/certs/server-cert.pem'
            key_file:  './tests/configs/certs/server-key.pem'
            ca_file:   './tests/configs/certs/ca.pem'
            timeout:   10
            verify:    true
          }
          """

        with Gnatsd(port=4446, http_port=8446, conf=conf) as gnatsd:
            yield c.nc.connect(**options)
            yield c.nc.subscribe("hello", cb=c.subscription_handler)
            yield c.nc.flush()
            for i in range(0, 10):
                msg = yield c.nc.timed_request("hello", b'world')
                c.msgs.append(msg)
            self.assertEqual(len(c.msgs), 10)
            self.assertFalse(c.disconnected_cb_called)
            self.assertFalse(c.close_cb_called)
            self.assertFalse(c.error_cb_called)
            self.assertFalse(c.reconnected_cb_called)

            # Should be able to close normally
            yield c.nc.close()
            self.assertTrue(c.disconnected_cb_called)
            self.assertTrue(c.close_cb_called)
            self.assertFalse(c.error_cb_called)
            self.assertFalse(c.reconnected_cb_called)

    @tornado.testing.gen_test(timeout=10)
    def test_tls_verify_short_timeout_no_servers_available(self):
        nc = NatsProtocol()
        c = self.Component(nc)
        options = {
            "servers": [
                "nats://127.0.0.1:4446"
            ],
            "allow_reconnect": False,
            "io_loop": self.io_loop,
            "close_cb": c.close_cb,
            "error_cb": c.error_cb,
            "disconnected_cb": c.disconnected_cb,
            "reconnected_cb": c.reconnected_cb,
            "tls": {
                "cert_reqs": ssl.CERT_REQUIRED,
                "ca_certs": "./tests/configs/certs/ca.pem",
                "keyfile":  "./tests/configs/certs/client-key.pem",
                "certfile": "./tests/configs/certs/client-cert.pem"
            }
        }

        conf = """
          # port: 4446
          port: 4446
          net: 127.0.0.1

          http_port: 8446
          tls {
            cert_file: './tests/configs/certs/server-cert.pem'
            key_file:  './tests/configs/certs/server-key.pem'
            ca_file:   './tests/configs/certs/ca.pem'
            timeout:   0.0001
            verify:    true
          }
          """

        with Gnatsd(port=4446, http_port=8446, conf=conf) as gnatsd:
            with self.assertRaises(ErrNoServers):
                yield c.nc.connect(**options)

    @tornado.testing.gen_test(timeout=10)
    def test_tls_verify_fails(self):
        nc = NatsProtocol()
        c = self.Component(nc)
        port = 4447
        http_port = 8447
        options = {
            "servers": [
                "nats://127.0.0.1:%d" % port
            ],
            "max_reconnect_attempts": 5,
            "io_loop": self.io_loop,
            "close_cb": c.close_cb,
            "error_cb": c.error_cb,
            "disconnected_cb": c.disconnected_cb,
            "reconnected_cb": c.reconnected_cb,
            "reconnect_time_wait": 0.1,
            "tls": {
                "cert_reqs": ssl.CERT_REQUIRED,
                # "ca_certs": "./tests/configs/certs/ca.pem",
                "keyfile":  "./tests/configs/certs/client-key.pem",
                "certfile": "./tests/configs/certs/client-cert.pem"
            }
        }

        conf = """
          port: %d
          net: 127.0.0.1

          http_port: %d
          tls {
            cert_file: './tests/configs/certs/server-cert.pem'
            key_file:  './tests/configs/certs/server-key.pem'
            ca_file:   './tests/configs/certs/ca.pem'
            timeout:   10
            verify: true
          }
          """ % (port, http_port)

        with Gnatsd(port=port, http_port=http_port, conf=conf) as gnatsd:
            with self.assertRaises(NatsError):
                yield c.nc.connect(**options)


class ShortControlLineNATSServer(tornado.tcpserver.TCPServer):
    @tornado.gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                info_line = """INFO {"max_payload": 1048576, "tls_required": false, "server_id":"zrPhBhrjbbUdp2vndDIvE7"}\r\n"""
                yield stream.write(info_line)

                # Client will be awaiting for a pong next before reaching connected state.
                yield stream.write("""PONG\r\n""")
                yield tornado.gen.sleep(1)
            except tornado.iostream.StreamClosedError:
                break


class LargeControlLineNATSServer(tornado.tcpserver.TCPServer):
    @tornado.gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            try:
                line = """INFO {"max_payload": 1048576, "tls_required": false, "server_id":"%s"}\r\n"""
                info_line = line % ("a" * 2048)
                yield stream.write(info_line)

                # Client will be awaiting for a pong next before reaching connected state.
                yield stream.write("""PONG\r\n""")
                yield tornado.gen.sleep(1)
            except tornado.iostream.StreamClosedError:
                break


class ClientConnectTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))
        super(ClientConnectTest, self).setUp()

    def tearDown(self):
        super(ClientConnectTest, self).tearDown()

    @tornado.testing.gen_test(timeout=5)
    def test_connect_info_large_protocol_line(self):
        # Start mock TCP Server
        server = LargeControlLineNATSServer(io_loop=self.io_loop)
        server.listen(4229)
        nc = NatsProtocol()
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://127.0.0.1:4229"
            ],
            "io_loop": self.io_loop,
            "verbose": False
        }
        yield nc.connect(**options)
        self.assertTrue(nc.is_connected)

    @tornado.testing.gen_test(timeout=5)
    def test_connect_info_large_protocol_line_2(self):
        # Start mock TCP Server
        server = ShortControlLineNATSServer(io_loop=self.io_loop)
        server.listen(4229)
        nc = NatsProtocol()
        options = {
            "dont_randomize": True,
            "servers": [
                "nats://127.0.0.1:4229"
            ],
            "io_loop": self.io_loop,
            "verbose": False
        }
        yield nc.connect(**options)
        self.assertTrue(nc.is_connected)

class ClientClusteringDiscoveryTest(tornado.testing.AsyncTestCase):

    def setUp(self):
        print("\n=== RUN {0}.{1}".format(
            self.__class__.__name__, self._testMethodName))
        super(ClientClusteringDiscoveryTest, self).setUp()

    def tearDown(self):
        super(ClientClusteringDiscoveryTest, self).tearDown()

    @tornado.testing.gen_test(timeout=15)
    def test_servers_discovery(self):
        conf = """
        cluster {
          routes = [
            nats-route://127.0.0.1:6222
          ]
        }
        """

        nc = NatsProtocol()
        options = {
            "servers": [
                "nats://127.0.0.1:4222"
            ],
            "io_loop": self.io_loop,
        }

        with Gnatsd(port=4222, http_port=8222, cluster_port=6222, conf=conf) as nats1:
            yield nc.connect(**options)
            yield tornado.gen.sleep(1)
            initial_uri = nc.connected_url
            with Gnatsd(port=4223, http_port=8223, cluster_port=6223, conf=conf) as nats2:
                yield tornado.gen.sleep(1)
                srvs = {}
                for item in nc._server_pool:
                    srvs[item.uri.port] = True
                self.assertEqual(len(srvs.keys()), 2)

                with Gnatsd(port=4224, http_port=8224, cluster_port=6224, conf=conf) as nats3:
                    yield tornado.gen.sleep(1)
                    for item in nc._server_pool:
                        srvs[item.uri.port] = True
                    self.assertEqual(3, len(srvs.keys()))

                    srvs = {}
                    for item in nc.discovered_servers:
                        srvs[item.uri.port] = True
                    self.assertTrue(2 <= len(srvs.keys()) <= 3)

                    srvs = {}
                    for item in nc.servers:
                        srvs[item.uri.port] = True
                    self.assertEqual(3, len(srvs.keys()))

                    # Terminate the first server and wait for reconnect
                    nats1.finish()
                    yield tornado.gen.sleep(1)
                    final_uri = nc.connected_url
                    self.assertNotEqual(initial_uri, final_uri)
        yield nc.close()

    @tornado.testing.gen_test(timeout=15)
    def test_servers_discovery_no_randomize(self):
        conf = """
        cluster {
          routes = [
            nats-route://127.0.0.1:6222
          ]
        }
        """

        nc = NatsProtocol()
        options = {
            "servers": [
                "nats://127.0.0.1:4222"
            ],
            "dont_randomize": True,
            "io_loop": self.io_loop,
        }

        with Gnatsd(port=4222, http_port=8222, cluster_port=6222, conf=conf) as nats1:
            yield nc.connect(**options)
            yield tornado.gen.sleep(0.5)
            with Gnatsd(port=4223, http_port=8223, cluster_port=6223, conf=conf) as nats2:
                yield tornado.gen.sleep(1)
                srvs = []
                for item in nc._server_pool:
                    if item.uri.port not in srvs:
                        srvs.append(item.uri.port)
                self.assertEqual(len(srvs), 2)

                with Gnatsd(port=4224, http_port=8224, cluster_port=6224, conf=conf) as nats3:
                    yield tornado.gen.sleep(1)
                    for item in nc._server_pool:
                        if item.uri.port not in srvs:
                            srvs.append(item.uri.port)
                    self.assertEqual([4222, 4223, 4224], srvs)
        yield nc.close()

if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
