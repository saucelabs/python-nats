# coding: utf-8
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
import logging
import random
import traceback

import tornado.ioloop
import tornado.gen
import time
from datetime import datetime

from twisted.internet import reactor, protocol, defer, task
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.error import TimeoutError

from nats.io.utils import new_inbox
from nats.io.client import NatsProtocol as NATS, NatsClientFactory, NatsClient


@inlineCallbacks
def main():
    try:
        nc = NatsClient()
        yield nc.connect("localhost", 4222)
        # ncf = NatsClientFactory(host="localhost", port=4222)
        # nc = yield ncf.connect()
        # nc = yield reactor.connectTCP("localhost", 4222, ncf)
        # client_creator = protocol.ClientCreator(reactor, NATS, ping_interval=3)
        # nc = yield client_creator.connectTCP("localhost", 4222)

        # nc = NATS()

        # Establish connection to the server.
        # options = {"verbose": True, "servers": ["nats://127.0.0.1:4222"]}
        # yield nc.connect(**options)

        def discover(msg=None):
            print("[Discover Received]: %s" % msg.data)

        def loop_cb(msg=None):
            print("[Loop Received]: %s" % msg.data)

        sid = yield nc.subscribe("discover", "", discover)

        # Only interested in 2 messages.
        yield nc.auto_unsubscribe(sid, 2)
        yield nc.publish("discover", "A")
        yield nc.publish("discover", "B")

        # Following two messages won't be received.
        yield nc.publish("discover", "C")
        yield nc.publish("discover", "D")

        # Request/Response
        def help_request_handler(msg):
            print("[Help Request Handler Received]: %s" % msg.data)
            nc.publish(msg.reply, "OK, I can help!")

        # Susbcription using distributed queue
        yield nc.subscribe("help", "workers", help_request_handler)

        try:
            response = yield nc.timed_request("help", "Hi, need help!", timeout=0.5)
            print("[Timed Response]: %s" % response.data)
        except TimeoutError, e:
            print("Timeout! Need to retry...")

        # Customize number of responses to receive
        def many_responses(msg=None):
            print("[Response]: %s" % msg.data)

        yield nc.request("help", "please", expected=2, cb=many_responses)

        # Publish inbox
        my_inbox = new_inbox()
        yield nc.subscribe(my_inbox)
        yield nc.publish_request("help", my_inbox, "I can help too!")

        yield nc.subscribe("looper", "", loop_cb)
        loop = task.LoopingCall(looping_publisher, nc)
        loop.start(2)
    except Exception, e:
        traceback.print_exc()


@inlineCallbacks
def looping_publisher(nc):
    yield nc.publish("looper", "iLoop " + str(random.randint(0, 100)))


if __name__ == '__main__':
    logging.basicConfig()
    reactor.callWhenRunning(main)
    reactor.run()
