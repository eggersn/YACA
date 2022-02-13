import random
import string
import threading
import sys
import time
import math
from collections import Counter

from src.core.unicast.sender import UnicastSender
from src.core.signatures.signatures import Signatures
from src.core.multicast.reliable_multicast import ReliableMulticast
from src.core.unicast.udp_listener import UDPUnicastListener
from src.core.utils.channel import Channel
from src.core.utils.configuration import Configuration
from src.components.client.client_co_reliable_multicast import ClientCausalOrderedReliableMulticast
from src.protocol.client.read.heartbeat import HearbeatQueryResponse, HeartbeatQuery
from src.protocol.client.read.messages import MessageQuery
from src.protocol.client.write.text_message import TextMessage
from src.protocol.base import Message
from nacl.signing import SigningKey, VerifyKey
from src.protocol.client.write.initial import *


class Client:
    def __init__(self):
        self._delivery_channel = Channel()
        self._server_channel = Channel()
        self._trash_channel = Channel()
        self._trash_channel.set_trash_flag(True)
        self._configuration = Configuration()
        self._identifier = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        self._signing_key = SigningKey.generate()
        self._signature = Signatures(self._signing_key, self._identifier)

        self._users = {}
        self._init_messages = {}

        self._write_multicast = ClientCausalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_client_write_multicast_port(),
            self._identifier,
            self._delivery_channel,
            None,
            self._configuration,
            open=True,
        )

        self._read_multicast = ReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_client_read_multicast_port(),
            self._identifier,
            self._trash_channel,
            None,
            self._configuration,
            open=True,
        )

        self._unicast_receiver = UDPUnicastListener(
            self._server_channel, listener=self._read_multicast._udp_sock
        )
        self._unicast_receiver.start()
        self._ack_receiver = UDPUnicastListener(
            self._server_channel, listener=self._write_multicast._udp_sock
        )
        self._ack_receiver.start()
        self._unicast_sender = UnicastSender(self._configuration, self._read_multicast._udp_sock)

    def start(self):
        self._create_user()

        polling_thread = threading.Thread(target=self._polling)
        polling_thread.start()

        consumer_thread = threading.Thread(target=self._consumer)
        consumer_thread.start()

    def send(self, text: str):
        msg = TextMessage.initFromData(text)
        msg.encode()

        self._write_multicast.send(msg)

    def _create_user(self):
        def timeout_handler():
            self._server_channel.produce(None)

        pk_string = base64.b64encode(self._signing_key.verify_key.encode()).decode("ascii")
        success = False
        while not success:
            initial_msg = InitMessage.initFromData(self._identifier, self._signing_key.verify_key)
            initial_msg.encode()

            self._write_multicast.send(initial_msg, sign=False)

            timer = threading.Timer(self._configuration.get_discovery_total_timeout(), timeout_handler)
            timer.start()
            responses = []
            nonces = []
            response = -1
            while response is not None:
                response = self._server_channel.consume()
                if response is not None:
                    if nonces == []:  # first message
                        timer.cancel()
                        timer = threading.Timer(self._configuration.get_timeout(), timeout_handler)
                        timer.start()
                    msg = InitResponse.initFromJSON(response)
                    msg.decode()

                    if msg.get_nonce() not in nonces:
                        responses.append(response)
                        nonces.append(msg.get_nonce())

            N = len(responses)
            if N > 0:
                f = math.ceil(N / 4) - 1

                success_count = 0
                for data in responses:
                    msg = InitResponse.initFromJSON(data)
                    msg.decode()

                    if msg.response == "success: user already exists with same pk":
                        success_count += 1
                    elif "success" in msg.response and msg.response.split(" ")[-1] == pk_string:
                        success_count += 1

                if success_count >= N - f:
                    print("Initialization: Finished with id: {}, pk: {}".format(self._identifier, pk_string))
                    success = True
                else:
                    print("Initialization: Retrying")
                    self._identifier = "".join(
                        random.choice(string.ascii_uppercase + string.digits) for _ in range(10)
                    )
                    self._read_multicast._identifier = self._identifier
                    self._write_multicast._identifier = self._identifier
                    self._signature = Signatures(self._signing_key, self._identifier)

        self._read_multicast._signature = self._signature
        self._write_multicast._signature = self._signature

    def _polling(self):
        while True:
            time.sleep(self._configuration.get_client_polling())
            nonce = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            heartbeat = HeartbeatQuery.initFromData(nonce)
            heartbeat.encode()
            heartbeat.sign(self._signature)
            self._read_multicast.send(heartbeat)

    def _consumer(self):
        while True:
            data = self._server_channel.consume()
            self._process_message(data)

    def _process_message(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        if msg.header == "Query Response: HeartBeat":
            self._process_heartbeat(data)
        elif msg.header == "Write: Initial":
            self._process_init_message(data)
        elif msg.header == "Write: TextMessage":
            self._process_text_message(data)

    def _process_heartbeat(self, data):
        heartbeat = HearbeatQueryResponse.initFromJSON(data)
        heartbeat.decode()

        nacks = self._write_multicast._handle_acks_open(heartbeat.acks, {})
        addr = (heartbeat.get_sender()[0], heartbeat.listening_port)
        if nacks is not None:
            init_nacks = {}
            for identifier in nacks:
                if len(nacks[identifier]) > 0 and nacks[identifier][0] == 0:
                    init_nacks[identifier] = [0]
                    nacks[identifier].pop(0)

            msg_query = MessageQuery.initFromData(init_nacks)
            msg_query.encode()
            self._read_multicast.send(msg_query)

            msg_query = MessageQuery.initFromData(nacks)
            msg_query.encode()
            self._unicast_sender.send_udp_without_ack(msg_query, addr)

    def _process_text_message(self, data):
        self._check_init_messages()

        msg = Message.initFromJSON(data)
        msg.decode()

        if msg.verify_signature(self._signature, self._users):
            self._write_multicast._receive_pb_message_open(data)

    def _process_init_message(self, data):
        msg = InitMessage.initFromJSON(data)
        msg.decode()

        if msg.identifier not in self._users:
            if msg.identifier not in self._init_messages:
                self._init_messages[msg.identifier] = [time.time_ns() / 10**9, []]

            self._init_messages[msg.identifier][1].append(data)
            self._check_init_messages()

    def _check_init_messages(self):
        ts = time.time_ns() / 10**9
        del_list = []
        for identifier in self._init_messages:
            if ts - self._init_messages[identifier][0] > self._configuration.get_timeout():
                c = Counter(self._init_messages[identifier][1])
                (majority_value, majority_count) = c.most_common()[0]
                if majority_count > len(self._init_messages[identifier][1]) / 2:
                    msg = InitMessage.initFromJSON(majority_value)
                    msg.decode()
                    self._users[identifier] = msg.pk
                    self._write_multicast._receive_pb_message_open(majority_value)
                
                del_list.append(identifier)

        for identifier in del_list:
            del self._init_messages[identifier]
