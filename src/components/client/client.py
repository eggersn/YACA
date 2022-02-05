import random
import string
import threading
import sys
import time

sys.path.append(sys.path[0] + "/../../..")
from src.core.multicast.reliable_multicast import ReliableMulticast
from src.core.unicast.udp_listener import UDPUnicastListener
from src.core.utils.channel import Channel
from src.core.utils.configuration import Configuration
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.protocol.client.read.heartbeat import HearbeatQueryResponse, HeartbeatQuery
from src.protocol.client.read.messages import MessageQuery
from src.protocol.client.write.text_message import TextMessage
from src.protocol.base import Message


class Client:
    def __init__(self):
        self._delivery_channel = Channel()
        self._server_channel = Channel()
        self._trash_channel = Channel()
        self._trash_channel.set_trash_flag(True)
        self._configuration = Configuration()
        self._identifier = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

        self._write_multicast = CausalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_client_write_multicast_port(),
            self._identifier,
            self._trash_channel,
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
        print(self._unicast_receiver.get_port())

    def start(self):
        polling_thread = threading.Thread(target=self._polling)
        polling_thread.start()

        consumer_thread = threading.Thread(target=self._consumer)
        consumer_thread.start()

    def send(self, text : str):
        msg = TextMessage.initFromData(text)
        msg.encode()

        self._write_multicast.send(msg)

    def _polling(self):
        while True:
            time.sleep(self._configuration.get_client_polling())
            nonce = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            heartbeat = HeartbeatQuery.initFromData(nonce)
            heartbeat.encode()
            self._read_multicast.send(heartbeat)

    def _consumer(self):
        while True:
            data = self._server_channel.consume()
            print(data)

            self._process_message(data)

    def _process_message(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        if msg.header == "Query Response: HeartBeat":
            self._process_heartbeat(data)

    def _process_heartbeat(self, data):
        heartbeat = HearbeatQueryResponse.initFromJSON(data)
        heartbeat.decode()

        nacks = self._write_multicast._handle_acks_open(
            heartbeat.acks, (heartbeat.get_sender()[0], heartbeat.listening_port), {}
        )
        if nacks is not None:
            msg_query = MessageQuery.initFromData(nacks)
            msg_query.encode()
            self._read_multicast.send(msg_query)


def main():
    client = Client()
    client.start()

    client.send("asdf")

    time.sleep(30)


if __name__ == "__main__":
    main()
