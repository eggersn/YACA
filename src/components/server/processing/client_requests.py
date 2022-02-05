import threading
from enum import Enum

from src.core.group_view.group_view import GroupView
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.protocol.client.read.heartbeat import *
from src.protocol.client.read.messages import *
from src.core.unicast.sender import UnicastSender
from src.protocol.multicast.piggyback import PiggybackMessage


class ClientRequestsProcessing:
    def __init__(
        self,
        client_channel: Channel,
        client_write_multicast: CausalOrderedReliableMulticast,
        group_view: GroupView,
        configuration: Configuration,
    ):
        self._channel = client_channel
        self._client_write_multicast = client_write_multicast
        self._group_view = group_view
        self._configuration = configuration

        self._responder = UnicastSender(self._configuration)

    def start(self):
        consumer_thread = threading.Thread(target=self.consumer)
        consumer_thread.start()
        self._responder.start()

    def consumer(self):
        while True:
            data = self._channel.consume()
            self._process_request(data)

    def _process_request(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        if msg.header == "Query: Heartbeat":
            self._handle_query_heartbeat(data)
        elif msg.header == "Query: Messages":
            self._handle_query_messages(data)

    def _handle_query_heartbeat(self, data):
        msg = HeartbeatQuery.initFromJSON(data)
        msg.decode()

        response_msg = HearbeatQueryResponse.initFromData(self._client_write_multicast._CO_R_g, self._group_view.get_my_port(), msg.nonce)
        self._responder.send_udp_without_ack(response_msg, msg.get_sender())

    def _handle_query_messages(self, data):
        msg = MessageQuery.initFromJSON(data)
        msg.decode()

        for identifier in msg.nacks:
            if identifier in self._client_write_multicast._storage:
                for seqno in msg.nacks[identifier]:
                    if seqno >= len(self._client_write_multicast._storage[identifier]):
                        break

                    response = Message.initFromJSON(
                        self._client_write_multicast._storage[identifier][seqno]
                    )
                    self._responder.send_udp_without_ack(response, msg.get_sender())
