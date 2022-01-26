import threading
from enum import Enum

from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.multicast.to_reliable_multicast import TotalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.broadcast.broadcast_listener import BroadcastListener
from src.protocol.base import Message
from src.protocol.client.read.system_query import *
from src.core.unicast.sender import UnicastSender


class ResponseKind(Enum):
    UDP = 0
    TCP = 1


class ClientRequestsProcessing:
    def __init__(self, client_channel: Channel, group_view: GroupView, configuration: Configuration):
        self._channel = client_channel
        self._group_view = group_view
        self._configuration = configuration

    def start(self):
        consumer_thread = threading.Thread(target=self.consumer)
        consumer_thread.start()

    def consumer(self):
        responder = UnicastSender(self._configuration)
        responder.start()
        while True:
            data = self._channel.consume()
            print(data)

            response_msg, response_kind, response_addr, response_id = self._process_request(data)

            if response_msg is not None:
                print(response_msg)
                if response_kind == ResponseKind.UDP:
                    responder.send_udp(response_msg, response_addr, response_id)
                else:
                    responder.send_tcp(response_msg, response_addr)

    def _process_request(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        response_msg = None
        response_kind = None
        response_addr = None
        response_id = ""

        if "Query:" in msg.header:
            query_msg = Query.initFromJSON(data)
            query_msg.decode()
            response_kind = ResponseKind.UDP if "UDP" in query_msg.type.value else ResponseKind.TCP
            response_addr = query_msg.get_sender()
            response_id = query_msg.nonce

            if "Query: NumberOfActiveServers" == msg.header:
                response_msg = self._handle_query_number_of_active_servers(query_msg.nonce)

        return response_msg, response_kind, response_addr, response_id

    def _handle_query_number_of_active_servers(self, nonce: str):
        N = self._group_view.get_number_of_unsuspended_servers()
        query_response = QueryResponseNumberOfActiveServers.initFromData(N, nonce)
        return query_response
