import threading

from src.protocol.group_view.join import *
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.multicast.to_reliable_multicast import TotalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.core.unicast.sender import UnicastSender
from src.protocol.multicast.to_message import TotalOrderMessage


class DiscoveryProcessing:
    def __init__(
        self,
        discovery_channel: Channel,
        group_view: GroupView,
        announcement_multicast: TotalOrderedReliableMulticast,
        db_multicast: CausalOrderedReliableMulticast,
        configuration: Configuration,
    ):
        self._channel = discovery_channel
        self._group_view = group_view
        self._announcement_multicast = announcement_multicast
        self._db_multicast = db_multicast
        self._configuration = configuration

        self._pending = {}

    def start(self):
        consumer_thread = threading.Thread(target=self.consumer)
        consumer_thread.start()

    def consumer(self):
        responder = UnicastSender(self._configuration)
        responder.start()
        while True:
            data, addr = self._channel.consume()

            response_msg = self._process_request(data)

            if response_msg is not None:
                responder.send_udp_without_ack(response_msg, addr)

    def _process_request(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        response_msg = None

        if msg.header == "View: Join Request":
            response_msg = self._process_join_request(data)

        return response_msg

    def _process_join_request(self, data):
        join_msg = JoinRequest.initFromJSON(data)
        join_msg.decode()

        print(join_msg.json_data)

        if join_msg.identifier not in self._group_view.servers:
            if join_msg.identifier not in self._pending:
                response_msg = JoinResponse.initFromData("processing")

                self._pending[join_msg.identifier] = join_msg.pk

                forwarded_join_msg = JoinMsg.initFromData(data)
                forwarded_join_msg.encode()
                to_join_msg = TotalOrderMessage.initFromMessage(forwarded_join_msg, join_msg.identifier)
                to_join_msg.encode()

                self._announcement_multicast.send(to_join_msg)
            else:
                if self._pending[join_msg.identifier] == join_msg.pk:
                    response_msg = JoinResponse.initFromData("processing")
                else:
                    response_msg = JoinResponse.initFromData("id already assigned")
        else:
            response_msg = JoinResponse.initFromData("id already assigned")

        return response_msg

