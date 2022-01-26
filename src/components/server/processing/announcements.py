import threading
import base64
from nacl.signing import VerifyKey

from src.core.signatures.signatures import Signatures
from src.protocol.group_view.join import JoinMsg, JoinRequest, JoinResponse
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.multicast.to_reliable_multicast import TotalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.broadcast.broadcast_listener import BroadcastListener
from src.protocol.base import Message
from src.core.unicast.sender import UnicastSender
from src.core.consensus.phase_king import PhaseKing


class AnnouncementProcessing:
    def __init__(
        self,
        announcement_channel: Channel,
        consensus_channel: Channel,
        announcement_multicast: TotalOrderedReliableMulticast,
        phase_king: PhaseKing,
        group_view: GroupView,
        configuration: Configuration,
    ):
        self._channel = announcement_channel
        self._consensus_channel = consensus_channel
        self._to_multicast = announcement_multicast
        self._phase_king = phase_king
        self._group_view = group_view
        self._configuration = configuration
        self._signature = Signatures(group_view.sk, group_view.identifier)

        self._udp_sender = UnicastSender(self._configuration)

    def start(self):
        consumer_thread = threading.Thread(target=self.consumer)
        consumer_thread.start()

    def consumer(self):
        while True:
            data = self._channel.consume()
            print("CONSUME", data)

            self._process_request(data)

    def _process_request(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        if "View: Join Message" == msg.header:
            self._process_join(data)

    def _process_join(self, data):
        join_msg = JoinMsg.initFromJSON(data)
        join_msg.decode()

        join_request = JoinRequest.initFromJSON(join_msg.request)
        join_request.decode()

        # run phaseking algorithm on shortened data
        pk_string = base64.b64encode(join_request.pk.encode()).decode("ascii")
        shortened_data = "{}#{}#{}#{}".format(
            join_request.identifier, pk_string, join_request.ip_addr, join_request.port
        )
        shortened_data = self._phase_king.consensus(shortened_data)

        data = shortened_data.split("#")
        if len(data) != 4:
            return

        pk = VerifyKey(base64.b64decode(data[1]))
        # this allows the new server to send nacks and consume the storage of to-multicast
        self._group_view.add_server(data[0], pk, data[2], int(data[3]))

        # verify signatures requires that the new server is added to the group view. If invalid, we simply suspend the server again
        # note that, due to the phaseking algorithm before, all honest servers suspend the server if at least one honest server does so
        if not join_request.verify_signature(self._signature, self._group_view):
            self._group_view.suspend_server(join_request.identifier)
            return

        # notify the new server that we halted further delivery and sending
        response = JoinResponse.initFromData("waiting")
        success = self._udp_sender.send_udp_sync(response, (data[2], int(data[3])))
        if not success:
            # TODO undo halt
            self._group_view.suspend_server(join_request.identifier)
            return

        # halt delivery and sending
        self._to_multicast.halt_multicast(join_request.identifier)