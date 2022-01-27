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


class JoinProcessing:
    def __init__(
        self,
        announcement_channel: Channel,
        consensus_channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
        semaphore: threading.Semaphore
    ):
        self._channel = announcement_channel
        self._consensus_channel = consensus_channel
        self._group_view = group_view
        self._configuration = configuration
        self._signature = Signatures(group_view.sk, group_view.identifier)
        self._phase_king = PhaseKing(
            self._consensus_channel, None, self._group_view, self._configuration, verbose=True
        )
        self._semaphore = semaphore

    def start(self):
        self.consumer()

    def consumer(self):
        finished = False 
        while not finished:
            data = self._channel.consume()
            finished = self._process_request(data)

        self._group_view.flag_ready_to_join()

    def _process_request(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        if "View: Join Message" == msg.header:
            return self._process_join(data)

        return False

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
            return False

        pk = VerifyKey(base64.b64decode(data[1]))
        # this allows the new server to send nacks and consume the storage of to-multicast
        self._group_view.add_server(data[0], pk, data[2], int(data[3]))

        # verify signatures requires that the new server is added to the group view. If invalid, we simply suspend the server again
        # note that, due to the phaseking algorithm before, all honest servers suspend the server if at least one honest server does so
        if not join_request.verify_signature(self._signature, self._group_view):
            self._group_view.suspend_server(join_request.identifier)
            return False 

        if join_request.identifier == self._group_view.identifier:
            return True 
        else:
            self._semaphore.acquire()
            return False