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
from src.core.election.election import Election
from src.protocol.client.write.initial import *
from src.components.server.processing.client_requests import ClientRequestsProcessing

class JoinProcessing:
    def __init__(
        self,
        announcement_channel: Channel,
        consensus_channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
        announcement_multicast : TotalOrderedReliableMulticast,
        client_processing : ClientRequestsProcessing,
    ):
        self._channel = announcement_channel
        self._consensus_channel = consensus_channel
        self._group_view = group_view
        self._configuration = configuration
        self._signature = Signatures(group_view.sk, group_view.identifier)
        self._phase_king = PhaseKing(
            self._consensus_channel, None, self._group_view, self._configuration, verbose=True
        )
        self._election = Election(self._phase_king, group_view, configuration, True)
        self._to_multicast = announcement_multicast
        self._client_processing = client_processing
        

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
        elif "Election: Announcement" == msg.header:
            self._process_election()
        elif "Client: Join Message" == msg.header:
            self._process_client_init(data)

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
            self._to_multicast._join_semaphores[join_request.identifier].release()
            return False

        pk = VerifyKey(base64.b64decode(data[1]))
        # this allows the new server to send nacks and consume the storage of to-multicast
        self._group_view.add_server(data[0], pk, data[2], int(data[3]))

        # verify signatures requires that the new server is added to the group view. If invalid, we simply suspend the server again
        # note that, due to the phaseking algorithm before, all honest servers suspend the server if at least one honest server does so
        if not join_request.verify_signature(self._signature, self._group_view.pks):
            self._group_view.suspend_server(join_request.identifier)
            self._to_multicast._join_semaphores[join_request.identifier].release()
            return False 

        self._to_multicast._join_semaphores[join_request.identifier].release()
        if join_request.identifier == self._group_view.identifier:
            return True 
        else:
            self._to_multicast.halt_multicast(join_request.identifier)
            return False

    def _process_election(self):
        consented_value = self._phase_king.consensus("election")
        
        if consented_value == "election":
            self._election.election()

    def _process_client_init(self, data):
        init_msg = TOInitMsg.initFromJSON(data)
        init_msg.decode()

        msg = InitMessage.initFromJSON(init_msg.request)
        msg.decode()

        pk_string = base64.b64encode(msg.pk.encode()).decode("ascii")
        consistent_pk = self._phase_king.consensus("client-init#"+pk_string)        

        if "#" in consistent_pk:
            self._client_processing._posthook_client_init(init_msg.request, consistent_pk.split("#")[1], quite=True)