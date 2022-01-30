import time

from src.protocol.consensus.suspect import GroupViewSuspect
from src.core.unicast.sender import UnicastSender
from src.protocol.election.announcement import ElectionAnnouncement
from src.core.unicast.udp_listener import UDPUnicastListener
from src.core.consensus.phase_king import PhaseKing
from src.core.signatures.signatures import Signatures
from src.components.server.discovery import ServerDiscovery
from src.components.server.processing.dicovery import DiscoveryProcessing
from src.core.multicast.reliable_multicast import ReliableMulticast
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.multicast.to_reliable_multicast import TotalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.broadcast.broadcast_listener import BroadcastListener
from src.protocol.base import Message
from src.protocol.group_view.join import JoinRequest, JoinMsg
from src.protocol.multicast.to_message import TotalOrderMessage
from src.components.server.processing.client_requests import ClientRequestsProcessing
from src.components.server.processing.announcements import AnnouncementProcessing
from src.components.server.processing.joining import JoinProcessing
from src.protocol.election.ping import ElectionPingMessage


class Server:
    def __init__(self, initial=False, i=0, verbose=False):
        self.__verbose = verbose
        self._client_channel = Channel()
        self._db_channel = Channel()
        self._announcement_channel = Channel()
        self._consensus_channel = Channel()
        self._discovery_channel = Channel()
        self._configuration = Configuration()

        if initial:
            self._group_view = GroupView.initFromFile(
                self._configuration.get_group_view_file(i), verbose=self.__verbose
            )
            self._signature = Signatures(self._group_view.sk, self._group_view.identifier)
            self._udp_listener = UDPUnicastListener(
                self._client_channel, listening_port=self._group_view.get_my_port()
            )
            self._udp_listener.start()
        else:
            self._udp_listener = UDPUnicastListener(self._client_channel)
            self._udp_listener.start()
            self._group_view = GroupView.generateOwnData(
                self._configuration.get_global_group_view_file(),
                self._udp_listener.get_port(),
                verbose=self.__verbose,
            )
            self._signature = Signatures(self._group_view.sk, self._group_view.identifier)
            discovery = ServerDiscovery(self._configuration)
            discovery.discover(self._group_view, self._signature, self._client_channel)

        # open reliable multicast for clients to make byzantine fault tolerant queries
        self._client_request_multicast = ReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_client_multicast_port(),
            self._group_view.identifier,
            self._client_channel,
            self._group_view,
            self._configuration,
            open=True,
        )
        self._client_request_multicast.start()

        self._client_processing = ClientRequestsProcessing(
            self._client_channel, self._group_view, self._configuration
        )

        # multicast handler for the database (reliable causal ordered multicast)
        self._db_multicast = CausalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_db_multicast_port(),
            self._group_view.identifier,
            self._db_channel,
            self._group_view,
            self._configuration,
        )
        self._db_multicast.start()

        # multicast handler for consensus (reliable causal ordered multicast)
        self._consensus_multicast = CausalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_consensus_multicast_port(),
            self._group_view.identifier,
            self._consensus_channel,
            self._group_view,
            self._configuration,
        )
        self._consensus_multicast.start()
        self._phase_king = PhaseKing(
            self._consensus_channel,
            self._consensus_multicast,
            self._group_view,
            self._configuration,
            verbose=self.__verbose,
        )

        # multicast handler for announcements (reliable total ordered multicast)
        self._announcement_multicast = TotalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_announcement_multicast_port(),
            self._group_view.identifier,
            self._announcement_channel,
            self._group_view,
            self._configuration,
            verbose=self.__verbose,
        )
        self._announcement_multicast.start(trash=not initial)

        self._announcement_processing = AnnouncementProcessing(
            self._announcement_channel,
            self._consensus_channel,
            self._announcement_multicast,
            self._phase_king,
            self._group_view,
            self._configuration,
        )

        # broadcast handler for service discovery
        self._discovery_listener = BroadcastListener(
            self._discovery_channel, self._configuration.get_broadcast_port()
        )

        self._discovery_processing = DiscoveryProcessing(
            self._discovery_channel,
            self._discovery_listener,
            self._group_view,
            self._announcement_multicast,
            self._db_multicast,
            self._configuration,
        )

        if not initial:
            self.__debug("Server: Start consuming as", self._group_view.identifier)
            join_processing = JoinProcessing(
                self._announcement_channel,
                self._consensus_channel,
                self._group_view,
                self._configuration,
                self._announcement_multicast._halting_semaphore,
            )
            join_processing.start()
            self._group_view.wait_till_I_am_added()

    def start(self):
        self._client_processing.start()
        self._announcement_processing.start()
        self._discovery_processing.start()

        if not self._group_view.check_if_manager():
            self.monitor_manager()

    def monitor_manager(self):
        self._udp_sender = UnicastSender(self._configuration)
        while True:
            time.sleep(self._configuration.get_heartbeat_interval())
            ping_msg = ElectionPingMessage.initFromData()
            addr = self._group_view.get_unicast_addr_of_server(self._group_view.manager)
            is_active = self._udp_sender.send_udp_sync(ping_msg, addr)

            if not is_active:
                suspect_msg = GroupViewSuspect.initFromData(
                    self._group_view.manager, "Timeout#{}".format(self._group_view.manager)
                )
                suspect_msg.encode()

                self._announcement_multicast.send(suspect_msg)
                self._group_view.wait_for_manager_to_be_elected()

    def __debug(self, *msgs):
        if self.__verbose:
            print(*msgs)
