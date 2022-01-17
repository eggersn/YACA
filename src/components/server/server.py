from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.multicast.to_reliable_multicast import TotalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.broadcast.broadcast_listener import BroadcastListener
from src.protocol.base import Message

from processing.announcements import AnnouncementProcessing

class Server:

    def __init__(self, initial=False, i=0):
        self._client_channel = Channel()
        self._announcement_channel = Channel()
        self._consensus_channel = Channel()
        self._discovery_channel = Channel()
        self._configuration = Configuration()

        if initial:
            self._group_view = GroupView(self._configuration.get_group_view_file(i))
        else:
            identifier = self._configuration.generate_uuid()
            sk = self._configuration.generate_key()

        # multicast handler for client requests (reliable causal ordered multicast)
        self._client_multicast = CausalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_client_multicast_port(),
            self._group_view.identifier,
            self._client_channel,
            self._group_view
        )
        self._client_multicast.start()

        # multicast handler for announcements (reliable total ordered multicast)
        self._announcement_multicast = TotalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_announcement_multicast_port(),
            self._group_view.identifier,
            self._announcement_channel,
            self._group_view
        )
        self._announcement_multicast.start()

        self._announcement_processing = AnnouncementProcessing(self._announcement_channel)

        # multicast handler for consensus and consensus (reliable causal ordered multicast)
        self._consensus_multicast = CausalOrderedReliableMulticast(
            self._configuration.get_multicast_addr(),
            self._configuration.get_consensus_multicast_port(),
            self._group_view.identifier,
            self._consensus_channel,
            self._group_view
        )
        self._consensus_multicast.start()

        self._broadcast_listener = BroadcastListener(self._discovery_channel, self._configuration.get_broadcast_port())
        self._broadcast_listener.start()

    def start(self):
        self._announcement_processing.start()

    