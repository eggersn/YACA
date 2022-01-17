import json
from src.protocol.base import Message
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.multicast.to_reliable_multicast import TotalOrderedReliableMulticast
from src.core.consensus.phase_king import PhaseKing
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel


class ByzantineTotalOrderedReliableMulticast(TotalOrderedReliableMulticast):
    def __init__(
        self,
        multicast_addr: str,
        multicast_port: int,
        identifier: str,
        channel: Channel,
        group_view: GroupView,
        consensus_channel: Channel,
        consensus_multicast: CausalOrderedReliableMulticast,
    ):
        super().__init__(
            multicast_addr, multicast_port, identifier, channel, group_view
        )

        self._phase_king = PhaseKing(consensus_channel, consensus_multicast, group_view)


    def _to_deliver(self, data):
        message = Message.initFromJSON(data)
        message.decode()

        value = json.dumps(message.content)

        value = self._phase_king.consensus(value)
        self._channel.produce(value)