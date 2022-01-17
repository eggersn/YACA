from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.protocol.consensus.pk_message import PhaseKingMessage
from collections import Counter


class PhaseKing:
    def __init__(
        self,
        consensus_channel: Channel,
        out_channel: Channel,
        consensus_multicast: CausalOrderedReliableMulticast,
        group_view: GroupView,
    ):
        self._channel = consensus_channel
        self._output = out_channel
        self._multicast = consensus_multicast
        self._group_view = group_view

    def consensus(self, value: str):
        N = self._group_view.get_number_of_servers()
        f = int(N / 4)

        for phase in range(f + 1):
            (majority_value, majority_count) = self._round1(value, phase)
            value = self._round2(majority_value, majority_count, phase)

        return value

    def _round1(self, value: str, phase: int):
        # send own value
        pk_message = PhaseKingMessage.initFromData(value, phase, 1)
        pk_message.encode()

        self._multicast.send(pk_message)

        # wait for N responses
        N = self._group_view.get_number_of_servers()

        values = []
        for i in range(N):
            while True:
                data = self._channel.consume()
                pk_message = PhaseKingMessage.initFromJSON(data)
                pk_message.decode()

                print(phase, 1, data)

                if pk_message.phase == phase and pk_message.round == 1:
                    break

            values.append(pk_message.value)

        c = Counter(values)
        return c.most_common()[0]

    def _round2(self, majority_value, majority_count, phase):
        phase_king = self._group_view.get_ith_server(phase)

        # check if this process is the phase king
        if phase_king == self._group_view.identifier:
            pk_message = PhaseKingMessage.initFromData(majority_value, phase, 2)
            pk_message.encode()
            self._multicast.send(pk_message)

        # wait for phase king message
        while True:
            data = self._channel.consume()
            pk_message = PhaseKingMessage.initFromJSON(data)
            pk_message.decode()

            print(phase, 2, data)

            if pk_message.phase == phase and pk_message.round == 2:
                break

        tiebreaker = pk_message.value

        N = self._group_view.get_number_of_servers()
        f = int(N / 4)
        if majority_count > N / 2 + f:
            return majority_value
        return tiebreaker
