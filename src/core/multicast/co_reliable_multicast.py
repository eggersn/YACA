import threading
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.protocol.multicast.piggyback import PiggybackMessage
from src.core.multicast.reliable_multicast import ReliableMulticast


class CausalOrderedReliableMulticast(ReliableMulticast):

    def __init__(self, multicast_addr: str, multicast_port: int, identifier: str, channel: Channel, group_view: GroupView):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view)

        self._co_holdback_queue: list[tuple[dict[str, int], str]] = []
        self._co_lock = threading.Lock()
        self._CO_R_g : dict[str, int] = {}

    def _deliver(self, data, identifier, seqno):
        self._update_storage(data, identifier, seqno)
        self._co_consume(data, identifier, seqno)

    def _co_deliver(self, data, identifier, seqno):
        self._channel.produce(data)

    def _co_consume(self, data, identifier, seqno):
        pb_message = PiggybackMessage.initFromJSON(data)
        pb_message.decode()

        seqno_dict = pb_message.acks.copy()
        seqno_dict[identifier] = seqno - 1

        with self._co_lock:
            if self._check_if_ready_to_deliver(seqno_dict):
                self._co_deliver(data, identifier, seqno)
                self._CO_R_g[identifier] = seqno
                self._check_co_holdback_queue()
            else:
                self._co_holdback_queue.append((seqno_dict, data))

    def _check_if_ready_to_deliver(self, seqno_dict):
        for identifier in seqno_dict:
            if identifier not in self._CO_R_g:
                self._CO_R_g[identifier] = -1
            if identifier == self._identifier:
                if seqno_dict[identifier] > self._S_p:
                    return False
            else:
                if seqno_dict[identifier] > self._CO_R_g[identifier]:
                    return False

        return True

    def _check_co_holdback_queue(self):
        change = True
        while change:
            change = False
            k = 0
            while k < len(self._co_holdback_queue):
                (seqno_dict, data) = self._co_holdback_queue[k]

                if self._check_if_ready_to_deliver(seqno_dict):
                    pb_message = PiggybackMessage.initFromJSON(data)
                    pb_message.decode()

                    self._co_deliver(data, pb_message.identifier, pb_message.seqno)
                    self._CO_R_g[pb_message.identifier] = pb_message.seqno
                    self._co_holdback_queue.pop(k)
                    change = True
                else:
                    k += 1
