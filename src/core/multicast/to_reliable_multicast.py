import threading
import time
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.protocol.multicast.to_message import TotalOrderMessage
from src.protocol.multicast.to_proposal import TotalOrderProposal
from src.protocol.base import Message

from co_reliable_multicast import CausalOrderedReliableMulticast

class TotalOrderedReliableMulticast(CausalOrderedReliableMulticast):
    _P_g = -1
    _A_g = -1

    _to_holdback_queue : list[list[tuple[int, str], str, str, list[str]]] = []
    _response_channel = Channel()
    _lock = threading.Lock()

    def __init__(self, multicast_addr: str, multicast_port: int, identifier: str, channel: Channel, group_view: GroupView):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view)

        consume_thread = threading.Thread(target=self._consume)
        consume_thread.start()

    def _co_deliver(self, data, identifier, seqno):
        self._to_consume(data, identifier, seqno)

    def _to_consume(self, data, identifier, seqno):
        message = Message.initFromJSON(data)
        message.decode()

        with self._lock:
            if message.header == "TO: Seqno Proposal":
                message = TotalOrderProposal.initFromJSON(data)
                message.decode()

                entry = None
                for e in self._to_holdback_queue:
                    if e[1] == message.msg_identifier:
                        entry = e

                if entry is not None and identifier not in entry[3]:
                    entry[3].append(identifier)
                    if entry[0] < (message.seqno, identifier):
                        entry[0] = (message.seqno, identifier)

                    N = self._group_view.get_number_of_servers()

                    if len(entry[3]) == N:
                        self._A_g = max(self._A_g, message.seqno)
                        self._to_holdback_queue.sort(key=lambda entry: entry[0])

                        while len(self._to_holdback_queue) > 0 and len(self._to_holdback_queue[0][3]) == N:
                            entry = self._to_holdback_queue.pop(0)
                            self._channel.produce(entry[2])
                        if len(self._to_holdback_queue) > 0:
                            print(self._to_holdback_queue[:min(5, len(self._to_holdback_queue))])


            else:
                message = TotalOrderMessage.initFromJSON(data)
                message.decode()

                self._P_g = max(self._A_g, self._P_g) + 1
                self._to_holdback_queue.append([(self._P_g, self._identifier), message.msg_identifier, data, [self._identifier]])
                self._to_holdback_queue.sort(key=lambda entry: entry[0])

                response = TotalOrderProposal.initFromData(self._P_g, message.msg_identifier)
                response.encode()
                
                self._response_channel.produce(response.json_data)

    def _consume(self):
        while True:
            response = self._response_channel.consume()
            message = Message.initFromJSON(response)
            self.send(message)

