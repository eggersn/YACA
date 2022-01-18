import threading
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.multicast.to_message import TotalOrderMessage
from src.protocol.multicast.to_proposal import TotalOrderProposal
from src.protocol.base import Message
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView


class TotalOrderedReliableMulticast(CausalOrderedReliableMulticast):
    def __init__(
        self,
        multicast_addr: str,
        multicast_port: int,
        identifier: str,
        channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
    ):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view, configuration)

        self._P_g = -1
        self._A_g = -1
        self._to_holdback_dict: dict[str, list[str, list[str]]] = {}
        self._to_holdback_queue: list[list[tuple[int, str], str, int]] = []
        self._produce_channel = Channel()
        self._to_lock = threading.Lock()


    def _co_deliver(self, data, identifier, seqno):
        self._to_consume(data, identifier, seqno)

    def _to_deliver(self, data):
        self._channel.produce(data)

    def _to_consume(self, data, identifier, seqno):
        message = Message.initFromJSON(data)
        message.decode()

        with self._to_lock:
            if message.header == "TO: Seqno Proposal":
                message = TotalOrderProposal.initFromJSON(data)
                message.decode()

                entry = None
                for e in self._to_holdback_queue:
                    if e[1] == message.msg_identifier:
                        entry = e

                if entry is not None and identifier not in self._to_holdback_dict[entry[1]][1]:
                    commited_servers = self._to_holdback_dict[entry[1]][1]
                    commited_servers.append(identifier)
                    entry[2] += 1
                    if entry[0] < (message.seqno, identifier):
                        entry[0] = (message.seqno, identifier)
                        self._A_g = max(self._A_g, message.seqno)
                        self._to_holdback_queue.sort(key=lambda entry: entry[0])

                    N = self._group_view.get_number_of_servers()

                    while len(self._to_holdback_queue) > 0:
                        entry = self._to_holdback_queue[0]

                        if entry[2] == N:
                            entry = self._to_holdback_queue.pop(0)
                            self._to_deliver(self._to_holdback_dict[entry[1]][0])
                            del self._to_holdback_dict[entry[1]]
                        else:
                            break

                    # if len(self._to_holdback_queue) > 0:
                    #     print("Queue:", len(self._to_holdback_queue), self._to_holdback_queue[:min(5, len(self._to_holdback_queue))])

            else:
                message = TotalOrderMessage.initFromJSON(data)
                message.decode()

                self._P_g = max(self._A_g, self._P_g) + 1
                self._to_holdback_dict[message.msg_identifier] = [data, [self._identifier]]
                self._to_holdback_queue.append([(self._P_g, self._identifier), message.msg_identifier, 1])
                self._to_holdback_queue.sort(key=lambda entry: entry[0])

                response_msg = TotalOrderProposal.initFromData(self._P_g, message.msg_identifier)
                response_msg.encode()

                self._response_channel.produce(response_msg.json_data)
