import threading
import time
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.protocol.multicast.to_message import TotalOrderMessage
from src.protocol.multicast.to_proposal import TotalOrderProposal
from src.protocol.base import Message
from src.protocol.multicast.piggyback import PiggybackMessage

from co_reliable_multicast import CausalOrderedReliableMulticast

class TotalOrderedReliableMulticast(CausalOrderedReliableMulticast):
    _P_g = -1
    _A_g = -1

    _to_holdback_dict : dict[str, list[str, list[str]]] = {}
    _to_holdback_queue : list[list[tuple[int, str], str, int]] = []
    _response_channel = Channel()
    _produce_channel = Channel()
    _to_lock = threading.Lock()

    def __init__(self, multicast_addr: str, multicast_port: int, identifier: str, channel: Channel, group_view: GroupView):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view)

        consume_thread = threading.Thread(target=self._consume)
        consume_thread.start()

    def _co_deliver(self, data, identifier, seqno):
        self._to_consume(data, identifier, seqno)

    def _to_consume(self, data, identifier, seqno):
        message = Message.initFromJSON(data)
        message.decode()

        print("CO-RECV", data)

        with self._to_lock:
            if message.header == "TO: Seqno Proposal":
                message = TotalOrderProposal.initFromJSON(data)
                message.decode()

                print("Proposal from ", identifier + ": ( ", message.msg_identifier, message.seqno, ")")

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
                            print("DELIVER", self._to_holdback_dict[entry[1]][0])
                            self._channel.produce(self._to_holdback_dict[entry[1]][0])
                            del self._to_holdback_dict[entry[1]]
                        else:
                            break

                    if len(self._to_holdback_queue) > 0:
                        print("Queue:", len(self._to_holdback_queue), self._to_holdback_queue[:min(5, len(self._to_holdback_queue))])


            else:
                print("MSG", data)

                message = TotalOrderMessage.initFromJSON(data)
                message.decode()

                self._P_g = max(self._A_g, self._P_g) + 1
                self._to_holdback_dict[message.msg_identifier] = [data, [self._identifier]]
                self._to_holdback_queue.append([(self._P_g, self._identifier), message.msg_identifier, 1])
                self._to_holdback_queue.sort(key=lambda entry: entry[0])

                response_msg = TotalOrderProposal.initFromData(self._P_g, message.msg_identifier)
                response_msg.encode()

                self._response_channel.produce(response_msg.json_data)
                
                
    def _consume(self):
        while True:
            data = None
            if not self._response_channel.is_empty():
                data = self._response_channel.consume()
            elif not self._produce_channel.is_empty():
                data = self._produce_channel.consume()
            else:
                time.sleep(0.05)

            if data is not None:
                message = Message.initFromJSON(data)
                self._send(message)

    def send(self, message: Message):
        if not message.is_encoded:
            message.encode()

        self._produce_channel.produce(message.json_data)

    def _send(self, message: Message):
        if not message.is_decoded:
            message.decode()
        self._R_g_lock.acquire()
        pb_message = PiggybackMessage.initFromMessage(
            message, self._identifier, self._S_p, self._R_g
        )
        pb_message.encode()
        pb_message.sign(self._signature)

        self._udp_sock.sendto(
            pb_message.json_data.encode(), (self._multicast_addr, self._multicast_port)
        )

        self._deliver(pb_message.json_data, self._identifier, self._S_p)
        self._S_p += 1
        self._check_holdback_queue()
        self._R_g_lock.release()

