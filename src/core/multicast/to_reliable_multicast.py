import threading
import time
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.multicast.to_message import TotalOrderMessage
from src.protocol.multicast.to_proposal import TotalOrderProposal
from src.protocol.base import Message
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.protocol.consensus.suspect import GroupViewSuspect


class TotalOrderedReliableMulticast(CausalOrderedReliableMulticast):
    def __init__(
        self,
        multicast_addr: str,
        multicast_port: int,
        identifier: str,
        channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
        verbose: bool = False
    ):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view, configuration)

        self._P_g = -1
        self._A_g = -1
        self._to_holdback_dict: dict[str, list[str, list[str], float]] = {}
        self._to_holdback_queue: list[list[tuple[int, str], str, int]] = []
        self._produce_channel = Channel()
        self._to_lock = threading.Lock()
        self.__verbose = verbose

        self._suspended_dict: dict[tuple[str, str], list[str]] = {}

        crash_fault_detection_thread = threading.Thread(target=self._check_holdback_timestamps)
        crash_fault_detection_thread.start()

    def _check_holdback_timestamps(self):
        timeout = self._configuration.get_timeout()
        while True:
            time.sleep(self._configuration.get_heartbeat_interval())
            ts = time.time_ns() / 10 ** 9
            for key in self._to_holdback_dict:
                entry = self._to_holdback_dict[key]
                if ts - entry[2] > timeout:
                    for server_id in self._group_view.servers:
                        if server_id not in entry[1] and not self._group_view.check_if_server_is_suspended(
                            server_id
                        ):
                            suspect_msg = GroupViewSuspect.initFromData(server_id, key)
                            suspect_msg.encode()

                            self._response_channel.produce(suspect_msg.json_data)

    def _co_deliver(self, data, identifier, seqno):
        self._to_consume(data, identifier, seqno)

    def _to_deliver(self, data):
        self._channel.produce(data)

    def _to_consume(self, data, identifier, seqno):
        message = Message.initFromJSON(data)
        message.decode()

        with self._to_lock:
            if message.header == "TO: Seqno Proposal":
                self._handle_to_seqno_proposal(data, identifier)
            elif message.header == "View: Suspect":
                self._handle_suspect_message(data)
            else:
                self._handle_to_message(data)

    def _handle_to_seqno_proposal(self, data, identifier):
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

            N = self._group_view.get_number_of_unsuspended_servers()

            while len(self._to_holdback_queue) > 0:
                entry = self._to_holdback_queue[0]
                commited_servers = self._to_holdback_dict[entry[1]][1]

                # remove suspended servers from commits
                k = 0
                while k < len(commited_servers):
                    if self._group_view.check_if_server_is_suspended(commited_servers[k]):
                        commited_servers.pop(k)
                        entry[2] -= 1
                    else:
                        k += 1

                if entry[2] == N:
                    entry = self._to_holdback_queue.pop(0)
                    self._to_deliver(self._to_holdback_dict[entry[1]][0])
                    del self._to_holdback_dict[entry[1]]
                else:
                    break

            # if len(self._to_holdback_queue) > 0:
            #     print("Queue:", len(self._to_holdback_queue), self._to_holdback_queue[:min(5, len(self._to_holdback_queue))])

    def _handle_suspect_message(self, data):
        suspect_msg = GroupViewSuspect.initFromJSON(data)
        suspect_msg.decode()

        sender_id, _ = suspect_msg.get_signature()

        if self._group_view.check_if_participant(suspect_msg.identifier):
            if (suspect_msg.identifier, suspect_msg.topic) not in self._suspended_dict:
                self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)] = []

            if sender_id not in self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)]:
                self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)].append(sender_id)

            N = self._group_view.get_number_of_unsuspended_servers()
            f = int(N / 4)

            if len(self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)]) >= N - f:
                if not self._group_view.check_if_server_is_suspended(suspect_msg.identifier):
                    self._group_view.suspend_server(suspect_msg.identifier)

                    self.__debug('TotalOrdering: Suspending "{}"'.format(suspect_msg.identifier))

    def _handle_to_message(self, data):
        message = TotalOrderMessage.initFromJSON(data)
        message.decode()

        timestamp = time.time_ns() / 10 ** 9
        self._P_g = max(self._A_g, self._P_g) + 1
        self._to_holdback_dict[message.msg_identifier] = [data, [self._identifier], timestamp]
        self._to_holdback_queue.append([(self._P_g, self._identifier), message.msg_identifier, 1])
        self._to_holdback_queue.sort(key=lambda entry: entry[0])

        response_msg = TotalOrderProposal.initFromData(self._P_g, message.msg_identifier)
        response_msg.encode()

        self._response_channel.produce(response_msg.json_data)

    def __debug(self, *msgs):
        if self.__verbose:
            print(*msgs)
