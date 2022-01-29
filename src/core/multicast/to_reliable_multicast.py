import threading
import time
from src.core.consensus.max_phase_king import MaxPhaseKing
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.multicast.to_message import TotalOrderMessage
from src.protocol.multicast.to_proposal import TotalOrderProposal
from src.protocol.base import Message
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.protocol.consensus.suspect import GroupViewSuspect
from src.protocol.multicast.halt import HaltMessage
from src.protocol.multicast.commence import CommenceMessage


class TotalOrderedReliableMulticast(CausalOrderedReliableMulticast):
    def __init__(
        self,
        multicast_addr: str,
        multicast_port: int,
        identifier: str,
        channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
        verbose: bool = False,
    ):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view, configuration)

        self._P_g = -1
        self._A_g = -1
        self._to_holdback_dict: dict[str, list[str, list[str], float]] = {}
        self._to_holdback_dict_lock = threading.Lock()
        self._to_holdback_queue: list[list[tuple[int, str], str, int]] = []
        self._produce_channel = Channel()
        self._to_lock = threading.Lock()
        self.__verbose = verbose

        self._suspended_dict: dict[tuple[str, str], list[str]] = {}
        self._halting_servers: dict[str, str] = {}
        self._halting_semaphore = threading.Semaphore(0)
        self._join_response_buffer: list[str] = []

        self._max_phase_king = MaxPhaseKing(
            self._response_channel, self._to_holdback_queue, self._group_view, self._configuration, verbose
        )

        crash_fault_detection_thread = threading.Thread(target=self._check_holdback_timestamps)
        crash_fault_detection_thread.start()

    def _check_holdback_timestamps(self):
        timeout = self._configuration.get_timeout()
        while True:
            time.sleep(5 * self._configuration.get_heartbeat_interval())
            with self._to_holdback_dict_lock:
                ts = time.time_ns() / 10 ** 9
                for key in self._to_holdback_dict:
                    entry = self._to_holdback_dict[key]
                    if ts - entry[2] > timeout:
                        for server_id in self._group_view.servers:
                            if (
                                server_id not in entry[1]
                                and not self._group_view.check_if_server_is_inactive(server_id)
                                and not server_id in self._halting_servers
                            ):
                                suspect_msg = GroupViewSuspect.initFromData(server_id, "TO-Proposal: " + key)
                                suspect_msg.encode()
                                self.__debug("TO-Multicast: Suspect for timeout on proposal")
                                self.send(suspect_msg)
            self._max_phase_king.check_timeouts(self._halting_servers, self.send)

    def _co_deliver(self, data, identifier, seqno):
        self.__debug("CO-Multicast: Deliver", data)
        self._to_consume(data, identifier, seqno)

    def _to_deliver(self, data):
        self.__debug("TO-Multicast: Deliver", data)
        self._channel.produce(data)

    def _to_consume(self, data, identifier, seqno):
        message = Message.initFromJSON(data)
        message.decode()

        with self._to_lock:
            if message.header == "TO: Seqno Proposal":
                self._handle_to_seqno_proposal(data, identifier)
            elif message.header == "View: Suspect":
                self._handle_suspect_message(data)
            elif message.header == "Halt Message":
                self._handle_halt_message(data)
            elif message.header == "Commence Message":
                self._handle_commence_message(data)
            elif message.header == "Phase King: Message":
                if self._max_phase_king.process_pk_message(data):
                    self._check_to_holdback_queue()
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
            self.__debug(
                "TO-Multicast: Received seqno proposal {} of server {} for message {} ({}/{})".format(
                    message.seqno,
                    identifier,
                    message.msg_identifier,
                    entry[2],
                    self._group_view.get_number_of_unsuspended_servers(),
                )
            )
            if entry[0] < (message.seqno, identifier):
                entry[0] = (message.seqno, identifier)
                self._A_g = max(self._A_g, message.seqno)
                self._to_holdback_queue.sort(key=lambda entry: entry[0])

            self._check_to_holdback_queue()

    def _check_to_holdback_queue(self):
        N = self._group_view.get_number_of_unsuspended_servers()
        while len(self._to_holdback_queue) > 0:
            entry = self._to_holdback_queue[0]
            commited_servers = self._to_holdback_dict[entry[1]][1]

            # remove suspended servers from commits
            k = 0
            while k < len(commited_servers):
                if self._group_view.check_if_server_is_inactive(commited_servers[k]):
                    commited_servers.pop(k)
                    entry[2] -= 1
                else:
                    k += 1

            if entry[2] == N and entry[3]:
                entry = self._to_holdback_queue.pop(0)
                self._to_deliver(self._to_holdback_dict[entry[1]][0])
                with self._to_holdback_dict_lock:
                    del self._to_holdback_dict[entry[1]]
            elif entry[2] == N:
                self._max_phase_king.start_new_execution(entry[0], entry[1])
                break
            else:
                break

    def _handle_suspect_message(self, data):
        suspect_msg = GroupViewSuspect.initFromJSON(data)
        suspect_msg.decode()

        sender_id, _ = suspect_msg.get_signature()

        if self._group_view.check_if_participant(suspect_msg.identifier):
            if (suspect_msg.identifier, suspect_msg.topic) not in self._suspended_dict:
                self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)] = []

            if sender_id not in self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)]:
                self.__debug(
                    "TO-Multicast: Received suspect message for {} of server {} (reason: {})".format(
                        suspect_msg.identifier, sender_id, suspect_msg.topic
                    )
                )
                self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)].append(sender_id)

            N = self._group_view.get_number_of_unsuspended_servers()
            f = int(N / 4)

            # remove suspect messages of inactive servers 
            k = 0
            while k < len(self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)]):
                server = self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)][k]
                if self._group_view.check_if_server_is_inactive(server):
                    self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)].pop(k)
                else:
                    k += 1

            if len(self._suspended_dict[(suspect_msg.identifier, suspect_msg.topic)]) > f:
                if not self._group_view.check_if_server_is_suspended(suspect_msg.identifier):
                    self.__debug('TotalOrdering: Suspending "{}"'.format(suspect_msg.identifier))
                    self._group_view.suspend_server(suspect_msg.identifier)

                if "TO-Proposal" in suspect_msg.topic:
                    self._check_to_holdback_queue()
                    self._check_hold_messages()

                if "PK-1:" in suspect_msg.topic:  # timeout of some server in round1 of maxphaseking
                    self._max_phase_king.handle_round1_suspension(suspect_msg.topic)
                if "PK-2:" in suspect_msg.topic:  # timeout of phaseking
                    self._max_phase_king.handle_round2_suspension(suspect_msg.topic)

    def _handle_halt_message(self, data):
        halt_msg = HaltMessage.initFromJSON(data)
        halt_msg.decode()

        sender_id, _ = halt_msg.get_signature()
        self._halting_servers[sender_id] = halt_msg.wait_until
        self.__debug(
            "TO-Multicast: Received halt message of server {} (wait until: {})".format(
                sender_id, halt_msg.wait_until
            )
        )

        self._check_hold_messages()

    def _check_hold_messages(self):
        if self._group_view.identifier in self._group_view.joining_servers:
            N = self._group_view.get_number_of_unsuspended_servers()
            if len(self._halting_servers.keys()) == N:
                for sender_id in self._halting_servers:
                    if self._halting_servers[sender_id] != self._group_view.identifier:
                        return

                # send commence message
                commence_msg = CommenceMessage.initFromData(self._group_view.identifier)
                commence_msg.encode()

                self._max_phase_king.reset()
                self._group_view.wait_till_ready_to_join()
                self._group_view.mark_server_as_joined(self._group_view.identifier)
                self._group_view.flag_I_am_added()
                with self._to_holdback_dict_lock:
                    ts = time.time_ns() / 10 ** 9
                    for key in self._to_holdback_dict:
                        self._to_holdback_dict[key][2] = ts
                self._halting_servers = {}
                self._response_channel.set_trash_flag(False)
                self._response_channel.produce((commence_msg.json_data, True))
                for proposal_json in self._join_response_buffer:
                    proposal_msg = TotalOrderProposal.initFromJSON(proposal_json)
                    proposal_msg.decode()

                    if proposal_msg.msg_identifier in self._to_holdback_dict:
                        self._response_channel.produce((proposal_json, False))
                self._join_response_buffer = []

        elif (
            self._group_view.identifier in self._halting_servers
            and self._group_view.check_if_server_is_suspended(
                self._halting_servers[self._group_view.identifier]
            )
        ):
            # joining server has failed to answer in time, resuming operation
            self.__debug("TO-Multicast: JOIN timeout, resuming normal operation")
            self._max_phase_king.reset()
            self._group_view.mark_server_as_joined(
                self._halting_servers[self._group_view.identifier]
            )  # removes server from joining list
            with self._to_holdback_dict_lock:
                ts = time.time_ns() / 10 ** 9
                for key in self._to_holdback_dict:
                    self._to_holdback_dict[key][2] = ts
            self._halting_servers = {}
            self.continue_sending()
            self._halting_semaphore.release()
            self._check_to_holdback_queue()

    def _handle_commence_message(self, data):
        commence_msg = CommenceMessage.initFromJSON(data)
        commence_msg.decode()

        sender_id, _ = commence_msg.get_signature()
        if (
            sender_id == commence_msg.wait_until
            and self._group_view.identifier in self._halting_servers
            and self._halting_servers[self._group_view.identifier] == sender_id
        ):
            self.__debug("TO-Multicast: Received commence message from", sender_id)
            self._max_phase_king.reset()
            self._group_view.mark_server_as_joined(sender_id)
            self.timer.cancel()
            self.timer1.cancel()
            with self._to_holdback_dict_lock:
                ts = time.time_ns() / 10 ** 9
                for key in self._to_holdback_dict:
                    self._to_holdback_dict[key][2] = ts
            self._halting_servers = {}
            self.continue_sending()
            self._halting_semaphore.release()

        elif (
            sender_id == commence_msg.wait_until
            and self._group_view.identifier in self._group_view.joining_servers
        ):
            self.__debug("TO-Multicast: Commence message from", sender_id)
            self._max_phase_king.reset()
            self._group_view.mark_server_as_joined(sender_id)
            self._halting_servers = {}
            self._halting_semaphore.release()

    def _handle_to_message(self, data):
        message = TotalOrderMessage.initFromJSON(data)
        message.decode()

        sender_id, _ = message.get_signature()
        if sender_id in self._halting_servers:
            suspect_msg = GroupViewSuspect.initFromData(sender_id, self._halting_servers[sender_id])
            suspect_msg.encode()
            self.__debug("TO-Multicast: Suspect for sending after halting: ", sender_id)
            self._response_channel.produce((suspect_msg.json_data, True))
        else:
            self._P_g = max(self._A_g, self._P_g) + 1
            with self._to_holdback_dict_lock:
                timestamp = time.time_ns() / 10 ** 9
                self._to_holdback_dict[message.msg_identifier] = [data, [], timestamp]
            self._to_holdback_queue.append([(self._P_g, ""), message.msg_identifier, 0, False])
            self._to_holdback_queue.sort(key=lambda entry: entry[0])

            response_msg = TotalOrderProposal.initFromData(self._P_g, message.msg_identifier)
            response_msg.encode()

            if self._group_view.identifier in self._group_view.joining_servers:
                self._join_response_buffer.append(response_msg.json_data)
            else:
                self._response_channel.produce((response_msg.json_data, False))

    def __existing_server_timeout_handler(self, wait_until):
        for server_id in self._group_view.servers:
            if (
                server_id not in self._group_view.joining_servers
                and server_id not in self._group_view.suspended_servers
            ):
                if server_id not in self._halting_servers or self._halting_servers[server_id] != wait_until:
                    suspect_msg = GroupViewSuspect.initFromData(server_id, wait_until)
                    suspect_msg.encode()
                    self.__debug("TO-Multicast: Suspect for not halting: ", server_id)
                    self.send(suspect_msg, True)

    def __new_server_timeout_handler(self, identification):
        suspect_msg = GroupViewSuspect.initFromData(identification, identification)
        suspect_msg.encode()
        self.__debug("TO-Multicast: Suspect for not replying: ", identification)
        self.send(suspect_msg, True)

    def halt_multicast(self, wait_until):
        self.__debug("TO-Multicast: Halting while waiting for", wait_until)
        config = not self._suspend_multicast
        self.halt_sending()
        halt_msg = HaltMessage.initFromData(wait_until)
        halt_msg.encode()

        self.send(halt_msg, config)

        self.timer = threading.Timer(
            self._configuration.get_timeout(), self.__existing_server_timeout_handler, args=(wait_until,)
        )
        self.timer.start()
        self.timer1 = threading.Timer(
            self._configuration.get_discovery_total_timeout(),
            self.__new_server_timeout_handler,
            args=(wait_until,),
        )
        self.timer1.start()

        self._halting_semaphore.acquire()

    def __debug(self, *msgs):
        if self.__verbose:
            print(*msgs)
