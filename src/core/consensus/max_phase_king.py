import math
import time
import numpy as np

from src.core.utils.configuration import Configuration
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.protocol.consensus.pk_message import PhaseKingMessage, Message
from src.protocol.consensus.suspect import GroupViewSuspect
from collections import Counter
from src.core.consensus.phase_king import PhaseKing


class MaxPhaseKing:
    def __init__(
        self,
        response_channel: Channel,
        to_holdback_queue: list[list[tuple[int, str], str, int, bool]],
        group_view: GroupView,
        configuration: Configuration,
        verbose=False,
    ):
        self._response_channel = response_channel
        self._to_holdback_queue = to_holdback_queue
        self._group_view = group_view
        self._configuration = configuration
        self.__verbose = verbose

        self._pk_storage: dict[
            str, list[tuple[int, int], dict[str, int], int, int, int, float]
        ] = {}  # pk_storage[msg_id] = [(phase, round), values, majority, count, max]
        self._list_of_kings: dict[str, list[str]] = {}

    def start_new_execution(self, initial_value: int, msg_id: str):
        if msg_id not in self._pk_storage:
            ts = time.time_ns() / 10 ** 9
            self._pk_storage[msg_id] = [
                (0, 1),
                {},
                -1,
                -1,
                ts,
                "",
            ]
        if msg_id not in self._list_of_kings:
            self._list_of_kings[msg_id] = []
        else:
            return

        pk_message = PhaseKingMessage.initFromData(initial_value, 0, 1, msg_id)
        pk_message.encode()

        self.__debug("MaxPhaseKing: Starting {} with initial value {}".format(msg_id, initial_value))
        self._response_channel.produce((pk_message.json_data, False), trash=True)

    def reset(self):
        self._pk_storage = {}
        self._list_of_kings = {}

    def process_pk_message(self, data):
        pk_message = PhaseKingMessage.initFromJSON(data)
        pk_message.decode()

        sender_id, _ = pk_message.get_signature()

        if pk_message.round == 1:
            self._process_round1_message(pk_message.value, pk_message.phase, pk_message.topic, sender_id)
        elif pk_message.round == 2:
            return self._process_round2_message(
                pk_message.value, pk_message.phase, pk_message.topic, sender_id
            )
        else:
            self.__send_suspect_message(sender_id, pk_message.topic, 0)
        return False

    def check_timeouts(self, halting_servers, send):
        timeout = self._configuration.get_timeout()
        ts = time.time_ns() / 10 ** 9
        for topic in self._pk_storage:
            if len(self._pk_storage[topic]) == 6:
                if ts - self._pk_storage[topic][4] > timeout:
                    if self._pk_storage[topic][0][1] == 1:
                        # round 1, suspect all servers that did not answer yet
                        for server in self._group_view.servers:
                            if (
                                not self._group_view.check_if_server_is_inactive(server)
                                and server not in self._pk_storage[topic][1]
                                and server not in halting_servers
                            ):
                                self.__debug("MaxPhaseKing: Suspecting {} in Round 1".format(server))
                                suspect_msg = GroupViewSuspect.initFromData(server, "PK-1: {}".format(topic))
                                suspect_msg.encode()
                                send(suspect_msg)
                    else:
                        # round 2, suspect phase king
                        phase_king = self._pk_storage[topic][5]
                        if phase_king != "" and phase_king not in halting_servers:
                            self.__debug("MaxPhaseKing: Suspecting {} in Round 2".format(phase_king))
                            suspect_msg = GroupViewSuspect.initFromData(phase_king, "PK-2: {}".format(topic))
                            suspect_msg.encode()
                            send(suspect_msg)

    def handle_round1_suspension(self, identifier : str):
        for topic in self._pk_storage:
            if self._pk_storage[topic][0][1] == 1:
                if identifier in self._pk_storage[topic][1]:
                    del self._pk_storage[topic][1][identifier]
                self._check_if_round1_finished(self._pk_storage[topic][0][0], topic)

    def handle_round2_suspension(self, identifier : str):
        # for topic in self._pk_storage:
        for topic in self._pk_storage:
            if self._pk_storage[topic][0][1] == 2 and self._pk_storage[topic][5] == identifier:
                ts = time.time_ns() / 10 ** 9
                self._pk_storage[topic][4] = ts
                self._pk_storage[topic][5] = self._group_view.get_next_active_after(identifier)

                if self._group_view.identifier == self._pk_storage[topic][5]:
                    # I am the phase king
                    tiebreaker = math.ceil(np.median(list(self._pk_storage[topic][1].values())))

                    pk_message = PhaseKingMessage.initFromData(tiebreaker, self._pk_storage[topic][0][0], 2, topic)
                    pk_message.encode()
                    self._response_channel.produce((pk_message.json_data, False), trash=True)

    def _process_round1_message(self, value: int, phase: int, topic: str, sender_id: str):
        if topic not in self._pk_storage:
            if phase == 0:
                self._pk_storage[topic] = [(0, 1), {sender_id: value}, -1, -1, -1, ""]
        else:
            if (phase, 1) == self._pk_storage[topic][0]:
                self._pk_storage[topic][1][sender_id] = value
                self._check_if_round1_finished(phase, topic)

    def _check_if_round1_finished(self, phase: int, topic: str):
        N = self._group_view.get_number_of_unsuspended_servers()
        if len(self._pk_storage[topic][1].keys()) == N:
            # all messages are received, continuing to round2
            c = Counter(self._pk_storage[topic][1].values())
            (majority_value, majority_count) = c.most_common()[0]
            self._pk_storage[topic][0] = (phase, 2)
            self._pk_storage[topic][2] = majority_value
            self._pk_storage[topic][3] = majority_count
            ts = time.time_ns() / 10 ** 9
            self._pk_storage[topic][4] = ts
            self._pk_storage[topic][5] = self._group_view.get_next_active_after(self._pk_storage[topic][5])

            self.__debug(
                "MaxPhaseKing [{}](Phase {} - Round 1): Maj {}, Count {}".format(
                    topic, phase, majority_value, majority_count
                )
            )

            if self._group_view.identifier == self._pk_storage[topic][5]:
                # I am the phase king
                tiebreaker = math.ceil(np.median(list(self._pk_storage[topic][1].values())))
                pk_message = PhaseKingMessage.initFromData(tiebreaker, phase, 2, topic)
                pk_message.encode()
                self._response_channel.produce((pk_message.json_data, False), trash=True)

    def _process_round2_message(self, tiebreaker: int, phase: int, topic: str, sender_id: str):
        if (
            topic not in self._pk_storage
            or sender_id != self._pk_storage[topic][5]
            or self._pk_storage[topic][0] != (phase, 2)
        ):
            return

        self._list_of_kings[topic].append(sender_id)
        N = self._group_view.get_number_of_unsuspended_servers()
        no_of_active_kings = len(
            [
                king
                for king in self._list_of_kings[topic]
                if not self._group_view.check_if_server_is_inactive(king)
            ]
        )
        f = math.ceil(N / 4) - 1  # assuming worst-case

        if self._pk_storage[topic][3] > N / 2 + f:
            value = self._pk_storage[topic][2]  # update value with majority
        elif len([v for v in self._pk_storage[topic][1].values() if v <= tiebreaker]) > f:
            value = tiebreaker

        if f + 1 - no_of_active_kings > 0:
            self.__debug("MaxPhaseKing [{}](Phase {} - Round 2): New value {}".format(topic, phase, value))
            # proceed with next phase
            ts = time.time_ns() / 10 ** 9
            self._pk_storage[topic] = [(phase + 1, 1), {}, -1, -1, ts, self._pk_storage[topic][5]]
            pk_message = PhaseKingMessage.initFromData(value, phase + 1, 1, topic)
            pk_message.encode()
            self._response_channel.produce((pk_message.json_data, False), trash=True)
        else:
            self.__debug("MaxPhaseKing [{}]: Result {}".format(topic, value))
            del self._list_of_kings[topic]
            del self._pk_storage[topic]
            k = 0
            while k < len(self._to_holdback_queue):
                if self._to_holdback_queue[k][1] == topic:
                    self._to_holdback_queue[k][0] = value
                    self._to_holdback_queue[k][3] = True
                    self._to_holdback_queue.sort(key=lambda entry: (entry[0], entry[1]))
                    return True
                else:
                    k += 1

        return False

    def __send_suspect_message(self, identifier: str, topic: str, round: int):
        suspect_msg = GroupViewSuspect.initFromData(identifier, "PK-{}: {}".format(round, topic))
        suspect_msg.encode()

        self._response_channel.produce((suspect_msg.json_data, False), trash=True)

    def __debug(self, *msgs):
        if self.__verbose:
            print(*msgs)
