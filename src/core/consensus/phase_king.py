import threading
import math

from src.core.utils.configuration import Configuration
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.protocol.consensus.pk_message import PhaseKingMessage, Message
from src.protocol.consensus.suspect import GroupViewSuspect
from collections import Counter


class PhaseKing:
    def __init__(
        self,
        consensus_channel: Channel,
        consensus_multicast: CausalOrderedReliableMulticast,
        group_view: GroupView,
        configuration: Configuration,
        topic="",
        verbose=False,
    ):
        self._channel = consensus_channel
        self._multicast = consensus_multicast
        self._group_view = group_view
        self._configuration = configuration
        self._topic = topic
        self.__verbose = verbose

    def consensus(self, value: str):
        # Importantly, we require that all processes have the same value for N.
        # This is achieved by the fact, that we execute this algorithm solely after delivering
        # a message via TO-multicast (which handles coordination and in particular JOIN messages)
        N = self._group_view.get_number_of_unsuspended_servers()
        f = math.ceil(N / 4) - 1
        kings = []
        phase = 0
        last_king = ""

        while f + 1 - len(kings) > 0:
            self.__debug(
                "PhaseKing (Phase {}): King {}".format(
                    phase, self._group_view.get_next_active_after(last_king)
                )
            )
            majority_value, majority_count = self._round1(value, phase)
            value, king = self._round2(majority_value, majority_count, phase, last_king)
            self.__debug('PhaseKing (Phase {}): Result "{}"'.format(phase, value))

            last_king = king
            kings.append(king)
            kings = [king for king in kings if not self._group_view.check_if_server_is_inactive(king)]
            N = self._group_view.get_number_of_unsuspended_servers()
            f = math.ceil(N / 4) - 1

        return value

    """
    In Round1 of the Phase-King algorithm, all (unsuspended) participants send their current value to all other participants
    using causal ordered reliable multicast. Each process starts a local timer for detecting crash / omission faults. In case of 
    such a timeout, the process multicasts a SUSPEND message. If a process collects at least N-f suspend messages of other processes
    he commits the suspend message to group_view. This entails that messages of the suspended process will be ignored in the future. 
    """

    def _round1(self, value: str, phase: int):
        # on timeout, send suspect message for all servers that did not respond
        def timeout_handler(sender_ids):
            self._channel.produce(None, topic=self._topic)
            for server_id in self._group_view.servers:
                if server_id not in sender_ids and not self._group_view.check_if_server_is_inactive(
                    server_id
                ):
                    print(self._group_view.servers)
                    self.__debug("PhaseKing: Suspecting {} in Round 1".format(server_id))
                    suspect_msg = GroupViewSuspect.initFromData(server_id, self._topic)
                    suspect_msg.encode()

                    self._multicast.send(suspect_msg)



        self.__debug(
            'PhaseKing ({}Phase {} - Round 1): Initial value "{}"'.format(
                (self._topic + "; ") if self._topic != "" else "", phase, value
            )
        )

        # send own value
        pk_message = PhaseKingMessage.initFromData(value, phase, 1, self._topic)
        pk_message.encode()

        if self._multicast is not None:
            self._multicast.send(pk_message)

        # wait for N responses
        N = self._group_view.get_number_of_unsuspended_servers()

        values = []
        sender_ids = []
        suspected_servers = {}

        # start timer for crash fault detection
        if self._multicast is not None:
            timer = threading.Timer(self._configuration.get_timeout(), timeout_handler, args=(sender_ids,))
            timer.start()

        i = 0
        while i < N:
            data = self._channel.consume(self._topic)

            if data is not None:
                message = Message.initFromJSON(data)
                message.decode()

                sender_id, _ = message.get_signature()
                if not self._group_view.check_if_server_is_inactive(sender_id):
                    if message.header == "Phase King: Message":
                        i = self._handle_round1_phaseking_msg(data, sender_ids, values, phase, i)

                    elif message.header == "View: Suspect":
                        self._handle_round1_suspect_msg(
                            data,
                            suspected_servers,
                            sender_ids,
                            values,
                            phase,
                            N,
                        )

            N = self._group_view.get_number_of_unsuspended_servers()

        # cancel timeout timer
        if self._multicast is not None:
            timer.cancel()

        self.__debug(
            'PhaseKing ({}Phase {} - Round 1): Results "{}" from "{}"'.format(
                (self._topic + "; ") if self._topic != "" else "", phase, values, sender_ids
            )
        )

        # determine majority value and return
        c = Counter(values)
        (majority_value, majority_count) = c.most_common()[0]

        if majority_count <= N / 2:
            majority_value = ""
            majority_count = 0

        self.__debug(
            'PhaseKing ({}Phase {} - Round 1): New value "{}" received from "{}" processes'.format(
                (self._topic + "; ") if self._topic != "" else "", phase, majority_value, majority_count
            )
        )

        return majority_value, majority_count

    def _handle_round1_phaseking_msg(self, data, sender_ids, values, phase, i):
        pk_message = PhaseKingMessage.initFromJSON(data)
        pk_message.decode()

        sender_id, _ = pk_message.get_signature()

        if pk_message.phase == phase and pk_message.round == 1 and sender_id not in sender_ids:
            sender_ids.append(sender_id)
            values.append(pk_message.value)
            i += 1

        return i

    def _handle_round1_suspect_msg(self, data, suspected_servers, sender_ids, values, phase, N):
        suspect_msg = GroupViewSuspect.initFromJSON(data)
        suspect_msg.decode()

        sender_id, _ = suspect_msg.get_signature()

        if self._group_view.check_if_participant(suspect_msg.identifier):
            if suspect_msg.identifier not in suspected_servers:
                suspected_servers[suspect_msg.identifier] = []

            if sender_id not in suspected_servers[suspect_msg.identifier]:
                suspected_servers[suspect_msg.identifier].append(sender_id)
                f = math.ceil(N / 4) - 1

                # check if enough servers suspect the same process
                if (
                    len(suspected_servers[suspect_msg.identifier]) > f
                    and self._group_view.identifier not in suspected_servers[suspect_msg.identifier]
                ):
                    # peer pressure
                    response_suspect_msg = GroupViewSuspect.initFromData(suspect_msg.identifier, self._topic)
                    response_suspect_msg.encode()
                    self._multicast.send(response_suspect_msg)
                if len(suspected_servers[suspect_msg.identifier]) >= N - f:
                    if not self._group_view.check_if_server_is_inactive(suspect_msg.identifier):
                        N -= 1

                        self.__debug(
                            'PhaseKing ({}Phase {} - Round 1): Suspending "{}"'.format(
                                (self._topic + "; ") if self._topic != "" else "",
                                phase,
                                suspect_msg.identifier,
                            )
                        )
                        self._group_view.suspend_server(suspect_msg.identifier)

                        if suspect_msg.identifier in sender_ids:
                            index = sender_ids.index(suspect_msg.identifier)
                            sender_ids.pop(index)
                            values.pop(index)

        return N

    """
    In Round2 of the Phase-King algorithm, the processes await the tiebreaker value from the phase king. 
    Similar to Round1, the processes start a local timer to detect crash / omission faults of the phase king. 
    If a phase king is suspended, the process with the next higher id is selected - until there exists a correct phase king. 
    """

    def _round2(self, majority_value, majority_count, phase, last_king):
        def timeout_handler(phase_king):
            self.__debug("PhaseKing: Suspecting {} in Round 2".format(phase_king))
            suspect_msg = GroupViewSuspect.initFromData(phase_king, self._topic)
            suspect_msg.encode()

            self._multicast.send(suspect_msg)

        tiebreaker = None
        N = self._group_view.get_number_of_unsuspended_servers()

        while tiebreaker is None:
            phase_king = self._group_view.get_next_active_after(last_king)
            suspecting_servers = []

            self.__debug(
                'PhaseKing ({}Phase {} - Round 2): Waiting for "{}"'.format(
                    (self._topic + "; ") if self._topic != "" else "", phase, phase_king
                )
            )

            timer = None
            # check if this process is the phase king
            if phase_king == self._group_view.identifier:
                pk_message = PhaseKingMessage.initFromData(majority_value, phase, 2, self._topic)
                pk_message.encode()
                if self._multicast is None:
                    raise RuntimeError("I should not be phaseking during simulation")
                self._multicast.send(pk_message)

            elif self._multicast is not None:
                # start timer for crash fault detection of phase king
                timer = threading.Timer(
                    self._configuration.get_timeout(),
                    timeout_handler,
                    args=(phase_king,),
                )
                timer.start()

            # wait for phase king message
            while True:
                data = self._channel.consume(self._topic)

                message = Message.initFromJSON(data)
                message.decode()

                sender_id, _ = message.get_signature()
                if not self._group_view.check_if_server_is_inactive(sender_id):
                    if message.header == "Phase King: Message":
                        pk_message = PhaseKingMessage.initFromJSON(data)
                        pk_message.decode()
                        sender_id, _ = pk_message.get_signature()

                        if pk_message.phase == phase and pk_message.round == 2 and sender_id == phase_king:
                            tiebreaker = pk_message.value
                            break

                    elif message.header == "View: Suspect":
                        suspect_msg = GroupViewSuspect.initFromJSON(data)
                        suspect_msg.decode()
                        sender_id, _ = suspect_msg.get_signature()

                        if suspect_msg.identifier == phase_king and sender_id not in suspecting_servers:
                            suspecting_servers.append(sender_id)
                            f = math.ceil(N / 4) - 1

                            if (
                                len(suspecting_servers) > f
                                and self._group_view.identifier not in suspecting_servers
                            ):
                                response_suspect_msg = GroupViewSuspect.initFromData(
                                    suspect_msg.identifier, suspect_msg.topic
                                )
                                response_suspect_msg.encode()
                                self._multicast.send(response_suspect_msg)

                            if len(suspecting_servers) >= N - f:
                                if not self._group_view.check_if_server_is_inactive(phase_king):
                                    self.__debug(
                                        'PhaseKing ({}Phase {} - Round 2): Suspending King "{}"'.format(
                                            (self._topic + "; ") if self._topic != "" else "",
                                            phase,
                                            phase_king,
                                        )
                                    )
                                    self._group_view.suspend_server(phase_king)
                                    N -= 1
                                    break

            if timer is not None:
                timer.cancel()
        f = math.ceil(N / 4) - 1
        if majority_count >= N - f:
            return majority_value, phase_king
        return tiebreaker, phase_king

    def __debug(self, *msgs):
        if self.__verbose:
            print(*msgs)
