import random
import threading
from src.protocol.consensus.pk_message import PhaseKingMessage
from src.core.utils.configuration import Configuration
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.protocol.multicast.piggyback import PiggybackMessage
from src.protocol.base import Message
from src.core.multicast.reliable_multicast import ReliableMulticast
from src.protocol.multicast.nack import NegativeAcknowledgement
from src.protocol.multicast.to_proposal import TotalOrderProposal


class CausalOrderedReliableMulticast(ReliableMulticast):
    def __init__(
        self,
        multicast_addr: str,
        multicast_port: int,
        identifier: str,
        channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
        open: bool = False,
        malicious: bool = False,
    ):
        super().__init__(multicast_addr, multicast_port, identifier, channel, group_view, configuration, open)

        self._co_holdback_queue: list[tuple[dict[str, int], str]] = []
        self._co_lock = threading.Lock()
        self._CO_R_g: dict[str, int] = {}
        self.__malicious = malicious

    def _deliver(self, data, identifier, seqno):
        self._update_storage(data, identifier, seqno)
        self._co_consume(data, identifier, seqno)

    def _co_deliver(self, data, identifier, seqno):
        message = Message.initFromJSON(data)
        message.decode()
        self._channel.produce(data, message.get_topic())

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

    def send(self, message: Message, config=False, sign=True):
        # for testing purposes only
        if self.__malicious:
            self.send_malicious(message, config, sign)
            return

        if not message.is_decoded:
            message.decode()

        if not self._suspend_multicast or config:
            with self._R_g_lock:
                with self._co_lock:
                    pb_message = PiggybackMessage.initFromMessage(
                        message, self._identifier, self._S_p, self._CO_R_g
                    )
                    pb_message.encode()

                if sign:
                    pb_message.sign(self._signature)

                self._udp_sock.sendto(
                    pb_message.json_data.encode(), (self._multicast_addr, self._multicast_port)
                )
                response = self._deliver(pb_message.json_data, self._identifier, self._S_p)
                self._check_holdback_queue()

                self._S_p += 1

            if not self._response_channel.is_empty():
                response, config = self._response_channel.consume()
                response_msg = Message.initFromJSON(response)
                self.send(response_msg, config)
        else:
            if not message.is_encoded:
                message.encode()
            self._response_channel.produce((message.json_data, False))

    #############################################################################################################
    #                                         FOR TESTING PURPOSES ONLY                                         #
    #############################################################################################################

    def send_malicious(self, message: Message, config=False, sign=True):
        if not message.is_decoded:
            message.decode()

        if not self._suspend_multicast or config:
            with self._R_g_lock:
                with self._co_lock:
                    pb_message = PiggybackMessage.initFromMessage(
                        message, self._identifier, self._S_p, self._CO_R_g
                    )

                if pb_message.header == "Phase King: Message":
                    pk_message = PhaseKingMessage.initFromJSON(message.json_data)
                    pk_message.decode()
                    for server in self._group_view.servers:
                        if server != self._identifier and not self._group_view.check_if_server_is_inactive(
                            server
                        ):
                            malicious_value = pk_message.value
                            if type(pk_message.value) is int:  # MaxPhaseKing message
                                malicious_value = random.randint(0, 2*pk_message.value)
                            elif self.__coin_flip(self._configuration.get_byzantine_coin_flip_probability()):
                                malicious_value = "malicious_value"
                            print(
                                "BYZANTINE [PhaseKing]: Send {} value {} instead of {}".format(
                                    server, malicious_value, pk_message.value
                                )
                            )
                            malicious_pk_message = PhaseKingMessage.initFromData(
                                malicious_value, pk_message.phase, pk_message.round, pk_message.topic
                            )
                            malicious_pk_message.encode()
                            malicious_pb_message = PiggybackMessage.initFromMessage(
                                malicious_pk_message, self._identifier, pb_message.seqno, pb_message.acks
                            )
                            malicious_pb_message.encode()
                            if sign:
                                malicious_pb_message.sign(self._signature)
                            self._udp_sock.sendto(
                                malicious_pb_message.json_data.encode(),
                                self._group_view.get_unicast_addr_of_server(server),
                            )
                elif pb_message.header == "TO: Seqno Proposal":
                    to_proposal = TotalOrderProposal.initFromJSON(message.json_data)
                    to_proposal.decode()
                    for server in self._group_view.servers:
                        if server != self._identifier and not self._group_view.check_if_server_is_inactive(
                            server
                        ):
                            malicious_value = random.randint(0, to_proposal.seqno + 5)
                            print(
                                "BYZANTINE [TO-Proposal]: Send {} value {} instead of {}".format(
                                    server, malicious_value, to_proposal.seqno
                                )
                            )
                            malicious_to_proposal = TotalOrderProposal.initFromData(
                                malicious_value, to_proposal.msg_identifier
                            )
                            malicious_to_proposal.encode()
                            malicious_pb_message = PiggybackMessage.initFromMessage(
                                malicious_to_proposal, self._identifier, pb_message.seqno, pb_message.acks
                            )
                            malicious_pb_message.encode()
                            if sign:
                                malicious_pb_message.sign(self._signature)
                            self._udp_sock.sendto(
                                malicious_pb_message.json_data.encode(),
                                self._group_view.get_unicast_addr_of_server(server),
                            )

                pb_message.encode()
                if sign:
                    pb_message.sign(self._signature)
                else:
                    self._udp_sock.sendto(
                        pb_message.json_data.encode(), (self._multicast_addr, self._multicast_port)
                    )
                response = self._deliver(pb_message.json_data, self._identifier, self._S_p)
                self._check_holdback_queue()

                self._S_p += 1

            if not self._response_channel.is_empty():
                response, config = self._response_channel.consume()
                response_msg = Message.initFromJSON(response)
                self.send(response_msg, config)
        else:
            if not message.is_encoded:
                message.encode()
            self._response_channel.produce((message.json_data, False))

    def _receive_nack(self, data, addr):
        if self.__malicious:
            self._receive_nack_malicious(data, addr)
            return

        nack = NegativeAcknowledgement.initFromJSON(data)
        nack.decode()

        for identifier in nack.nacks:
            if identifier in self._storage:
                for seqno in nack.nacks[identifier]:
                    if seqno >= len(self._storage[identifier]):
                        break

                    nack_response = PiggybackMessage.initFromJSON(self._storage[identifier][seqno])
                    self._send_unicast(nack_response, addr)

    def _receive_nack_malicious(self, data, addr):
        nack = NegativeAcknowledgement.initFromJSON(data)
        nack.decode()

        for identifier in nack.nacks:
            if identifier in self._storage:
                for seqno in nack.nacks[identifier]:
                    if seqno >= len(self._storage[identifier]):
                        break

                    nack_response = PiggybackMessage.initFromJSON(self._storage[identifier][seqno])
                    nack_response.decode()

                    if nack_response.header == "Phase King: Message":
                        pk_message = PhaseKingMessage.initFromJSON(nack_response.json_data)
                        pk_message.decode()

                        malicious_value = pk_message.value
                        if type(pk_message.value) is int:  # MaxPhaseKing message
                            malicious_value = random.randint(0, 2*pk_message.value)
                        elif self.__coin_flip(0.5):
                            malicious_value = "malicious_value"
                        malicious_pk_message = PhaseKingMessage.initFromData(
                            malicious_value, pk_message.phase, pk_message.round, pk_message.topic
                        )
                        malicious_pk_message.encode()
                        malicious_pb_message = PiggybackMessage.initFromMessage(
                            malicious_pk_message, self._identifier, nack_response.seqno, nack_response.acks
                        )
                        malicious_pb_message.encode()
                        malicious_pb_message.sign(self._signature)
                        self._send_unicast(malicious_pb_message, addr)
                    elif nack_response.header == "TO: Seqno Proposal":
                        to_proposal = TotalOrderProposal.initFromJSON(nack_response.json_data)
                        to_proposal.decode()
                        malicious_value = random.randint(0, to_proposal.seqno+5)
                        malicious_to_proposal = TotalOrderProposal.initFromData(
                            malicious_value, to_proposal.msg_identifier
                        )
                        malicious_to_proposal.encode()
                        malicious_pb_message = PiggybackMessage.initFromMessage(
                            malicious_to_proposal, self._identifier, nack_response.seqno, nack_response.acks
                        )
                        malicious_pb_message.encode()
                        malicious_pb_message.sign(self._signature)
                        self._send_unicast(malicious_pb_message, addr)
                    else:
                        self._send_unicast(nack_response, addr)

    def __coin_flip(self, probability):
        value = random.random()
        return value < probability
