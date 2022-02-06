import time

from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.protocol.multicast.piggyback import PiggybackMessage
from src.protocol.base import Message


class ClientCausalOrderedReliableMulticast(CausalOrderedReliableMulticast):
    def _handle_acks_open(self, acks, nack_messages):
        # send nacks if detecting missing messages
        for ack in acks:
            if ack != self._identifier:
                if ack not in self._R_g:
                    self._R_g[ack] = -1
                    self._holdback_queue[ack] = {}
                    self._requested_messages[ack] = [0, -1]

                if (
                    self._requested_messages[ack][0] + 5 * self._configuration.get_heartbeat_interval()
                    < time.time_ns() / 10**9
                ):
                    self._requested_messages[ack][1] = self._R_g[ack]

                missing_messages = list(
                    set(range(self._requested_messages[ack][1] + 1, acks[ack] + 1))
                    - set(self._holdback_queue[ack].keys())
                )
                if len(missing_messages) != 0:
                    nack_messages[ack] = missing_messages

        if len(nack_messages) > 0:
            nack_count = sum([len(nack_messages[identifier]) for identifier in nack_messages])
            if nack_count > 100:
                for identifier in nack_messages:
                    l = len(nack_messages[identifier])
                    nack_messages[identifier] = nack_messages[identifier][: int(100 * l / nack_count)]

            for identifier in nack_messages:
                l = len(nack_messages[identifier])
                if l > 0:
                    self._requested_messages[identifier][0] = time.time_ns() / 10**9
                    self._requested_messages[identifier][1] = max(
                        self._requested_messages[identifier][1],
                        nack_messages[identifier][-1],
                    )
            return nack_messages

    def _receive_pb_message_open(self, data):
        pb_message = PiggybackMessage.initFromJSON(data)
        pb_message.decode()

        if pb_message.identifier == self._identifier:
            return

        nack_messages = {}

        if pb_message.identifier not in self._R_g:
            self._R_g[pb_message.identifier] = -1
            self._holdback_queue[pb_message.identifier] = {}
            self._requested_messages[pb_message.identifier] = [0, -1]

        if pb_message.seqno == self._R_g[pb_message.identifier] + 1:
            # message can be delivered instantly
            self._deliver(data, pb_message.identifier, pb_message.seqno)
            self._check_holdback_queue()

        elif pb_message.seqno > self._R_g[pb_message.identifier] + 1:
            # there are missing messages => store message in holdback queue
            with self._holdback_queue_lock:
                self._holdback_queue[pb_message.identifier][pb_message.seqno] = data

                if (
                    self._requested_messages[pb_message.identifier][0]
                    + 5 * self._configuration.get_heartbeat_interval()
                    < time.time_ns() / 10**9
                ):
                    self._requested_messages[pb_message.identifier][1] = self._R_g[pb_message.identifier]

                # send nacks
                missing_messages = list(
                    set(
                        range(
                            self._requested_messages[pb_message.identifier][1] + 1,
                            pb_message.seqno,
                        )
                    )
                    - set(self._holdback_queue[pb_message.identifier].keys())
                )

                if len(missing_messages) != 0:
                    nack_messages[pb_message.identifier] = missing_messages

    def _update_storage(self, data, identifier, seqno):
        # update storage and acks
        self._R_g[identifier] = seqno
        if identifier != self._identifier:
            self._requested_messages[identifier][0] = time.time_ns() / 10**9
            self._requested_messages[identifier][1] = max(seqno, self._requested_messages[identifier][1])
