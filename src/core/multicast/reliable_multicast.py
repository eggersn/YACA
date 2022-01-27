import socket
import struct
import threading
import time
import select

from src.core.utils.configuration import Configuration
from src.core.signatures.signatures import Signatures
from src.core.group_view.group_view import GroupView
from src.core.utils.channel import Channel
from src.protocol.multicast.piggyback import PiggybackMessage
from src.protocol.multicast.heartbeat import HeartBeat
from src.protocol.multicast.nack import NegativeAcknowledgement
from src.protocol.base import Message


class ReliableMulticast:
    def __init__(
        self,
        multicast_addr: str,
        multicast_port: int,
        identifier: str,
        channel: Channel,
        group_view: GroupView,
        configuration: Configuration,
        open: bool = False,
    ):
        self._S_p = 0  # local sender sequence number
        self._R_g: dict[str, int] = {identifier: -1}  # delivered sequence numbers
        self._max_R_g: dict[str, int] = {}  # max delivered sequence number registered by heartbeat

        self._holdback_queue: dict[str, dict[int, str]] = {identifier: {}}
        self._storage: dict[str, list[str]] = {}
        self._requested_messages: dict[str, tuple[int, int]] = {}

        self._holdback_queue_lock = threading.Lock()
        self._R_g_lock = threading.Lock()

        self._response_channel = Channel()

        self._multicast_addr = multicast_addr
        self._multicast_port = multicast_port
        self._identifier = identifier
        self._channel = channel
        self._group_view = group_view
        self._configuration = configuration
        self._open = open

        if self._group_view is not None:
            self._signature = Signatures(group_view.sk, self._group_view.identifier)

        self._setup_multicast_listener()
        self._setup_udp_sock()

        self._storage = {identifier: []}

        self._timeoffset = time.time_ns() / 10 ** 9

        self._suspend_multicast = False
        self._contraint_multicast = False

        self.terminate = False
        self.listening_thread = None
        self.heartbeat_thread = None

    def _setup_multicast_listener(self):
        # create listener socket
        self._multicast_listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        # Enable to run multiple clients and servers on a single (host,port)
        self._multicast_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        # Enable broadcasting mode
        self._multicast_listener.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        self._multicast_listener.bind(("", self._multicast_port))

        # Tell the operating system to add the socket to the multicast group
        # on all interfaces.
        group = socket.inet_aton(self._multicast_addr)
        mreq = struct.pack("4sL", group, socket.INADDR_ANY)
        self._multicast_listener.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def _setup_udp_sock(self):
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_sock.bind(("", 0))

    def send(self, message: Message, config=False):
        if not message.is_decoded:
            message.decode()

        if not self._suspend_multicast or config:
            self._R_g_lock.acquire()
            pb_message = PiggybackMessage.initFromMessage(message, self._identifier, self._S_p, self._R_g)
            pb_message.encode()

            if not self._open:
                pb_message.sign(self._signature)

            self._udp_sock.sendto(pb_message.json_data.encode(), (self._multicast_addr, self._multicast_port))
            response = self._deliver(pb_message.json_data, self._identifier, self._S_p)
            self._check_holdback_queue()

            self._S_p += 1
            self._R_g_lock.release()

            if not self._response_channel.is_empty():
                response, config = self._response_channel.consume()
                response_msg = Message.initFromJSON(response)
                self.send(response_msg, config)
        else:
            if not message.is_encoded:
                message.encode()
            self._response_channel.produce((message.json_data, False))


    def _send_unicast(self, message: Message, addr: tuple[str, int]):
        if not message.is_encoded:
            message.encode()

        self._udp_sock.sendto(message.json_data.encode(), addr)

    def _send_nack(self, messages, addr):
        nack = NegativeAcknowledgement.initFromData(messages)
        nack.encode()
        nack.sign(self._signature)
        self._send_unicast(nack, addr)

    def start(self, listen=True, trash=False):
        if trash:
            self._response_channel.set_trash_flag(True)

        self.terminate = False
        if self.listening_thread is None or not self.listening_thread.is_alive():
            if listen and not self._open:
                self.listening_thread = threading.Thread(target=self._listen)
            elif listen and self._open:
                self.listening_thread = threading.Thread(target=self._listen_open)
            else:
                self.listening_thread = threading.Thread(target=self._listen_for_nacks)
            self.listening_thread.start()

        self.heartbeat_thread = threading.Thread(target=self._heartbeat)
        self.heartbeat_thread.start()

    def stop(self):
        self.terminate = True

        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()
            self.heartbeat_thread = None
        if self.listening_thread is not None:
            self.listening_thread.join()
            self.listening_thread = None

    def disable_responses(self):
        self._response_channel.set_trash_flag(True)
    
    def enable_responses(self):
        self._response_channel.set_trash_flag(False)

    def _heartbeat(self):
        interval = self._configuration.get_heartbeat_interval()
        while not self.terminate:
            time.sleep(interval)
            heartbeat = HeartBeat.initFromData(self._R_g)
            heartbeat.encode()
            if not self._open:
                heartbeat.sign(self._signature)

            self._udp_sock.sendto(
                heartbeat.json_data.encode(),
                (self._multicast_addr, self._multicast_port),
            )

    def _listen(self):
        while not self.terminate:
            ready_socks, _, _ = select.select([self._udp_sock, self._multicast_listener], [], [])
            for sock in ready_socks:
                data, addr = sock.recvfrom(1024)
                data = data.decode()

                msg = Message.initFromJSON(data)
                msg.decode()
                sender_id, _ = msg.get_signature()

                if msg.header == "HeartBeat":
                    self._receive_heartbeat(data, addr)
                else:
                    if msg.verify_signature(self._signature, self._group_view):
                        if msg.header == "NACK":
                            if not self._group_view.check_if_server_is_suspended(sender_id):
                                self._receive_nack(data, addr)
                        else:
                            if sock == self._udp_sock or not self._group_view.check_if_server_is_suspended(
                                sender_id
                            ):
                                self._receive_pb_message(data, addr)

    def _listen_open(self):
        while not self.terminate:
            ready_socks, _, _ = select.select([self._udp_sock, self._multicast_listener], [], [])
            for sock in ready_socks:
                data, addr = sock.recvfrom(1024)
                data = data.decode()

                msg = Message.initFromJSON(data)
                msg.decode()

                if msg.header == "HeartBeat":
                    self._receive_heartbeat(data, addr)
                elif msg.header == "NACK":
                    if msg.verify_signature(self._signature, self._group_view):
                        sender_id, _ = msg.get_signature()
                        if not self._group_view.check_if_server_is_inactive(sender_id):
                            self._receive_nack(data, addr)
                else:
                    msg.set_sender(addr)
                    msg.encode()
                    data = msg.json_data

                    self._receive_pb_message(data, addr)

    def _listen_for_nacks(self):
        while not self.terminate:
            ready_socks, _, _ = select.select([self._udp_sock], [], [])
            for sock in ready_socks:
                data, addr = sock.recvfrom(1024)
                data = data.decode()

                msg = Message.initFromJSON(data)
                msg.decode()

                if msg.header == "NACK":
                    self._receive_nack(data, addr)

    def _receive_heartbeat(self, data, addr):
        heartbeat = HeartBeat.initFromJSON(data)
        heartbeat.decode()
        with self._R_g_lock:
            self._handle_acks(heartbeat.acks, addr, {})

    def _receive_nack(self, data, addr):
        nack = NegativeAcknowledgement.initFromJSON(data)
        nack.decode()

        for identifier in nack.nacks:
            if identifier in self._storage:
                for seqno in nack.nacks[identifier]:
                    if seqno >= len(self._storage[identifier]):
                        break

                    nack_response = PiggybackMessage.initFromJSON(self._storage[identifier][seqno])
                    self._send_unicast(nack_response, addr)

    def _receive_pb_message(self, data, addr):
        pb_message = PiggybackMessage.initFromJSON(data)
        pb_message.decode()

        if pb_message.identifier == self._identifier:
            return

        # only accept old messages of suspended servers
        if (
            self._group_view.check_if_server_is_suspended(pb_message.identifier)
            and pb_message.seqno > self._max_R_g[pb_message.identifier]
        ):
            return

        nack_messages = {}
        check_responses = False

        self._R_g_lock.acquire()
        if pb_message.identifier not in self._storage:
            self._storage[pb_message.identifier] = []
            self._R_g[pb_message.identifier] = -1
            self._holdback_queue[pb_message.identifier] = {}
            self._requested_messages[pb_message.identifier] = [0, -1]

        if pb_message.seqno == self._R_g[pb_message.identifier] + 1:
            # message can be delivered instantly
            self._deliver(data, pb_message.identifier, pb_message.seqno)
            self._check_holdback_queue()
            check_responses = True

        elif pb_message.seqno > self._R_g[pb_message.identifier] + 1:
            # there are missing messages => store message in holdback queue
            with self._holdback_queue_lock:
                self._holdback_queue[pb_message.identifier][pb_message.seqno] = data

                if (
                    self._requested_messages[pb_message.identifier][0]
                    + self._configuration.get_heartbeat_interval()
                    < time.time_ns() / 10 ** 6
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

        self._handle_acks(pb_message.acks, addr, nack_messages)

        self._R_g_lock.release()

        if check_responses:
            if not self._response_channel.is_empty():
                response, config = self._response_channel.consume()
                response_msg = Message.initFromJSON(response)
                self.send(response_msg, config)

    def _handle_acks(self, acks, addr, nack_messages):
        # send nacks if detecting missing messages
        for ack in acks:
            if ack in self._group_view.servers:
                if ack not in self._max_R_g or (acks[ack] > self._max_R_g[ack]):
                    self._max_R_g[ack] = acks[ack]

                if ack != self._identifier:
                    if ack not in self._storage:
                        self._storage[ack] = []
                        self._R_g[ack] = -1
                        self._holdback_queue[ack] = {}
                        self._requested_messages[ack] = [0, -1]

                    if (
                        self._requested_messages[ack][0] + self._configuration.get_heartbeat_interval()
                        < time.time_ns() / 10 ** 6
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

                    l = len(nack_messages[identifier])
                    if l > 0:
                        self._requested_messages[identifier][1] = max(
                            self._requested_messages[identifier][1],
                            nack_messages[identifier][-1],
                        )
            self._send_nack(nack_messages, addr)

    def _deliver(self, data, identifier, seqno):
        if self._channel is not None:
            self._channel.produce(data)
        self._update_storage(data, identifier, seqno)

    def _update_storage(self, data, identifier, seqno):
        # update storage and acks
        self._storage[identifier].append(data)
        self._R_g[identifier] = seqno
        if identifier != self._identifier:
            self._requested_messages[identifier][0] = time.time_ns() / 10 ** 6
            self._requested_messages[identifier][1] = max(seqno, self._requested_messages[identifier][1])

    def _check_holdback_queue(self):
        change = True
        while change:
            change = False
            for identifier in self._holdback_queue:
                next_seqno = self._R_g[identifier] + 1
                while next_seqno in self._holdback_queue[identifier]:
                    self._deliver(
                        self._holdback_queue[identifier][next_seqno],
                        identifier,
                        next_seqno,
                    )
                    next_seqno += 1
                    change = True

        # remove stale elements of the holdback queue
        with self._holdback_queue_lock:
            for identifier in self._holdback_queue:
                stale_messages = [
                    stale_seqno
                    for stale_seqno in self._holdback_queue[identifier]
                    if stale_seqno <= self._R_g[identifier]
                ]
                for stale_message in stale_messages:
                    del self._holdback_queue[identifier][stale_message]

    def halt_sending(self):
        self._suspend_multicast = True

    def continue_sending(self):
        self._suspend_multicast = False

