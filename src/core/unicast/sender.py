import socket
import threading
import time
import random 
import string

from src.core.utils.configuration import Configuration
from src.core.unicast.tcp_sender import TCPUnicastSender
from src.protocol.base import Message


class UnicastSender:
    def __init__(self, configuration: Configuration):
        self._configuration = configuration
        self._storage = {}
        self._storage_semaphore = threading.Semaphore(0)

        self.upd_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def _ack_sender(self):
        while True:
            with self._storage_semaphore:
                ts = time.time_ns() / 10 ** 6
                remove_list = []
                for nonce in self._storage:
                    if ts - self._storage[nonce][2] > 5 * self._configuration.get_heartbeat_interval():
                        remove_list.append(nonce)
                    self.upd_sender.sendto(self._storage[nonce][0].encode(), self._storage[nonce][1])

                for nonce in remove_list:
                    del self._storage[nonce]

            time.sleep(self._configuration.get_heartbeat_interval())

    def _ack_listener(self):
        while True:
            data, _ = self.upd_sender.recvfrom(1024)

            msg = Message.initFromJSON(data)
            msg.decode()

            if msg.has_nonce:
                if msg.get_nonce() in self._storage:
                    self._storage_semaphore.acquire()
                    del self._storage[msg.get_nonce()]

    def start(self):
        ack_sender_thread = threading.Thread(target=self._ack_sender)
        ack_sender_thread.start()

        ack_listener_thread = threading.Thread(target=self._ack_listener)
        ack_listener_thread.start()

    def send_udp(self, msg: Message, addr: tuple[str, int], response_id: str):
        msg.encode()
        self.upd_sender.sendto(msg.json_data.encode(), addr)
        ts = time.time_ns() / 10 ** 6
        self._storage[response_id] = [msg.json_data, addr, ts]
        self._storage_semaphore.release()

    def send_udp_without_ack(self, msg: Message, addr: tuple[str, int]):
        msg.encode()
        self.upd_sender.sendto(msg.json_data.encode(), addr)

    def send_udp_sync(self, msg: Message, addr: tuple[str, int]):
        nonce = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        msg.set_nonce(nonce)
        msg.encode()

        self.upd_sender.sendto(msg.json_data.encode(), addr)
        self.upd_sender.settimeout(self._configuration.get_heartbeat_interval())

        k = 5
        while k > 0:
            try:
                data, _ = self.upd_sender.recvfrom(1024)
            except socket.timeout:
                self.upd_sender.sendto(msg.json_data.encode(), addr)
                k -= 1
            else:
                msg = Message.initFromJSON(data)
                msg.decode()

                if msg.has_nonce:
                    if msg.get_nonce() == nonce:
                        return True 
        return False


    def send_tcp(self, msg: Message, addr: tuple[str, int]):
        msg.encode()
        tcp_sender = TCPUnicastSender(addr)
        tcp_sender.send(msg.json_data.encode())
