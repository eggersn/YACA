import socket

from src.core.unicast.tcp_sender import TCPUnicastSender
from src.protocol.base import Message

class UnicastSender:
    def __init__(self):
        self.upd_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send_udp(self, msg : Message, addr : tuple[str, int]):
        msg.encode()
        self.upd_sender.sendto(msg.json_data.encode(), addr)

    def send_tcp(self, msg : Message, addr : tuple[str, int]):
        msg.encode()
        tcp_sender = TCPUnicastSender(addr)
        tcp_sender.send(msg.json_data.encode())