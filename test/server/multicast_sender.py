import socket

from src.protocol.base import Message


class MulticastSender:
    def __init__(self, multicast_addr: str, multicast_port: int):
        self._multicast_addr = multicast_addr
        self.multicast_port = multicast_port

        self.sender = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        self.multicast_group = (multicast_addr, multicast_port)

        # Enable broadcasting mode
        self.sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def get_port(self):
        return self.sender.getsockname()[1]

    def send(self, message: Message):
        self.sender.sendto(str(message.json_data).encode(), self.multicast_group)
