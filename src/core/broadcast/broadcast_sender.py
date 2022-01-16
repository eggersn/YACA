import socket
from src.core.messages.message import Message


class BroadcastSender:
    def __init__(self, port):
        self.broadcast_group = ("<broadcast>", port)
        self.sender = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        self.sender.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sender.bind(("", 0))

    def get_port(self):
        return self.sender.getsockname()[1]

    def send(self, message: Message):
        self.sender.sendto(str(message.json_data).encode(), self.broadcast_group)
