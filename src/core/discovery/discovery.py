import socket

from src.core.utils.configuration import Configuration
from src.protocol.group_view.join import JoinRequest


class Discovery:
    def __init__(self, configuration: Configuration):
        self._configuration = configuration

        self.broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.settimeout(0.2)

    def discover(self, identifier, pk, listening_port):
        join_request = JoinRequest.initFromData(identifier, pk, listening_port)
        join_request.encode()

        discovered_manager = False 
