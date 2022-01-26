import socket
import threading

from src.core.utils.channel import Channel
from src.core.signatures.signatures import Signatures
from src.protocol.base import Message
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.protocol.group_view.join import JoinRequest, JoinResponse


class ServerDiscovery:
    def __init__(self, configuration: Configuration):
        self._configuration = configuration

        self._broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def discover(self, group_view: GroupView, signature: Signatures, unicast_channel: Channel):
        def timeout_handler():
            unicast_channel.produce(None)

        join_request = JoinRequest.initFromData(
            group_view.identifier,
            group_view.get_my_pk(),
            group_view.get_my_ip_addr(),
            group_view.get_my_port(),
        )
        join_request.encode()
        join_request.sign(signature)

        while True:
            # discovery manager
            discovered_manager = False
            self._broadcast_socket.settimeout(self._configuration.get_discovery_manager_timeout())
            while not discovered_manager:
                self._broadcast_socket.sendto(
                    join_request.json_data.encode(), ("<broadcast>", self._configuration.get_broadcast_port())
                )

                try:
                    data, addr = self._broadcast_socket.recvfrom(1024)
                except:
                    pass
                else:
                    response = Message.initFromJSON(data)
                    response.decode()

                    if response.header == "View: Join Response":
                        response = JoinResponse.initFromJSON(data)
                        response.decode()

                        if response.response == "processing":
                            discovered_manager = True
                        else:
                            raise RuntimeError("server id is already assigned.")

            # wait till receiving at least one waiting message
            timer = threading.Timer(self._configuration.get_discovery_total_timeout(), timeout_handler)
            timer.start()

            while True:
                data = unicast_channel.consume()
                if data == None:
                    break 

                msg = Message.initFromJSON(data)
                msg.decode()

                if msg.header == "View: Join Response":
                    msg = JoinResponse.initFromJSON(data)
                    msg.decode()

                    if msg.response == "waiting":
                        timer.cancel()
                        return
