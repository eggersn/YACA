import socket
import threading

from src.core.utils.channel import Channel
from src.protocol.base import Message

class UDPUnicastListener:
    def __init__(
        self, channel: Channel, listener: socket.socket = None, listening_port: int = 0
    ):
        self._request_channel = channel
        if not listener:
            self._listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._listener.bind(("", listening_port))  # bind to available port
        else:
            self._listener = listener

    def get_port(self):
        return self._listener.getsockname()[1]

    def start(self):
        listening_thread = threading.Thread(target=self._listen)
        listening_thread.start()

    def _listen(self):
        while True:
            data, addr = self._listener.recvfrom(1024)
            data = data.decode()

            # handle acks 
            msg = Message.initFromJSON(data)
            try:
                msg.decode()
            except Exception as err:
                print("ERROR", data)
                print("ERROR", err)
            else:
                msg.set_sender(addr)
                msg.encode()

                if "Ping" not in msg.header:
                    # write data to channel to be consumed by db_server
                    self._request_channel.produce(msg.json_data)


                if msg.has_nonce and msg.header != "ACK":
                    response = Message.initFromData("ACK", meta={"nonce": msg.get_nonce()})
                    response.encode()
                    self._listener.sendto(response.json_data.encode(), addr)


