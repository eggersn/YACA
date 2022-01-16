import socket
import threading 

class BroadcastListener:
    def __init__(self, channel, port):
        self._request_channel = channel

        self.listener = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
        # Enable to run multiple clients and servers on a single (host,port)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # Enable broadcasting mode
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        self.listener.bind(("", port))

    def listen(self):
        while True:
            # wait for incoming data
            data, addr = self.listener.recvfrom(1024)
            data = data.decode()

            # write data to channel to be consumed by server
            self._request_channel.produce(data)

    def start(self):
        listening_thread = threading.Thread(target=self.listen)
        listening_thread.start()
