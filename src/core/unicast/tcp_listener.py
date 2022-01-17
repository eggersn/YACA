import socket
import struct

from src.core.utils.channel import Channel


class TCPUnicastListener:
    def __init__(self, channel: Channel):
        self._request_channel = channel

        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("", 0))  # bind to available port
        self.listener.listen()

    def get_port(self):
        return self.listener.getsockname()[1]

    def listen(self):
        while True:
            conn, addr = self.listener.accept()

            # receive full message
            data = self.recv_msg(conn)

            if data is not None:
                # write data to channel to be consumed by server
                self._request_channel.produce((data, conn, addr))

            self.listener.listen()

    def recv_msg(self, conn):
        # Read message length and unpack it into an integer
        raw_msglen = self.recvall(conn, 4)
        if not raw_msglen:
            return None
        msglen = struct.unpack(">I", raw_msglen)[0]
        # Read the message data
        message = self.recvall(conn, msglen)
        return str(message.decode())

    def recvall(self, conn, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = conn.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data