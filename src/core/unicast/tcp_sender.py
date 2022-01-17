import socket
import struct

class TCPUnicastSender:
    def __init__(self, addr, timeout = 15):
        self.sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sender.connect(addr)
        self.sender.settimeout(timeout)
    
    def __del__(self):
        self.sender.close()

    def send(self, msg):
        msg = struct.pack(">I", len(msg)) + msg
        self.sender.sendall(msg)