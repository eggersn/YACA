import base64
from nacl.signing import VerifyKey
from src.protocol.base import Message

class ServerDiscoveryResponse(Message):
    servers: list[str]  # list of server identifiers
    ports: dict[str, int] # list of listening ports for unicast communication
    pks: dict[str, VerifyKey]  # list of public keys
    seqno: int
    manager: str

    def encode(self):
        pks_str = {}
        for identifier in self.pks:
            pks_str[identifier] = base64.b64encode(self.pks[identifier]).decode("ascii")

        self.content = {"servers": self.servers, "ports": self.ports, "pks": pks_str, "mgr": self.manager}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        

    @classmethod
    def initFromData(cls, servers, ports, pks, manager, seqno):
        message = cls()
        message.header = "Discovery Response"
        message.servers = servers 
        message.ports = ports 
        message.pks = pks 
        message.manager = manager
        message.seqno = seqno

        return message
