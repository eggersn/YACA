from enum import Enum
from src.protocol.base import Message


class DiscoveryKind(Enum):
    SERVER = 0
    CLIENT = 1


class DiscoveryRequest(Message):
    identifier: str
    pk: str
    port: int
    kind: DiscoveryKind

    def encode(self):
        self.content = {"identifier": self.identifier, "pk": self.pk, "port": self.port}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.identifier = self.content["identifier"]
        self.pk = self.content["pk"]
        self.port = self.content["port"]
        self.kind = (
            DiscoveryKind.SERVER
            if self.header == "Discovery Request: Server"
            else DiscoveryKind.CLIENT
        )

    @classmethod
    def initFromData(
        cls, identifier: str, pk: str, port: int, discovery_kind: DiscoveryKind
    ):
        message = cls()
        message.header = (
            "Discovery Request: Server"
            if discovery_kind == DiscoveryKind.SERVER
            else "Discovery Request: Client"
        )
        message.identifier = identifier
        message.pk = pk
        message.port = port
        message.kind = discovery_kind

        return message
