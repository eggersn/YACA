from src.protocol.base import Message


class JoinRequest(Message):
    identifier: str
    pk: str
    port: int

    def encode(self):
        self.content = {"identifier": self.identifier, "pk": self.pk, "port": self.port}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.identifier = self.content["identifier"]
        self.pk = self.content["pk"]
        self.port = self.content["port"]

    @classmethod
    def initFromData(cls, identifier: str, pk: str, port: int):
        message = cls()
        message.header = "View: Join"
        message.identifier = identifier
        message.pk = pk
        message.port = port

        return message
