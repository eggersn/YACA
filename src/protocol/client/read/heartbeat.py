from src.protocol.client.read.query import *


class HeartbeatQuery(Query):
    @classmethod
    def initFromData(cls, nonce):
        message = cls()
        message.header = "Query: Heartbeat"
        message.nonce = nonce

        return message


class HearbeatQueryResponse(QueryResponse):
    def __init__(self):
        super().__init__()
        self.acks: dict[str, int]
        self.listening_port: int

    def encode(self):
        self.content = {"acks": self.acks, "listening_port": self.listening_port}
        QueryResponse.encode(self)

    def decode(self):
        QueryResponse.decode(self)
        self.acks = self.content["acks"]
        self.listening_port = self.content["listening_port"]

    @classmethod
    def initFromData(cls, acks, listening_port, nonce):
        message = cls()
        message.header = "Query Response: HeartBeat"
        message.acks = acks.copy()
        message.listening_port = listening_port
        message.nonce = nonce

        return message
