import json

from src.protocol.base import Message

class NegativeAcknowledgement(Message):
    nacks : dict[str, list[int]]

    def encode(self):
        self.content = {"nacks": self.nacks}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.nacks = self.content["nacks"]

    @classmethod
    def initFromData(cls, nacks):
        message = cls()
        message.header = "NACK"
        message.meta = {}
        message.nacks = nacks

        return message