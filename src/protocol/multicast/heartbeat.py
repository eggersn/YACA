from src.protocol.base import Message

class HeartBeat(Message):
    acks: dict[str, int]

    def encode(self):
        self.content = {"acks": self.acks}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.acks = self.content["acks"]

    @classmethod
    def initFromData(cls, acks):
        message = cls()
        message.header = "HeartBeat"
        message.meta = {}
        message.acks = acks

        return message