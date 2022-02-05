from src.protocol.base import Message

class MessageQuery(Message):

    def __init__(self):
        super().__init__()
        self.nacks : dict[str, list[int]]

    def encode(self):
        self.content = {"nacks": self.nacks}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.nacks = self.content["nacks"]

    @classmethod
    def initFromData(cls, nacks):
        message = cls()
        message.header = "Query: Messages"
        message.meta = {}
        message.nacks = nacks.copy()

        return message