from src.protocol.base import Message

class GroupViewJoin(Message):
    identifier : str 

    def encode(self):
        self.content = {"identifier": self.identifier}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.identifier = self.content["identifier"]

    @classmethod
    def initFromData(cls, identifier : str):
        message = cls()
        message.header = "View: Leave"
        message.identifier = identifier

        return message