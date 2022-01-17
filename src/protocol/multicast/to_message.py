from src.protocol.base import Message


class TotalOrderMessage(Message):

    def __init__(self):
        super().__init__()
        self.msg_identifier : str

    def encode(self):
        self.meta["MsgIdentifier"] = {"id": self.msg_identifier}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.msg_identifier = self.meta["MsgIdentifier"]["id"]

    @classmethod
    def initFromData(cls, header, content, msg_identifier):
        message = cls()
        message.header = header 
        message.content = content 
        message.msg_identifier = msg_identifier

        return message

    @classmethod 
    def initFromMessage(cls, msg : Message, msg_identifier):
        message = cls()
        message.header = msg.header
        message.content = msg.content
        message.meta = msg.meta
        message.msg_identifier = msg_identifier

        return message