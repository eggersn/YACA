from src.protocol.base import Message


class TotalOrderCommit(Message):

    def __init__(self):
        super().__init__()
        self.msg_identifier : str
        self.identifier : str
        self.seqno : int

    def encode(self):
        self.content = {"seqno": self.seqno, "identifier": self.identifier, "msg_identifier": self.msg_identifier}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.seqno = self.content["seqno"]
        self.identifier = self.content["identifier"]
        self.msg_identifier = self.content["msg_identifier"]

    @classmethod
    def initFromData(cls, identifier, seqno, msg_identifier):
        message = cls()
        message.header = "TO: Seqno Commit"
        message.identifier = identifier
        message.seqno = seqno
        message.msg_identifier = msg_identifier

        return message