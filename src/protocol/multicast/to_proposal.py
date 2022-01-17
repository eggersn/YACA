from src.protocol.base import Message


class TotalOrderProposal(Message):

    def __init__(self):
        super().__init__()
        self.msg_identifier : str
        self.seqno : int

    def encode(self):
        self.content = {"seqno": self.seqno, "msg_identifier": self.msg_identifier}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.seqno = self.content["seqno"]
        self.msg_identifier = self.content["msg_identifier"]

    @classmethod
    def initFromData(cls, seqno, msg_identifier):
        message = cls()
        message.header = "TO: Seqno Proposal"
        message.seqno = seqno
        message.msg_identifier = msg_identifier
        message.meta = {}

        return message