from src.protocol.base import Message


class PiggybackMessage(Message):
    identifier : str 
    seqno : int
    acks : dict[str, int]

    def encode(self):
        self.meta["SeqVector"] = {"identifier": self.identifier, "seqno": self.seqno, "acks": self.acks}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.seqno = self.meta["SeqVector"]["seqno"]
        self.identifier = self.meta["SeqVector"]["identifier"]
        self.acks = self.meta["SeqVector"]["acks"]

    @classmethod
    def initFromData(cls, header, content, identifier, seqno, acks):
        message = cls()
        message.header = header 
        message.content = content 
        message.identifier = identifier
        message.seqno = seqno
        message.acks = acks.copy()

        return message

    @classmethod 
    def initFromMessage(cls, msg : Message, identifier, seqno, acks):
        message = cls()
        message.header = msg.header
        message.content = msg.content
        message.meta = msg.meta
        message.identifier = identifier
        message.seqno = seqno
        message.acks = acks.copy()

        return message