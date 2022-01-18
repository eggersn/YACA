from src.protocol.base import Message


class GroupViewSuspect(Message):
    def __init__(self):
        super().__init__()
        self.identifier: str
        self.topic: str

    def encode(self):
        self.content = {"identifier": self.identifier}
        if self.topic != "":
            self.meta["topic"] = self.topic
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.identifier = self.content["identifier"]
        self.topic = self.get_topic()

    @classmethod
    def initFromData(cls, identifier: str, topic: str = ""):
        message = cls()

        message.header = "View: Suspect"
        message.identifier = identifier
        message.topic = topic

        return message
