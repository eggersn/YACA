from src.protocol.base import Message


class PhaseKingMessage(Message):
    def __init__(self):
        super().__init__()
        self.value
        self.phase: int
        self.round: int
        self.topic: str

    def encode(self):
        self.content = {"value": self.value, "phase": self.phase, "round": self.round}
        if self.topic != "":
            self.meta["topic"] = self.topic
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.value = self.content["value"]
        self.phase = self.content["phase"]
        self.round = self.content["round"]
        self.topic = self.get_topic()

    @classmethod
    def initFromData(cls, value, phase: int, round: int, topic: str = ""):
        message = cls()
        message.header = "Phase King: Message"
        message.value = value
        message.phase = phase
        message.round = round
        message.topic = topic

        return message
