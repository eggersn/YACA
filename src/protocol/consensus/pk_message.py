from src.protocol.base import Message


class PhaseKingMessage(Message):
    def __init__(self):
        super().__init__()
        self.value : str 
        self.phase : int 
        self.round : int 

    def encode(self):
        self.content = {"value": self.value, "phase": self.phase, "round": self.round}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.value = self.content["value"]
        self.phase = self.content["phase"]
        self.round = self.content["round"]

    @classmethod
    def initFromData(cls, value : str, phase : int, round : int):
        message = cls()
        message.header = "Phase King: Message"
        message.value = value 
        message.phase = phase 
        message.round = round

        return message
