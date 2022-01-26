from src.protocol.base import Message

class HaltMessage(Message):

    def __init__(self):
        super().__init__()
        self.wait_until : str

    def encode(self):
        self.content = {"wait_until": self.wait_until }
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.wait_until = self.content["wait_until"]

    @classmethod
    def initFromData(cls, wait_until):
        message = cls()
        message.header = "Halt Message"
        message.meta = {}
        message.wait_until = wait_until

        return message