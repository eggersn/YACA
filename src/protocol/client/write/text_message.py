from src.protocol.base import Message

class TextMessage(Message):
    def __init__(self):
        super().__init__()
        self.text : str

    def encode(self):
        self.content = {"text": self.text}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.text = self.content["text"]

    @classmethod
    def initFromData(cls, text):
        message = cls()
        message.header = "Write: TextMessage"
        message.text = text

        return message