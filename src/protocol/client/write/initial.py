import base64
from nacl.signing import VerifyKey

from src.protocol.base import Message


class InitMessage(Message):
    def __init__(self):
        super().__init__()

        self.identifier: str
        self.pk: VerifyKey

    def encode(self):
        pk_string = base64.b64encode(self.pk.encode()).decode("ascii")
        self.content = {
            "identifier": self.identifier,
            "pk": pk_string,
        }
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.identifier = self.content["identifier"]
        self.pk = VerifyKey(base64.b64decode(self.content["pk"]))

    @classmethod
    def initFromData(cls, identifier: str, pk: VerifyKey):
        message = cls()
        message.header = "Write: Initial"
        message.identifier = identifier
        message.pk = pk

        return message

class InitResponse(Message):
    def __init__(self):
        super().__init__()

        self.response: str

    def encode(self):
        self.content = {"response": self.response}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.response = self.content["response"]

    @classmethod
    def initFromData(cls, response):
        message = cls()
        message.header = "Write Response: Initial"
        message.response = response

        return message