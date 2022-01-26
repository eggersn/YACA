import base64
from nacl.signing import VerifyKey

from src.protocol.base import Message


class JoinRequest(Message):
    def __init__(self):
        super().__init__()

        self.identifier: str
        self.pk: VerifyKey
        self.ip_addr: str
        self.port: int

    def encode(self):
        pk_string = base64.b64encode(self.pk.encode()).decode("ascii")
        self.content = {
            "identifier": self.identifier,
            "pk": pk_string,
            "ip_addr": self.ip_addr,
            "port": self.port,
        }
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.identifier = self.content["identifier"]
        self.pk = VerifyKey(base64.b64decode(self.content["pk"]))
        self.ip_addr = self.content["ip_addr"]
        self.port = self.content["port"]

    @classmethod
    def initFromData(cls, identifier: str, pk: VerifyKey, ip_addr: str, port: int):
        message = cls()
        message.header = "View: Join Request"
        message.identifier = identifier
        message.pk = pk
        message.ip_addr = ip_addr
        message.port = port

        return message


class JoinResponse(Message):
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
        message.header = "View: Join Response"
        message.response = response

        return message


class JoinMsg(Message):
    def __init__(self):
        super().__init__()

        self.request: str

    def encode(self):
        self.content = {"request": self.request}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.request = self.content["request"]

    @classmethod
    def initFromData(cls, request: str):
        message = cls()
        message.header = "View: Join Message"
        message.request = request

        return message


class JoinCommence(JoinResponse):
    @classmethod
    def initFromData(cls, response):
        message = cls()
        message.header = "View: Join Commence"
        message.response = response

        return message
