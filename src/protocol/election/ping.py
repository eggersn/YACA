from src.protocol.base import Message


class ElectionPingMessage(Message):
    def __init__(self):
        super().__init__()
        self.content = {}

    @classmethod
    def initFromData(cls):
        message = cls()
        message.header = "Election: Ping"

        return message
