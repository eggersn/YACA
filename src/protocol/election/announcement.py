from src.protocol.base import Message


class ElectionAnnouncement(Message):
    def __init__(self):
        super().__init__()
        self.old_manager : str 

    def encode(self):
        self.content = {"old_manager": self.old_manager}
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.old_manager = self.content["old_manager"]

    @classmethod
    def initFromData(cls, old_manager:str):
        message = cls()
        message.header = "Election: Announcement"
        message.old_manager = old_manager

        return message