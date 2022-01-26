from src.protocol.base import Message
from enum import Enum 


class QueryType(Enum):
    UDPQuery = "UDPQuery"
    TCPQuery = "TCPQuery"

class Query(Message):
    def __init__(self):
        super().__init__()
        self.type : QueryType
        self.nonce : str
        self.content = {}

    def encode(self):
        self.meta["type"] = self.type.value
        self.meta["nonce"] = self.nonce 
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.type = QueryType[self.meta["type"]]
        self.nonce = self.meta["nonce"]

class QueryResponse(Message):
    def __init__(self):
        super().__init__()
        self.nonce : str

    def encode(self):
        self.meta["nonce"] = self.nonce 
        Message.encode(self)

    def decode(self):
        Message.decode(self)
        self.nonce = self.meta["nonce"]
