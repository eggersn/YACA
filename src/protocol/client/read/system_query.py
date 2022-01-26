from src.protocol.client.read.query import *


class QueryNumberOfActiveServers(Query):
    @classmethod
    def initFromData(cls, nonce : str):
        message = cls()
        message.header = "Query: NumberOfActiveServers"
        message.type = QueryType.UDPQuery
        message.nonce = nonce

        return message

class QueryResponseNumberOfActiveServers(QueryResponse):
    def __init__(self):
        super().__init__()
        self.N : int

    def encode(self):
        self.content = {"N": self.N}
        QueryResponse.encode(self)

    def decode(self):
        QueryResponse.decode(self)
        self.N = self.content["N"]

    @classmethod
    def initFromData(cls, N : int, nonce : str):
        message = cls()
        message.header = "Query Response: NumberOfActiveServers"
        message.N = N 
        message.nonce = nonce

        return message

