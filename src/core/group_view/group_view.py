import json
import base64
import uuid
from nacl.signing import SigningKey, VerifyKey


class GroupView:
    servers: list[str]  # list of server identifiers
    ports: dict[str, int] # list of listening ports for unicast communication
    pks: dict[str, VerifyKey]  # list of public keys
    sk: SigningKey  # private key
    identifier: str
    manager: str

    def __init__(self, file):
        f = open(file)
        data = json.load(f)
        self.pks = {}

        self.servers = data["servers"]
        self.servers.sort()
        self.ports = data["ports"]
        self.manager = self.servers[-1]
        self.identifier = data["id"]
        for identifier in data["pks"]:
            pk_encoded = data["pks"][identifier]
            self.pks[identifier] = VerifyKey(base64.b64decode(pk_encoded))

        self.sk = SigningKey(base64.b64decode(data["sk"]))

    def check_if_participant(self, id : str):
        return id in self.servers

    def get_number_of_servers(self):
        return len(self.servers)

    def get_ith_server(self, i):
        return self.servers[i]






