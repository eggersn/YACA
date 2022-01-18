import json
import base64
import uuid
from nacl.signing import SigningKey, VerifyKey


class GroupView:
    def __init__(self, file):
        f = open(file)
        data = json.load(f)
        self.pks = {}

        self.servers = data["servers"]
        self.servers.sort(key=lambda s: int(s.split("server")[1]))
        self.suspended_servers = []
        self.ports = data["ports"]
        self.manager = self.servers[-1]
        self.identifier = data["id"]
        for identifier in data["pks"]:
            pk_encoded = data["pks"][identifier]
            self.pks[identifier] = VerifyKey(base64.b64decode(pk_encoded))

        self.sk = SigningKey(base64.b64decode(data["sk"]))

    def check_if_participant(self, id: str):
        return id in self.servers

    def get_number_of_servers(self):
        return len(self.servers)

    def get_number_of_unsuspended_servers(self):
        return len(self.servers) - len(self.suspended_servers)

    def get_ith_server(self, i):
        return self.servers[i]

    def suspend_server(self, identifier):
        if identifier in self.servers and identifier not in self.suspended_servers:
            self.suspended_servers.append(identifier)

    def check_if_server_is_suspended(self, identifier):
        return identifier in self.suspended_servers

    def remove_server(self, identifier):
        print("View: Remove", identifier)
        self.servers.remove(identifier)
        del self.ports[identifier]
        del self.pks[identifier]
