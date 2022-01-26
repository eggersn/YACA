import json
import os
import uuid
from nacl.signing import SigningKey

class Configuration:
    def __init__(self, file_location="config/server.json"):
        f = open(file_location)
        self.data = json.load(f)

    def get_multicast_addr(self):
        return self.data["multicast"]["addr"]

    def get_client_multicast_port(self):
        return self.data["multicast"]["client_port"]

    def get_db_multicast_port(self):
        return self.data["multicast"]["database_port"]

    def get_announcement_multicast_port(self):
        return self.data["multicast"]["announcement_port"]

    def get_consensus_multicast_port(self):
        return self.data["multicast"]["consensus_port"]

    def get_broadcast_port(self):
        return self.data["discovery"]["broadcast_port"]

    def get_discovery_manager_timeout(self):
        return self.data["discovery"]["manager_timeout"]

    def get_discovery_total_timeout(self):
        return self.data["discovery"]["total_timeout"]

    def get_group_view_file(self, i):
        files = os.listdir("config/" + self.data["initial"]["path"] + "/")
        files.remove("global.json")
        files.sort()
        return "config/" + self.data["initial"]["path"] + "/" + files[i]

    def get_global_group_view_file(self):
        return "config/" + self.data["initial"]["path"] + "/global.json"

    def get_heartbeat_interval(self):
        return self.data["heartbeat"]["interval"]

    def get_timeout(self):
        return self.data["crash_fault_detection"]["timeout"]

    def generate_uuid(self):
        return str(uuid.uuid4())

    def generate_key(self):
        return SigningKey.generate()

