import json
import base64
import random
import string
import threading
from nacl.signing import SigningKey, VerifyKey
import os

from src.core.utils.ip_addr import get_host_ip


class GroupView:
    def __init__(self, verbose=False):
        self.__verbose = verbose
        
        self.pks = {}
        self.ip_addrs = {}
        self.ports = {}

        self.servers = []
        self.suspended_servers = []
        self.joining_servers = []

        self.identifier: str
        self.manager: str
        self.sk: SigningKey

        self._ready_to_join_semaphore = threading.Semaphore(0)
        self._added_semaphore = threading.Semaphore(0)

    def check_if_participant(self, id: str):
        return id in self.servers

    def get_number_of_servers(self):
        return len(self.servers)

    def get_number_of_unsuspended_servers(self):
        return len(self.servers) - len(self.suspended_servers) - len(self.joining_servers)

    def get_ith_server(self, i):
        return self.servers[i]

    def get_next_active_after_ith_server(self, i):
        k = 0
        server = self.servers[i+k]
        while self.check_if_server_is_inactive(server):
            server = self.servers[i+k]
        return server

    def suspend_server(self, identifier):
        self.__debug("GroupView: Suspend", identifier)
        if identifier in self.servers and identifier not in self.suspended_servers:
            self.suspended_servers.append(identifier)
            if identifier == self.identifier:
                os.system('kill %d' % os.getpid())

    def check_if_server_is_inactive(self, identifier):
        return identifier in self.suspended_servers or identifier in self.joining_servers

    def check_if_server_is_suspended(self, identifier):
        return identifier in self.suspended_servers

    def remove_server(self, identifier):
        self.__debug("GroupView: Remove", identifier)
        self.servers.remove(identifier)
        del self.ports[identifier]
        del self.pks[identifier]

    def get_my_pk(self):
        return self.pks[self.identifier]

    def get_my_ip_addr(self):
        return self.ip_addrs[self.identifier]

    def get_my_port(self):
        return self.ports[self.identifier]

    def check_if_manager(self):
        return self.manager == self.identifier

    def add_server(self, identifier, pk, ip_addr, port):
        self.__debug("GroupView: Joining", identifier)
        if identifier not in self.servers:
            self.servers.append(identifier)
            self.joining_servers.append(identifier)
            self.pks[identifier] = pk 
            self.ip_addrs[identifier] = ip_addr 
            self.ports[identifier] = port

    def mark_server_as_joined(self, identifier):
        self.__debug("GroupView: Finished Joining", identifier)
        if identifier in self.joining_servers:
            self.joining_servers.remove(identifier)

    def wait_till_ready_to_join(self):
        self._ready_to_join_semaphore.acquire()

    def flag_ready_to_join(self):
        self._ready_to_join_semaphore.release()

    def wait_till_I_am_added(self):
        self._added_semaphore.acquire()

    def flag_I_am_added(self):
        self._added_semaphore.release()

    @classmethod
    def initFromFile(cls, file, verbose=False):
        group_view = cls(verbose)

        f = open(file)
        data = json.load(f)

        group_view.sk = SigningKey(base64.b64decode(data["sk"]))

        group_view.servers = data["servers"]
        group_view.servers.sort()
        group_view.manager = group_view.servers[-1]

        group_view.identifier = data["id"]

        for identifier in data["pks"]:
            pk_encoded = data["pks"][identifier]
            group_view.pks[identifier] = VerifyKey(base64.b64decode(pk_encoded))
            group_view.ip_addrs[identifier] = data["ip_addrs"][identifier]
            group_view.ports[identifier] = data["ports"][identifier]

        return group_view

    @classmethod
    def generateOwnData(cls, global_file, port=-1, verbose=False):
        group_view = cls(verbose)

        # generate own data
        group_view.sk = SigningKey.generate()
        group_view.identifier = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(10)
        )

        group_view.ip_addrs[group_view.identifier] = get_host_ip()
        if port == -1:
            group_view.ports[group_view.identifier] = random.randint(10000, 40000)
        else:
            group_view.ports[group_view.identifier] = port
        group_view.pks[group_view.identifier] = group_view.sk.verify_key

        # load global group view file, containing the information of the initial participants
        f = open(global_file)
        data = json.load(f)

        group_view.servers = []

        for identifier in data["pks"]:
            pk_encoded = data["pks"][identifier]
            group_view.pks[identifier] = VerifyKey(base64.b64decode(pk_encoded))
            group_view.ip_addrs[identifier] = data["ip_addrs"][identifier]
            group_view.ports[identifier] = data["ports"][identifier]
            group_view.servers.append(identifier)

        group_view.servers.sort()
        group_view.manager = group_view.servers[-1]
        
        group_view.servers.append(group_view.identifier)
        group_view.joining_servers = [group_view.identifier]

        return group_view

    def __debug(self, *msgs):
        if self.__verbose:
            print("\n",*msgs, "\n")