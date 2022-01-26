import sys
import multiprocessing
import time
import random
import string
import os
import socket
from urllib import response
import uuid


sys.path.append(sys.path[0] + "/../..")
from src.core.multicast.reliable_multicast import ReliableMulticast
from test.server.multicast_sender import MulticastSender
from src.core.utils.configuration import Configuration
from src.components.server.server import Server
from src.protocol.client.read.system_query import *


def launch_process(i):
    config = Configuration()

    if i < 4:
        files = os.listdir("config/" + config.data["initial"]["path"] + "/")
        files.remove("global.json")
        files.sort()
        sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
        sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

        server = Server(True, i, verbose=True)
        server.start()
    else:
        # time.sleep((i-4)*10)

        sys.stdout = open("logs/server" + str(i) + ".out", "a", buffering=1)
        sys.stderr = open("logs/server" + str(i) + ".out", "a", buffering=1)
        server = Server(verbose=True)
        server.start()

        # identifier = str(uuid.uuid4())


        # reliable_multicast = ReliableMulticast(config.get_multicast_addr(), config.get_client_multicast_port(), identifier, None, None, config, open=True)
        # reliable_multicast.start(listen=False)

        # nonce = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        # query_msg = QueryNumberOfActiveServers.initFromData(nonce)
        # query_msg.encode()

        # reliable_multicast.send(query_msg)
        # listener = reliable_multicast._udp_sock
        # print("listening port", listener.getsockname()[1])

        # for _ in range(4):
        #     data, addr = listener.recvfrom(1024)
            
        #     print(data, addr)

        #     ack = QueryAck.initFromData(nonce)
        #     ack.encode()

        #     listener.sendto(ack.json_data.encode(), addr)



if __name__ == "__main__":
    processes = []
    config = Configuration()
    for i in range(config.data["initial"]["instances"] + 4):
        p = multiprocessing.Process(target=launch_process, args=(i,))
        p.start()
        processes.append(p)

    time.sleep(30)
    for p in processes:
        p.terminate()
        p.join()
