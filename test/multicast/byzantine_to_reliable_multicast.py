import os
import sys
import multiprocessing
import time
import uuid
import threading

sys.path.append(sys.path[0] + "/../..")
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.protocol.multicast.to_message import TotalOrderMessage
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.core.multicast.byzantine_to_reliable_multicast import ByzantineTotalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView

def consume(channel, group_view):
    for i in range(40):
        data = channel.consume()

        print(i, data)

    if i == 39:
        print("SUCCESS: Process {}".format(group_view.identifier))

def launch_process(i):
    multicast_addr = "224.3.115.1"
    to_multicast_port = 33984
    co_multicast_port = 30293
    channel = Channel()
    consensus_channel = Channel()

    config = Configuration()
    files = os.listdir("config/" + config.data["initial"]["path"] + "/")
    files.sort()
    file = "config/" + config.data["initial"]["path"] + "/" + files[i]
    group_view = GroupView(file)

    sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
    sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

    consensus_multicast = CausalOrderedReliableMulticast(
        multicast_addr, co_multicast_port, group_view.identifier, consensus_channel, group_view
    )
    consensus_multicast.start()

    byzantine_to_reliable_multicast = ByzantineTotalOrderedReliableMulticast(
        multicast_addr, to_multicast_port, group_view.identifier, channel, group_view, consensus_channel, consensus_multicast
    )
    byzantine_to_reliable_multicast.start()

    consume_thread = threading.Thread(target=consume, args=(channel, group_view,))
    consume_thread.start()

    for i in range(10):
        msg_id = str(uuid.uuid4())
        message = TotalOrderMessage.initFromData("Test", {"identifier": group_view.identifier, "value": i}, msg_id)
        message.encode()
        byzantine_to_reliable_multicast.send(message)
        time.sleep(0.05)

if __name__ == "__main__":
    processes = []
    for i in range(4):
        p = multiprocessing.Process(target=launch_process, args=(i,))
        p.start()
        processes.append(p)

    time.sleep(20)
    for p in processes:
        p.terminate()
        p.join()
