import os
import sys
import multiprocessing
import time

sys.path.append(sys.path[0] + "/../..")
from src.core.consensus.phase_king import PhaseKing
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView

def launch_process(i):
    multicast_addr = "224.3.115.1"
    multicast_port = 33984
    consensus_channel = Channel()
    out_channel = Channel()

    config = Configuration()
    files = os.listdir("config/" + config.data["initial"]["path"] + "/")
    file = "config/" + config.data["initial"]["path"] + "/" + files[i]
    group_view = GroupView(file)

    sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
    sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

    reliable_multicast = CausalOrderedReliableMulticast(
        multicast_addr, multicast_port, group_view.identifier, consensus_channel, group_view
    )
    reliable_multicast.start()

    phase_king = PhaseKing(consensus_channel, out_channel, reliable_multicast, group_view)

    value = phase_king.consensus("asdf")
    print(value)

if __name__ == "__main__":
    processes = []
    for i in range(16):
        p = multiprocessing.Process(target=launch_process, args=(i,))
        p.start()
        processes.append(p)

    time.sleep(10)
    for p in processes:
        p.terminate()
        p.join()