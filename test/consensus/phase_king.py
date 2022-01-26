import os
import sys
import multiprocessing
import time
import threading

sys.path.append(sys.path[0] + "/../..")
from src.core.consensus.phase_king import PhaseKing
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.utils.crash_channel import CrashChannel
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.protocol.base import Message

def normal_test(i):
    multicast_addr = "224.3.115.1"
    multicast_port = 33984
    consensus_channel = Channel()

    config = Configuration()
    files = os.listdir("config/" + config.data["initial"]["path"] + "/")
    files.sort(key=lambda s: int(s.split("server")[1].split(".json")[0]))
    file = "config/" + config.data["initial"]["path"] + "/" + files[i]
    group_view = GroupView.initFromFile(file)

    sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
    sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

    reliable_multicast = CausalOrderedReliableMulticast(
        multicast_addr, multicast_port, group_view.identifier, consensus_channel, group_view, config
    )
    reliable_multicast.start()

    phase_king = PhaseKing(consensus_channel, reliable_multicast, group_view, config, True)

    value = "correct value"

    value = phase_king.consensus(value)
    print("OUTPUT", value)

def topic_test(i):
    def topic_thread(value, topic):
        phase_king = PhaseKing(consensus_channel, reliable_multicast, group_view, config, topic, True)
        value = phase_king.consensus(value)
        print("OUTPUT", value)


    multicast_addr = "224.3.115.1"
    multicast_port = 33984
    consensus_channel = Channel()
    consensus_channel.create_topic("topic1")
    consensus_channel.create_topic("topic2")

    config = Configuration()
    files = os.listdir("config/" + config.data["initial"]["path"] + "/")
    files.sort(key=lambda s: int(s.split("server")[1].split(".json")[0]))
    file = "config/" + config.data["initial"]["path"] + "/" + files[i]
    group_view = GroupView.initFromFile(file)

    sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
    sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

    reliable_multicast = CausalOrderedReliableMulticast(
        multicast_addr, multicast_port, group_view.identifier, consensus_channel, group_view, config
    )
    reliable_multicast.start()

    topic1_thread = threading.Thread(target=topic_thread, args=("topic1","topic1",))
    topic1_thread.start()

    topic2_thread = threading.Thread(target=topic_thread, args=("topic2","topic2",))
    topic2_thread.start()

    topic1_thread.join()
    topic2_thread.join()


def crash_test(i):
    multicast_addr = "224.3.115.1"
    multicast_port = 33984
    consensus_channel = (CrashChannel(r".*Phase King: Message.*\"phase\": {}.*".format(i), 0.5) if i < 4 else Channel())

    config = Configuration()
    files = os.listdir("config/" + config.data["initial"]["path"] + "/")
    files.sort(key=lambda s: int(s.split("server")[1].split(".json")[0]))
    file = "config/" + config.data["initial"]["path"] + "/" + files[i]
    group_view = GroupView.initFromFile(file)

    sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
    sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

    reliable_multicast = CausalOrderedReliableMulticast(
        multicast_addr, multicast_port, group_view.identifier, consensus_channel, group_view, config
    )
    reliable_multicast.start()

    phase_king = PhaseKing(consensus_channel, reliable_multicast, group_view, config, verbose=True)

    value = "correct value"

    value = phase_king.consensus(value)
    print("OUTPUT", value)

def launch_process(i):
    # normal_test(i)
    topic_test(i)
    # crash_test(i)

if __name__ == "__main__":
    processes = []
    config = Configuration()
    for i in range(config.data["initial"]["instances"]):
        p = multiprocessing.Process(target=launch_process, args=(i,))
        p.start()
        processes.append(p)

    time.sleep(30)
    for p in processes:
        p.terminate()
        p.join()