import os
import sys
import multiprocessing
import time
import threading
import uuid
sys.path.append(sys.path[0] + "/../..")
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.core.multicast.reliable_multicast import ReliableMulticast
from src.core.group_view.group_view import GroupView


def consume(channel, group_view):
    seqno_dict = {}
    for i in range(4000):
        data = channel.consume()
        message = Message.initFromJSON(data)
        message.decode()

        t = time.time_ns() / 10 ** 6

        print(i, t, message.content)

        msg_identifier = message.content["identifier"]
        msg_value = message.content["value"]

        if msg_identifier not in seqno_dict:
            if msg_value == 0:
                seqno_dict[msg_identifier] = 0
            else:
                print("FAILED: Process {1} expected ({2}, {3}) but got ({2}, {4})".format(group_view.identifier, msg_identifier, 0, msg_value))
                break 
        
        else:
            if msg_value == seqno_dict[msg_identifier] + 1:
                seqno_dict[msg_identifier] = msg_value
            else:
                print("FAILED: Process {1} expected ({2}, {3}) but got ({2}, {4})".format(group_view.identifier, msg_identifier, seqno_dict[msg_identifier], msg_value))
                break 

    if i == 3999:
        print("SUCCESS: Process {}".format(group_view.identifier))

def launch_process(i):

    multicast_addr = "224.3.115.1"
    multicast_port = 33984
    channel = Channel()

    config = Configuration()

    if i < 4:
        files = os.listdir("config/" + config.data["initial"]["path"] + "/")
        file = "config/" + config.data["initial"]["path"] + "/" + files[i]
        group_view = GroupView.initFromFile(file)

        sys.stdout = open("logs/" + files[i] + ".out", "a", buffering=1)
        sys.stderr = open("logs/" + files[i] + ".out", "a", buffering=1)

        reliable_multicast = ReliableMulticast(
            multicast_addr, multicast_port, group_view.identifier, channel, group_view, config, open=True
        )
        reliable_multicast.start()

        consume_thread = threading.Thread(target=consume, args=(channel, group_view,))
        consume_thread.start()
    else:
        identifier = str(uuid.uuid4())
        reliable_multicast = ReliableMulticast(
            multicast_addr, multicast_port, identifier, channel, None, config, open=True
        )
        reliable_multicast.start(listen=False)

        for j in range(1000):
            message = Message.initFromData("Test", content={"identifier": identifier, "value": j})
            reliable_multicast.send(message)
        
        
if __name__ == "__main__":
    processes = []
    for i in range(8):
        p = multiprocessing.Process(target=launch_process, args=(i,))
        p.start()
        processes.append(p)

    time.sleep(10)
    for p in processes:
        p.terminate()
        p.join()
