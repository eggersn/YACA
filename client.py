import os
import threading

from src.components.client.client import Client
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.protocol.client.write.initial import InitMessage
from src.protocol.client.write.text_message import TextMessage


def print_messages(client : Client):
    channel = client._delivery_channel
    columns, _ = os.get_terminal_size(0)
    while True:
        data = channel.consume()
        msg = Message.initFromJSON(data)
        msg.decode()

        if msg.header == "Write: Initial":
            msg = InitMessage.initFromJSON(data)
            msg.decode()
            print("{} joined the chat".format(msg.identifier).center(columns, " "))
        elif msg.header == "Write: TextMessage":
            msg = TextMessage.initFromJSON(data)
            msg.decode()

            sender_id = msg.get_signature()[0]
            print("{}: {}".format(sender_id, msg.text))


def write(client : Client):
    while True:
        text = input()
        client.send(text)

def main():
    client = Client()
    client.start()

    print_thread = threading.Thread(target=print_messages, args=(client,))
    print_thread.start()

    write_thread = threading.Thread(target=write, args=(client,))
    write_thread.start()

if __name__ == "__main__":
    main()

