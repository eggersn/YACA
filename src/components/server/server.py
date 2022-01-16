from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.broadcast.broadcast_listener import BroadcastListener
from src.protocol.base import Message

class Server:

    def __init__(self, initial=False, i=0):
        self._channel = Channel()
        self._discovery_channel = Channel()
        self._configuration = Configuration()

        if initial:
            self._group_view = GroupView(self._configuration.get_group_view_file(i))
            self._multicast = CausalOrderedReliableMulticast(
                self._configuration.get_multicast_addr(),
                self._configuration.get_multicast_port(),
                self._group_view.identifier,
                self._channel,
                self._group_view
            )
            self._multicast.start()

        self._broadcast_listener = BroadcastListener(self._channel, self._configuration.get_broadcast_port())
        self._broadcast_listener.start()

    def _discovery_consume(self):
        while True:
            data = self._discovery_channel.consume()

            message = Message.initFromJSON(data)
            message.decode()

            if "View" == message.header[:4]:
                if self._group_view.check_if_manager():
                    self._multicast.send(message)
                else:
                    print("observing MGR behavior") #todo

    def _consume(self):
        while True:
            data = self._channel.consume()

            message = Message.initFromJSON(data)
            message.decode()

            if "View" == message.header[:4]:
                self._group_view.process_group_view_message(message)