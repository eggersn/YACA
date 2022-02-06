import threading
import random 
import string 

from src.core.group_view.group_view import GroupView
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.protocol.base import Message
from src.protocol.client.read.heartbeat import *
from src.protocol.client.read.messages import *
from src.core.unicast.sender import UnicastSender
from src.protocol.multicast.piggyback import PiggybackMessage
from src.core.consensus.phase_king import PhaseKing
from src.protocol.client.write.initial import *


class ClientRequestsProcessing:
    def __init__(
        self,
        client_channel: Channel,
        client_write_multicast: CausalOrderedReliableMulticast,
        group_view: GroupView,
        configuration: Configuration,
    ):
        self._channel = client_channel
        self._client_write_multicast = client_write_multicast
        self._group_view = group_view
        self._configuration = configuration

        self._responder = UnicastSender(self._configuration)
        
    def start(self):
        consumer_thread = threading.Thread(target=self.consumer)
        consumer_thread.start()
        self._responder.start()

    def consumer(self):
        while True:
            data = self._channel.consume()
            self._process_request(data)

    def _process_request(self, data):
        msg = Message.initFromJSON(data)
        msg.decode()

        if msg.header == "Query: Heartbeat":
            self._handle_query_heartbeat(data)
        elif msg.header == "Query: Messages":
            self._handle_query_messages(data)
        elif msg.header == "Write: Initial":
            self._handle_init_message(data)

    def _handle_query_heartbeat(self, data):
        msg = HeartbeatQuery.initFromJSON(data)
        msg.decode()

        seqno_vector = {}
        for identifier in self._client_write_multicast._CO_R_g:
            if identifier not in self._group_view.servers:
                seqno_vector[identifier] = self._client_write_multicast._CO_R_g[identifier]

        response_msg = HearbeatQueryResponse.initFromData(seqno_vector, self._group_view.get_my_port(), msg.nonce)
        self._responder.send_udp_without_ack(response_msg, msg.get_sender())

    def _handle_query_messages(self, data):
        msg = MessageQuery.initFromJSON(data)
        msg.decode()

        for identifier in msg.nacks:
            if identifier in self._client_write_multicast._storage:
                for seqno in msg.nacks[identifier]:
                    if seqno >= len(self._client_write_multicast._storage[identifier]):
                        break

                    response = Message.initFromJSON(
                        self._client_write_multicast._storage[identifier][seqno]
                    )
                    response.decode()
                    self._responder.send_udp_without_ack(response, msg.get_sender())

    def _handle_init_message(self, data):
        msg = InitMessage.initFromJSON(data)
        msg.decode()

        if msg.identifier in self._group_view.users and self._group_view.users[msg.identifier] == msg.pk:
            response_msg = InitResponse.initFromData("success: user already exists with same pk")
            self._responder.send_udp(response_msg, msg.get_sender())
        elif msg.identifier in self._group_view.users:
            response_msg = InitResponse.initFromData("fail: user already exists with different pk")
            self._responder.send_udp(response_msg, msg.get_sender())
        else:
            self._channel.create_topic(msg.identifier)
            phase_king = PhaseKing(self._channel, self._client_write_multicast, self._group_view, self._configuration, msg.identifier, verbose=True)
            pk_string = base64.b64encode(msg.pk.encode()).decode("ascii")
            consistent_pk = phase_king.consensus(pk_string)        

            # update message in storage
            self._group_view.users[msg.identifier] = VerifyKey(base64.b64decode(consistent_pk))
            msg.pk = self._group_view.users[msg.identifier]
            msg.meta["SeqVector"]["acks"] = self._client_write_multicast._CO_R_g.copy()
            msg.meta["SeqVector"]["acks"][msg.identifier] = -1
            for server_id in self._group_view.servers:
                del msg.meta["SeqVector"]["acks"][server_id]
            msg.encode()
            self._client_write_multicast._storage[msg.identifier][0] = msg.json_data

            response_msg = InitResponse.initFromData("success: initialized user with pk {}".format(consistent_pk))
            nonce = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            response_msg.set_nonce(nonce)

            self._responder.send_udp(response_msg, msg.get_sender(), msg.identifier)

                    
    
