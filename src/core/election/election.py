from src.core.unicast.sender import UnicastSender
from src.core.utils.configuration import Configuration
from src.core.utils.channel import Channel
from src.core.multicast.co_reliable_multicast import CausalOrderedReliableMulticast
from src.core.group_view.group_view import GroupView
from src.core.consensus.phase_king import PhaseKing
from src.protocol.ping.ping import PingMessage

class Election:
    def __init__(self, phase_king : PhaseKing, group_view : GroupView, configuration : Configuration, quite=False):
        self._phase_king = phase_king 
        self._group_view = group_view 
        self._quite = quite
        if not self._quite:
            self._udp_sender = UnicastSender(configuration)

    def election(self):
        found_leader = False 
        i = 0

        while not found_leader:
            potential_leader = self._group_view.get_ith_server(i)
            is_active : bool 
            if self._group_view.check_if_server_is_inactive(i):
                is_active = False 
            elif self._quite: # used for joining processes
                is_active = True
            else:
                ping_msg = PingMessage.initFromData()
                addr = self._group_view.get_unicast_addr_of_server(potential_leader)
                is_active = self._udp_sender.send_udp_sync(ping_msg, addr)
            
            consented_value = self._phase_king.consensus("1" if is_active else "")
            if consented_value == "1":
                # new leader is elected
                self._group_view.set_manager(potential_leader)
                found_leader = True 
            else:
                i += 1


    