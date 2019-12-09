import zmq


class node:
    def __init__(self, addr, succ):
        self.addr = addr
        self.succ = self
    pass

    def __eq__(self, other):
        return self.addr == other.addr 

