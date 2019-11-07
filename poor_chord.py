from zmq import *
import zmq
from time import sleep
import sys

context_sender = zmq.Context()
succ = {}

#Una porción del código que le envíe la información
#a la otra pidiéndole por el sucesor.
def insert_new(addr_connect, my_addr):
    
    sock = context_sender.socket(zmq.REQ)
    sock.connect(addr_connect)    
    count = 0
    print("req")    
    print('before REQ')        
    sock.send_string("INS " + my_addr)                    
    buff = sock.recv()
    print(buff)
    print(count + 1)
    parse_command(my_addr)


def parse_command(addr):
    sock = context_sender.socket(zmq.REP)
    sock.bind(addr)    
    
    while True:        
        buff = sock.recv()
        print(buff)
        instr = buff.split()
        if instr[0] == "INS":
            sock.send(b"ACK recieved!")
            succ.update({addr: instr[1]})


if len (sys.argv) > 2:        
    insert_new(sys.argv[1], sys.argv[2])

else:
    parse_command(sys.argv[1])
