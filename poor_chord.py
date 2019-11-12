from zmq import *
import zmq
from time import sleep
import sys
import json


succ = None

#Una porción del código que le envíe la información
#a la otra pidiéndole por el sucesor.
#Un código que empiece, ¿qué puede hacer?
#Empezar insertando un nodo, o cayendo en waiting_for_command
'''
añadir una capa de manipulación completa para muchas cosas. 
Es como que el sistema de chord está etereo por algún lado dando
vueltas (en mi cabeza, quiero decir) y el código (escrito en cada máquina)
se encarga de organizarlo... Esto por ejemplo, me hace preguntarme 
"¿En qué estado comienza el código en cada máquina?"
¿A qué método llamo, tal como estábamos escogiendo? 
El énfasis en esto es que el estado de cada máquina con código, 
es diferente del de los nodos insertados... ñó.
Espero haberme explicado bien. Es que cuando lo pienso, me enredo...
El tema es que los nodos del Chord pueden ser cualquier cosa.
El código entonces que los manipula, puede correr también en cualquier lado.
El código A manipula el nodo A', ¿A puede variar?
Como y al final de insert_new hago waiting_for_command entonces
la dirección que había puesto como parámetro al arrancar el código
(que es la dirección que identifica el nodo) hace bind. Y todo en talla...
¿Cómo ver nuevos comandos?
Lo mejor que ahora puedo hacer es lograr que 3 se comuniquen.
'''

class Node:
    def __init__(self, addr, succ_node = None):
        self.addr = addr
        self.succ_node = succ_node 
        self.context_sender = zmq.Context()
        self.commands = {"INS": self.send_succ}   
        if not succ_node:
            self.waiting_for_command()
        print("Por aki pase todo en talla")
        sock = self.context_sender.socket(zmq.REQ)
        sock.connect(succ_node)
        print(succ_node)    
        sock.send_string("INS " + addr)
        print("Por aki pase todo en talla")
        buff = sock.recv_string()
        print(buff, " buff")        
        self.succ_node = buff
        sock.close()
        self.waiting_for_command()
    


    def waiting_for_command(self):
        self.sock = self.context_sender.socket(zmq.REP)
        self.sock.bind(self.addr)    
        print(self.succ_node, " ", self.addr)

        while True:
            print("59: waiting")
            buff = self.sock.recv_string()
            print("buff: ", buff)
            instr = buff.split()
            print(instr)
            if instr[0] in self.commands:
                self.commands[instr[0]](instr[1:])
                

    def send_succ(self, addr):    
        self.sock.send_string(self.addr)
        if not self.succ_node:
            self.succ_node = addr


if len(sys.argv) > 2:
    n = Node(succ_node = sys.argv[1], addr= sys.argv[2])
    
else:
    n = Node(addr = sys.argv[1])
    
