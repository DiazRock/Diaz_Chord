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
A partir del id, yo necesito un addr = (ip, port)
succ(k), recibe un id y devuelve un nodo (o sea, debe ser un ip y un addr)
En Chord , como cada nodo necesita un tipo de comportamiento autónomo, cada uno
tiene que tener más o menos una noción concisa de la arquitectura de la red.
¿Con qué id inicializo un Node?
¿Cómo yo sé el nodo que está listo para recibir el nuevo nodo en la red?
El id mapea la dirección. Un nodo se inicializa con una direccion.
Eso es decisión mía.
Después resuelvo el tema del id. Por ahora lo que tengo que hacer
es buscar al succ_node disponible.
¿Tiene que importarme obtener un id y un puerto?
Yo envío el id por la red, y quien tenga ese id es quien
tiene que saltar.
No es el sucesor node lo que yo debo enviar por la red
Es el id, y a partir de él obtener el sucesor node.
El id te sirve namá, pa enterarte del sucesor.
Tengo que pensar ya en fragmentación en unidades atómicas.
Debo dividir los pasos en el __init__:
Hasta ahora, en el __init__, si yo no escribo un introduction_node,
tengo que hacer waiting command directo.
Si escribo un introduction_node, significa que la red tiene al menos
un nodo. Por tanto, debo ir hacia la dirección de ese nodo (aquí hay un
posible fallo), ese nodo es el sucesor del nodo que pregunta, en cuyo caso
le dice: eh!!, mira, yo soy el nodo, o en otro caso, le dice: to tranquilo,
pero busca a este tipo (otro nodo, que es el que quizás sea el que tu buscas,
o que conoce a alguien que tal vez sea el que tu buscas.) 
Aprender cómo lidiar con los JSON que me hace falta para enviar
respuestas de un lado a otro sin que me duela tanto.
El id siempre me va a decir el intervalo en el que va a caer el
nodo, respecto de los demás (en la finger table) pero el addr lo necesito
para poder enviar información a dicho node.
'''

class Node:
    def __init__(self, addr, id, introduction_node = None):
        self.addr = addr
        self.id = id        
        self.context_sender = zmq.Context()
        self.m = 7
        self.start = lambda i : (self.id + 2**i) % 2**self.m         
        self.inside = lambda id, interval : id >= interval[0] and id < interval[1]
        self.belongs_to = lambda id, interval: self.inside(id, interval) if interval[0] < interval[1] else not self.inside(id, interval) 
        self.commands = {"JOIN": self.init_finger_table, "FIND_SUCC": self.find_succesor}   
        if introduction_node:                    
            sock = self.context_sender.socket(zmq.REQ)
            sock.connect(introduction_node)  #¿Qué pasa si el succ_node ya tiene un sucesor? Mejor dicho, si ya hay alguien para quien él es un sucesor.            
            sock.send_json({"command": "JOIN", "params": {"addr": self.addr, "id": id}})
            buff = sock.recv_json()            
            while buff['response'] != "ACK":                
                pass
            
            
            self.succ_addr, self.succ_id, self.predeccesor_addr, self.predeccesor_id = buff['return_info']         
            
            sock.close()

        else:
            self.finger_table = [(self.id, self.addr) for i in range(self.m)]        
        self.waiting_for_command()


    # Este método inicializa la finger_table de un nodo entrante, expresado 
    # aquí en los parámetros addres_n, id_n.
    # Tengo dos opciones: construir el mensaje completo 
    # o enviar la información paso a paso. La primera opción
    # me parece la mejor, definitivamente.    
    def init_finger_table(self, address_n, id_n):
        return_json = {}
        finger_table = []
        

        self.sock.send_json(return_json)
        
        
        
    


    def waiting_for_command(self):
        self.sock = self.context_sender.socket(zmq.REP)
        self.sock.bind(self.addr)    
        #print(self.succ_addr, " ", self.addr) 

        while True:
            #print("59: waiting")
            buff = self.sock.recv_json()
            #print("buff: ", buff)
            
            
            if buff['commnad'] in self.commands:
                self.commands[buff['command']](buff['params'])
                

    #Si voy a pasar una propiedad, necesito el objeto en sí, porque siempre pueden
    #existir supuestas modificaciones.
    
    def find_succesor(self, id):    
        predeccesor = self.find_predecesor(id)
        succ_connector = self.context_sender.sock(zmq.REQ)
        succ_connector.connect(predeccesor)
        succ_connector.send_json({"command": "GET_SUCC", "params": {"addr_requester": self.addr}})
        addr_succ = succ_connector.recv_json()['return_info']
        return addr_succ


    def find_predecesor(self, id):
        predecesor_addr = self.addr
        predecesor_id = self.id
        sock = self.context_sender.sock(zmq.REQ)
        

        while not self.belongs_to(id, (self.start (predecesor_id), self.start(self.finger_table[0][0]))):
            sock.connect(predecesor_addr)
            sock.send_json({"command": "CLOSEST_PRED_FING", "params": { "id": id, "addr_to_rep": self.addr }})
            predecesor_id, predecesor_addr = sock.recv_json()['return_info']
            


        return predecesor_addr

    def closesr_pred_fing(self, id, addr_to_rep):
                
        for i in range(self.m-1, 0, -1):
            if self.belongs_to(self.finger_table[i][0], (self.id, id)) :
                self.sock.send_json({"response": "ACK", "return_info": (self.finger_table[i]) })
        
        self.sock.send_json({"response": "ACK", "return_info": (self.id, self.addr)})
            
        
    
if len(sys.argv) > 3:
    n = Node(introduction_node = sys.argv[1], addr= sys.argv[2], id = int(sys.argv[3]))
    
else:
    n = Node(addr = sys.argv[1], id = int(sys.argv[2]))
    
