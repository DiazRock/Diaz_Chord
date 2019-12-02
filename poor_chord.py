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
HASTA AHORA, SIEMPRE VOY A TENER REFERENCIA DE LOS NODOS, PERO TOOODA SU INFO
EN CADA NODO AISLADO, SINO UNA REFERENCIA, UNA MANERA DE PINCHARLES Y AVERIGUAR SU
ESTADO.
'''

class Node:
    def __init__(self, addr, id, introduction_node = None):
        self.addr = addr
        self.id = id        
        self.context_sender = zmq.Context()
        self.m = 7
        self.start = lambda i : (self.id + 2**(i)) % 2**self.m         
        self.inside = lambda id, interval : id >= interval[0] and id < interval[1]
        self.belongs_to = lambda id, interval: self.inside(id, interval) if interval[0] < interval[1] else not self.inside(id, interval) 
        self.commands = {"JOIN": self.init_parametters, "FIND_SUCC": self.find_succesor, "GET_SUCC": self.get_succ, "SET_SUCC": self.set_as_succ, "UPD_FING" : self.update_finger_table}   
        self.sock_req = self.context_sender.socket(zmq.REQ)
        if introduction_node:                    
            self.sock_req.connect("tcp://" + introduction_node)  #¿Qué pasa si el succ_node ya tiene un sucesor? Mejor dicho, si ya hay alguien para quien él es un sucesor.            
            self.sock_req.send_json({"command": "JOIN", "params": {"addr_node": self.addr, "start_indexes": [self.start(i) for i in range(self.m) ]}})
            buff = self.sock_req.recv_json()            
            if buff['response'] == "ACK":
                self.finger_table = buff['return_info']['finger_table']
                self.predeccesor_id = buff['return_info']['pred_id']
                self.predeccesor_addr = buff['return_info']['pred_addr']
                self.update_others()
            
            
            self.succ_addr, self.succ_id, self.predeccesor_addr, self.predeccesor_id = buff['return_info']         
            
            

        else:
            self.succ_addr, self.succ_id, self.predeccesor_addr, self.predeccesor_id = self.addr, self.id, self.addr, self.id
            self.finger_table = [(self.id, self.addr) for i in range(self.m)]        
        self.waiting_for_command()


    # Este método inicializa la finger_table de un nodo entrante junto con los otros datos, expresado 
    # aquí en los parámetros addres_n, id_n.
    # Tengo dos opciones: construir el mensaje completo 
    # o enviar la información paso a paso. La primera opción
    # me parece la mejor, definitivamente. 
    # AQUI HAY UN PROBLEMA SI EL NODO RECEPTOR FALLA. SI LA RESPUESTA NO LLEGA BIEN, HAY QUE REMITIR A OTRO NODO.
    # COMO MEDIDA A LO ANTERIOR, PUEDO NOTIFICAR AL SUCESOR DEL NODO RECEPTOR QUE ALGUIEN SE ESTA METIENDO EN LA RED, DE ESA FORMA EL OTRO LO PUEDE ATENDER EN CASO DE QUE LA RED CAIGA.   
    def init_parametters(self, addr_node, start_indexes):
        return_json = {}
        finger_table = []
        pred_addr, pred_id, succ_id, succ_addr = self.find_succesor(start_indexes[0])
        finger_table.append((succ_id, succ_addr ))
        print(pred_addr, pred_id, "  ", succ_id, succ_addr) 

        
        sock = self.context_sender.socket(zmq.REQ)
        sock.connect("tcp://"+pred_addr)
        sock.send_json( {"command": "SET_SUCC", "params": { "id_succ": self.id, "add_succ" : self.addr }})
        buff = sock.recv_json()
        sock.close()
        for i in range(self.m-1):
            if(self.belongs_to(start_indexes[i + 1], interval =(self.id, finger_table[i][0]))):
                finger_table[i + 1] = finger_table[i]
            else:
                finger_table[i + 1] = self.find_succesor(start_indexes[i + 1])[2:]                
        
        return_json.update({"response": "ACK" , "return_info": {"finger_table" : finger_table, "pred_id": pred_id, "pred_addr" : pred_addr  } })

        self.sock_rep.send_json(return_json)
        
    def set_as_succ(self, id_succ, addr_succ):
        self.succ_id = id_succ
        self.succ_addr = addr_succ
        self.sock_rep.send_json({"response": "ACK"})

    
    def update_finger_table(self, new_node_addr, new_node_id, index_to_actualize):
        if self.belongs_to(new_node_id, interval = (self.id, self.finger_table[index_to_actualize][1])):
            self.finger_table[index_to_actualize] = (new_node_id, new_node_addr)
            self.sock_req.connect("tcp://"+self.predeccesor_addr)
            self.sock_req.send_json({"command" : "UPD_FING", "params": {"new_node_addr": new_node_addr, "new_node_id": new_node_id, "index_to_actualize": index_to_actualize}})
            self.sock_req.disconnect("tcp://"+self.predeccesor_addr)
        


    def get_succ(self):
        self.sock_rep.send_json({"response": "ACK", "return_info": self.finger_table[0]})
        
    # Este método es para los nodos que necesitan
    # ser actualizados a partir del nuevo que entró.    
    def update_others(self):
        
        for i in range(self.m):
            pred_addr =  self.find_predecesor((self.id - 2**(i-1)))[1] 
            self.sock_req.connect("tcp://"+pred_addr)
            self.sock_req.send_json({"command": "UPD_FING", "params": { "new_node_addr" : self.addr, "new_node_id" : self.id, "index_to_actualize" : i }})

        

    def waiting_for_command(self):
        self.sock_rep = self.context_sender.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
        print(self.id, " ", self.addr, "\n", self.finger_table,  "\n", self.predeccesor_id, " ", self.predeccesor_addr, "\n", self.succ_id, " ", self.succ_addr) 

        while True:
            #print("59: waiting")
            buff = self.sock_rep.recv_json()
            print("buff: ", buff)
            
            
            if buff['command'] in self.commands:
                
                print(buff['command'])
                self.commands[buff['command']](**buff['params'])
                

    #Si voy a pasar una propiedad, necesito el objeto en sí, porque siempre pueden
    #existir supuestas modificaciones.
    
    def find_succesor(self, id):    
        (predeccesor_id, predeccesor_addr)  = self.find_predecesor(id)
        print(predeccesor_addr, self.addr)                
        self.sock_req.connect("tcp://"+predeccesor_addr)
        self.sock_req.send_json({"command" : "GET_SUCC", "params": None})

        print("ACA")
        to_return = self.sock_req.recv_json()["return_info"]
        self.sock_req.disconnect("tcp://"+predeccesor_addr)        
        return (predeccesor_id, predeccesor_addr) + to_return
        


    def find_predecesor(self, id):
        predecesor_addr = self.addr
        predecesor_id = self.id
                        

        while not self.belongs_to(id, (self.start (predecesor_id), self.start(self.finger_table[0][0]))):
            self.sock_req.connect("tcp://"+predecesor_addr)
            print("por acá pasé")
            self.sock_req.send_json({"command": "CLOSEST_PRED_FING", "params": { "id": id, "addr_to_rep": self.addr }})
            predecesor_id, predecesor_addr = self.sock_req.recv_json()['return_info']
            self.sock_req.disconnect("tcp://"+predecesor_addr)
            

        print("por acá pasé")
        return (predecesor_id,predecesor_addr)

    def closesr_pred_fing(self, id, addr_to_rep):
                
        for i in range(self.m-1, 0, -1):
            if self.belongs_to(self.finger_table[i][0], (self.id, id)) :
                self.sock_rep.send_json({"response": "ACK", "return_info": (self.finger_table[i]) })
        
        self.sock_rep.send_json({"response": "ACK", "return_info": (self.id, self.addr)})
            
        
    
if len(sys.argv) > 3:
    n = Node(introduction_node = sys.argv[1], addr= sys.argv[2], id = int(sys.argv[3]))
    
else:
    n = Node(addr = sys.argv[1], id = int(sys.argv[2]))
    
