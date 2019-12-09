from zmq import *
import zmq
from time import sleep
import sys
import json
import _thread as thread

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
        self.commands = {"JOIN": self.init_parametters, "FIND_SUCC": self.find_succesor, "GET_SUCC": self.get_succ, "SET_SUCC": self.set_as_succ, "UPD_FING" : self.update_finger_table, "CLOSEST_PRED_FING": self.closest_pred_fing}   
        self.sock_req = self.context_sender.socket(zmq.REQ)
        if introduction_node:                    
            self.sock_req.connect("tcp://" + introduction_node)  #¿Qué pasa si el succ_node ya tiene un sucesor? Mejor dicho, si ya hay alguien para quien él es un sucesor.            
            self.sock_req.send_json({"command": "JOIN", "params": {"addr_node": self.addr, "start_indexes": [self.start(i) for i in range(self.m) ]}, "procedence" : "__init__"}) # Se supone que este recv se enganche con el recv_json de del waiting_for_command que se hace del lado del server.
            buff = self.sock_req.recv_json()            
            if buff['response'] == "ACK":
                print(buff)
                self.finger_table = buff['return_info']['finger_table']
                self.predeccesor_id = buff['return_info']['pred_id']
                self.predeccesor_addr = buff['return_info']['pred_addr']
                self.succ_id = buff['return_info']['succ_id']
                self.succ_addr = buff['return_info']['succ_addr']
                self.update_others()
            
            
            #self.succ_addr, self.succ_id, self.predeccesor_addr, self.predeccesor_id = buff['return_info']         
            
            

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
        
        #print("init_parametters")
        return_json = {}
        finger_table = []
        pred_id, pred_addr, succ_id, succ_addr = self.find_succesor(start_indexes[0])
        finger_table.append((succ_id, succ_addr ) )
        #print(pred_id, pred_addr, "  ", succ_id, succ_addr, " en init_parametters") 
                        
        for i in range(self.m-1):
            print(finger_table, " ", start_indexes)
            if(self.belongs_to(start_indexes[i + 1], interval =(self.id, finger_table[i][0]))):
                finger_table.append(finger_table[i]) 
            else:
                finger_table.append(self.find_succesor(start_indexes[i + 1])[2:])                 
        
        
        return_json.update({"response": "ACK" , "return_info": {"finger_table" : finger_table, "pred_id": pred_id, "pred_addr" : pred_addr, "succ_id" : self.id, "succ_addr": self.addr } })
        
        self.sock_rep.send_json(return_json)
        
    def set_as_succ(self, id_succ, addr_succ):
        self.succ_id = id_succ
        self.succ_addr = addr_succ
        self.sock_rep.send_json({"response": "ACK", "procedence": "set_as_succ"})

    
    def update_finger_table(self, new_node_addr, new_node_id, index_to_actualize):
        
        if self.belongs_to(new_node_id, interval = (self.id, self.finger_table[index_to_actualize][0])):
            print("Dentro de update", self.predeccesor_addr == self.addr, index_to_actualize, sep = ' ')
                        
            self.finger_table[index_to_actualize] = (new_node_id, new_node_addr)
            # Estos son san parches puestos
            if not index_to_actualize: self.succ_id, self.succ_addr = (new_node_id, new_node_addr)
            if (self.predeccesor_addr, self.predeccesor_id) == (self.addr, self.id) : 
                self.predeccesor_id, self.predeccesor_addr = (new_node_id, new_node_addr)                
            # fin de los parches
            else:
                if self.predeccesor_addr != self.succ_addr:
                    print("¿por qué entre aquí? ", "id:", self.id, "predeccesor_id:", self.predeccesor_id, "predeccesor_addr:", self.predeccesor_addr, "addr:", self.addr, sep = ' ')
                    self.sock_req.connect("tcp://" + self.predeccesor_addr)                    
                    self.sock_req.send_json({"command" : "UPD_FING", "params": {"new_node_addr": new_node_addr, "new_node_id": new_node_id, "index_to_actualize": index_to_actualize }, "procedence": "update_finger_table" } )
                    self.sock_req.recv_json()
                    self.sock_req.disconnect("tcp://"+self.predeccesor_addr)
                    print("en update_finger_table", self.addr, self.predeccesor_addr, sep = ' ')

        self.sock_rep.send_json({"response": "ACK"})


    def get_succ(self):
        print("GET_SUCC")
        self.sock_rep.send_json( {"response": "ACK", "return_info": tuple (self.finger_table[0]) } )
        
    # Este método es para los nodos que necesitan
    # ser actualizados a partir del nuevo que entró.    
    def update_others(self):
        print("update_others client_side")
        for i in range(1, self.m + 1):
            pred_addr =  self.find_predecesor((self.id - 2**(i-1))%2**self.m)[1]
            #print("en update_others, " + pred_addr, " ", (self.id - 2**(i-1))%2**self.m) 
            self.sock_req.connect("tcp://"+pred_addr)
            self.sock_req.send_json({"command": "UPD_FING", "params": { "new_node_addr" : self.addr, "new_node_id" : self.id, "index_to_actualize" : i -1 }})
            self.sock_req.recv_json()
            self.sock_req.disconnect("tcp://"+pred_addr)
        
    #MANTRA : "Do not use or close sockets except in the thread that created them."
    def waiting_for_command(self):
        self.sock_rep = self.context_sender.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
        #print(self.id, " ", self.addr, "\n", self.finger_table,  "\n", self.predeccesor_id, " ", self.predeccesor_addr, "\n", self.succ_id, " ", self.succ_addr) 

        while True:            
            
            print("waiting")
            buff = self.sock_rep.recv_json()
                                    
            if buff['command'] in self.commands:

                print("jejejejejejejejejejejejej ", buff)
                
                
                self.commands[buff["command"]](**buff["params"])
                                
    
    def find_succesor(self, id):
        print("find_succesor")    
        (predeccesor_id, predeccesor_addr)  = self.find_predecesor(id)
        to_return = (predeccesor_id, predeccesor_addr)
        if predeccesor_addr != self.addr:
            
            print(predeccesor_addr, " inside the if")            
            self.sock_req.connect("tcp://"+ predeccesor_addr)
            self.sock_req.send_json({"command" : "GET_SUCC", "params": {}})

            print("ACA find_succesor")
            buff = self.sock_req.recv_json()
            to_return = tuple (buff["return_info"])
            print(buff)
            self.sock_req.disconnect("tcp://"+ predeccesor_addr)        
        return (predeccesor_id, predeccesor_addr) + to_return
        


    def find_predecesor(self, id):
        predecesor_addr = self.addr
        predecesor_id = self.id
        print("buscando predecesor antes del while ", id, predecesor_addr, predecesor_id, self.finger_table[0], sep = ' ')

        while not self.belongs_to(id, (self.start (predecesor_id), self.start(self.finger_table[0][0]))):
            
            if predecesor_addr != self.addr:
                self.sock_req.connect("tcp://"+predecesor_addr)
                print("por acá pasé ", predecesor_addr == self.addr)
                
                self.sock_req.send_json({"command": "CLOSEST_PRED_FING", "params": { "id": id, "is_a_request": True }})                
                predecesor_id, predecesor_addr = self.sock_req.recv_json()['return_info']
                
                self.sock_req.disconnect("tcp://"+predecesor_addr)
                print("Es probable que este while esté dando el berro.")
            else:
                predecesor_id, predecesor_addr = self.closest_pred_fing(id = id)
                

        print("por acá pasé_find_predeccesor_out_while")
        return (predecesor_id,predecesor_addr)

    def closest_pred_fing(self, id, is_a_request = False):
        
        print(id, 'en closet_pred_fing', sep = ' ')

        for i in range(self.m-1, 0, -1):
            if self.belongs_to(self.finger_table[i][0], (self.id, id)) :
                if is_a_request:
                    self.sock_rep.send_json({"response": "ACK", "return_info": tuple(self.finger_table[i]), "procedence": "closest_pred_fing" })
                else:
                    return self.finger_table[i]
        
        if is_a_request:
            self.sock_rep.send_json({"response": "ACK", "return_info": (self.id, self.addr), "procedence": "closest_pred_fing"})
        else:
            return (self.id, self.addr)
        
    
if len(sys.argv) > 3:
    n = Node(introduction_node = sys.argv[1], addr= sys.argv[2], id = int(sys.argv[3]))
    
else:
    n = Node(addr = sys.argv[1], id = int(sys.argv[2]))
    
