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
        self.m = 3
        self.start = lambda i : (self.id + 2**(i)) % 2**self.m         
        #self.inside = lambda id, interval : id >= interval[0] and id < interval[1]
        #Esto está mal, porque no mide bien la pertenencia al intervalo si este cle da la vuelta al círculo.
        #self.belongs_to = lambda id, interval: self.inside(id, interval) if interval[0] < interval[1] else not self.inside(id, interval)
        self.commands = {"JOIN": self.init_parametters, "FIND_SUCC": self.find_succesor, "GET_SUCC": self.get_succ, "SET_SUCC": self.set_as_succ, "UPD_FING" : self.update_finger_table, "CLOSEST_PRED_FING": self.closest_pred_fing, "ALIVE": self.alive, "GET_PARAMS": self.get_params, "GET_PROP": self.get_prop, "SET_PRED" : self.set_pred, "BELONG": self.belongs_messager, "CHANGE_INTRO": self.change_intro, "GET_PRED": self.get_pred}
        self.sock_req = self.context_sender.socket(zmq.REQ)
        self.is_introduction_node = not introduction_node
        if not self.is_introduction_node:
                        
            self.sock_req.connect("tcp://" + introduction_node)  #¿Qué pasa si el succ_node ya tiene un sucesor? Mejor dicho, si ya hay alguien para quien él es un sucesor.            
            self.sock_req.send_json({"command": "JOIN", "params": {"id_node": self.id, "addr_node": self.addr, "start_indexes": [self.start(i) for i in range(self.m) ]}, "procedence" : "__init__"}) # Se supone que este recv se enganche con el recv_json de del waiting_for_command que se hace del lado del server.
            buff = self.sock_req.recv_json()
            self.sock_req.disconnect("tcp://" + introduction_node)
            if buff['response'] == "ACK":
                ##print(buff)
                self.finger_table = buff['return_info']['finger_table']
                self.predeccesor_id = buff['return_info']['pred_id']
                self.predeccesor_addr = buff['return_info']['pred_addr']
                                                
                self.sock_req.connect("tcp://" + introduction_node)
                self.sock_req.send_json({"command": "CHANGE_INTRO", "params": {} })
                buff = self.sock_req.recv_json()
                self.sock_req.disconnect("tcp://" + introduction_node)
                self.update_others(buff["return_info"])
                #print("sobreviví")
            
            #self.succ_addr, self.succ_id, self.predeccesor_addr, self.predeccesor_id = buff['return_info']         
                        
        else:
            self.predeccesor_addr, self.predeccesor_id = self.addr, self.id
            self.finger_table = [(self.id, self.addr) for i in range(self.m)]
        #print(self.finger_table)
        self.waiting_for_command()

    def belongs_to(self, id, interval):
        
        #if id == 4:
        #print("id: ", id, " belongs_to ", interval, " ", interval[0] == interval[1], " ", interval[0],  " ", interval[1])
        if interval[0] == interval[1]:
            #print("WHYYYYYYYYYY") 
            return False
        if interval[0] < interval[1]:            
            #print("entro en el if")
            return id >= interval[0] and id < interval[1]
        return self.belongs_to(id, (interval[0], 2**self.m)) or (self.belongs_to(id, (0, interval[1])) )
        
    def belongs_messager(self, id, interval):        
        return_value = self.belongs_to(id, interval)
        self.sock_rep.send_json({"response": "ACK", "return_info": (return_value)})
    
    # Este método inicializa la finger_table de un nodo entrante junto con los otros datos, expresado 
    # aquí en los parámetros addres_n, id_n.
    # Tengo dos opciones: construir el mensaje completo 
    # o enviar la información paso a paso. La primera opción
    # me parece la mejor, definitivamente. 
    # AQUI HAY UN PROBLEMA SI EL NODO RECEPTOR FALLA. SI LA RESPUESTA NO LLEGA BIEN, HAY QUE REMITIR A OTRO NODO.
    # COMO MEDIDA A LO ANTERIOR, PUEDO NOTIFICAR AL SUCESOR DEL NODO RECEPTOR QUE ALGUIEN SE ESTA METIENDO EN LA RED, DE ESA FORMA EL OTRO LO PUEDE ATENDER EN CASO DE QUE LA RED CAIGA.   
    def init_parametters(self, id_node, addr_node, start_indexes):
        
        print("init params")
        return_json = {}
        finger_table = [None for i in range(self.m)]
        pred_id, pred_addr, succ_id, succ_addr = self.find_succesor(start_indexes[0])
        print("pase el find_succ ", (succ_id, succ_addr, start_indexes[0]))
        finger_table[0] = (succ_id, succ_addr )  #Para el nodo nuevo asignar como sucesor el tipo que encontré.
        #print(pred_id, pred_addr, self.predeccesor_id, self.predeccesor_addr, " en init_parametters", sep = ' ') 
        if self.is_introduction_node:
            self.predeccesor_id, self.predeccesor_addr = ( id_node, addr_node)
            self.finger_table[0]= ( id_node, addr_node)
        else:
            #print("MORRRIIIIIIIIIIIII ", (pred_addr, self.addr))
            if self.addr != pred_addr:                
                self.sock_req.connect("tcp://" + pred_addr)
                self.sock_req.send_json({"command": "GET_SUCC", "params": { }})
                buff_0 = self.sock_req.recv_json()            
                temp_succ_addr = buff_0['return_info'][1]
                self.sock_req.send_json({"command" : 'SET_SUCC', "params" : { "id_succ": id_node, "addr_succ": addr_node }})
                self.sock_req.recv_json()
                self.sock_req.disconnect("tcp://" + pred_addr)
            else:
                print("Aquí anduve else")
                temp_succ_addr = self.finger_table[0][1]
                self.finger_table[0] = (id_node, addr_node)

            print("Pasé el if ", temp_succ_addr)
            if temp_succ_addr != self.addr:
                self.sock_req.connect("tcp://" + temp_succ_addr)
                self.sock_req.send_json({"command": "SET_PRED", "params": { "id_pred": id_node, "addr_pred" : addr_node }})
                self.sock_req.recv_json()
                self.sock_req.disconnect("tcp://" + temp_succ_addr)
            else:
                self.predeccesor_id, self.predeccesor_addr = (id_node, addr_node)
        #Voy a anotar lo que primero debo hacer mañana:
        #Debo insertar un nodo nuevo, buscar su pred, al pred pedirle el succ, luego para el pred el nuevo succ es node, y para el succ el pred es el nuevo. 
        #print("CCCCCCCC")
        self.fill_table(id_node, finger_table, start_indexes)
        if self.is_introduction_node: self.fill_table(self.id, self.finger_table, [self.start(i) for i in range(self.m)] )
        
        print("init_parametters", finger_table, sep = ' ')
        return_json.update({"response": "ACK" , "return_info": {"finger_table" : finger_table, "pred_id": pred_id, "pred_addr" : pred_addr, "succ_id" : self.id, "succ_addr": self.addr }, "procedence": (self.id, self.addr), "method" : 'init_parametters' })
        
        self.sock_rep.send_json(return_json)

    def fill_table(self, id_node, finger_table ,start_indexes):
        for i in range(self.m - 1):
                                                 
            print(start_indexes[i + 1],  (id_node, finger_table[i][0]), self.id, finger_table[i], sep = ' ')
            if self.belongs_to(start_indexes[i + 1], interval =(id_node, finger_table[i][0])) or finger_table[i][0] == start_indexes[i + 1]:
                print("if")
                finger_table[i + 1] = finger_table[i] 
            else:
                print("else ", start_indexes[i + 1], " ", (self.id, finger_table[i][0]))
                finger_table [i + 1] = self.find_succesor(start_indexes[i + 1])[2:]
        
        

    def set_pred(self, id_pred, addr_pred):
        self.predeccesor_id, self.predeccesor_addr = id_pred, addr_pred
        self.sock_rep.send_json({"response": "ACK", "procedence": "set_pred"})

    def get_params(self):        
        self.sock_rep.send_json({"response": "ACK", "return_info": (self.finger_table, self.predeccesor_addr, self.predeccesor_id )})

    def get_prop(self, prop_name):
        if prop_name == "start_indexes":
            self.sock_rep.send_json({'response': "ACK", "return_info" : [self.start(i) for i in range(self.m)] })    

        self.sock_rep.send_json({'response': 'ACK', "response": self.__dict__[prop_name] })

    def set_as_succ(self, id_succ, addr_succ):        
        self.finger_table[0] = (id_succ, addr_succ)
        self.sock_rep.send_json({"response": "ACK", "procedence": "set_as_succ"})

    def get_pred(self):
        self.sock_rep.send_json({"response": "ACK", "return_info": (self.predeccesor_id, self.predeccesor_addr)})


    def alive(self):
        self.sock_rep.send_json({"response": "ACK"})

    def update_finger_table(self, new_node_addr, new_node_id, index_to_actualize, addr_requester):
        
        #if new_node_id == 3:
        print("Dentro de update\n", "new_node_id:", new_node_id, "\n\tindex_to_actualize:", index_to_actualize,"id:", self.id,"\n\tfinger_table[index_to_actualize]:", self.finger_table[index_to_actualize], sep = '\t')

        if self.belongs_to(new_node_id, interval = (self.id, self.finger_table[index_to_actualize][0]) ) or self.finger_table[index_to_actualize] == (self.id,self.addr) :
                        
            self.finger_table[index_to_actualize] = (new_node_id, new_node_addr)
            #if new_node_id == 3:
                #print("\tUn chino cayó en un pozo.")
            print("\tpredeccesor_addr:\t", self.predeccesor_addr, ' ', addr_requester)
            print("\tfinger_table[index_to_actualize]:]\t", self.finger_table[index_to_actualize])            
            if self.predeccesor_addr != addr_requester:
                print("UPD_FING ", "id:", self.id, "predeccesor_id:", self.predeccesor_id, "predeccesor_addr:", self.predeccesor_addr, "addr:", self.addr, sep = ' ')
                self.sock_req.connect("tcp://" + self.predeccesor_addr)                    
                self.sock_req.send_json({"command" : "UPD_FING", "params": {"new_node_addr": new_node_addr, "new_node_id": new_node_id, "index_to_actualize": index_to_actualize, "addr_requester": addr_requester }, "procedence": ("update_finger_table", self.addr) } )
                self.sock_req.recv_json()
                self.sock_req.disconnect("tcp://"+self.predeccesor_addr)
            #print("en update_finger_table", self.addr, self.predeccesor_addr, sep = ' ')
        
        
        self.sock_rep.send_json({"response": "ACK", "procedence": self.addr})


    def get_succ(self):
        ##print("GET_SUCC")
        self.sock_rep.send_json( {"response": "ACK", "return_info": tuple (self.finger_table[0]) } )
    
    def change_intro(self):
        val = self.is_introduction_node
        self.is_introduction_node = False
        self.sock_rep.send_json({"response" : "ACK", "return_info": val})

    # Este método es para los nodos que necesitan
    # ser actualizados a partir del nuevo que entró.    
    def update_others(self , introduction_node = False):
        if introduction_node: return
        print("update_others client_side")
        for i in range( 1, self.m + 1):
            print("update_others ", (self.id - 2**(i-1))%2**self.m)            
            pred_addr =  self.find_predecesor((self.id - 2**(i-1))% 2**self.m)[1]
            if pred_addr == self.addr: continue     #Este if está aquí porque uno se puede tener en su finger_table. Sobre todo cuando halla m o menos nodos en la red.
            print("en update_others, conéctate a ", ( pred_addr), " ", (self.id - 2**(i-1))%2**self.m) 
            self.sock_req.connect("tcp://" + pred_addr)
            self.sock_req.send_json({"command": "UPD_FING", "params": { "new_node_addr" : self.addr, "new_node_id" : self.id, "index_to_actualize" : i -1, "addr_requester": self.addr }, "procedence": (self.addr, "update_others")})
            print(self.sock_req.recv_json()) 
            self.sock_req.disconnect("tcp://" + pred_addr)
            #print("No puedo creer que haya muerto")
        
    #MANTRA : "Do not use or close sockets except in the thread that created them."
    def waiting_for_command(self):
        self.sock_rep = self.context_sender.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
        ###print(self.id, " ", self.addr, "\n", self.finger_table,  "\n", self.predeccesor_id, " ", self.predeccesor_addr, "\n", self.succ_id, " ", self.succ_addr) 

        while True:            
            
            print("waiting")
            buff = self.sock_rep.recv_json()
                                    
            if buff['command'] in self.commands:
                #if buff['command'] != "CLOSEST_PRED_FING":                
                print(buff) 
                
                self.commands[buff["command"]](**buff["params"])
                                    
    def find_succesor(self, id):
        if self.is_introduction_node: return (self.id, self.addr, self.id, self.addr)
        (predeccesor_id, predeccesor_addr)  = self.find_predecesor(id)        
        
        print("\tfind_succ ", (predeccesor_addr, self.addr))        
        if predeccesor_addr != self.addr:
                        
            print(predeccesor_addr, predeccesor_id, "inside the if", sep = ' ')            
            self.sock_req.connect("tcp://"+ predeccesor_addr)
            self.sock_req.send_json({"command" : "GET_SUCC", "params": {}})
            ##print("ACA find_succesor")
            buff = self.sock_req.recv_json()
            to_return = (predeccesor_id, predeccesor_addr) + tuple (buff["return_info"])
            ##print(buff)
            self.sock_req.disconnect("tcp://"+ predeccesor_addr)

        else:            
            to_return = (predeccesor_id, predeccesor_addr) + tuple (self.finger_table[0])
            #print("lolololol", to_return)
        if id == predeccesor_id:
            print("predeccesor_id == id ", (id,predeccesor_id))
            if self.addr == predeccesor_addr:
                to_return = (self.predeccesor_id, self.predeccesor_addr, self.id, self.addr)                
            else:
                print("else aquí")
                self.sock_req.connect("tcp://"+ predeccesor_addr)
                self.sock_req.send_json({"command" : "GET_PRED", "params": {}})
                buff = self.sock_req.recv_json()
                to_return = tuple(buff["return_info"]) + (predeccesor_id,predeccesor_addr)
                self.sock_req.disconnect("tcp://"+ predeccesor_addr)

        print("find_succesor to_return", to_return)

        return  to_return
        

    #En la manere en que pregunto el predecesor evito tener que forgardear un mensaje demasiadas veces.
    def find_predecesor(self, id):
        current_id = self.id
        current_succ_id = self.finger_table[0][0]
        current_addr = self.addr  #se supone que sea la direccin del nodo al que le pido el closest pred fing, en la primea iter no lo necesito.
        print("find_predecesor ",id, current_id, current_succ_id, sep = " ")
        while not self.belongs_to(id, interval = (current_id, current_succ_id)) :
            if current_id != self.id:
                self.sock_req.connect("tcp://" + current_addr)
                self.sock_req.send_json({"command": "CLOSEST_PRED_FING", "params": { "id": id, "is_a_request": True }})
                buff = self.sock_req.recv_json()
                self.sock_req.disconnect("tcp://" + current_addr)
                current_id, current_addr = buff['return_info']
            else:
                current_id, current_addr = self.closest_pred_fing(id)
            print("inside the while ", id, current_id, current_succ_id, current_addr)
            if current_addr != self.addr:
                self.sock_req.connect("tcp://" + current_addr)
                self.sock_req.send_json({"command": "GET_SUCC", "params": {}})
                buff = self.sock_req.recv_json()
                self.sock_req.disconnect("tcp://" + current_addr)
                current_succ_id, current_succ_addr = buff['return_info']
            else:
                current_succ_id, current_succ_addr = self.finger_table[0]
            if current_succ_id == id: 
                return (current_succ_id, current_succ_addr)
            
        print((current_id, current_addr), "  ", (current_succ_id))            
        return current_id, current_addr

    def closest_pred_fing(self, id, is_a_request = False):        
        print("\t", self.id, id, 'en closest_pred_fing', sep = ' ')

        for i in range(self.m-1, -1, -1):
            print("\t", self.finger_table[i], "\t", (self.id, id))
            if self.finger_table[i][0] == self.id : continue
            if self.belongs_to(self.finger_table[i][0], (self.id, id)) :
                #print("ME moriiiiiiii ", is_a_request)
                if is_a_request:
                    self.sock_rep.send_json({"response": "ACK", "return_info": tuple(self.finger_table[i]), "procedence": self.addr })
                    return
                else:
                    #print("este es el tipo en el for", self.finger_table[i])
                    return self.finger_table[i]
        
        #print("este es el tipo ", (self.id, self.addr, is_a_request))
        if is_a_request:
            self.sock_rep.send_json({"response": "ACK", "return_info": (self.id, self.addr), "procedence": self.addr})
        else:
            return (self.id, self.addr)
        
    
if len(sys.argv) > 3:
    n = Node(introduction_node = sys.argv[1], addr= sys.argv[2], id = int(sys.argv[3]))
    
else:
    n = Node(addr = sys.argv[1], id = int(sys.argv[2]))
    
