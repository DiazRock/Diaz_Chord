import zmq
from time import time
from time import sleep
import sys
import json
import threading
from sched import scheduler
from utils import request, wrapper_request, property_request, initial_request, put_final_message
succ = None

#Una porción del código que le envíe la información
#a la otra pidiéndole por el sucesor.
#Un código que empiece, ¿qué puede hacer?
#Empezar insertando un nodo, o cayendo en waiting_for_command
#Me espera una noche intensa!!!!
'''
AHORA NECESITO QUE CADA REQUEST TENGA EN CUENTA QUE PUEDE FALLAR (PINGA....... :( )
'''

        
#Si cada diez seg no hay un comando, stabilize, si me entraron un comando, hasta que no me diga 
# que una rutina terminó bien, continuar haciendo request, a pesar de que pasen 10 seg., 
# si el cliente envía un mensaje de succesfull_request, entonces ejecuto stabilize,y vuelvo
# a la rutina de cada 10 seg.        

# Una solucion para que cuando un nodo muera no puede ser tener dentro de él
# la info de quién lo tiene como referencia, porque si el tipo se muere, cómo
# le va a decir a sus referenciados que él está muerto, a menos que sus sucesores
# sepan a quiénes él refrencia. Otra solucion es hacer un PUB/SUB sockets-messager.
# Hacer un PUB del lado del nodo que solicita al que muere, y que el resto de los nodos
# hagan suscribe (pero esto debe ser en un hilo aparte, para que no se bloquee el flujo del
# programa). En fin, que ahora lo que se debe hacer es emitir un mensajito mierdero, que diga:
# moriiiii.
class Node:
    def __init__(self, addr, id, introduction_node = None, introduction_id = None):        
        self.addr = addr
        self.id = id        
        self.context_sender = zmq.Context()
        self.m = 3
        self.length_succ_list = 3
        self.constant_time_wait = 20
        self.succ_list = [(self.id, self.addr) for i in range(self.length_succ_list)]
        self.start = lambda i : (self.id + 2**(i)) % 2**self.m
        self.finger_table = []
        self.waiting_time = 10
        
        #self.inside = lambda id, interval : id >= interval[0] and id < interval[1]
        #Esto está mal, porque no mide bien la pertenencia al intervalo si este cle da la vuelta al círculo.
        #self.belongs_to = lambda id, interval: self.inside(id, interval) if interval[0] < interval[1] else not self.inside(id, interval)
        self.commands = {"JOIN": self.answer_to_join, "FIND_SUCC": self.find_succesor, "FIND_PRED" : self.find_predecesor_wrapper, "GET_SUCC_LIST": self.get_succ_list, "SET_SUCC": self.set_as_succ, "UPD_FING" : self.update_finger_table, "CLOSEST_PRED_FING": self.closest_pred_fing, "ALIVE": self.alive, "GET_PARAMS": self.get_params, "GET_PROP": self.get_prop, "SET_PRED" : self.set_pred, "BELONG": self.belongs_messager, "CHANGE_INTRO": self.change_intro, "GET_PRED": self.get_pred, "STAB": self.stabilize, "RECT": self.rectify, "SUCC_REQ" : self.alive }
        
        self.is_introduction_node = not introduction_node
        if not self.is_introduction_node:
            first_request = initial_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr} , destination_addr= introduction_node, destination_id = introduction_id, context = self.context_sender)
            recieved_json = first_request.make_request()
            #print("En Node.__init__") 
            while recieved_json is first_request.error_json:                
                recieved_json = first_request.action_for_error()
            
            print(recieved_json)
            if recieved_json['return_info']['need_principal']:
                print("Warning: There's not enough principal nodes in the Chord ring. You need to be careful\nmientras este warning se emita y cuidar que no se desconecten los nodos entrados.\nOtherwise, there's no guaranty of a correct function of the protocol.")
                self.isPrincipal = True
            
            
            self.execute_join(introduction_node, introduction_id, self.start(0))
            #self.predeccesor_id = recieved_json['return_info']['pred_id']
            #self.predeccesor_addr = recieved_json['return_info']['pred_addr']
            #self.succ_list = recieved_json['return_info']['succ_list']

                
            ''' self.sock_req.connect("tcp://" + introduction_node)  #¿Qué pasa si el succ_node ya tiene un sucesor? Mejor dicho, si ya hay alguien para quien él es un sucesor.            
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
             '''    #print("sobreviví")
            
            #self.succ_addr, self.succ_id, self.predeccesor_addr, self.predeccesor_id = buff['return_info']         
                        
        else:
            self.predeccesor_addr, self.predeccesor_id = self.addr, self.id
            self.finger_table = [(self.id, self.addr) for i in range(self.m)]
            self.isPrincipal = True
            
        #print(self.finger_table)
        self.wrapper_action()


    def stabilize(self):
        print("I enter to stabilize")
        if self.predeccesor_addr == self.addr: return
        #print("pasé del if de stabilize")
        #stabilize from succesor.
        succ_pred_request = property_request(json_to_send = {"command_name" : "GET_PRED", "method_params" : {}, "procedence_addr" : self.addr}, destination_addr = self.succ_list[0][1], context = self.context_sender, destination_id = self.succ_list[0][0])
        recv_json_pred = succ_pred_request.make_request(requester_obj = self,  asked_properties = ('predeccesor_id', 'predeccesor_addr'))
        
        while recv_json_pred is succ_pred_request.error_json and self.succ_list:
            self.succ_list.pop(0)
            succ_pred_request.destination_id, succ_pred_request.destination_addr = self.succ_list[0]
            recv_json_pred = succ_pred_request.make_request(requester_obj = self,  asked_properties = ('predeccesor_id', 'predeccesor_addr'))
        succ_list_request = property_request( json_to_send = {'command_name' : "GET_SUCC_LIST", 'method_params' : {}, 'procedence_addr' : self.addr}, destination_addr = self.succ_list[0][1], destination_id = self.succ_list[0][0], context = self.context_sender )
        recv_json_succ_list = succ_list_request.make_request(requester_obj = self, asked_properties = ("succ_list",))
        #print(recv_json_succ_list)
        print("recv_json_succ_list ", recv_json_succ_list)
        self.succ_list =  [self.succ_list[0]] + recv_json_succ_list['return_info']["succ_list"][:-1]
        if self.belongs_to(recv_json_pred['return_info']['predeccesor_id'], interval = (self.id, self.succ_list[0][0])):
            pred_succ_list_request = property_request( json_to_send ={"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr}, destination_id = recv_json_pred['return_info']['predeccesor_id'], destination_addr = recv_json_pred['return_info'][ 'predeccesor_addr'], context = self.context_sender)
            recv_json_pred_succ_list = pred_succ_list_request.make_request(requester_obj = self, asked_properties = ('succ_list',) )
            if not recv_json_pred_succ_list is pred_succ_list_request.error_json:
                self.succ_list = [[recv_json_pred['return_info']['predeccesor_id'], recv_json_pred['return_info']['predeccesor_addr']]] + recv_json_pred_succ_list['return_info']['succ_list'][:-1]
                
            else:
                pred_succ_list_request.action_for_error()
        print("How many more times I send to rectify the succesor. succ_list ", self.succ_list)
        rectify_request = request(json_to_send = {"command_name" : "RECT", "method_params" : { "predeccesor_id": self.id, "predeccesor_addr" : self.addr }, "procedence_addr" : self.addr}, destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1], context = self.context_sender)
        if rectify_request.make_request() is rectify_request.error_json:
            rectify_request.action_for_error()
        
        


    def between(self, id, interval):
        if interval[0] < interval[1]:
            return id > interval[0] and id < interval[1] 
        return id > interval[0] or id < interval[1]

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

    def ask_property(self, addr_ask, command_name, dict_params, procedence_addr):
        
        pass


    def rectify(self, predeccesor_id, predeccesor_addr):
        print("in rectify ", (predeccesor_id, predeccesor_addr))
        if self.belongs_to(predeccesor_id, interval = (self.predeccesor_id, self.id)) or self.id == self.predeccesor_id:
            print('rectify inside if ')
            self.predeccesor_id, self.predeccesor_addr = predeccesor_id, predeccesor_addr
        else:
            alive_pred_request = request( json_to_send = {"command_name" : "ALIVE", "method_params" : {}, "procedence_addr" : self.addr}, destination_id = self.predeccesor_id, destination_addr = self.predeccesor_addr, context = self.context_sender)
            recv_json_alive = alive_pred_request.make_request()
            if recv_json_alive is alive_pred_request.error_json:
               self.predeccesor_id, self.predeccesor_addr = predeccesor_id, predeccesor_addr 
            else:
                alive_pred_request.action_for_error()
        print("mando el response")
        self.sock_rep.send_json( { "response": "ACK" } )
        

    def answer_to_join(self):        
        self.sock_rep.send_json({"response": "ACK_to_join", "return_info": {"need_principal": self.isPrincipal and self.has_repeated(self.succ_list) }})
        
    def has_repeated(self, l):
        for i in range(len(l)):
            for j in range(i+1, len(l)):
                if l[i] == l[j]: 
                    #print((i,j), " ", (l[i],l[j]))
                    return True
        return False


    
    def execute_join(self, introduction_node, introduction_id, id_to_found_pred):
        
        print("execute_join before find_predecessor")        
        find_pred_request = wrapper_request ( json_to_send = {"command_name" : "FIND_PRED", "method_params" : {"id" : id_to_found_pred}, "procedence_addr" : self.addr}, destination_addr = introduction_node, destination_id = introduction_id, context = self.context_sender )
        recv_json = find_pred_request.make_request(requester_obj = self, method_for_wrap = "find_predecesor")
        if recv_json["response"] == "ERR":
            return recv_json
        print("Pasé el find_pred ", recv_json)
        self.predeccesor_id, self.predeccesor_addr = recv_json['return_info']['pred_id'], recv_json['return_info']['pred_addr']
        get_succ_list_request = property_request( json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr}, destination_addr = introduction_node, destination_id = introduction_id, context = self.context_sender) 
        recv_json = get_succ_list_request.make_request(requester_obj = self, asked_properties = "succ_list")

        if recv_json["response"] == "ERR":
            return recv_json
        self.succ_list = recv_json['return_info']['succ_list']
        
        print("JOIN SUCCESS!!!")
        #succ_list_request = property_request(command_name = "GET_SUCC", method_params = {}, procedence_addr = self.addr)
        
        #if self.is_introduction_node:
        #    #Estas dos son instrucciones para stabilize, no para este momento.
        #    self.predeccesor_id, self.predeccesor_addr = ( id_node, addr_node)
        #    self.finger_table[0]= ( id_node, addr_node)
        #    
        #else:
        #    #print("MORRRIIIIIIIIIIIII ", (pred_addr, self.addr))
        #    if self.addr != pred_addr:                
        #        self.sock_req.connect("tcp://" + pred_addr)
        #        self.sock_req.send_json({"command": "GET_SUCC", "params": { }})
        #        buff_0 = self.sock_req.recv_json()            
        #        temp_succ_addr = buff_0['return_info'][1]
        #        self.sock_req.send_json ( {"command" : 'SET_SUCC', "params" : { "id_succ": id_node, "addr_succ": addr_node }})
        #        self.sock_req.recv_json()
        #        self.sock_req.disconnect("tcp://" + pred_addr)
        #    else:
        #        print("Aquí anduve else")
        #        temp_succ_addr = self.finger_table[0][1]
        #        self.finger_table[0] = (id_node, addr_node)
#
        #    print("Pasé el if ", temp_succ_addr)
        #    if temp_succ_addr != self.addr:
        #        self.sock_req.connect("tcp://" + temp_succ_addr)
        #        self.sock_req.send_json({"command": "SET_PRED", "params": { "id_pred": id_node, "addr_pred" : addr_node }})
        #        self.sock_req.recv_json()
        #        self.sock_req.disconnect("tcp://" + temp_succ_addr)
        #    else:
        #        self.predeccesor_id, self.predeccesor_addr = (id_node, addr_node)
        ##Voy a anotar lo que primero debo hacer mañana:
        ##Debo insertar un nodo nuevo, buscar su pred, al pred pedirle el succ, luego para el pred el nuevo succ es node, y para el succ el pred es el nuevo. 
        ##print("CCCCCCCC")
        #self.fill_table(id_node, finger_table, start_indexes)
        #if self.is_introduction_node: self.fill_table(self.id, self.finger_table, [self.start(i) for i in range(self.m)] )
        #
        #print("join", finger_table, sep = ' ')
        #return_json.update({"response": "ACK" , "return_info": {"finger_table" : finger_table, "pred_id": pred_id, "pred_addr" : pred_addr, "succ_id" : self.id, "succ_addr": self.addr }, "procedence": (self.id, self.addr), "method" : 'join' })
        #
        #self.sock_rep.send_json(return_json)

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
        self.sock_rep.send_json({"response": "ACK", "return_info": {"finger_table" : self.finger_table, "predeccesor_addr" : self.predeccesor_addr, "predeccesor_id" : self.predeccesor_id, "succ_list" : self.succ_list } })

    def get_prop(self, prop_name):
        if prop_name == "start_indexes":
            self.sock_rep.send_json({'response': "ACK", "return_info" : [self.start(i) for i in range(self.m)] })    

        self.sock_rep.send_json({'response': 'ACK', "return_info": self.__dict__[prop_name] })

    def set_as_succ(self, id_succ, addr_succ):        
        self.finger_table[0] = (id_succ, addr_succ)
        self.sock_rep.send_json({"response": "ACK", "procedence": "set_as_succ"})

    def get_pred(self):
        self.sock_rep.send_json({"response": "ACK", "return_info": {"predeccesor_id" : self.predeccesor_id, "predeccesor_addr" : self.predeccesor_addr } } )


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
            #if self.predeccesor_addr != addr_requester:
                
                #print("UPD_FING ", "id:", self.id, "predeccesor_id:", self.predeccesor_id, "predeccesor_addr:", self.predeccesor_addr, "addr:", self.addr, sep = ' ')
                #self.sock_req.connect("tcp://" + self.predeccesor_addr)                    
                #self.sock_req.send_json({"command" : "UPD_FING", "params": {"new_node_addr": new_node_addr, "new_node_id": new_node_id, "index_to_actualize": index_to_actualize, "addr_requester": addr_requester }, "procedence": ("update_finger_table", self.addr) } )
                #self.sock_req.recv_json()
                #self.sock_req.disconnect("tcp://"+self.predeccesor_addr)
            #print("en update_finger_table", self.addr, self.predeccesor_addr, sep = ' ')
        
        
        self.sock_rep.send_json({"response": "ACK", "procedence": self.addr})


    def get_succ_list(self):
        
        self.sock_rep.send_json( {"response": "ACK", "return_info": {"succ_list" : self.succ_list} } )
    
    def change_intro(self):
        val = self.is_introduction_node
        self.is_introduction_node = False
        self.sock_rep.send_json({"response" : "ACK", "return_info": val})

    # Este método es para los nodos que necesitan
    # ser actualizados a partir del nuevo que entró.    
    def update_others(self , introduction_node = False):
        
        #print("update_others client_side")
        for i in range( 1, self.m + 1):
            #print("update_others ", (self.id - 2**(i-1))%2**self.m)            
            pred_id, pred_addr =  self.find_predecesor((self.id - 2**(i-1))% 2**self.m)
            if pred_addr == self.addr: continue     #Este if está aquí porque uno se puede tener en su finger_table. Sobre todo cuando halla m o menos nodos en la red.
            #print("en update_others, conéctate a ", ( pred_addr), " ", (self.id - 2**(i-1))%2**self.m) 
            request_update = request(json_to_send = {"command": "UPD_FING", "params": { "new_node_addr" : self.addr, "new_node_id" : self.id, "index_to_actualize" : i -1, "addr_requester": self.addr }, "procedence": (self.addr, "update_others")}, destination_addr = pred_addr, destination_id = pred_id, context = self.context_sender )
            recieved_json = request_update.make_request()
            while recieved_json is request_update.error_json:
                request_update.action_for_error()
                request_update.destination_id, request_update.destination_addr =  self.find_predecesor((self.id - 2**(i-1))% 2**self.m)
                if request_update.destination_addr == self.addr : continue
                recieved_json = request_update.make_request()
            
            #print("No puedo creer que haya muerto")
        
    #MANTRA : "Do not use or close sockets except in the thread that created them."
    # This method is the articulation point of the all engine.
    # It waits for the commands proceding from other nodes, and excecutes the corresponding
    # rutine. Also this method excecutes the waiting_for_command function in other thread. 
    # 



    #Razón por la que hace falta un hilo. Supongamos que tenemos un nodo en rol de cliente haciendo join, 
    # que es un conjunto de requests, 
    # a un nodo s haciendo rol de server (el que lo recibe en la red). 
    # Digamos que cada paso del join puede demorarse más del waiting_time asingado
    # para activar el stabilize. Esto significa que entre un paso y otro del join 
    # yo mando a hacer s.stabilize, por lo cual el nodo s está ocupado para el próximo
    # request del join. Ahora, ¿qué pasa, si el stabilize demora tanto, que el nodo cliente
    # no recibe una respuesta en el tiempo necesario y considera al nodo s muerto, cuando en
    # realidad no lo está? Pues que estamos en presencia de un comportamiento inesperado.
    # Es por ello que necesitamos un hilo. Para que la capacidad de respuesta del nodo s
    # no se vea interrumpida.
    
    def wrapper_action(self):
        self.stabilize()        
        thr_stabilize = threading.Thread(target = self.wrapper_loop_stabilize, args =() )
        thr_stabilize.start()        
        self.waiting_for_command()
        

    def wrapper_loop_stabilize(self):
        countdown = time()
        while True:
            if abs (countdown - time()) > self.waiting_time:
                countdown = time()
                self.stabilize()
        

    def waiting_for_command(self):
        
        self.sock_rep = self.context_sender.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
        ###print(self.id, " ", self.addr, "\n", self.finger_table,  "\n", self.predeccesor_id, " ", self.predeccesor_addr, "\n", self.succ_id, " ", self.succ_addr)    
        
        print ("waiting")
        while True:
            
            #while abs(countdown - time()) < self.waiting_time:        
        
            buff = self.sock_rep.recv_json()                    
            if buff['command_name'] in self.commands:
                print(buff)                        
                self.commands[buff["command_name"]](**buff["method_params"])
            
            
        self.sock_rep.close()        


    def find_succesor(self, id):
        #if self.is_introduction_node: return (self.id, self.addr, self.id, self.addr)
        #(predeccesor_id, predeccesor_addr)  = self.find_predecesor(id)        
        #
        #print("\tfind_succ ", (predeccesor_addr, self.addr))        
        #if predeccesor_addr != self.addr:
        #                
        #    print(predeccesor_addr, predeccesor_id, "inside the if", sep = ' ')            
        #    self.sock_req.connect("tcp://"+ predeccesor_addr)
        #    self.sock_req.send_json({"command" : "GET_SUCC", "params": {}})
        #    ##print("ACA find_succesor")
        #    buff = self.sock_req.recv_json()
        #    to_return = (predeccesor_id, predeccesor_addr) + tuple (buff["return_info"])
        #    ##print(buff)
        #    self.sock_req.disconnect("tcp://"+ predeccesor_addr)
#
        #else:            
        #    to_return = (predeccesor_id, predeccesor_addr) + tuple (self.finger_table[0])
        #    #print("lolololol", to_return)
        #if id == predeccesor_id:
        #    print("predeccesor_id == id ", (id,predeccesor_id))
        #    if self.addr == predeccesor_addr:
        #        to_return = (self.predeccesor_id, self.predeccesor_addr, self.id, self.addr)                
        #    else:
        #        print("else aquí")
        #        self.sock_req.connect("tcp://"+ predeccesor_addr)
        #        self.sock_req.send_json({"command" : "GET_PRED", "params": {}})
        #        buff = self.sock_req.recv_json()
        #        to_return = tuple(buff["return_info"]) + (predeccesor_id,predeccesor_addr)
        #        self.sock_req.disconnect("tcp://"+ predeccesor_addr)
#
        #print("find_succesor to_return", to_return)

        #return  to_return
        pass

    def find_predecesor_wrapper(self, id):
        pred_id, pred_addr = self.find_predecesor(id)
        self.sock_rep.send_json({"response": "ACK", "return_info": {"pred_id": pred_id, "pred_addr": pred_addr}, "procedence": self.addr } )
        pass

    #En la manere en que pregunto el predecesor evito tener que forgardear un mensaje demasiadas veces.
    def find_predecesor(self, id):
        current_id = self.id
        current_succ_id, current_succ_addr = self.succ_list[0]
        current_addr = self.addr  #se supone que sea la direccin del nodo al que le pido el closest pred fing, en la primea iter no lo necesito.
        print("find_predecesor ",id, current_id, current_succ_id, sep = " ")
         
        while not self.belongs_to(id, interval = (current_id, current_succ_id)) and current_id != current_succ_id :
            print('while find_predecesor')    
            closest_pred_fing_req = wrapper_request(json_to_send = {"command_name" : "CLOSEST_PRED_FING", "method_params" : {"id": id}, "procedence_addr" : self.addr}, destination_addr = current_addr, destination_id = current_id, context = self.context_sender)
            recv_json_closest = closest_pred_fing_req.make_request(requester_obj = self, method_for_wrap = "closest_pred_fing")
            if recv_json_closest is closest_pred_fing_req.error_json:
                #Aquí hay que hacer un action error que elimine toda referencia a current_addr en el anillo.
                closest_pred_fing_req.action_for_error()
                current_id, current_addr = current_succ_id, current_succ_addr
                continue
            #current_id, current_addr = recv_json_closest['return_info']['closest_id'],recv_json_closest['return_info']['closest_addr']
            #if current_id != self.id:
            #    self.sock_req.connect("tcp://" + current_addr)
            #    self.sock_req.send_json({"command": "CLOSEST_PRED_FING", "params": { "id": id, "is_a_request": True }})
            #    buff = self.sock_req.recv_json()
            #    self.sock_req.disconnect("tcp://" + current_addr)
            #    current_id, current_addr = buff['return_info']
            #else:
            #    current_id, current_addr = self.closest_pred_fing(id)
            #    print("inside the while ", id, current_id, current_succ_id, current_addr)
            #SI PEDIR EL SUCESOR DE CURRENT_SUCC FALLA, SIGNIFICA QUE CURRENT_SUCC ESTA MUERTO, EN CUYO CASO HAY QUE BUSCAR OTRO SUCC DE LA LISTA DE CURRENT

            succ_list_request = property_request( json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr}, destination_addr = recv_json_closest['return_info']['closest_addr'], destination_id = recv_json_closest['return_info']['closest_id'], context = self.context_sender)
            recv_json_succ = succ_list_request.make_request(requester_obj = self, asked_properties = "succ_list")
            if recv_json_succ is succ_list_request.error_json:
                succ_list_request.action_for_error()
                continue
                #Voy a suponer que la lista de sucesores se estabiliza en stabilize
                #Tienes que cambiar el succ_list[0], y cuando vayas a succ_list.next(), tienes que \
                
            #if current_addr != self.addr:
            #    self.sock_req.connect("tcp://" + current_addr)
            #    self.sock_req.send_json({"command": "GET_SUCC", "params": {}})
            #    buff = self.sock_req.recv_json()
            #    self.sock_req.disconnect("tcp://" + current_addr)
            #    current_succ_id, current_succ_addr = buff['return_info']
            #else:
            #    current_succ_id, current_succ_addr = self.finger_table[0]
            if current_succ_id == id: 
                return (current_succ_id, current_succ_addr)
        #print("Tu pasaste por aqui find_predeccesor")    
        #print((current_id, current_addr), "  ", (current_succ_id))            
        return current_id, current_addr

    def closest_pred_fing_wrap (self, id):
        closest_id, closest_addr = self.closest_pred_fing(id)
        self.sock_rep.send_json({"response" : "ACK", "return_info" : {"closest_id" : closest_id, "closest_addr": closest_addr}, "procedence": self.addr})
        

    def closest_pred_fing(self, id):        
        #print("\t", self.id, id, 'en closest_pred_fing', sep = ' ')
        #el chiste aquí está en que cuando yo busque el closest, los finger tables van a estar acutalizados con los nodos en la red que se hallen vivos.
        for i in range(self.m-1, -1, -1):
            #print("\t", self.finger_table[i], "\t", (self.id, id))
            if self.finger_table[i][0] == self.id : continue
            if self.belongs_to(self.finger_table[i][0], (self.id, id)) :
                #print("ME moriiiiiiii ", is_a_request)                
                #print("este es el tipo en el for", self.finger_table[i])
                return self.finger_table[i]
        
        #print("este es el tipo ", (self.id, self.addr, is_a_request))
        
        return (self.id, self.addr)




if len(sys.argv) > 3:
    n = Node(introduction_node = sys.argv[1], introduction_id= sys.argv[2], addr= sys.argv[3], id = int(sys.argv[4]))
    
else:
    n = Node(addr = sys.argv[1], id = int(sys.argv[2]))
    
