import zmq
from time import time
from time import sleep
import sys
import json
import threading
from sched import scheduler
from utils import request, bcolors
from random import Random
succ = None

#Una porción del código que le envíe la información
#a la otra pidiéndole por el sucesor.
#Un código que empiece, ¿qué puede hacer?
#Empezar insertando un nodo, o cayendo en waiting_for_command
#NOTA 
#Puede ser que la notificación de la posición 0 de la finger table no llegue a tiempo a find_predecesor y esto causa problemas. 
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
        self.finger_table = [None for i in range(self.m)]
        self.waiting_time = 10
        
        self.commands = {"JOIN": self.answer_to_join, "FIND_SUCC": self.find_succesor, "FIND_PRED" : self.find_predecesor_wrapper, "GET_SUCC_LIST": self.get_succ_list, "CLOSEST_PRED_FING": self.closest_pred_fing_wrap, "ALIVE": self.alive, "GET_PARAMS": self.get_params, "GET_PROP": self.get_prop,  "CHANGE_INTRO": self.change_intro, "GET_PRED": self.get_pred, "STAB": self.stabilize, "RECT": self.rectify, "SUCC_REQ" : self.alive }        
        self.commands_that_need_request = {"RECT", "FIND_SUCC", "FIND_PRED", "CLOSEST_PRED_FING", "STAB"}
        self.is_introduction_node = not introduction_node
        client_requester = request(context = self.context_sender)
        if not self.is_introduction_node:            
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr}, destination_addr = introduction_node, destination_id = introduction_id)
            #print("En Node.__init__") 
            while recieved_json is client_requester.error_json:                
                client_requester.action_for_error(introduction_node)
                print("Enter address to retry ")
                introduction_id, introduction_node = input().split()
                introduction_id = int(introduction_id)
                print("Connecting now to ", (introduction_node, introduction_id))
                
                recieved_json = client_requester.make_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr}, destination_id = introduction_id, destination_addr = introduction_node)

            #print(recieved_json)
            if recieved_json['return_info']['need_principal']:
                print("Warning: There's not enough principal nodes in the Chord ring. You need to be careful\nmientras este warning se emita y cuidar que no se desconecten los nodos entrados.\nOtherwise, there's no guaranty of a correct function of the protocol.")
                self.isPrincipal = True
            
            
            while not self.execute_join(introduction_node, introduction_id, self.start(0), client_requester):
                client_requester.action_for_error(introduction_node)
                print("Enter address to retry ")
                introduction_id, introduction_node = input().split()
                introduction_id = int(introduction_id)
                print("Connecting now to ", (introduction_id, introduction_node))                
                recieved_json = client_requester.make_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr}, destination_id = introduction_id, destination_addr = introduction_node)
                        
        else:
            self.predeccesor_addr, self.predeccesor_id = self.addr, self.id
            
            self.isPrincipal = True
            
        
        self.wrapper_action(client_requester)


    def stabilize(self, sock_req : request):
        
        #print("pasé del if de stabilize")
        #stabilize from succesor.
        
        
        #El get_pred se va a ir posiblemente y me voy a quedar con el find_predeccesor.
        recv_json_pred = sock_req.make_request(json_to_send = {"command_name" : "GET_PRED", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method": "stabilize_95"}, requester_object = self, asked_properties = ('predeccesor_id', 'predeccesor_addr'), destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1])
        if recv_json_pred is sock_req.error_json:
            #self.succ_list.pop(0)
            #Aquí retorno y espero a la próxima vuelta.
            print('expulsé a ',self.succ_list.pop(0))
            return
                      
        recv_json_succ_list = sock_req.make_request(json_to_send = {'command_name' : "GET_SUCC_LIST", 'method_params' : {}, 'procedence_addr' : self.addr, "procedence_method": "stabilize_104"}, requester_object = self, asked_properties = ("succ_list",), destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1])
        if recv_json_succ_list is sock_req.error_json: return        
        self.succ_list = [self.succ_list[0]] + recv_json_succ_list['return_info']["succ_list"][:-1]
                    
        if self.between(recv_json_pred['return_info']['predeccesor_id'], interval = (self.id, self.succ_list[0][0]) ):
                                    
            recv_json_pred_succ_list = sock_req.make_request( json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method":  "stabilize_109"}, requester_object = self, asked_properties = ('succ_list',), destination_id = recv_json_pred['return_info'][ 'predeccesor_id'], destination_addr = recv_json_pred['return_info'][ 'predeccesor_addr'])
            if not recv_json_pred_succ_list is sock_req.error_json:
                
                self.succ_list = [[recv_json_pred['return_info']['predeccesor_id'], recv_json_pred['return_info']['predeccesor_addr']]] + recv_json_pred_succ_list['return_info']['succ_list'][:-1]
                
            else:
                print("Aquí fue que murió ", {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr}, " ", sock_req )
                print("sending error in stabilize from predeccesor")

                sock_req.action_for_error( recv_json_pred['return_info']['predeccesor_addr'] )
                
        if sock_req.make_request(json_to_send = {"command_name" : "RECT", "method_params" : { "predeccesor_id": self.id, "predeccesor_addr" : self.addr }, "procedence_addr" : self.addr}, destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1] ) is sock_req.error_json:
            print("Aquí fue que murió intentando rectify en stabilize", {"command_name" : "RECT", "method_params" : { "predeccesor_id": self.id, "predeccesor_addr" : self.addr }, "procedence_addr" : self.addr}, " ", sock_req )
            sock_req.action_for_error(self.succ_list[0][1])
        
        

    def between(self, id, interval):
        if interval[0] < interval[1]:
            return id > interval[0] and id < interval[1] 
        return id > interval[0] or id < interval[1]

        
    
    def ask_property(self, addr_ask, command_name, dict_params, procedence_addr):
        
        pass


    def rectify(self, predeccesor_id, predeccesor_addr, sock_req):
        #print("in rectify ", (predeccesor_id, predeccesor_addr))
        if self.between(predeccesor_id, interval = (self.predeccesor_id, self.id)) or self.id == self.predeccesor_id:
            #print('rectify inside if ')
            if self.predeccesor_id == self.id: 
                self.succ_list[0] = (predeccesor_id, predeccesor_addr)                
            self.predeccesor_id, self.predeccesor_addr = predeccesor_id, predeccesor_addr

        else:
            #print("en el else de rectify")            
            #print("I'm going to send ALIVE command to ", (predeccesor_id, predeccesor_addr))            
            recv_json_alive = sock_req.make_request(json_to_send = {"command_name" : "ALIVE", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method": "rectify"}, destination_id = self.predeccesor_id, destination_addr = self.predeccesor_addr)
            #print(sock_req.error_json, " ", recv_json_alive)
            if recv_json_alive is sock_req.error_json:
                   
                self.predeccesor_id, self.predeccesor_addr = predeccesor_id, predeccesor_addr             
                sock_req.action_for_error(self.predeccesor_addr)
        #print("mando el response")
        self.sock_rep.send_json( { "response": "ACK" } )
        

    def answer_to_join(self):        
        self.sock_rep.send_json({"response": "ACK_to_join", "return_info": {"need_principal": self.isPrincipal and self.has_repeated(self.succ_list) }})
        
    def has_repeated(self, l):
        for i in range(len(l)):
            for j in range(i+1, len(l)):
                if l[i] == l[j]: 
                    
                    return True
        return False


    
    def execute_join(self, introduction_node, introduction_id, id_to_found_pred, sock_req):
        
        #print("execute_join before find_predecessor")
        #print("en execute_join ", id_to_found_pred)
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "FIND_PRED", "method_params" : {"id" : id_to_found_pred}, "procedence_addr" : self.addr}, requester_object = self, method_for_wrap = "find_predecesor", destination_id = introduction_id, destination_addr = introduction_node)
        if recv_json is sock_req.error_json:
            return False
        #print("Pasé el find_pred ", recv_json)
        self.predeccesor_id, self.predeccesor_addr = recv_json['return_info']['pred_id'], recv_json['return_info']['pred_addr']        
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr}, requester_object = self, asked_properties = "succ_list", destination_id = recv_json['return_info']['pred_id'], destination_addr = recv_json['return_info']['pred_addr'] )         
        
        if recv_json is sock_req.error_json:
            return False
        self.succ_list = recv_json['return_info']['succ_list']
        
        
        return True
                    
    def get_params(self):        
        self.sock_rep.send_json({"response": "ACK", "return_info": {"finger_table" : self.finger_table, "predeccesor_addr" : self.predeccesor_addr, "predeccesor_id" : self.predeccesor_id, "succ_list" : self.succ_list } })

    def get_prop(self, prop_name):
        if prop_name == "start_indexes":
            self.sock_rep.send_json({'response': "ACK", "return_info" : [self.start(i) for i in range(self.m)] })    

        self.sock_rep.send_json({'response': 'ACK', "return_info": self.__dict__[prop_name] })

    
    def get_pred(self):
        self.sock_rep.send_json({"response": "ACK", "return_info": {"predeccesor_id" : self.predeccesor_id, "predeccesor_addr" : self.predeccesor_addr } } )


    def alive(self):
        self.sock_rep.send_json({"response": "ACK", "procedence_addr": self.addr})

    

    def get_succ_list(self):
        
        self.sock_rep.send_json( {"response": "ACK", "return_info": {"succ_list" : self.succ_list} } )
    
    def change_intro(self):
        val = self.is_introduction_node
        self.is_introduction_node = False
        self.sock_rep.send_json({"response" : "ACK", "return_info": val})

        
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
    
    def wrapper_action(self, client_requester):
        if self.addr != self.predeccesor_addr: self.stabilize(client_requester)        
        thr_stabilize = threading.Thread(target = self.wrapper_loop_stabilize, args =() )
        thr_stabilize.start()        
        self.waiting_for_command(client_requester)
        

    def wrapper_loop_stabilize(self):
        countdown = time()
        rand = Random()
        rand.seed()
        requester = request(context = self.context_sender)
        choices = [i for i in range(self.m)]
        while True:
            if abs (countdown - time( ) ) > self.waiting_time:
                if self.predeccesor_addr != self.addr:
                    self.stabilize(sock_req = requester)                
                    index = rand.choice( choices )                    
                    self.finger_table[ index ] = self.find_succesor(self.start(index), sock_req = requester)                
                countdown = time()
        

    def waiting_for_command(self, client_requester):
        
        self.sock_rep = self.context_sender.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
        ####print(self.id, " ", self.addr, "\n", self.finger_table,  "\n", self.predeccesor_id, " ", self.predeccesor_addr, "\n", self.succ_id, " ", self.succ_addr)    
        
        print ("waiting")
        while True:
            
            #while abs(countdown - time()) < self.waiting_time:                    
            buff = self.sock_rep.recv_json()

            if buff['command_name'] in self.commands:
                #print(buff, " recieved buff in the main thread")                        
                
                if buff['command_name'] in self.commands_that_need_request:
                    self.commands[buff["command_name"]](**buff["method_params"], sock_req = client_requester)
                else:
                    self.commands[buff["command_name"]](**buff["method_params"])
            
        self.sock_rep.close()        


    def find_succesor(self, id, sock_req):
        
        destination_id, destination_addr = self.find_predecesor(id, sock_req)
        if destination_id == id : return destination_id, destination_addr
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params": {}, "procedence_addr": self.addr, "procedence_method": "find_succesor_286"}, requester_object= self, asked_properties = ('succ_list', ), destination_id = destination_id, destination_addr = destination_addr ) 
        #print(recv_json)
        while recv_json is sock_req.error_json:
            print("Aquí fue que murió find_succesor", {"command_name" : "GET_SUCC_LIST", "method_params": {}, "procedence_addr": self.addr}, " ", sock_req)
            destination_id, destination_addr = self.find_predecesor(destination_id, sock_req)
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params": {}, "procedence_addr": self.addr}, requester_object= self, asked_properties = ('succ_list', ), destination_id = destination_id, destination_addr = destination_addr ) 
        
        return recv_json['return_info']['succ_list'][0]
            
    def find_predecesor_wrapper(self, id, sock_req):
        pred_id, pred_addr = self.find_predecesor(id, sock_req)

        self.sock_rep.send_json({"response": "ACK", "return_info": {"pred_id": pred_id, "pred_addr": pred_addr}, "procedence_addr": self.addr } )
        pass

    #En la manere en que pregunto el predecesor evito tener que forgardear un mensaje demasiadas veces.
    def find_predecesor(self, id, sock_req):
        current_id = self.id
        current_succ_id, current_succ_addr = self.succ_list[0]
        self.finger_table[0] = self.succ_list[0]
        current_addr = self.addr  #se supone que sea la direccin del nodo al que le pido el closest pred fing, en la primea iter no lo necesito.
        
         
        while not self.between(id, interval = (current_id, current_succ_id)) and current_id != current_succ_id :            
            
            #La primera vez que yo hago esto no tiene sentido preguntar si se cae la conexión. Porque estoy parado en self,
            #que es un nodo que nunca voy a asumir muerto. 
            #Pero si otro current_addr está muerto, la búsqueda puede dar error. ¿Esto, qué significa? 
            #El nuevo current_addr es resultado de closest_pred_fing. Es un finger que está muerto,
            #cosa que no es cómoda verificar aquí.
            recv_json_closest = sock_req.make_request(json_to_send = {"command_name" : "CLOSEST_PRED_FING", "method_params" : {"id": id}, "procedence_addr" : self.addr, "procedence_method": "find_predecesor"}, method_for_wrap = 'closest_pred_fing', requester_object = self, destination_id = current_id, destination_addr = current_addr)
            #print("en find_predeccesor ", recv_json_closest, " ", id, " ", (current_id, current_succ_id) )
            
            
            
            recv_json_succ = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method" : "find_predecesor" }, requester_object = self, asked_properties = ("succ_list", ), destination_id = recv_json_closest['return_info'][0], destination_addr = recv_json_closest['return_info'][1] )

            #Por acá estoy pidiendo info al finger_table que yo supongo está vivo. Que si no está vivo hay que hacer closest_pred_fing de nuevo.
            #Esto significa que puedo mandar a hacer continue sin miedo porque va a caer en closest_pred_fing y va a ver que el nodo está muerto.
            if recv_json_succ is sock_req.error_json:
                print("Aquí fue que murió find_predeccesor", {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method" : "find_predecesor" }, " ", sock_req)
                sock_req.action_for_error(recv_json_closest['return_info'][1])
                continue
                #Voy a suponer que la lista de sucesores se estabiliza en stabilize
                
            current_id, current_addr = recv_json_closest['return_info'][0], recv_json_closest['return_info'][1]
            current_succ_id, current_succ_addr = recv_json_succ['return_info']['succ_list'][0]    
            if current_succ_id == id: 
                return (current_succ_id, current_succ_addr)
        
        return current_id, current_addr

    def closest_pred_fing_wrap (self, id, sock_req):        
        closest_id, closest_addr = self.closest_pred_fing(id, sock_req)
        self.sock_rep.send_json({"response" : "ACK", "return_info" : (closest_id, closest_addr), "procedence": self.addr})
        

    def closest_pred_fing(self, id, sock_req):
        #print("\t ", self.finger_table)
        for i in range(self.m-1, -1, -1):            
            if self.finger_table[i] is None : continue #Este if puede existir si crean conflictos las últimas entradas de la finger table, aquellas que no tienen más dedos activos en ese intervalo.            
            if self.between(self.finger_table[i][0], (self.id, id) ) :
                #Aquí debo averiguar si el finger que estoy tomando está vivo o no.
                #print("\tte cogí ", self.finger_table[i])
                recv_json = sock_req.make_request(json_to_send = {"command_name": "SUCC_REQ", "method_params": {}, "procedence": self.addr}, destination_addr = self.finger_table[i][1], destination_id = self.finger_table[i][0])
                if not recv_json is sock_req.error_json:
                    return self.finger_table[i]
                #No me queda claro si esto se vuelve recursivo infinito.
                self.finger_table[i] = self.find_succesor(self.start(i), sock_req)
                i += 1

        return (self.id, self.addr)



if len(sys.argv) > 3:
    n = Node(introduction_node = sys.argv[1], introduction_id= sys.argv[2], addr= sys.argv[3], id = int(sys.argv[4]))
    
else:
    n = Node(addr = sys.argv[1], id = int(sys.argv[2]))
    
