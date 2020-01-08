import sys
import zmq
from time import time
import threading
import hashlib
from utils import request, bcolors
from random import Random
from functools import reduce


#Node class of the chord ring. Each node represents an active index in the ring.
#An index that store data about other index, maintinig only a simple invariant:
#each node, knows its succesor.
class Node:
    def __init__(self, addr, introduction_node = None):        
        self.addr = addr
        self.domain_addr = lambda value : reduce((lambda x,y : x + y), [x for x in value.split(":")[0].split(".") + [value.split(":")[1] ] ]) 
        self.turn_in_hash = lambda input_to_id : int(hashlib.sha1(bytes( self.domain_addr (input_to_id), 'utf-8') ).hexdigest(), 16 )
        self.id = self.turn_in_hash(addr)
        
        self.context_sender = zmq.Context()
        self.m = 64
        self.length_succ_list = 3        
        self.succ_list = [(self.id, self.addr) for i in range(self.length_succ_list)]
        self.start = lambda i : (self.id + 2**(i)) % 2**self.m
        self.finger_table = [None for i in range(self.m)]
        self.waiting_time = 10
        
        self.commands = {"JOIN": self.answer_to_join, "FIND_SUCC": self.find_succesor_wrapper, "FIND_PRED" : self.find_predecesor_wrapper, "GET_SUCC_LIST": self.get_succ_list, "CLOSEST_PRED_FING": self.closest_pred_fing_wrap, "ALIVE": self.alive, "GET_PARAMS": self.get_params, "GET_PROP": self.get_prop, "GET_PRED": self.get_pred, "STAB": self.stabilize, "RECT": self.rectify }        
        self.commands_that_need_request = {"RECT", "FIND_SUCC", "FIND_PRED", "CLOSEST_PRED_FING", "STAB"}
        
        client_requester = request(context = self.context_sender)
        if introduction_node:
            introduction_id = self.turn_in_hash(introduction_node)
            recieved_json = client_requester.make_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr}, destination_addr = introduction_node, destination_id = introduction_id)
            
            while recieved_json is client_requester.error_json:                
                client_requester.action_for_error(introduction_node)
                print("Enter address to retry ")
                introduction_node = input()
                introduction_id = self.turn_in_hash(introduction_node)            
                print("Connecting now to ", (introduction_node, introduction_id))
                
                recieved_json = client_requester.make_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr}, destination_id = introduction_id, destination_addr = introduction_node)
                        
            while not self.execute_join(introduction_node, introduction_id, self.start(0), client_requester):
                client_requester.action_for_error(introduction_node)
                print("Enter address to retry ")
                introduction_node = input()
                introduction_id = self.turn_in_hash(introduction_node)
                print("Connecting now to ", (introduction_id, introduction_node))                
                recieved_json = client_requester.make_request(json_to_send = {"command_name" : "JOIN", "method_params" : {}, "procedence_addr" : self.addr}, destination_id = introduction_id, destination_addr = introduction_node)
                        
        else:
            self.predeccesor_addr, self.predeccesor_id = self.addr, self.id
            
            self.isPrincipal = True
            
        
        self.wrapper_action(client_requester)

    
    #The stabilize step ask to the succesor of the node (self), for its predecessor.
    #If the predecessor_id of the succesor is between self.id and succesor.id, then
    #self has a new succesor: the predecessor of its succesor. In that case, self also
    #needs to actualize its succesor list, putting after its new succesor the elements
    #of the new succesor succ_list, but the last one. 
    def stabilize(self, sock_req : request):
                                       
        #If the succesor query fails, then we pop the death succesor, and 
        #return. It is important to know that if the length of the succ_list
        #is and we have k succesive fails in the Chord, we can't expect a good
        #working of the network.  
        recv_json_pred = sock_req.make_request(json_to_send = {"command_name" : "GET_PRED", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method": "stabilize_95"}, requester_object = self, asked_properties = ('predeccesor_id', 'predeccesor_addr'), destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1])
        if recv_json_pred is sock_req.error_json:            
            self.succ_list.pop(0)
            self.succ_list += [(self.id, self.addr)]                                    
            return

        recv_json_succ_list = sock_req.make_request(json_to_send = {'command_name' : "GET_SUCC_LIST", 'method_params' : {}, 'procedence_addr' : self.addr, "procedence_method": "stabilize_104"}, requester_object = self, asked_properties = ("succ_list",), destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1])
        if recv_json_succ_list is sock_req.error_json: return 
        
        self.succ_list = [self.succ_list[0]] + recv_json_succ_list['return_info']["succ_list"][:-1]
                    
        if self.between(recv_json_pred['return_info']['predeccesor_id'], interval = (self.id, self.succ_list[0][0]) ):
            

            recv_json_pred_succ_list = sock_req.make_request( json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method":  "stabilize_109"}, requester_object = self, asked_properties = ('succ_list',), destination_id = recv_json_pred['return_info'][ 'predeccesor_id'], destination_addr = recv_json_pred['return_info'][ 'predeccesor_addr'])
            if not recv_json_pred_succ_list is sock_req.error_json:

            #If it's true that self has a new succesor and this new succesor is alive, then self has to actualize its succ_list    
                self.succ_list = [[recv_json_pred['return_info']['predeccesor_id'], recv_json_pred['return_info']['predeccesor_addr']]] + recv_json_pred_succ_list['return_info']['succ_list'][:-1]                                       
        

    def between(self, id, interval):
        if interval[0] < interval[1]:
            return id > interval[0] and id < interval[1] 
        return id > interval[0] or id < interval[1]

        
        
    #The rectify step is executed by a notification of the predeccesor of the node.    
    def rectify(self, predeccesor_id, predeccesor_addr, sock_req):
        
        if self.between(predeccesor_id, interval = (self.predeccesor_id, self.id)) or self.id == self.predeccesor_id:
            
            if self.predeccesor_id == self.id: 

                self.succ_list[0] = (predeccesor_id, predeccesor_addr)                
            self.predeccesor_id, self.predeccesor_addr = predeccesor_id, predeccesor_addr

        else:
                        
            recv_json_alive = sock_req.make_request(json_to_send = {"command_name" : "ALIVE", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method": "rectify"}, destination_id = self.predeccesor_id, destination_addr = self.predeccesor_addr)
            
            if recv_json_alive is sock_req.error_json:
                   
                self.predeccesor_id, self.predeccesor_addr = predeccesor_id, predeccesor_addr             
                sock_req.action_for_error(self.predeccesor_addr)
        
        self.sock_rep.send_json( { "response": "ACK" } )
        

    def answer_to_join(self):        
        self.sock_rep.send_json({"response": "ACK_to_join", "return_info": {}})
        
    

    #The execute_join function introduce self in the Chord ring, founding the predecessor
    #of the self.id, and assigning it to self.predecessor value.
    def execute_join(self, introduction_node, introduction_id, id_to_found_pred, sock_req):
        
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "FIND_PRED", "method_params" : {"id" : id_to_found_pred}, "procedence_addr" : self.addr}, requester_object = self, method_for_wrap = "find_predecesor", destination_id = introduction_id, destination_addr = introduction_node)
        if recv_json is sock_req.error_json:
            return False
        
        self.predeccesor_id, self.predeccesor_addr = recv_json['return_info']['pred_id'], recv_json['return_info']['pred_addr']        
        recv_json = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr}, requester_object = self, asked_properties = "succ_list", destination_id = recv_json['return_info']['pred_id'], destination_addr = recv_json['return_info']['pred_addr'] )         
        
        if recv_json is sock_req.error_json:
            return False
        self.succ_list = recv_json['return_info']['succ_list']
        
        
        return True
     
    #Those are auxliar methods for see some properties from the client side                
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
    
    #In here I created the threads for the activies of the node.
    #The main reason why I did it is because of avialability.
    #The node must to stabilize frecuently, after a period of time (self.waiting_time), 
    #and during that time it needs to be open to request from other nodes. 
    def wrapper_action(self, client_requester):
        
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
                	#Periodically, the node stabilize its information about the network,
                	#and actuallize a finger table, that is an optmizitation for found succesors,
                	#in a better way.
                    self.stabilize(sock_req = requester)
                    #Independetly of the result of the stabilize, the node sends a notify message to its succesor, asking for a rectification.
                    requester.make_request(json_to_send = {"command_name" : "RECT", "method_params" : { "predeccesor_id": self.id, "predeccesor_addr" : self.addr }, "procedence_addr" : self.addr, "procedence_method": "wrapper_loop_stabilize", "time": time()}, destination_id = self.succ_list[0][0], destination_addr = self.succ_list[0][1])
                    index = rand.choice( choices )                    
                    self.finger_table[ index ] = self.find_succesor(self.start(index), sock_req = requester)                    
                countdown = time()
        

    #In this method the nodes bind its address for possible connections, and recieve messages.
    def waiting_for_command(self, client_requester):
        
        self.sock_rep = self.context_sender.socket(zmq.REP)
        self.sock_rep.bind("tcp://" + self.addr)    
                
        while True:
            
            
            buff = self.sock_rep.recv_json()

            if buff['command_name'] in self.commands:
                
                if buff['command_name'] == "FIND_SUCC": print(buff)
                if buff['command_name'] in self.commands_that_need_request:
                    self.commands[buff["command_name"]](**buff["method_params"], sock_req = client_requester)
                else:
                    self.commands[buff["command_name"]](**buff["method_params"])
            
        self.sock_rep.close()        



    def find_succesor_wrapper(self, id, sock_req):
        info = self.find_succesor(id, sock_req)
        self.sock_rep.send_json({"response": "ACK", "return_info": info})
        pass


    def find_succesor(self, id, sock_req):
        tuple_info = self.find_predecesor(id, sock_req)
        if tuple_info:
            destination_id, destination_addr = tuple_info            
            recv_json = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params": {}, "procedence_addr": self.addr, "procedence_method": "find_succesor_286"}, requester_object= self, asked_properties = ('succ_list', ), destination_id = destination_id, destination_addr = destination_addr ) 
            if recv_json is sock_req.error_json: return None
            return recv_json['return_info']['succ_list'][0]
        return None
            
    def find_predecesor_wrapper(self, id, sock_req):
        pred_id, pred_addr = self.find_predecesor(id, sock_req)

        self.sock_rep.send_json({"response": "ACK", "return_info": {"pred_id": pred_id, "pred_addr": pred_addr}, "procedence_addr": self.addr } )
        

	#In this method the node executes a query about an input id. 
	#There is a particular case if the query id is equal to current_succ_id.
	#In that case, without the and clause in line 256, the while loop executes infinity, because the id never be
	#between current_id and current_succ_id in future iterations.        
    def find_predecesor(self, id, sock_req):
        current_id = self.id
        current_succ_id, current_succ_addr = self.succ_list[0]
        self.finger_table[0] = self.succ_list[0]
        current_addr = self.addr  
        
         
        while not self.between(id, interval = (current_id, current_succ_id)) and current_succ_id != id :            
            
            recv_json_closest = sock_req.make_request(json_to_send = {"command_name" : "CLOSEST_PRED_FING", "method_params" : {"id": id}, "procedence_addr" : self.addr, "procedence_method": "find_predecesor"}, method_for_wrap = 'closest_pred_fing', requester_object = self, destination_id = current_id, destination_addr = current_addr)
            
            if recv_json_closest is sock_req.error_json : return None
            
            
            recv_json_succ = sock_req.make_request(json_to_send = {"command_name" : "GET_SUCC_LIST", "method_params" : {}, "procedence_addr" : self.addr, "procedence_method" : "find_predecesor" }, requester_object = self, asked_properties = ("succ_list", ), destination_id = recv_json_closest['return_info'][0], destination_addr = recv_json_closest['return_info'][1] )

            if recv_json_succ is sock_req.error_json:
                return None
            
                
            current_id, current_addr = recv_json_closest['return_info'][0], recv_json_closest['return_info'][1]
            current_succ_id, current_succ_addr = recv_json_succ['return_info']['succ_list'][0]                
        
        return current_id, current_addr

    def closest_pred_fing_wrap (self, id, sock_req):        
        closest_id, closest_addr = self.closest_pred_fing(id, sock_req)
        self.sock_rep.send_json({"response" : "ACK", "return_info" : (closest_id, closest_addr), "procedence": self.addr})
        

    def closest_pred_fing(self, id, sock_req):
        #In here is where we use the finger_table as an optimization of the search of the succesor of an id.
        for i in range(self.m-1, -1, -1):            
            if self.finger_table[i] is None : continue 
            if self.between(self.finger_table[i][0], (self.id, id) ) :
                return self.finger_table[i]
                
                
        return (self.id, self.addr)



if len(sys.argv) > 2:
    n = Node(introduction_node = sys.argv[1], addr= sys.argv[2])
    
else:
    n = Node(addr = sys.argv[1] )
    
