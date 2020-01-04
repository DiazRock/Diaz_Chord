import zmq 



class request:
    def __init__(self, context, error_json = {"response": "ERR"},  request_timeout = 6e3, request_retries = 3):                
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.context = context
        self.error_json = error_json            #Esta línea depende de cada request.                
        self.sock_req = self.context.socket(zmq.REQ) 
    #Puede pasar que mientras un nodo i esté intentando comunicarse con otro j, le pidan hacer request desde otro nodo k, lo cual va a dar una excepción.
    #Ese problema no lo tendría si decidiera hacer un socket nuevo para cada request. Solo que esta no es una solución escalable. 
    #
    def make_request(self, json_to_send, destination_addr, destination_id, requester_object = None, asked_properties = None, method_for_wrap = None, procedence = None):        
        #print("Entre a make request ", procedence)
        #poll = zmq.Poller()
        if asked_properties and destination_addr == json_to_send['procedence_addr']:
            #print("\testuve en el asked_properties")
            return {"response": "ACK", "procedence_addr": json_to_send['procedence_addr'], "return_info": {asked_property: requester_object.__dict__[asked_property] for asked_property in asked_properties } }
        if method_for_wrap and destination_addr == json_to_send['procedence_addr']:
            #print("\t", json_to_send)
            return {"response": "ACK", "procedence_addr": json_to_send['procedence_addr'], "return_info": requester_object.__class__.__dict__ [method_for_wrap] (requester_object, **json_to_send['method_params'])}


        for i in range(self.request_retries, 0, -1):
            
            
            self.sock_req.connect("tcp://" + destination_addr)            
            #Nota: El json que yo voy a enviar no tiene por qué estar cabledo dentro de la clase, eso es un mal diseño, me acabo de dar cuenta, eso significa que tengo que modificar las clases cada vez que quiera enviar un json diferente lo cual es una pesadez.            
            #if self.command_name == "RECT": json_to_send.update({"cosa": "ppppppp"})
            json_to_send.update({"time" : (self.request_retries - i) + 1})
            
            self.sock_req.send_json(json_to_send)            
            #print("I'm going to send info to %s" %destination_addr)
            if self.sock_req.poll(self.request_timeout):
                #print("\tEntré al if en make_request() ", json_to_send)

                recv = self.sock_req.recv_json()
                #print("\tDesconecté la conexión ", self.destination_addr)
                
                self.sock_req.disconnect("tcp://" + destination_addr) 
                return recv

            #else:
            print("Retrying to connect, time: ", (self.request_retries - i) + 1)
            print("I'm trying to send to ", json_to_send, " ", destination_addr)
            self.sock_req.disconnect("tcp://" + destination_addr)
            
            self.sock_req.setsockopt(zmq.LINGER, 0)
            self.sock_req.close()
            
            self.sock_req = self.context.socket(zmq.REQ)
            

        return self.error_json

    def action_for_error(self, destination_addr):
        print("In here I'm going to write a function over the death of destination_node")
        print('Remember: %s is dead, you need to make some work to fix the death in the ring' %destination_addr)
        
        
class initial_request(request):
    def __init__(self, json_to_send, context, request_timeout = 4e3, request_retries = 3):        
        super().__init__(json_to_send = json_to_send, error_json = {"response": "ERR", "return_info": "cannot connect to %s, something went wrong" % json_to_send ['procedence_addr'], "action": self.action_for_error}, context = context, request_timeout = request_timeout, request_retries= request_retries)

    def action_for_error(self, destination_addr):
        
        super().action_for_error(destination_addr)
        print("Enter address to retry ")
        addr = input()
        print("Connecting now to " %addr)
        
        

#class property_request(request):    
#    def make_request(self, requester_obj, asked_properties):
#        if self.json_to_send['procedence_addr'] == self.destination_addr:
#            return {"response": "ACK", "procedence_addr": self.json_to_send['procedence_addr'], "return_info": {asked_property: requester_obj.__dict__[asked_property] for asked_property in asked_properties } }
#        return super().make_request()
#        
#
#
#class wrapper_request(request):
#    def make_request(self, requester_obj, method_for_wrap):
#        if self.json_to_send['procedence_addr'] == self.destination_addr:
#            return {"response": "ACK", "procedence_addr": self.json_to_send['procedence_addr'], "return_info": method_for_wrap(**self.json_to_send['method_params']) }
#        return super().make_request()


    
