import zmq 

class request:
    def __init__(self, json_to_send, destination_addr, destination_id, context, error_json = {"response": "ERR"},  request_timeout = 6e3, request_retries = 3):
        self.destination_addr = destination_addr        
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.context = context
        self.error_json = error_json            #Esta línea depende de cada request.
        self.destination_id = destination_id
        self.json_to_send = json_to_send
        self.sock_req = self.context.socket(zmq.REQ) 
    
    def make_request(self):        
        #print("Entre a make request")
        #poll = zmq.Poller()
        for i in range(self.request_retries, 0, -1):
            
            
            self.sock_req.connect("tcp://" + self.destination_addr)            
            #Nota: El json que yo voy a enviar no tiene por qué estar cabledo dentro de la clase, eso es un mal diseño, me acabo de dar cuenta, eso significa que tengo que modificar las clases cada vez que quiera enviar un json diferente lo cual es una pesadez.            
            #if self.command_name == "RECT": json_to_send.update({"cosa": "ppppppp"})
            self.json_to_send.update({"time" : (self.request_retries - i) + 1})
            self.sock_req.send_json(self.json_to_send)            
            #if self.json_to_send['command_name'] == "RECT": print("Entré a make_request por el lado de RECT")
            if self.sock_req.poll(self.request_timeout):
                #print("\tEntré al if en make_request()")
                recv = self.sock_req.recv_json()                
                self.sock_req.disconnect("tcp://" + self.destination_addr) 
                return recv

            #else:
            print("Retrying to connect, time: ", (self.request_retries - i) + 1)
            print("I'm trying to send %s " %self.json_to_send)
            self.sock_req.setsockopt(zmq.LINGER, 0)
            self.sock_req.disconnect("tcp://" + self.destination_addr)
            self.sock_req.close()                
            self.sock_req = self.context.socket(zmq.REQ)
            
        return self.error_json

    def action_for_error(self):
        print("In here I'm going to write a function over the death of destination_node")
        print('Remember: %s is dead, you need to make some work to fix the death in the ring' %self.destination_addr)
        
        
class initial_request(request):
    def __init__(self, json_to_send, destination_addr, destination_id, context, request_timeout = 4e3, request_retries = 3):        
        super().__init__(json_to_send = json_to_send, destination_addr = destination_addr, destination_id = destination_id, error_json = {"response": "ERR", "return_info": "cannot connect to %s, something went wrong" % json_to_send ['procedence_addr'], "action": self.action_for_error}, context = context, request_timeout = request_timeout, request_retries= request_retries)

    def action_for_error(self):
        
        super().action_for_error()
        print("Enter address to retry ")
        addr = input()
        print("Connecting now to " %addr)
        return self.make_request()
        

class property_request(request):    
    def make_request(self, requester_obj, asked_properties):
        if self.json_to_send['procedence_addr'] == self.destination_addr:
            return {"response": "ACK", "procedence_addr": self.json_to_send['procedence_addr'], "return_info": {asked_property: requester_obj.__dict__[asked_property] for asked_property in asked_properties } }
        return super().make_request()
        


class wrapper_request(request):
    def make_request(self, requester_obj, method_for_wrap):
        if self.json_to_send['procedence_addr'] == self.destination_addr:
            return {"response": "ACK", "procedence_addr": self.json_to_send['procedence_addr'], "return_info": method_for_wrap(**self.json_to_send['method_params']) }
        return super().make_request()

def put_final_message(destination_addr, destination_id):
    def inside_decorator(func):
        def wrapper(*args, **kwargs):
            return_value = func(*args, **kwargs)
            success_request = request(json_to_send = {"command_name": "SUCC_REQ", "method_params": {}, "procedence_addr" : args[0].addr}, destination_addr = kwargs [destination_addr], destination_id = kwargs ['destination_id'], context = args[0].context_sender) 
            success_request.make_request()
            return return_value
        return wrapper
    return inside_decorator
    
