import zmq 


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    


class request:
    def __init__(self, context, error_json = "ERR",  request_timeout = 6e3, request_retries = 3):                
        self.request_timeout = request_timeout
        self.request_retries = request_retries
        self.context = context
        self.error_json = error_json            
        self.sock_req = self.context.socket(zmq.REQ) 
    def make_request(self, json_to_send, destination_addr, destination_id, requester_object = None, asked_properties = None, method_for_wrap = None, procedence = None):        
        if asked_properties and destination_addr == json_to_send['procedence_addr']:
            
            return {"response": "ACK", "procedence_addr": json_to_send['procedence_addr'], "return_info": {asked_property: requester_object.__dict__[asked_property] for asked_property in asked_properties } }
        if method_for_wrap and destination_addr == json_to_send['procedence_addr']:
            
            if json_to_send['command_name'] == 'CLOSEST_PRED_FING': 
                json_to_send['method_params'].update({"sock_req" : self})                
            return {"response": "ACK", "procedence_addr": json_to_send['procedence_addr'], "return_info": requester_object.__class__.__dict__ [method_for_wrap] (requester_object, **json_to_send['method_params'])}
        retried = False

        for i in range(self.request_retries, 0, -1):
                        
            self.sock_req.connect("tcp://" + destination_addr)            
            
            
            self.sock_req.send_json(json_to_send)            
            
            if self.sock_req.poll(self.request_timeout):
                #print("\tEntré al if en make_request() ", json_to_send)

                recv = self.sock_req.recv_json()
                #print("\tDesconecté la conexión ", self.destination_addr)
                
                self.sock_req.disconnect("tcp://" + destination_addr) 
                if retried: print(f"{bcolors.OKGREEN}Yea, baby, I did it %s, to %s {bcolors.ENDC}" %(json_to_send, destination_addr))
                return recv

            #else:
            print("Retrying to connect, time: ", (self.request_retries - i) + 1)
            
            print(f"{bcolors.WARNING}I'm trying to send %s to %s {bcolors.ENDC}" %(json_to_send, destination_addr))
            self.sock_req.disconnect("tcp://" + destination_addr)
            retried = True
            self.sock_req.setsockopt(zmq.LINGER, 0)
            self.sock_req.close()
            
            self.sock_req = self.context.socket(zmq.REQ)
            

        return self.error_json

    def action_for_error(self, destination_addr):
        
        print(f'{bcolors.FAIL}Remember: %s is dead, :({bcolors.ENDC}' %destination_addr)
        
        
class initial_request(request):
    def __init__(self, json_to_send, context, request_timeout = 4e3, request_retries = 3):        
        super().__init__(json_to_send = json_to_send, error_json = {"response": "ERR", "return_info": "cannot connect to %s, something went wrong" % json_to_send ['procedence_addr'], "action": self.action_for_error}, context = context, request_timeout = request_timeout, request_retries= request_retries)