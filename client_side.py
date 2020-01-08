
import zmq


class client_side:
    def __init__(self, ip_port):
        self.context = zmq.Context()
        self.sock_req = self.context.socket(zmq.REQ)
        self.send_info()
        

    def send_info(self):
        while True:
            buff = input().split()
            self.sock_req.connect("tcp://"+ buff[0])
            params = {buff[i] : buff[i + 1] for i in range(2, len(buff), 2) }
            
            if "BELONG" in buff:
                params['interval'] = params['interval'].split(',')
                params['interval'] = ( int ( params['interval'][0][1:]), int(params['interval'][1][:-1] ) )
                params['id'] = int(params['id'])
                
            if "FIND_SUCC" in buff:
                params['id'] = int(params["id"])
                

            self.sock_req.send_json({"command_name": buff[1], "method_params": params , "procedence_addr": "127.0.0.1:5050"})
            info = self.sock_req.recv_json()
            print(info)
            self.sock_req.disconnect("tcp://"+ buff[0])

client_side("127.0.0.1:5050")