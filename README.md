# Diaz Chord project

## Chord ring architecture. A peer to peer network

This is a Chord architecture written in python3.7 using ZMQ library. The diaz_chord.py file defines a Node class, wich represent a node with an standard network interface. There is also a utils.py file, in where the request class, that wraps all zmq.socket.req of the code,  is defined.

----------

### Ways of use this code

> There is two forms in where you can use the code.

Wich one is using a docker image builded by the Dockerfile. You can make:

`sudo docker run --name diaz_chord1 -it -P  [NAME_IMAGE] --v` 

This will bind automatically your container to a disponible IP of the Docker host (because of -P option).

After that, you have a first node in the network.
If you want to connect other node to the network, you have to run again the image and create a container. Like following:

`sudo docker run --name diaz_chord2 -it -P [NAME_IMAGE] --addr_known [IP_FROM_diaz_chord1] --v`

You can see the IP from diaz_chord1 using **docker inspect diaz_chord1**  command. This node is going to connect with the previous one. Now, you can scale the network easily.

In the other way, you have to run **pip install zmq** command or have the zmq library in your OS.
Then you can execute:

`python diaz_chord.py --v`

Which has the same result as the first command that I expose using docker, only in a non-isolated context. Read about Docker, is a great technology ;)

----------

### Command line for user input


> There is three optional paramatters that you can use:

- --addr_id : This is the address of the node that identifies it in the hash space. If no address is set, this automatically set the local address asigned from the local network.
- --addr_known : This is an IP address that identifies reference a node in the network.If you wanna join new nodes to an existing network, you have to enter this value, otherwise your node never bee connected to the network.
- --v : This is the verbose option. You can see the activity of the node if you enter it.

----------

### Software versions used

- Ubuntu 18.04
- Docker: 18.09.7, build 2d0083d
- pyzmq: 16.0.3

And that's all. Any feedback will be appreciated.