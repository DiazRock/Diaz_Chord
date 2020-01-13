import re 
import sys
import diaz_chord
from subprocess import call

def execute_function(parametters):
    if len (parametters) == 1:
            if matcher.fullmatch(parametters[0]):
                call(['python', 'diaz_chord.py', parametters[0]] )
                
            else:
                print("Sorry, but the entry must be an IPv4 address, try again")
                
    elif len (parametters) == 2:            
        if not matcher.fullmatch(parametters[0]):
            print("Sorry, %s was a bad input, it must be an IPv4 address, try again" %parametters[0])
        elif not matcher.fullmatch(parametters[1]):
            print("Sorry, %s was a bad input, it must be an IPv4 address, try again" %parametters[1])
        else:
            call(['python', 'diaz_chord.py', parametters[0], parametters[1]] )
            #exec('diaz_chord.py %s %s' %parametters[0] %parametters[1])
        



if __name__ == "__main__":
    addr_pattern = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,4}"
    matcher = re.compile(addr_pattern)
    while True:
        if len (sys.argv) > 1:
            execute_function(sys.argv[1:])
        else:
            print("input an IPv4 address if you want an initial node of the network.\nOtherwise, enter the IPv4 address of the node, and other address known of the network:")
            params = input().split()
            if len(params) > 2:
                print("Too much info. Try again.")
            else:
                execute_function(params)
        

