FROM python:3.7-slim
MAINTAINER DiazRock corolariodiaz@gmail.com

#RUN Step
RUN pip install zmq



#Adding files.
ADD diaz_chord.py /root/diaz_chord.py
ADD utils.py /root/utils.py
ADD client_side.py /root/client_side.py


EXPOSE 8080

#Default running
ENTRYPOINT ["python", "/root/diaz_chord.py"] 







