import threading
import socket
import snowflake
import message_pb2 as message_pb2

id_list = []
lock = threading.Lock()
CONNECT_MESSAGE = 'CONNECT'
ACK_MESSAGE = 'ACK'

class Peer:
    
    def __init__(self, ip, port, desired_id=None):
        self.ip = ip
        self.port = port
        self.peer_id = self.generate_id(desired_id)
        self.peers = []  

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip, self.port))

        self._start_server_thread()
        print(f"Peer {self.peer_id} started at {self.ip}:{self.port}")

    def generate_id(self, desired_id):
        if desired_id is not None:
            if desired_id in id_list:
                print('id already in use')
                return snowflake.derive_id()
            else:
                with lock:
                    id_list.append(desired_id)
                return desired_id
        else:
            return snowflake.derive_id()

    def _start_server_thread(self):
        self.server_thread = threading.Thread(target=self.listen_for_messages)
        self.server_thread.daemon = True
        self.server_thread.start()

    def listen_for_messages(self):
        while True:
            data, addr = self.socket.recvfrom(1024)
            self.process_incoming_message(data, addr)

    def process_incoming_message(self, data, addr):
        message = message_pb2.Message()
        message.ParseFromString(data)

        if message.text_message == CONNECT_MESSAGE:
            sender_ip = addr[0]
            sender_port = message.sender_port
            self.peers.append((sender_ip, sender_port))
            return
        
        if message.text_message == ACK_MESSAGE and message.destination_id == self.peer_id:
            print(f"\nACK received from {addr}")
            return

        if message.destination_id == self.peer_id:
            print(f"\nMessage received: {message.text_message}")
            ack_message = self.create_ack_message(message.sender_id)
            self.send_serialized_message(ack_message, addr)
        else:
            self.forward_message(message, addr)

    def forward_message(self, message, sender_addr):
        for peer_addr in self.peers:
            if peer_addr != sender_addr:  # do not forward back to the sender
                self.send_serialized_message(message, peer_addr)

    def send_serialized_message(self, message, peer_addr):
        try:
            self.socket.sendto(message.SerializeToString(), peer_addr)
        except Exception as e:
            print(f"Error sending message to {peer_addr}: {e}")

    def connect_to_peer(self, peer_ip, peer_port):
        if (peer_ip, peer_port) not in self.peers:
            self.peers.append((peer_ip, peer_port))
            connect_message = self.create_connect_message(CONNECT_MESSAGE, self.port)
            self.send_serialized_message(connect_message, (peer_ip, peer_port))

    def broadcast_message(self, message_text, destination_id):
        message = self.create_message(message_text, destination_id)
        for peer_addr in self.peers:
            self.send_serialized_message(message, peer_addr)

    def create_message(self, text, destination_id):
        message = message_pb2.Message()
        message.text_message = text
        message.sender_id = self.peer_id
        message.destination_id = destination_id
        return message
    
    def create_ack_message(self, destination_id):
        ack_message = message_pb2.Message()
        ack_message.text_message = ACK_MESSAGE
        ack_message.sender_id = self.peer_id
        ack_message.destination_id = destination_id
        return ack_message
    
    def create_connect_message(self, text, sender_port):
        message = message_pb2.Message()
        message.text_message = text
        message.sender_id = self.peer_id
        message.sender_port = sender_port
        return message
    
