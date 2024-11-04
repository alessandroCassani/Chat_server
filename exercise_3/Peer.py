import threading
import socket
import snowflake
import message_pb2 as message_pb2

class Peer:
    def __init__(self, ip, port, desired_id=None):
        self.ip = ip
        self.port = port
        self.peer_id = self.generate_id(desired_id)
        self.peers = []  

        # Create a single shared socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.ip, self.port))

        # Start a separate thread to handle incoming messages
        self._start_server_thread()
        print(f"Peer {self.peer_id} started at {self.ip}:{self.port}")

    def generate_id(self, desired_id):
        """Generate a unique ID for the peer."""
        if desired_id is not None:
            return desired_id
        else:
            return snowflake.derive_id()

    def _start_server_thread(self):
        """Start a server thread to listen for incoming messages."""
        self.server_thread = threading.Thread(target=self.listen_for_messages)
        self.server_thread.daemon = True
        self.server_thread.start()

    def listen_for_messages(self):
        """Listen for incoming UDP messages on the peer's shared socket."""
        print(f"Listening for messages on {self.ip}:{self.port}")
        while True:
            data, addr = self.socket.recvfrom(1024)
            self.process_incoming_message(data, addr)

    def process_incoming_message(self, data, addr):
        """Parse and handle incoming messages based on the destination."""
        message = message_pb2.Message()
        message.ParseFromString(data)

        # Handle connection messages
        if message.text_message == "CONNECT":
            sender_ip = addr[0]
            sender_port = message.sender_port
            self.peers.append((sender_ip, sender_port))
            print(f'Peer {self.peer_id}: Added peer {sender_ip}:{sender_port}. Current peers: {self.peers}')
            return
        
        if message.text_message == "ACK" and message.destination_id == self.peer_id:
            print(f"ACK received from {addr}: Message correctly received.")
            return

        if message.destination_id == self.peer_id:
            print(f"Message directed to this peer: {message.text_message}")
            ack_message = self.create_ack_message(message.sender_id)
            self.send_serialized_message(ack_message, addr)
        else:
            print(f"Message not mine, forwarding to other peers.")
            self.forward_message(message, addr)

    def forward_message(self, message, sender_addr):
        """Forward a message to all peers except the sender."""
        for peer_addr in self.peers:
            if peer_addr != sender_addr:
                print(f"Forwarding to peer: {peer_addr}")
                self.send_serialized_message(message, peer_addr)

    def send_serialized_message(self, message, peer_addr):
        """Send a serialized message in Protobuf format to a specific peer using the shared socket."""
        try:
            print(f'Sending message to peer address: {peer_addr}')
            self.socket.sendto(message.SerializeToString(), peer_addr)
        except Exception as e:
            print(f"Error sending message to {peer_addr}: {e}")

    def connect_to_peer(self, peer_ip, peer_port):
        """Add a new peer to the list of known peers and notify them."""
        if (peer_ip, peer_port) not in self.peers:
            self.peers.append((peer_ip, peer_port))
            print(f'Peer {self.peer_id}: Added peer {peer_ip}:{peer_port}. Current peers: {self.peers}')

            connect_message = self.create_connect_message("CONNECT", self.port)
            self.send_serialized_message(connect_message, (peer_ip, peer_port))

    def broadcast_message(self, message_text, destination_id):
        """Broadcast a message to all connected peers."""
        print(f"Broadcasting message: '{message_text}' to all peers")
        message = self.create_message(message_text, destination_id)
        for peer_addr in self.peers:
            self.send_serialized_message(message, peer_addr)

    def create_message(self, text, destination_id):
        """Create a Protobuf message with specified text and destination."""
        message = message_pb2.Message()
        message.text_message = text
        message.sender_id = self.peer_id
        message.destination_id = destination_id
        return message
    
    def create_ack_message(self, destination_id):
        """Create an acknowledgment message."""
        ack_message = message_pb2.Message()
        ack_message.text_message = "ACK"
        ack_message.sender_id = self.peer_id
        ack_message.destination_id = destination_id
        return ack_message
    
    def create_connect_message(self, text, sender_port):
        """Create a Protobuf message for connection setup."""
        message = message_pb2.Message()
        message.text_message = text
        message.sender_id = self.peer_id
        message.sender_port = sender_port
        return message
