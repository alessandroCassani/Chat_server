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
        """Listen for incoming UDP messages on the peer's socket."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
            server_socket.bind((self.ip, self.port))
            print(f"Listening for messages on {self.ip}:{self.port}")
            while True:
                data, addr = server_socket.recvfrom(1024)
                self.process_incoming_message(data, addr)

    def process_incoming_message(self, data, addr):
        """Parse and handle incoming messages based on the destination."""
        message = message_pb2.Message()
        message.ParseFromString(data)

        # Handle connection messages
        if message.text_message == "CONNECT":
            sender_ip = addr[0]
            sender_port = message.sender_port
            print(f'Peer {self.peer_id}: Added peer {sender_ip}:{sender_port}. Current peers: {self.peers}')
            self.peers.append((sender_ip,sender_port))
            return

        if message.destination_id == self.peer_id:
            print(f"Message directed to this peer: {message.text_message}")
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
        """Send a serialized message in Protobuf format to a specific peer."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            try:
                sock.sendto(message.SerializeToString(), peer_addr)
            except Exception as e:
                print(f"Error sending message to {peer_addr}: {e}")

    def connect_to_peer(self, peer_ip, peer_port):
        """Add a new peer to the list of known peers."""
        if (peer_ip, peer_port) not in self.peers:
            self.peers.append((peer_ip, peer_port))
            print(f'Peer {self.peer_id}: Added peer {peer_ip}:{peer_port}. Current peers: {self.peers}')

            connect_message = self.create_connect_message("CONNECT", self.port)
            self.send_serialized_message(connect_message, (peer_ip, peer_port))

    def broadcast_message(self, message_text, destination_id):
        """Send a message to all connected peers."""
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
    
    def create_connect_message(self, text, sender_port):
        """Create a Protobuf message with specified text and destination."""
        message = message_pb2.Message()
        message.text_message = text
        message.sender_id = self.peer_id
        message.sender_port = sender_port
        return message
    
    
