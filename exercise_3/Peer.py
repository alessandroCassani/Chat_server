import threading
import socket
import sys
import snowflake
import message_pb2 as message_pb2

class Peer:
    def __init__(self, ip, port, desired_id=None):
        self.ip = ip
        self.port = port
        self.peer_id = self.generate_id(desired_id)
        self.peers = []

        # Start a separate thread for handling incoming messages
        self._start_server_thread()
        print(f"Peer {self.peer_id} started at {self.ip}:{self.port}")

    def generate_id(self, desired_id):
        id_generator = snowflake.derive_id if desired_id is None else lambda: desired_id
        return id_generator()

    def _start_server_thread(self):
        """Start a server thread for listening to incoming messages."""
        self.server_thread = threading.Thread(target=self.listen_for_messages)
        self.server_thread.daemon = True  
        self.server_thread.start()

    def listen_for_messages(self):
        """Listen for incoming UDP messages on the peer’s socket."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
            server_socket.bind((self.ip, self.port))
            while True:
                data, addr = server_socket.recvfrom(1024)
                self.process_incoming_message(data, addr)

    def process_incoming_message(self, data, addr):
        """Parse and handle incoming messages based on destination."""
        message = message_pb2.Message()
        message.ParseFromString(data)

        print(f"Received message from {message.sender_id}: {message.text_message} (Destination: {message.destination_id})")

        if message.destination_id == self.peer_id:
            print(f"Message directed to this peer: {message.text_message}")
        else:
            print(f"Message not intended for this peer. Forwarding to other peers.")
            self.forward_message(message, addr)
            
    def forward_message(self, message, sender_addr):
        """Forward a message to all peers except the original sender."""
        for peer_addr in self.peers:
            if peer_addr != sender_addr:  
                self.send_serialized_message(message, peer_addr)

    def send_serialized_message(self, message, peer_addr):
        """Send a serialized Protobuf message to a specified peer address."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(message.SerializeToString(), peer_addr)

    def connect_to_peer(self, peer_ip, peer_port):
        """Add a new peer to the list of known peers."""
        self.peers.append((peer_ip, peer_port))
        
        print(f'peers list: {self.peers}')

    def broadcast_message(self, message_text, destination_id):
        """Broadcast a message to all connected peers."""
        message = self.create_message(message_text, destination_id)
        for peer_addr in self.peers:
            self.send_serialized_message(message, peer_addr)

    def create_message(self, text, destination_id):
        """Helper to create a Protobuf message with specified text and destination."""
        message = message_pb2.Message()
        message.text_message = text
        message.sender_id = self.peer_id
        message.destination_id = destination_id
        return message

def main():
    if len(sys.argv) < 3:
        print("Usage: python Peer.py [my ip]:[my port] --desired-id [my id] [peer ip]:[peer port] ...")
        sys.exit(1)

    # Parse the local peer address
    my_address = sys.argv[1].split(":")
    my_ip = my_address[0]
    my_port = int(my_address[1])
    desired_id = None

    # Check for the desired ID flag and extract the ID
    if '--desired-id' in sys.argv:
        desired_id_index = sys.argv.index('--desired-id')
        desired_id = int(sys.argv[desired_id_index + 1])

    # Initialize the peer
    peer = Peer(my_ip, my_port, desired_id)

    # Connect to other peers
    for arg in sys.argv[2:]:
        if arg == '--desired-id':
            continue  # Skip the flag itself
        if arg == str(desired_id):
            continue  # Skip the desired ID value

        # Split the remaining arguments into IP and port
        peer_ip_port = arg.split(":")
        if len(peer_ip_port) == 2:
            peer_ip = peer_ip_port[0]
            peer_port = int(peer_ip_port[1])
            peer.connect_to_peer(peer_ip, peer_port)
        else:
            print(f"Invalid peer address format: {arg}. Expected format is ip:port.")

    while True:
        msg_text = input("Enter message text to broadcast: ")
        destination_id = int(input("Enter destination ID: "))
        peer.broadcast_message(msg_text, destination_id)


if __name__ == "__main__":
    main()
