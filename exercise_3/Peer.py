import threading
import socket
import sys
import time
import snowflake
import message_pb2 as message_pb2

class Peer:
    def __init__(self, ip, port, desired_id=None):
        # Initialize peer attributes
        self.ip = ip
        self.port = port
        # Generate a unique peer ID using Snowflake
        self.id_generator = snowflake.derive_id if desired_id is None else lambda: desired_id
        self.peer_id = self.id_generator()
        self.peers = []  # List to hold connected peers

        # Start the server thread to handle incoming messages
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.daemon = True  # Allow thread to exit when the main program exits
        self.server_thread.start()

        print(f"Peer {self.peer_id} started at {self.ip}:{self.port}")

    def start_server(self):
        """Start the server to listen for incoming messages."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind((self.ip, self.port))

        while True:
            data, addr = server_socket.recvfrom(1024)  # Receive data from any peer
            self.handle_message(data, addr)  # Process the received message

    def handle_message(self, data, addr):
        """Handle incoming messages from peers."""
        message = message_pb2.Message()
        message.ParseFromString(data)  # Deserialize the message

        print(f"Received message from {message.sender_id}: {message.text_message} (Destination: {message.destination_id})")

        # Check if the message is intended for this peer
        if message.destination_id == self.peer_id:
            print(f"Message for me. Sending ACK to {message.sender_id}.")
            self.send_ack(addr, message.sender_id)  # Send ACK if it's for us
        else:
            print(f"Message not for me. Forwarding to peers.")
            self.forward_message(message, addr)  # Forward the message to other peers

    def forward_message(self, message, addr):
        """Forward the message to all known peers, except the sender."""
        for peer in self.peers:
            if (peer[0], peer[1]) != addr:  # Avoid sending it back to the original sender
                self.send_message(message, peer)

    def send_message(self, message, peer):
        """Send a serialized message to a specified peer."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.sendto(message.SerializeToString(), (peer[0], peer[1]))  # Serialize and send the message

    def send_ack(self, addr, sender_id):
        """Send an acknowledgment back to the message sender."""
        ack_message = message_pb2.Message()
        ack_message.acknowledgment = f"ACK from {self.peer_id} to {sender_id}"
        self.send_message(ack_message, addr)  # Send the ACK message

    def connect_to_peer(self, peer_ip, peer_port):
        """Add a new peer to the list of known peers."""
        self.peers.append((peer_ip, peer_port))

    def broadcast_message(self, message_text, destination_id):
        """Broadcast a message to all connected peers."""
        message = message_pb2.Message()
        message.text_message = message_text
        message.sender_id = self.peer_id
        message.destination_id = destination_id
        
        # Send the message to all known peers
        for peer in self.peers:
            self.send_message(message, peer)

def main():
    if len(sys.argv) < 3:
        print("Usage: python e3.py [my ip]:[my port] --desired-id [my id] [peer ip]:[peer port] ...")
        sys.exit(1)

    my_address = sys.argv[1].split(":")
    my_ip = my_address[0]
    my_port = int(my_address[1])
    desired_id = None
    
    if '--desired-id' in sys.argv:
        desired_id_index = sys.argv.index('--desired-id')
        desired_id = int(sys.argv[desired_id_index + 1])

    peer = Peer(my_ip, my_port, desired_id)

    # Connect to other peers 
    for arg in sys.argv[2:]:
        if arg != '--desired-id':
            peer_ip_port = arg.split(":")
            peer.connect_to_peer(peer_ip_port[0], int(peer_ip_port[1]))

    while True:
        msg_text = input("Enter message text: ")
        destination_id = input("Enter destination ID (or '*' to broadcast): ")
        if destination_id == "*":
            # Broadcast to all peers
            peer.broadcast_message(msg_text, "*")
        else:
            # Send to specific destination
            destination_ip = input("Enter destination IP: ")
            destination_port = int(input("Enter destination port: "))
            peer.send_message(msg_text, destination_ip, destination_port)

if __name__ == "__main__":
    main()
