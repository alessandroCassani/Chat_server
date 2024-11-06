import sys
from Peer import Peer

def main():
    if len(sys.argv) < 3:
        print("Usage: python Peer.py [my ip]:[my port] --desired-id [my id] [peer ip]:[peer port] ...")
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
        if arg == '--desired-id':
            continue 
        if arg == str(desired_id):
            continue  

        peer_ip_port = arg.split(":")
        print(peer_ip_port)
        if len(peer_ip_port) == 2:
            peer_ip = peer_ip_port[0]
            peer_port = int(peer_ip_port[1])
            print(peer_port)
            peer.connect_to_peer(peer_ip, peer_port)
        else:
            print(f"Invalid peer address format: {arg}. Expected format is ip:port.")

    while True:
        msg_text = input("Enter message text to broadcast: ")
        destination_id = int(input("Enter destination ID: "))
        peer.broadcast_message(msg_text, destination_id)

if __name__ == "__main__":
    main()