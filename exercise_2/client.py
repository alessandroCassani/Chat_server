import socket
from sys import argv
import template_pb2
from threading import Thread


def send_message(conn, m):
    serialized = m.SerializeToString()
    conn.sendall(len(serialized).to_bytes(4, byteorder="big"))
    conn.sendall(serialized)

def receive_message(conn, m):
    msg = m()
    size = int.from_bytes(conn.recv(4), byteorder="big")
    data = conn.recv(size)
    msg.ParseFromString(data)
    return msg

def main():
    host = None
    port = None
    try:
        if len(argv) == 2:
            host = host or "127.0.0.1"
            port = 8080
        elif len(argv) == 3:
            host = argv[1]
            port = int(argv[2])
            new_id = None
        elif len(argv) == 4:
            host = argv[1]
            port = int(argv[2])
            new_id = int(argv[3])
        else:
            raise ValueError("Numero di argomenti non valido")
    except:
        host = host or "127.0.0.1"
        port = 8080
        new_id = None

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        print("Connected to the server")
        
        if new_id:
            handshake = template_pb2.FastHandshake(id=new_id,change_id = True)
            send_message(s, handshake)
        else:
            handshake = template_pb2.FastHandshake(change_id = False)
            send_message(s, handshake)
        
        handshake = receive_message(s, template_pb2.FastHandshake)
        
        if handshake.change_id:
            print('new id accepted!')
            print(f"we are client #{handshake.change_id}")
            id = handshake.change_id
        else:
            print('id autonoumously created')
            id = handshake.id
            
        if handshake.error:
            print(f"Handshake failed")
            return

        Thread(target=handle_incoming_messages,args=(s,), daemon=True).start()
        
        while True:
            while True:
                try:
                    data = input("Enter a message (format: <receiver_id> <message>): \n")
                    data_chunked = data.split(' ', 1)
                    
                    if len(data_chunked) != 2:
                        raise ValueError("Input must be in format '<receiver_id> <message>'")
                    
                    receiver_id = int(data_chunked[0])
                    message = data_chunked[1]
                    break
                    
                except ValueError as ve:
                    print(f"Invalid input: {ve}")
                    continue
                
            msg = template_pb2.Message(fr=id, to=receiver_id, msg=message)
            send_message(s, msg)
            
            if message == "end":
                break
        
        print("Closing connection")


def handle_incoming_messages(conn):
    print('waiting for messages...')
    while True:
        msg = receive_message(conn, template_pb2.Message)
        print(f"New message arrived: {msg.msg} from client #{msg.fr}")

if __name__ == "__main__":
    main()


