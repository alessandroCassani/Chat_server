import socket
from sys import argv
import template_pb2 as template_pb2
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
        if len(argv) > 2:
            host = argv[1]
            port = int(argv[2])
        elif len(argv) > 1:
            port = int(argv[1])
        else:
            raise ValueError
    except:
        host = host or "127.0.0.1"
        port = 8080

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        print("Connected to the server")
        handshake = receive_message(s, template_pb2.FastHandshake)
        if handshake.error:
            print("Server rejected the connection")
            return

        print(f"we are client #{handshake.id}")
        id = handshake.id

        Thread(target=handle_incoming_messages,args=(s,), daemon=True).start()
        while True:
            try:
                data = input("Enter a message: \n")
                data_chunked = data.split(' ', 1)
                receiver_id = int(data_chunked[0])
                message = data_chunked[1]
            except:
                error = -1
                message = "end"
                msg = template_pb2.Message(fr=id, to=error, msg=message)
                
            msg = template_pb2.Message(fr=id, to=receiver_id, msg=message)
            send_message(s, msg)
            
            if message == "end":
                break
        
        print("Closing connection")


def handle_incoming_messages(conn):
    print('waiting for messages...')
    while True:
        msg = receive_message(conn, template_pb2.Message)
        print(f"New message arrived: {msg.msg}")

if __name__ == "__main__":
    main()


