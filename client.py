import socket
from sys import argv
from template_pb2 import Message, FastHandshake


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
        # Wait for the server to send the FastHandshake message
        handshake = receive_message(s, FastHandshake)
        if handshake.error:
            print("Server rejected the connection")
            return

        print(f"we are client #{handshake.id}")
        id = handshake.id

        while True:
            try:
                data = input("Enter a message: ")
            except:
                data = "end"
            msg = Message(fr=id, to=id, msg=data)
            send_message(s, msg)
            if data == "end":
                break
            msg = receive_message(s, Message)
            print(f"Received: {msg.msg} from {msg.fr} to {msg.to}")
            if msg.msg == "end":
                break
        print("Closing connection")


if __name__ == "__main__":
    main()
