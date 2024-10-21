import socket
from sys import argv
from threading import Thread
from template_pb2 import Message, FastHandshake

CLIENTS = {}
LAST_ID = 0


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


def handle_client(conn: socket.socket, addr):
    global LAST_ID
    id = LAST_ID
    LAST_ID += 1
    CLIENTS[id] = conn

    try:
        handshake = FastHandshake(id=id, error=False)
        send_message(conn, handshake)
        print(f"Client #{id} connected from {addr}")

        while True:
            msg = receive_message(conn, Message)
            print(f"Message from {msg.fr} to {msg.to}: {msg.msg}")

            if msg.msg.lower() == "end":
                break

            receiver_conn = CLIENTS.get(msg.to)
            if receiver_conn:
                send_message(receiver_conn, msg)
            else:
                print(f"Client #{msg.to} does not exist. Dropping message.")

    except Exception as e:
        print(f"Error handling client #{id}: {e}")
    finally:
        print(f"Closing connection to client #{id}")
        CLIENTS.pop(id, None)
        conn.close()


def loop_main(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("0.0.0.0", port))
            print(f"Server started on port {port}")
            print("Waiting for a client...")
            s.listen()
            while True:
                try:
                    conn, addr = s.accept()
                    Thread(target=handle_client, args=(conn, addr)).start()
                except KeyboardInterrupt:
                    break
    except:
        pass


def main():
    global CLIENTS

    try:
        port = int(argv[1])
    except:
        port = 8080

    loop = Thread(target=loop_main, args=(port,))
    loop.daemon = True
    loop.start()

    while True:
        try:
            command = input("op> ").strip().lower()
        except:
            break

        if command == "num_users":
            print(f"Number of users: {len(CLIENTS)}")
        else:
            print("Invalid command")
            print("Available commands:")
            print("- num_users: Get the number of connected users")


if __name__ == "__main__":
    main()
