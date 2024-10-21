import socket
from sys import argv
from threading import Thread
from exercise_1.template_pb2 import Message, FastHandshake
from queue import Queue

CLIENTS = {}
MESSAGES = {}
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

def change_client_id(id, conn):
    if CLIENTS.get(id):
        return False
    else:
        CLIENTS[id] = conn
        return True

def handle_client(conn: socket.socket, addr):
    global LAST_ID
    id = LAST_ID
    LAST_ID += 1
    CLIENTS[id] = conn

    try:
        handshake_message = receive_message(conn, FastHandshake)
        
        if handshake_message.change_id:
            new_id = handshake_message.id
            if change_client_id(new_id, conn):
                handshake = FastHandshake(id=new_id, error=False, change_id=True)
                print(f"Client requested ID {new_id}. ID change successful from {addr}")
            else:
                handshake = FastHandshake(id=id, error=False, change_id=False)
                print(f"Client requested ID {new_id}. ID change failed, already in use.")
                
        else:
            handshake = FastHandshake(id=id, error=False, change_id=False)
            print(f"Client connected with default ID {id} from {addr}.")

        send_message(conn, handshake)

        deliver_stored_messages(id, conn)  # deliver stored messages if any

    except Exception as e:
        print(f"Error handling client #{id}: {e}")
        handshake_failed = FastHandshake(id=-1, error=True)
        send_message(conn, handshake_failed)

    try:
        while True:
            msg = receive_message(conn, Message)
            print(f"Message from {msg.fr} to {msg.to}: {msg.msg}")

            if msg.msg.lower() == "end":
                break

            if msg.msg == '':
                msg.msg = 'empty string'

            receiver_conn = CLIENTS.get(msg.to)
            if receiver_conn:
                send_message(receiver_conn, msg)
            else:
                store_message(msg.to, msg)

    except Exception as e:
        print(f"Error handling client #{id}: {e}")
    finally:
        print(f"Closing connection to client #{id}")
        CLIENTS.pop(id, None)
        conn.close()

def store_message(receiver_id, msg):
    if receiver_id in MESSAGES:
        MESSAGES[receiver_id].put(msg)
    else:
        MESSAGES[receiver_id] = Queue()
        MESSAGES[receiver_id].put(msg)

def deliver_stored_messages(client_id, client_conn):
    if client_id in MESSAGES:
        while not MESSAGES[client_id].empty():
            stored_message = MESSAGES[client_id].get()
            send_message(client_conn, stored_message)
    else:
        print(f"No stored messages for client #{client_id}")

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
    except Exception as e:
        print(f"Server error: {e}")

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
