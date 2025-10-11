import selectors
import socket  # noqa: F401

PONG_RESPONSE: str = "+PONG\r\n"
PONG_RESPONSE_BYTES: bytes = PONG_RESPONSE.encode("utf-8")
MAX_BYTES: int = 1024
REDIS_PORT: int = 6379

selector = selectors.DefaultSelector()


def accept_connection(server_sock: socket):
    client_socket, client_address = server_sock.accept()
    print("Connected:", client_address)
    client_socket.setblocking(False)
    # We are registering on the client-socket here and invoking the read_message callback
    # when the socket is ready to send
    selector.register(client_socket, selectors.EVENT_READ, read_message)


def read_message(client_socket: socket):
    try:
        data = client_socket.recv(MAX_BYTES)
        if not data:  # client closed
            print("Client disconnected")
            selector.unregister(client_socket)
            client_socket.close()
        else:
            client_socket.send(PONG_RESPONSE_BYTES)
    except ConnectionError:
        selector.unregister(client_socket)
        client_socket.close()


def main():
    server_socket = socket.create_server(("localhost", REDIS_PORT), reuse_port=False)
    server_socket.listen()
    server_socket.setblocking(False)
    selector.register(server_socket, selectors.EVENT_READ, accept_connection)

    while True:
        for key, _ in selector.select():
            callback = key.data
            callback(key.fileobj)
    # server_socket.close()


if __name__ == "__main__":
    main()
