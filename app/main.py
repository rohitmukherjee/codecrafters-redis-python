import socket  # noqa: F401

PONG_RESPONSE: str = "+PONG\r\n"
PONG_RESPONSE_BYTES: bytes = PONG_RESPONSE.encode("utf-8")
MAX_BYTES: int = 1024


def main():
    while True:
        # We are creating a new connection for each client
        server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        socket_connection, address = server_socket.accept()  # wait for client
        print("Connected to the client: " + str(address))
        data: bytes = socket_connection.recv(MAX_BYTES)
        message: str = data.decode("utf-8")
        if message is None or len(message) == 0:
            return
        socket_connection.send(PONG_RESPONSE_BYTES)
        server_socket.close()


if __name__ == "__main__":
    main()
