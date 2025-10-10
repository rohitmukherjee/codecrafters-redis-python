import socket  # noqa: F401

PONG_RESPONSE: str = "+PONG\r\n"
PONG_RESPONSE_BYTES: bytes = PONG_RESPONSE.encode("utf-8")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    socket_connection, address = server_socket.accept()  # wait for client
    print("Connected to the client")
    socket_connection.send(PONG_RESPONSE_BYTES)


if __name__ == "__main__":
    main()
