import re
import selectors
import socket  # noqa: F401

PONG_RESPONSE: str = "+PONG\r\n"
STRING_ENCODING: str = "utf-8"
PONG_RESPONSE_BYTES: bytes = PONG_RESPONSE.encode(STRING_ENCODING)
MAX_BYTES: int = 1024
REDIS_PORT: int = 6379
NEW_LINE: str = "\n"
CARRIAGE_RETURN: str = "\r"
PING_COMMAND: str = "PING"
ECHO_COMMAND: str = "ECHO"

selector = selectors.DefaultSelector()


def accept_connection(server_sock: socket):
    client_socket, client_address = server_sock.accept()
    print("Connected:", client_address)
    client_socket.setblocking(False)
    # We are registering on the client-socket here and invoking the read_message callback
    # when the socket is ready to send
    selector.register(client_socket, selectors.EVENT_READ, read_message)


def is_echo(data: list[str]) -> bool:
    return len(data) > 2 and data[2].upper() == ECHO_COMMAND


def is_ping(data: list[str]) -> bool:
    return len(data) > 2 and data[2].upper() == PING_COMMAND


def read_message(client_socket: socket):
    try:
        raw_data: bytes = client_socket.recv(MAX_BYTES)
        if not raw_data:  # client closed
            print("Client disconnected")
            selector.unregister(client_socket)
            client_socket.close()
        else:
            data: str = raw_data.decode(STRING_ENCODING)
            data_list: list[str] = re.split(r'[\r\n]+', data)
            print(data_list)
            if is_ping(data_list):
                handle_ping(client_socket)
            elif is_echo(data_list):
                handle_echo(client_socket, data_list)

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


def handle_ping(client_socket: socket):
    client_socket.send(PONG_RESPONSE_BYTES)


def handle_echo(client_socket: socket, data_list: list[str]):
    response_string: str = data_list[4]
    response = f"${len(response_string)}{CARRIAGE_RETURN}{NEW_LINE}{response_string}{CARRIAGE_RETURN}{NEW_LINE}"
    print("Sending back: " + response)
    client_socket.send(response.encode(STRING_ENCODING))


if __name__ == "__main__":
    main()
