import re
import selectors
import socket  # noqa: F401

STRING_ENCODING: str = "utf-8"
MAX_BYTES: int = 1024
REDIS_PORT: int = 6379
NEW_LINE: str = "\n"
CARRIAGE_RETURN: str = "\r"


class KVStore:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.store = {}

    def accept_connection(self, server_sock: socket):
        client_socket, client_address = server_sock.accept()
        print("Connected:", client_address)
        client_socket.setblocking(False)
        # We are registering on the client-socket here and invoking the read_message callback
        # when the socket is ready to send
        self.selector.register(client_socket, selectors.EVENT_READ, self.read_message)

    def read_message(self, client_socket: socket):
        try:
            raw_data: bytes = client_socket.recv(MAX_BYTES)
            if not raw_data:  # client closed
                print("Client disconnected")
                self.selector.unregister(client_socket)
                client_socket.close()
            else:
                data: str = raw_data.decode(STRING_ENCODING)
                data_list: list[str] = re.split(r'[\r\n]+', data)
                print(data_list)
                if Command.is_ping(data_list):
                    self.handle_ping(client_socket)
                elif Command.is_echo(data_list):
                    self.handle_echo(client_socket, data_list)
                elif Command.is_set(data_list):
                    self.handle_set(client_socket, data_list)
                elif Command.is_get(data_list):
                    self.handle_get(client_socket, data_list)
                else:
                    print("Unrecognized Command")


        except ConnectionError:
            self.selector.unregister(client_socket)
            client_socket.close()

    def start(self):
        server_socket = socket.create_server(("localhost", REDIS_PORT), reuse_port=False)
        server_socket.listen()
        server_socket.setblocking(False)
        self.selector.register(server_socket, selectors.EVENT_READ, self.accept_connection)

        while True:
            for key, _ in self.selector.select():
                callback = key.data
                callback(key.fileobj)
        # server_socket.close()

    def handle_ping(self, client_socket: socket):
        client_socket.send(Response.PPONG_RESPONSE_BYTES)

    def handle_set(self, client_socket: socket, data_list: list[str]):
        key: str = data_list[4]
        value: str = data_list[6]
        self.store[key] = value
        print(str(self.store))
        client_socket.send(Response.encode_resp(Response.OK))

    def handle_get(self, client_socket: socket, data_list: list[str]):
        key: str = data_list[4]
        if key in self.store:
            client_socket.send(Response.encode_resp(self.store[key]))
        else:
            client_socket.send(Response.NULL_BULK_STRING_BYTES)

    def handle_echo(self, client_socket: socket, data_list: list[str]):
        client_socket.send(Response.encode_resp(data_list[4]))


class Command:
    PING: str = "PING"
    ECHO: str = "ECHO"
    GET: str = "GET"
    SET: str = "SET"

    @staticmethod
    def is_echo(data: list[str]) -> bool:
        return len(data) > 2 and data[2].upper() == Command.ECHO and len(data) == 6

    @staticmethod
    def is_ping(data: list[str]) -> bool:
        return len(data) > 2 and data[2].upper() == Command.PING and len(data) == 4

    @staticmethod
    def is_get(data: list[str]) -> bool:
        return len(data) > 2 and data[2].upper() == Command.GET and len(data) == 6

    @staticmethod
    def is_set(data: list[str]):
        return len(data) > 2 and data[2].upper() == Command.SET and len(data) == 8


class Response:
    OK: str = "OK"
    PPONG_RESPONSE_BYTES: bytes = "+PONG\r\n".encode(STRING_ENCODING)
    NULL_BULK_STRING_BYTES: bytes = "$-1\r\n".encode(STRING_ENCODING)

    @staticmethod
    def encode_resp(response: str) -> bytes:
        if response is None:
            return "".encode(STRING_ENCODING)
        else:
            return f"${len(response)}{CARRIAGE_RETURN}{NEW_LINE}{response}{CARRIAGE_RETURN}{NEW_LINE}".encode(
                STRING_ENCODING)


if __name__ == "__main__":
    instance = KVStore()
    instance.start()
