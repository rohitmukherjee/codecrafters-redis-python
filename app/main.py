import re
import selectors
import socket  # noqa: F401
import time
from abc import ABC, abstractmethod

STRING_ENCODING: str = "utf-8"
MAX_BYTES: int = 1024
REDIS_PORT: int = 6379
NEW_LINE: str = "\n"
CARRIAGE_RETURN: str = "\r"


class KVStore:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.commands: list[Command] = [Get(), Set(), Ping(), Echo()]
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
                found_matching_command: bool = False
                for command in self.commands:
                    if command.is_command(data_list):
                        found_matching_command = True
                        command.handle(client_socket, data_list, self.store)
                if not found_matching_command:
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


class Command(ABC):
    @abstractmethod
    def is_command(self, data_list: list[str]) -> bool:
        pass

    @abstractmethod
    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        pass

    def get_options(self, data_list: list[str]) -> dict[str, str]:
        return {}


class Ping(Command):
    def __init__(self):
        self.name = "PING"

    def is_command(self, data: list[str]) -> bool:
        return len(data) == 4 and data[2].upper() == self.name

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        client_socket.send(Response.PONG_RESPONSE_BYTES)


class Echo(Command):
    def __init__(self):
        self.name = "ECHO"

    def is_command(self, data_list: list[str]) -> bool:
        return len(data_list) == 6 and data_list[2].upper() == self.name

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        client_socket.send(Response.encode_resp(data_list[4]))


class Get(Command):
    def __init__(self):
        self.name = "GET"

    def is_command(self, data_list: list[str]) -> bool:
        return len(data_list) == 6 and data_list[2].upper() == self.name

    @staticmethod
    def current_millis():
        return round(time.time() * 1000)

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        key: str = data_list[4]
        if key in store:
            value_dict = store[key]
            if "expiryTimeMillis" in value_dict:
                if Get.current_millis() > value_dict["expiryTimeMillis"]:
                    client_socket.send(Response.NULL_BULK_STRING_BYTES)
                else:
                    client_socket.send(Response.encode_resp(value_dict["value"]))
            else:
                client_socket.send(Response.encode_resp(value_dict["value"]))
        else:
            client_socket.send(Response.NULL_BULK_STRING_BYTES)


class Set(Command):
    def __init__(self):
        self.name = "SET"

    def get_options(self, data_list: list[str]) -> dict[str, str]:
        options = {}
        if len(data_list) > 8:
            option = data_list[8]
            if option == "EX" or option == "PX":
                options[option] = int(data_list[10])
        return options

    def is_command(self, data_list: list[str]) -> bool:
        return len(data_list) >= 8 and data_list[2].upper() == self.name

    @staticmethod
    def current_millis():
        return round(time.time() * 1000)

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        key: str = data_list[4]
        value: str = data_list[6]
        value_dict: dict = {"value": value}
        for option, option_value in self.get_options(data_list).items():
            expiration_millis: int = option_value if option == "PX" else (int(option_value) * 1000)
            value_dict["expiryTimeMillis"] = Set.current_millis() + expiration_millis
        store[key] = value_dict
        client_socket.send(Response.encode_resp(Response.OK))


class Response:
    OK: str = "OK"
    PONG_RESPONSE_BYTES: bytes = "+PONG\r\n".encode(STRING_ENCODING)
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
