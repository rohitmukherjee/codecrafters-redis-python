from __future__ import annotations

import re
import selectors
import socket  # noqa: F401
import time
from abc import ABC, abstractmethod
from typing import Union

STRING_ENCODING: str = "utf-8"
MAX_BYTES: int = 1024
REDIS_PORT: int = 6379
NEW_LINE: str = "\n"
CARRIAGE_RETURN: str = "\r"


class KVStore:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.commands: list[Command] = [Get(), Set(), Ping(), Echo(), RPush(), LPush(), LRange()]
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

    def get_value(self, key: str) -> Union[dict, None]:
        return self.store.get(key)

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
        client_socket.send(Response.encode_resp_string_bytes(data_list[4]))


class LRange(Command):
    def __init__(self):
        self.name = "LRANGE"

    def is_command(self, data_list: list[str]) -> bool:
        return len(data_list) == 10 and data_list[2].upper() == self.name

    @staticmethod
    def get_key(data_list: list[str]) -> str:
        return data_list[4]

    @staticmethod
    def get_start_and_end_indices(data_list: list[str], array_length: int) -> tuple[int, int]:
        start_index, end_index = int(data_list[6]), int(data_list[8])
        if start_index < 0:
            start_index = array_length + start_index
        if end_index < 0:
            end_index = array_length + end_index
        return start_index, end_index

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        key: str = LRange.get_key(data_list)
        value_dict: dict = store.get(key)
        if value_dict is None or "value" not in value_dict:
            client_socket.send(Response.encode_resp_array_bytes([]))
        else:
            array: list = value_dict.get("value")
            start_index, end_index = LRange.get_start_and_end_indices(data_list, len(array))
            if start_index < 0 or end_index >= len(array) or start_index > end_index:
                client_socket.send(Response.encode_resp_array_bytes([]))
                return
            client_socket.send(Response.encode_resp_array_bytes(array[start_index:end_index + 1]))


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
            value_dict = store.get(key)
            if "expiryTimeMillis" in value_dict:
                if Get.current_millis() > value_dict["expiryTimeMillis"]:
                    client_socket.send(Response.NULL_BULK_STRING_BYTES)
                else:
                    client_socket.send(Response.encode_resp_string_bytes(value_dict["value"]))
            else:
                client_socket.send(Response.encode_resp_string_bytes(value_dict["value"]))
        else:
            client_socket.send(Response.NULL_BULK_STRING_BYTES)


class LPush(Command):

    def __init__(self):
        self.name = "LPUSH"

    def is_command(self, data_list: list[str]) -> bool:
        return len(data_list) >= 8 and data_list[2].upper() == self.name

    @staticmethod
    def get_key(data_list: list[str]) -> Union[str, None]:
        return data_list[4]

    @staticmethod
    def get_values_to_insert(data_list: list[str]) -> list[str]:
        return data_list[6: len(data_list): 2][::-1]

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        key = self.get_key(data_list)
        values_to_insert: list = self.get_values_to_insert(data_list)
        if key not in store:
            store[key] = {}
        value_dict = store.get(key)
        if "value" not in value_dict:
            value_dict["value"] = list()
        for item in value_dict["value"]:
            values_to_insert.append(item)
        value_dict["value"] = values_to_insert
        size: int = len(value_dict["value"])
        print(store[key])
        client_socket.send(Response.encode_resp_integer_bytes(size))


class RPush(Command):

    def __init__(self):
        self.name = "RPUSH"

    def is_command(self, data_list: list[str]) -> bool:
        return len(data_list) >= 8 and data_list[2].upper() == self.name

    @staticmethod
    def get_key(data_list: list[str]) -> Union[str, None]:
        return data_list[4]

    @staticmethod
    def get_values_to_insert(data_list: list[str]) -> list[str]:
        values_to_insert = []
        for i in range(6, len(data_list), 2):
            values_to_insert.append(data_list[i])
        return values_to_insert

    def handle(self, client_socket: socket, data_list: list[str], store: dict):
        key = self.get_key(data_list)
        values_to_insert = self.get_values_to_insert(data_list)
        if key not in store:
            store[key] = {}
        value_dict = store.get(key)
        if "value" not in value_dict:
            value_dict["value"] = list()
        for values_to_insert in values_to_insert:
            value_dict["value"].append(values_to_insert)
        size: int = len(value_dict["value"])
        print(store[key])
        client_socket.send(Response.encode_resp_integer_bytes(size))


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
        client_socket.send(Response.encode_resp_string_bytes(Response.OK))


class Response:
    OK: str = "OK"
    PONG_RESPONSE_BYTES: bytes = "+PONG\r\n".encode(STRING_ENCODING)
    NULL_BULK_STRING: str = "$-1\r\n"
    NULL_BULK_STRING_BYTES: bytes = NULL_BULK_STRING.encode(STRING_ENCODING)
    NULL_ARRAY_STRING: str = "*0\r\n"
    NULL_ARRAY_STRING_BYTES: bytes = NULL_ARRAY_STRING.encode(STRING_ENCODING)

    @staticmethod
    def encode_resp_array_bytes(array: list) -> bytes:
        return Response.encode_resp_array(array).encode(STRING_ENCODING)

    @staticmethod
    def encode_resp_array(array: list) -> str:
        if array is None or len(array) == 0:
            return Response.NULL_ARRAY_STRING
        else:
            response: str = f"*{len(array)}\r\n"
            for element in array:
                if type(element) is int:
                    response += Response.encode_resp_integer(element)
                elif type(element) is list:
                    response += Response.encode_resp_array(element)
                else:
                    response += Response.encode_resp_string(element)
            return response

    @staticmethod
    def encode_resp_string(string: str) -> str:
        if string is None:
            return ""
        else:
            return f"${len(string)}{CARRIAGE_RETURN}{NEW_LINE}{string}{CARRIAGE_RETURN}{NEW_LINE}"

    @staticmethod
    def encode_resp_string_bytes(string: str) -> bytes:
        return Response.encode_resp_string(string).encode(STRING_ENCODING)

    @staticmethod
    def encode_resp_integer(value: int) -> str:
        if value is None:
            return Response.NULL_BULK_STRING
        else:
            sign: str = "" if int(value) > 0 else "-"
            print(sign)
            return f":{sign}{value}{CARRIAGE_RETURN}{NEW_LINE}"

    @staticmethod
    def encode_resp_integer_bytes(value: int) -> bytes:
        return Response.encode_resp_integer(value).encode(STRING_ENCODING)


if __name__ == "__main__":
    instance = KVStore()
    instance.start()
