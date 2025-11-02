from app.main import *

pingCommand = Ping()
getCommand = Get()
setCommand = Set()
echoCommand = Echo()
rpushCommand = RPush()
lpushCommand = LPush()
lrangeCommand = LRange()


class DummySocket:
    def __init__(self):
        self.sent_messages: list[bytes] = []

    def send(self, data: bytes):
        self.sent_messages.append(data)


def build_lrange_payload(start: int, end: int) -> list[str]:
    return [
        '*4',
        '$6',
        'LRANGE',
        '$8',
        'list_key',
        f'${len(str(start))}',
        str(start),
        f'${len(str(end))}',
        str(end),
        ''
    ]


def build_lpush_payload(*values: str, key: str = 'list_key') -> list[str]:
    payload = [
        f'*{2 + len(values)}',
        '$5',
        'LPUSH',
        f'${len(key)}',
        key,
    ]
    for value in values:
        payload.extend([f'${len(value)}', value])
    payload.append('')
    return payload


def test_command_is_echo_returns_true_for_resp_echo_command():
    data = ["*2", "$4", "ECHO", "$3", "hey", '']

    assert echoCommand.is_command(data) is True


def test_command_is_echo_handles_mixed_case_command():
    data = ["*2", "$4", "eChO", "$3", "hi", '']

    assert echoCommand.is_command(data) is True


def test_command_is_echo_returns_false_when_payload_is_too_short():
    data = ["*1", "$4"]

    assert echoCommand.is_command(data) is False


def test_command_is_ping_returns_true_for_resp_ping_command():
    data = ["*1", "$4", "PING", '']

    assert pingCommand.is_command(data) is True


def test_command_is_ping_handles_mixed_case_command():
    data = ["*1", "$4", "pInG", '']

    assert pingCommand.is_command(data) is True


def test_command_is_ping_returns_false_when_payload_is_too_short():
    data = ["$4", "PING"]

    assert pingCommand.is_command(data) is False


def test_command_is_get_identifies_get_command():
    data = ["*2", "$3", "GET", "$3", "key", '']

    assert getCommand.is_command(data) is True


def test_command_is_get_rejects_non_matching_command():
    data = ["*2", "$3", "SET", "$3", "key"]

    assert getCommand.is_command(data) is False


def test_command_is_set_with_options_identifies_set_command():
    data = ['*5', '$3', 'SET', '$5', 'apple', '$9', 'pineapple', '$2', 'PX', '$3', '100', '']

    assert setCommand.is_command(data) is True


def test_get_options_for_set_command():
    data = ['*5', '$3', 'SET', '$5', 'apple', '$9', 'pineapple', '$2', 'PX', '$3', '100', '']
    command = Set()
    options: dict = command.get_options(data)
    assert "PX" in options
    assert options["PX"] == 100


def test_command_is_set_identifies_set_command():
    data = ["*3", "$3", "SET", "$3", "key", "$5", "value", '']

    assert setCommand.is_command(data) is True


def test_command_is_set_rejects_non_matching_command():
    data = ["*2", "$3", "GET", "$3", "key"]

    assert setCommand.is_command(data) is False


def test_response_encode_resp_returns_bulk_string():
    assert Response.encode_resp_string_bytes("PONG") == b"$4\r\nPONG\r\n"


def test_response_encode_resp_returns_empty_bytes_for_none():
    assert Response.encode_resp_string_bytes(None) == b""


# RPush Test Cases

def test_command_is_rpush_command():
    data = ['*3', '$5', 'RPUSH', '$8', 'list_key', '$3', 'foo', '']
    assert rpushCommand.is_command(data)
    assert rpushCommand.get_values_to_insert(data) == ["foo"]


def test_get_values_to_insert():
    data = ['*5', '$5', 'RPUSH', '$8', 'list_key', '$3', 'foo', '$3', 'bar', '$3', 'baz', '']
    assert rpushCommand.get_key(data) == "list_key"
    assert rpushCommand.get_values_to_insert(data) == ["foo", "bar", "baz"]


# LRange Test Cases
def test_LRange_is_command():
    data = ['*4', '$6', 'LRANGE', '$8', 'list_key', '$1', '0', '$1', '2', '']
    assert lrangeCommand.is_command(data)


def test_lrange_index_extraction():
    data = ['*4', '$6', 'LRANGE', '$8', 'list_key', '$1', '0', '$1', '2', '']
    assert lrangeCommand.get_start_and_end_indices(data, 3) == (0, 2)
    assert lrangeCommand.get_start_and_end_indices(['*4', '$6', 'LRANGE', '$8', 'list_key', '$1', '-2', '$1', '-1', ''],
                                                   5) == (3, 4)
    assert lrangeCommand.get_start_and_end_indices(['*4', '$6', 'LRANGE', '$8', 'list_key', '$1', '-3', '$1', '-1', ''],
                                                   5) == (2, 4)


# Test cases for lpush
def test_command_is_lpush_command():
    data = ['*3', '$5', 'LPUSH', '$8', 'list_key', '$1', 'a', '']
    assert lpushCommand.is_command(data)


def test_lpush_get_values_to_insert_preserves_argument_order():
    data = ['*5', '$5', 'LPUSH', '$8', 'list_key', '$1', 'a', '$1', 'b', '$1', 'c', '']
    assert lpushCommand.get_key(data) == 'list_key'
    print(lpushCommand.get_values_to_insert(data))
    assert lpushCommand.get_values_to_insert(data) == ['c', 'b', 'a']


def test_lpush_handle_creates_list_and_prepends_values():
    store: dict = {}
    socket = DummySocket()
    payload = build_lpush_payload('a', 'b', 'c')

    lpushCommand.handle(socket, payload, store)

    assert store['list_key']['value'] == ['c', 'b', 'a']
    assert socket.sent_messages == [Response.encode_resp_integer_bytes(3)]


def test_lpush_handle_prepends_to_existing_list_in_correct_order():
    store: dict = {'list_key': {'value': ['z']}}
    socket = DummySocket()
    payload = build_lpush_payload('b', 'a')

    lpushCommand.handle(socket, payload, store)

    print(store['list_key']['value'])
    assert store['list_key']['value'] == ['a', 'b', 'z']
    # assert socket.sent_messages == [Response.encode_resp_integer_bytes(3)]


def test_lrange_handles_tail_slice_with_negative_indices():
    store = {'list_key': {'value': ['a', 'b', 'c', 'd', 'e']}}
    socket = DummySocket()
    payload = build_lrange_payload(-2, -1)

    lrangeCommand.handle(socket, payload, store)

    assert socket.sent_messages == [Response.encode_resp_array_bytes(['d', 'e'])]


def test_lrange_handles_positive_start_and_negative_end_index():
    store = {'list_key': {'value': ['a', 'b', 'c', 'd', 'e']}}
    socket = DummySocket()
    payload = build_lrange_payload(2, -1)

    lrangeCommand.handle(socket, payload, store)

    assert socket.sent_messages == [Response.encode_resp_array_bytes(['c', 'd', 'e'])]


def test_lrange_negative_start_out_of_range_maps_to_zero():
    store = {'list_key': {'value': ['a', 'b', 'c', 'd', 'e']}}
    socket = DummySocket()
    payload = build_lrange_payload(-6, -1)

    lrangeCommand.handle(socket, payload, store)

    assert socket.sent_messages == [Response.NULL_ARRAY_STRING_BYTES]


def test_lrange_negative_end_out_of_range_maps_to_zero():
    store = {'list_key': {'value': ['a', 'b', 'c', 'd', 'e']}}
    socket = DummySocket()
    payload = build_lrange_payload(0, -6)

    lrangeCommand.handle(socket, payload, store)

    assert socket.sent_messages == [Response.NULL_ARRAY_STRING_BYTES]


# RESP Encoding Test Cases

def test_RESP_encoding_for_arrays():
    assert Response.encode_resp_array(["hello", "world"]) == "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    assert Response.encode_resp_array([1, 2, 3]) == "*3\r\n:1\r\n:2\r\n:3\r\n"
    assert Response.encode_resp_array([1, 2, 3, 4, "hello"]) == "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n"
