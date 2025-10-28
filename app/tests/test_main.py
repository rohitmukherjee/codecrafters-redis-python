from app.main import *

pingCommand = Ping()
getCommand = Get()
setCommand = Set()
echoCommand = Echo()
rpushCommand = RPush()


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
    assert Response.encode_resp("PONG") == b"$4\r\nPONG\r\n"


def test_response_encode_resp_returns_empty_bytes_for_none():
    assert Response.encode_resp(None) == b""


# RPush Test Cases

def test_command_is_rpush_command():
    data = ['*3', '$5', 'RPUSH', '$8', 'list_key', '$3', 'foo', '']
    assert rpushCommand.is_command(data)
    assert rpushCommand.get_values_to_insert(data) == ["foo"]


def test_get_values_to_insert():
    data = ['*5', '$5', 'RPUSH', '$8', 'list_key', '$3', 'foo', '$3', 'bar', '$3', 'baz', '']
    assert rpushCommand.get_key(data) == "list_key"
    assert rpushCommand.get_values_to_insert(data) == ["foo", "bar", "baz"]
