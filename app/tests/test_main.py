from app.main import *

pingCommand = Ping()
getCommand = Get()
setCommand = Set()
echoCommand = Echo()


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
