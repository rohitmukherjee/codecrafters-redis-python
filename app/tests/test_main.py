from app.main import is_echo, is_ping


def test_is_echo_returns_true_for_resp_echo_command():
    data = ["*2", "$4", "ECHO", "$3", "hey"]

    assert is_echo(data) is True


def test_is_echo_handles_mixed_case_command():
    data = ["*2", "$4", "eChO", "$3", "hi"]

    assert is_echo(data) is True


def test_is_echo_returns_false_when_payload_is_too_short():
    data = ["*1", "$4"]

    assert is_echo(data) is False


def test_is_ping_returns_true_for_resp_ping_command():
    data = ["*1", "$4", "PING"]

    assert is_ping(data) is True


def test_is_ping_handles_mixed_case_command():
    data = ["*1", "$4", "pInG"]

    assert is_ping(data) is True


def test_is_ping_returns_false_when_payload_is_too_short():
    data = ["$4", "PING"]

    assert is_ping(data) is False
