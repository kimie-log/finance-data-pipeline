from unittest import mock

import pytest

from utils.retry import run_with_retry


def test_success_first_try():
    action = mock.Mock(return_value="ok")

    result = run_with_retry(action, action_name="unit-test", retries=2)

    assert result == "ok"
    action.assert_called_once()


@mock.patch("utils.retry.time.sleep")
def test_retry_then_success(mock_sleep):
    calls = []

    def action():
        if len(calls) < 2:
            calls.append("fail")
            raise ValueError("boom")
        return "ok"

    result = run_with_retry(
        action,
        action_name="unit-test",
        retries=3,
        initial_delay=0.01,
        backoff=1.0,
        jitter=0.0,
    )

    assert result == "ok"
    assert mock_sleep.call_count == 2


@mock.patch("utils.retry.time.sleep")
def test_exhausted_retries(mock_sleep):
    action = mock.Mock(side_effect=ValueError("boom"))

    with pytest.raises(ValueError):
        run_with_retry(
            action,
            action_name="unit-test",
            retries=2,
            initial_delay=0.01,
            backoff=1.0,
            jitter=0.0,
        )

    assert mock_sleep.call_count == 2
