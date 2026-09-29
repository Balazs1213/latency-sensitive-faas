import pytest
from unittest.mock import MagicMock
from event import extract_event, create_event

def test_extract_event_with_json() -> None:
    # Mocking the Context and its request
    mock_context = MagicMock()
    mock_context.request.json = {"key": "value"}
    mock_context.request.data = None
    mock_context.request.is_json = True

    event = extract_event(mock_context)
    assert event.json == {"key": "value"}
    assert event.data is None


def test_extract_event_with_data() -> None:
    # Mocking the Context and its request
    mock_context = MagicMock()
    mock_context.request.json = None
    mock_context.request.data = b"binary data"
    mock_context.request.is_json = False

    event = extract_event(mock_context)
    assert event.json is None
    assert event.data == b"binary data"


def test_extract_event_with_both_none() -> None:
    # Mocking the Context and its request
    mock_context = MagicMock()
    mock_context.request.json = None
    mock_context.request.data = None

    event = extract_event(mock_context)
    assert event.json is None
    assert event.data is None


def test_create_event_with_dict() -> None:
    data = {"key": "value"}
    event = create_event(data)
    assert event.json == data
    assert event.data is None


def test_create_event_with_bytes() -> None:
    data = b"binary data"
    event = create_event(data)
    assert event.json is None
    assert event.data == data


def test_create_event_with_invalid_type() -> None:
    data = 12345  # Invalid type
    event = create_event(data) # type: ignore
    assert event.json is None
    assert event.data is None