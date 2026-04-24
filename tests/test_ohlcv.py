import pytest
from processing.ohlcv_aggregator import (
    get_window_start,
    is_window_closed,
    WINDOW_SIZE_MS,
    ALLOWED_LATENESS_MS
)

# -------------------------------
# 1. Windowing Tests
# -------------------------------

def test_get_window_start_basic():
    ts = 1700000054321

    expected = (ts // WINDOW_SIZE_MS) * WINDOW_SIZE_MS
    assert get_window_start(ts) == expected


def test_get_window_alignment():
    # Exact boundary timestamp should remain aligned
    ts = 1700000060000

    expected = (ts // WINDOW_SIZE_MS) * WINDOW_SIZE_MS
    assert get_window_start(ts) == expected


# -------------------------------
# 2. Window Closure Logic
# -------------------------------

def test_window_not_closed_within_lateness():
    window_start = 1000

    # still inside allowed lateness window
    max_event_time = window_start + WINDOW_SIZE_MS + ALLOWED_LATENESS_MS - 1

    assert is_window_closed(window_start, max_event_time) is False


def test_window_closed_after_lateness():
    window_start = 1000

    # beyond allowed lateness window
    max_event_time = window_start + WINDOW_SIZE_MS + ALLOWED_LATENESS_MS + 1

    assert is_window_closed(window_start, max_event_time) is True


# -------------------------------
# 3. OHLCV Aggregation Logic (Pure Model)
# -------------------------------

def create_candle():
    return {
        "open": 100,
        "high": 100,
        "low": 100,
        "close": 100,
        "volume": 1
    }


def update_candle(candle, price, qty):
    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["volume"] += qty


def test_candle_update():
    candle = create_candle()

    update_candle(candle, 120, 2)
    update_candle(candle, 90, 3)

    assert candle["high"] == 120
    assert candle["low"] == 90
    assert candle["close"] == 90
    assert candle["volume"] == 6


# -------------------------------
# 4. Full OHLCV Scenario
# -------------------------------

def test_full_ohlcv_sequence():
    candle = create_candle()

    trades = [
        (110, 1),
        (115, 2),
        (105, 1),
        (130, 3),
    ]

    for price, qty in trades:
        update_candle(candle, price, qty)

    assert candle["open"] == 100
    assert candle["high"] == 130
    assert candle["low"] == 100
    assert candle["close"] == 130
    assert candle["volume"] == 8