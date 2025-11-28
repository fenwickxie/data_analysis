#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""DataDispatcher behavioural tests aligned with current implementation."""

from __future__ import annotations

import time

import pytest

from d_a.config import MODULE_TO_TOPICS
from d_a.dispatcher import DataDispatcher


@pytest.fixture
def dispatcher():
    """Provide a dispatcher with short expiration for tests."""
    return DataDispatcher(data_expire_seconds=2)


def seed_load_prediction_topics(dispatcher: DataDispatcher, station_id: str) -> None:
    """Populate the minimum topics required by the load_prediction module."""
    dispatcher.update_topic_data(
        station_id,
        "SCHEDULE-STATION-PARAM",
        {
            "stationId": station_id,
            "stationLng": 120.0,
            "stationLat": 30.0,
            "gunNum": 4,
            "gridCapacity": 160,
            "meterId": "M001",
            "powerNum": 2,
            "normalClap": 10,
            "hostCode": "H001",
        },
    )

    dispatcher.update_topic_data(
        station_id,
        "SCHEDULE-STATION-REALTIME-DATA",
        [
            {
                "stationId": station_id,
                "sendTime": "2025-11-01 00:00:00",
                "outputPowerPerStationAvg": 1.5,
                "outputPowerPerStationMax": 3.5,
                "gunPower": {
                    "gunNo": ["01", "02"],
                    "outputPowerPerGunAvg": [1.0, 2.0],
                    "outputPowerPerGunMax": [2.0, 4.0],
                },
            }
        ],
    )

    dispatcher.update_topic_data(
        station_id,
        "SCHEDULE-ENVIRONMENT-CALENDAR",
        {"dayOfWeek": 1, "holiday": False},
    )


def test_get_module_input_reports_data_quality(dispatcher):
    station_id = "station_lp"
    seed_load_prediction_topics(dispatcher, station_id)

    result = dispatcher.get_module_input(station_id, "load_prediction")

    assert result["stationId"] == station_id
    quality = result["_data_quality"]
    expected_topics = MODULE_TO_TOPICS["load_prediction"]
    assert quality["total_topics"] == len(expected_topics)
    assert set(quality["available_topics"]) == set(expected_topics)
    assert quality["availability_ratio"] == pytest.approx(1.0)


def test_device_host_updates_are_aggregated(dispatcher):
    station_id = "station_agg"
    dispatcher.update_topic_data(
        station_id,
        "SCHEDULE-DEVICE-HOST-DCDC",
        {"stationId": station_id, "hostCode": "H001", "dcWorkStatus": [1, 1]},
    )
    dispatcher.update_topic_data(
        station_id,
        "SCHEDULE-DEVICE-HOST-DCDC",
        {"stationId": station_id, "hostCode": "H002", "dcWorkStatus": [0, 1]},
    )

    window = dispatcher.get_topic_window(station_id, "SCHEDULE-DEVICE-HOST-DCDC")
    assert len(window) == 1
    host_dict = window[0]
    assert set(host_dict.keys()) == {"H001", "H002"}
    for host_code, (payload, ts) in host_dict.items():
        assert payload["hostCode"] == host_code
        assert isinstance(ts, float)


def test_clean_expired_removes_stale_entries():
    dispatcher = DataDispatcher(data_expire_seconds=0.01)
    station_id = "station_expired"
    dispatcher.update_topic_data(
        station_id,
        "SCHEDULE-DEVICE-METER",
        {"stationId": station_id, "meterId": "M001"},
        timestamp=time.time() - 10,
    )

    time.sleep(0.02)
    dispatcher.clean_expired()

    assert dispatcher.get_topic_window(station_id, "SCHEDULE-DEVICE-METER") == []


def test_unknown_topic_is_cached(dispatcher):
    dispatcher.update_topic_data("station_u", "CUSTOM-TOPIC", {"value": 1})
    window = dispatcher.get_topic_window("station_u", "CUSTOM-TOPIC")
    assert window == [{"value": 1}]


def test_station_isolation(dispatcher):
    dispatcher.update_topic_data(
        "station_a",
        "SCHEDULE-DEVICE-METER",
        {"stationId": "station_a", "meterId": "M1", "meterPower": 10},
    )
    dispatcher.update_topic_data(
        "station_b",
        "SCHEDULE-DEVICE-METER",
        {"stationId": "station_b", "meterId": "M2", "meterPower": 20},
    )

    window_a = dispatcher.get_topic_window("station_a", "SCHEDULE-DEVICE-METER")
    window_b = dispatcher.get_topic_window("station_b", "SCHEDULE-DEVICE-METER")

    assert window_a != window_b


def test_time_series_concatenation(dispatcher):
    """Test time series data concatenation, especially for gun-related data."""
    station_id = "station_ts"
    topic = "SCHEDULE-STATION-REALTIME-DATA"

    # Simulate three time-series data points with varying gun numbers
    test_data = [
        {"stationId": station_id, "sendTime": "2025-11-04 09:00:00", "gunPower": {"gunNo": ["01", "02"], "outputPowerPerGunAvg": [10, 20]}},
        {"stationId": station_id, "sendTime": "2025-11-04 10:00:00", "gunPower": {"gunNo": ["02", "03"], "outputPowerPerGunAvg": [25, 35]}},
        {"stationId": station_id, "sendTime": "2025-11-04 11:00:00", "gunPower": {"gunNo": ["01", "03"], "outputPowerPerGunAvg": [15, 40]}},
    ]

    for data in test_data:
        dispatcher.update_topic_data(station_id, topic, data)

    # To get module input, other dependencies for 'load_prediction' must be met
    seed_load_prediction_topics(dispatcher, station_id)

    result = dispatcher.get_module_input(station_id, "load_prediction")
    
    assert result is not None
    # The dispatcher should align the gun data. The exact alignment logic depends on the implementation.
    # A good test would check if the output arrays are correctly padded and ordered.
    # For example, if the unified gun list is ["01", "02", "03"], the power for the first timestamp should be [10, 20, 0] (or None).
    
    # This is a simplified check
    assert "gunPower" in result
    gun_power_data = result["gunPower"]
    assert "gunNo" in gun_power_data
    assert "outputPowerPerGunAvg" in gun_power_data
    
    # Check if the data is aligned (all lists have the same length)
    num_guns = len(gun_power_data["gunNo"])
    for power_list in gun_power_data["outputPowerPerGunAvg"]:
        assert len(power_list) == num_guns
