#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-28
project: data_analysis
filename: test_data_generation.py
version: 1.0
description: Tests for mock data generation utilities.
"""

import pytest
from tests.fixtures.data_generator_base import WindowDataBuilder
from tests.fixtures.test_utils import DataGeneratorFactory

def test_window_builder():
    """Test the WindowDataBuilder for creating time-series data."""
    builder = WindowDataBuilder(window_size=10, interval_seconds=60)
    data = builder.add_metadata("station_id", "S001") \
                  .add_random_field("power", 100, 200) \
                  .add_constant_field("status", 1) \
                  .add_linear_field("soc", 20, 80) \
                  .build()

    assert data["station_id"] == "S001"
    assert len(data["timestamps"]) == 10
    assert len(data["power"]) == 10
    assert all(100 <= p <= 200 for p in data["power"])
    assert all(s == 1 for s in data["status"])
    assert data["soc"][0] == 20
    assert data["soc"][-1] == 80

def test_data_generator_factory():
    """Test the DataGeneratorFactory for producing different data types."""
    factory = DataGeneratorFactory()
    
    # Test station parameter generation
    station_param = factory.generate("station_param", entity_id="S001")
    assert station_param["stationId"] == "S001"
    assert "gunNum" in station_param

    # Test time-series data generation
    realtime_data = factory.generate("station_realtime_data", entity_id="S001", window_size=5)
    assert realtime_data["stationId"] == "S001"
    assert "gunPower" in realtime_data
    assert len(realtime_data["sendTime"]) == 5

    # Test generation of another data type
    car_price_data = factory.generate("car_price", entity_id="S001")
    assert car_price_data[0]["stationId"] == "S001"
    assert "gridPrice" in car_price_data[0]

def test_extract_station_data_logic(monkeypatch):
    """Test the logic for extracting station data from various message formats."""
    # This logic is in ServiceBase, so we can test it there.
    from d_a.service_base import ServiceBase

    # Format 1: List of dicts with 'stationId'
    data1 = [{"stationId": "S001", "value": 1}, {"stationId": "S002", "value": 2}]
    extracted1 = ServiceBase.extract_station_data("some_topic", data1)
    assert extracted1 == [("S001", data1[0]), ("S002", data1[1])]

    # Format 2: Dict with a specific key (e.g., 'realTimeData')
    data2 = {"realTimeData": [{"stationId": "S003", "value": 3}]}
    extracted2 = ServiceBase.extract_station_data("SCHEDULE-STATION-REALTIME-DATA", data2)
    assert extracted2 == [("S003", data2["realTimeData"][0])]

    # Format 3: Global data
    data3 = {"calendar": [{"date": "2025-01-01"}]}
    extracted3 = ServiceBase.extract_station_data("SCHEDULE-ENVIRONMENT-CALENDAR", data3)
    assert extracted3 == [("__global__", data3)]

    # Format 4: Single dict with 'stationId'
    data4 = {"stationId": "S004", "value": 4}
    extracted4 = ServiceBase.extract_station_data("some_other_topic", data4)
    assert extracted4 == [("S004", data4)]
