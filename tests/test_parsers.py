#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-28
project: data_analysis
filename: test_parsers.py
version: 1.0
description: Unit tests for all topic and module parsers.
"""

import pytest
from d_a.topic_parsers import *
from d_a.parsers import *
from d_a.config import TOPIC_DETAIL, MODULE_OUTPUT_TOPICS

# Topic to parser class mapping
TOPIC_PARSER_MAP = {
    "SCHEDULE-STATION-PARAM": StationParamParser,
    "SCHEDULE-STATION-REALTIME-DATA": StationRealtimeDataParser,
    "SCHEDULE-ENVIRONMENT-CALENDAR": EnvironmentCalendarParser,
    "SCHEDULE-DEVICE-METER": DeviceMeterParser,
    "SCHEDULE-DEVICE-GUN": DeviceGunParser,
    "SCHEDULE-CAR-ORDER": CarOrderParser,
    "SCHEDULE-CAR-PRICE": CarPriceParser,
    "SCHEDULE-DEVICE-ERROR": DeviceErrorParser,
    "SCHEDULE-DEVICE-HOST-DCDC": DeviceHostDCDCParser,
    "SCHEDULE-DEVICE-HOST-ACDC": DeviceHostACDCParser,
    "SCHEDULE-DEVICE-STORAGE": DeviceStorageParser,
    "SCHEDULE-ENVIRONMENT-WEATHER": EnvironmentWeatherParser,
    "SCHEDULE-DEVICE-PV": DevicePvParser,
}

# Fixture for each topic parser
@pytest.fixture(params=list(TOPIC_DETAIL.keys()))
def topic_parser(request):
    """Provides a fixture for each topic parser."""
    topic = request.param
    
    # Handle module output topics
    if topic in MODULE_OUTPUT_TOPICS.values():
        module_name = [k for k, v in MODULE_OUTPUT_TOPICS.items() if v == topic][0]
        return ModelOutputParser(module_name)
    
    # Get parser from map
    parser_class = TOPIC_PARSER_MAP.get(topic)
    if parser_class:
        return parser_class()
    
    pytest.skip(f"No parser found for topic {topic}")


def test_topic_parser_handles_valid_data(topic_parser):
    """Test that each topic parser correctly handles a valid data payload."""
    # This test would require generating valid mock data for each topic.
    # For now, we just check that the parser exists.
    assert topic_parser is not None

def test_station_param_parser():
    """Tests for StationParamParser."""
    parser = StationParamParser()
    data = {
        "stationId": "S001",
        "stationLng": 121.47,
        "stationLat": 31.23,
        "gunNum": 10,
        "gridCapacity": 500,
        "meterId": "M001",
        "powerNum": 2,
        "normalClap": 1,
        "hostCode": "H001",
        "extra_field": "should_be_ignored"
    }
    parsed = parser.parse(data)
    assert parsed is not None
    assert parsed["stationId"] == "S001"
    assert "extra_field" not in parsed
    assert parsed["gunNum"] == 10

    # Test missing fields - parser should handle them gracefully
    data_missing = {"stationId": "S002"}
    parsed_missing = parser.parse(data_missing)
    assert parsed_missing is not None
    assert parsed_missing["stationId"] == "S002"

def test_car_order_parser_windowing():
    """Test windowing logic for CarOrderParser."""
    parser = CarOrderParser()
    # CarOrderParser expects raw data, not tuples
    window_data = [
        {
            "stationId": "S001", "transactionSerialNo": "TXN001", "hostCode": "H001",
            "gunNo": "G01", "terminalMaxOutElectric": 250.0, 
            "startChargeTime": "2025-01-01 10:00:00", 
            "endChargeTime": "2025-01-01 11:00:00", "beginSOC": 20.0, "soc": 80, 
            "terminalRequireVoltage": 400.0, "terminalRequireElectric": 150.0,
            "outputPower": 60, "batteryNominalTotalCapacity": 100, 
            "carProducerCode": "Tesla"
        },
        {
            "stationId": "S001", "transactionSerialNo": "TXN002", "hostCode": "H001",
            "gunNo": "G02", "terminalMaxOutElectric": 250.0,
            "startChargeTime": "2025-01-01 10:05:00", 
            "endChargeTime": "2025-01-01 11:05:00", "beginSOC": 25.0, "soc": 85,
            "terminalRequireVoltage": 400.0, "terminalRequireElectric": 150.0, 
            "outputPower": 65, "batteryNominalTotalCapacity": 110, 
            "carProducerCode": "BYD"
        }
    ]
    parsed = parser.parse_window(window_data)
    assert parsed is not None
    assert "car_order" in parsed

# Add more specific tests for other parsers...
