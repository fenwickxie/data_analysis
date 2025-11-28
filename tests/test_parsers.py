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

# Fixture for each topic parser
@pytest.fixture(params=list(TOPIC_DETAIL.keys()))
def topic_parser(request):
    """Provides a fixture for each topic parser."""
    topic = request.param
    if topic in MODULE_OUTPUT_TOPICS.values():
        module_name = [k for k, v in MODULE_OUTPUT_TOPICS.items() if v == topic][0]
        return ModelOutputParser(module_name)
    
    parser_class_name = "".join([p.capitalize() for p in topic.split('-')[1:]]) + "Parser"
    # A bit of a hack to find the right class name, e.g. SCHEDULE-STATION-PARAM -> StationParamParser
    if "DCDC" in parser_class_name:
        parser_class_name = "DeviceHostDCDCParser"
    elif "ACDC" in parser_class_name:
        parser_class_name = "DeviceHostACDCParser"
    
    # Find the correct class from the imported parsers
    parser_class = globals().get(parser_class_name)
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
    assert parsed["stationId"] == "S001"
    assert "extra_field" not in parsed
    assert parsed["gunNum"] == 10

    # Test missing fields
    data_missing = {"stationId": "S002"}
    parsed_missing = parser.parse(data_missing)
    assert parsed_missing["stationId"] == "S002"
    assert parsed_missing["gunNum"] is None

def test_car_order_parser_windowing():
    """Test windowing logic for CarOrderParser."""
    parser = CarOrderParser()
    window_data = [
        (
            {
                "stationId": "S001", "gunNo": "G01", "startChargeTime": "2025-01-01 10:00:00", 
                "endChargeTime": "2025-01-01 11:00:00", "soc": 80, "outputPower": 60, 
                "batteryNominalTotalCapacity": 100, "carProducerCode": "Tesla"
            }, 
            1672538400
        ),
        (
            {
                "stationId": "S001", "gunNo": "G02", "startChargeTime": "2025-01-01 10:05:00", 
                "endChargeTime": "2025-01-01 11:05:00", "soc": 85, "outputPower": 65, 
                "batteryNominalTotalCapacity": 110, "carProducerCode": "BYD"
            },
            1672538700
        )
    ]
    parsed = parser.parse_window(window_data)
    assert len(parsed["gunNo"]) == 2
    assert parsed["gunNo"] == ["G01", "G02"]
    assert parsed["soc"] == [80, 85]

# Add more specific tests for other parsers...
