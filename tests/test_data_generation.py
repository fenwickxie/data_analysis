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
from tests.fixtures.data_generator_base import WindowDataBuilder, DataGeneratorFactory

def test_window_builder():
    """Test the WindowDataBuilder for creating time-series data."""
    builder = WindowDataBuilder(window_size=10, interval=60)
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
    assert station_param["station_id"] == "S001"
    assert "gun_count" in station_param

    # Test time-series data generation
    realtime_data = factory.generate("station_realtime", entity_id="S001", window_size=5)
    assert realtime_data["station_id"] == "S001"
    assert "history_curve_station_avg" in realtime_data
    assert len(realtime_data["timestamps"]) == 5

    # Test generation of another data type
    car_price_data = factory.generate("car_price", entity_id="S001")
    assert car_price_data["station_id"] == "S001"
    assert "periods" in car_price_data
    assert len(car_price_data["periods"]) > 0

def test_data_generation_with_factory():
    """Test comprehensive data generation using factory."""
    factory = DataGeneratorFactory()
    
    # Test all available data types
    available_types = factory.get_available_types()
    assert len(available_types) > 0
    
    # Test generating each type
    for data_type in ["station_param", "station_realtime", "car_order", "car_price"]:
        if data_type in available_types:
            data = factory.generate(data_type, entity_id="S001", window_size=5)
            assert data is not None
            assert "station_id" in data or isinstance(data, dict)
