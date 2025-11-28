#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Config-based parser and topic mapping tests."""

from __future__ import annotations

import pytest

from d_a import config
from d_a.parser_base import ConfigBasedParser


@pytest.fixture
def station_param_parser() -> ConfigBasedParser:
    return ConfigBasedParser("SCHEDULE-STATION-PARAM")


def test_config_parser_filters_extra_fields(station_param_parser):
    payload = {
        "stationId": "station001",
        "stationLng": 120.1,
        "stationLat": 30.2,
        "gunNum": 4,
        "gridCapacity": 160,
        "meterId": "M001",
        "powerNum": 2,
        "normalClap": 10,
        "hostCode": "H001",
        "unexpected": "ignored",
    }

    parsed = station_param_parser.parse(payload)

    assert parsed["stationId"] == payload["stationId"]
    assert "unexpected" not in parsed
    assert set(parsed.keys()) == set(config.TOPIC_DETAIL["SCHEDULE-STATION-PARAM"]["fields"])


def test_config_parser_sets_missing_fields_to_none(station_param_parser):
    payload = {"stationId": "station002"}

    parsed = station_param_parser.parse(payload)

    assert parsed["stationId"] == "station002"
    assert parsed["gunNum"] is None


def test_config_parser_window_merges_series():
    parser = ConfigBasedParser("SCHEDULE-CAR-PRICE")
    window = [
        {
            "stationId": "station001",
            "FeeNo": "fee-1",
            "gridPrice": 0.5,
            "serviceFee": 0.2,
            "startTime": "00:00",
            "endTime": "06:00",
            "periodType": "valley",
        },
        {
            "stationId": "station001",
            "FeeNo": "fee-2",
            "gridPrice": 0.8,
            "serviceFee": 0.3,
            "startTime": "06:00",
            "endTime": "12:00",
            "periodType": "flat",
        },
    ]

    parsed = parser.parse_window(window)

    assert parsed["gridPrice"] == [0.5, 0.8]
    assert parsed["FeeNo"] == ["fee-1", "fee-2"]


def test_module_topics_are_subset_of_all_topics():
    all_topics = set(config.TOPIC_DETAIL.keys())
    for module, topics in config.MODULE_TO_TOPICS.items():
        assert set(topics).issubset(all_topics), f"{module} references unknown topics"


def test_topic_module_mapping_is_bidirectional():
    for topic, meta in config.TOPIC_DETAIL.items():
        modules = meta.get("modules", [])
        for module in modules:
            assert topic in config.MODULE_TO_TOPICS.get(module, []), (
                f"Topic {topic} should appear in MODULE_TO_TOPICS[{module}]"
            )


def test_topic_metadata_contains_window_size():
    for topic, meta in config.TOPIC_DETAIL.items():
        assert "window_size" in meta
        assert isinstance(meta["window_size"], int)
        assert meta["window_size"] >= 1


def test_module_output_topics_registered():
    for module, output_topic in config.MODULE_OUTPUT_TOPICS.items():
        assert output_topic in config.TOPIC_DETAIL
        consumers = set(config.TOPIC_DETAIL[output_topic].get("modules", []))
        dependents = {
            consumer
            for consumer, deps in config.MODULE_DEPENDENCIES.items()
            if module in deps
        }
        assert dependents == consumers, "Model output topic consumers must match dependency graph"
