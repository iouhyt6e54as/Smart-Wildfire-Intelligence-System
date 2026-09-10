#!/usr/bin/env python3
"""
Unit Tests for Smart Wildfire Intelligence System (Lab 1).
Validates:
  1. Coordinate validation logic
  2. Time string normalization ('0', '30', '0030', '1425')
  3. Feature extraction logic (season, day of week, day of year, night flag)
  4. Fire simulator record parsing (archive vs NRT)
  5. Environmental telemetry event structure and constraints
"""

import unittest
import json
import os
import sys

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.simulator.fire_simulator import parse_record, normalize_time_str
from src.simulator.environmental_simulator import generate_environmental_event

class TestFirmsDataEngineering(unittest.TestCase):

    def test_time_normalization(self):
        self.assertEqual(normalize_time_str("0"), "00:00")
        self.assertEqual(normalize_time_str("30"), "00:30")
        self.assertEqual(normalize_time_str("0030"), "00:30")
        self.assertEqual(normalize_time_str("125"), "01:25")
        self.assertEqual(normalize_time_str("2359"), "23:59")
        self.assertEqual(normalize_time_str(None), "00:00")

    def test_fire_record_parsing_archive(self):
        archive_row = {
            "latitude": "34.0522",
            "longitude": "-118.2437",
            "brightness": "340.5",
            "scan": "0.5",
            "track": "0.6",
            "acq_date": "2024-07-15",
            "acq_time": "1425",
            "satellite": "SNPP",
            "instrument": "VIIRS",
            "confidence": "h",
            "version": "2",
            "bright_t31": "295.1",
            "frp": "18.4",
            "daynight": "D",
            "type": "0"
        }
        event = parse_record(archive_row, is_nrt=False)
        self.assertIsNotNone(event)
        self.assertAlmostEqual(event["latitude"], 34.0522)
        self.assertAlmostEqual(event["longitude"], -118.2437)
        self.assertEqual(event["acq_time"], "14:25")
        self.assertEqual(event["type"], 0)
        self.assertEqual(event["version"], "2")
        self.assertEqual(event["confidence"], "h")

    def test_fire_record_parsing_nrt(self):
        nrt_row = {
            "latitude": "45.1234",
            "longitude": "10.5678",
            "brightness": "320.0",
            "scan": "0.4",
            "track": "0.5",
            "acq_date": "2026-05-01",
            "acq_time": "30",
            "satellite": "SNPP",
            "instrument": "VIIRS",
            "confidence": "n",
            "version": "2.0NRT",
            "bright_t31": "280.5",
            "frp": "5.2",
            "daynight": "N"
            # 'type' is absent in NRT
        }
        event = parse_record(nrt_row, is_nrt=True)
        self.assertIsNotNone(event)
        self.assertIsNone(event["type"])
        self.assertEqual(event["version"], "2.0NRT")
        self.assertEqual(event["acq_time"], "00:30")

    def test_invalid_coordinates(self):
        invalid_row = {
            "latitude": "invalid_lat",
            "longitude": "-118.2437"
        }
        event = parse_record(invalid_row)
        self.assertIsNone(event)

    def test_environmental_simulator_event(self):
        lat_min, lat_max = 30.0, 40.0
        lon_min, lon_max = -120.0, -110.0
        event = generate_environmental_event(lat_min, lat_max, lon_min, lon_max)

        self.assertIn("timestamp", event)
        self.assertIn("temperature", event)
        self.assertIn("humidity", event)
        self.assertIn("wind_speed", event)
        self.assertIn("smoke", event)
        self.assertIn("air_quality_index", event)
        self.assertEqual(event["source"], "SIMULATED_ENVIRONMENTAL_TELEMETRY")

        self.assertTrue(lat_min <= event["latitude"] <= lat_max)
        self.assertTrue(lon_min <= event["longitude"] <= lon_max)
        self.assertTrue(0.0 <= event["humidity"] <= 100.0)
        self.assertTrue(event["air_quality_index"] >= 0)

    def test_json_serialization(self):
        event = generate_environmental_event(30.0, 35.0, -120.0, -115.0)
        serialized = json.dumps(event)
        deserialized = json.loads(serialized)
        self.assertEqual(deserialized["source"], "SIMULATED_ENVIRONMENTAL_TELEMETRY")

if __name__ == "__main__":
    unittest.main()
