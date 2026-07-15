import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import config


class ConfigPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.temp.name) / "FilmeDownloader"
        self.patcher = patch.object(config, "_config_dir", return_value=self.config_dir)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp.cleanup()

    def test_empty_or_corrupt_settings_do_not_skip_setup(self):
        self.config_dir.mkdir(parents=True)
        (self.config_dir / "settings.ini").write_text("broken", encoding="utf-8")
        self.assertFalse(config.is_initialized())

    def test_settings_are_written_atomically_and_mark_setup_complete(self):
        self.assertTrue(config.save("D:/Movies"))
        self.assertTrue(config.is_initialized())
        self.assertEqual(config.load(), "D:/Movies")
        self.assertFalse(any(path.suffix == ".tmp" for path in self.config_dir.iterdir()))

    def test_queue_survives_restart_roundtrip(self):
        self.assertTrue(config.save_queue({"film-a", "serie:s01e01"}))
        self.assertEqual(config.load_queue(), ["film-a", "serie:s01e01"])
        self.assertTrue(config.save_queue([]))
        self.assertEqual(config.load_queue(), [])

    def test_jellyfin_settings_survive_restart_roundtrip(self):
        self.assertTrue(config.save_jellyfin(
            "http://jellyfin:8096", "secret", "user-1", "Max",
        ))
        self.assertEqual(config.load_jellyfin(), {
            "url": "http://jellyfin:8096",
            "api_key": "secret",
            "user_id": "user-1",
            "user_name": "Max",
        })

    def test_seerr_settings_and_request_state_survive_restart_roundtrip(self):
        self.assertTrue(config.save_seerr(
            True, "http://seerr:5055/", "seerr-secret", 45,
        ))
        self.assertEqual(config.load_seerr(), {
            "enabled": True,
            "url": "http://seerr:5055",
            "api_key": "seerr-secret",
            "poll_interval_seconds": 45,
        })
        requests = {
            "17": {
                "request_id": 17,
                "status": "queued",
                "pending_slugs": ["movie-17"],
            },
        }
        self.assertTrue(config.save_seerr_requests(requests))
        self.assertEqual(config.load_seerr_requests(), requests)

    def test_provider_priorities_survive_restart_roundtrip(self):
        self.assertTrue(config.save_provider_priorities(
            ["kinox", "moflix", "filmpalast", "einschalten"],
            ["moflix", "filmpalast", "serienstream"],
        ))
        self.assertEqual(config.load_provider_priorities(), {
            "movies": ["kinox", "moflix", "filmpalast", "einschalten"],
            "series": ["moflix", "filmpalast", "serienstream"],
        })

    def test_provider_priorities_repair_unknown_duplicates_and_missing_values(self):
        self.assertTrue(config.save_provider_priorities(
            ["moflix", "unknown", "moflix"],
            ["filmpalast"],
        ))
        self.assertEqual(config.load_provider_priorities(), {
            "movies": ["moflix", "filmpalast", "einschalten", "kinox"],
            "series": ["filmpalast", "serienstream", "moflix"],
        })


if __name__ == "__main__":
    unittest.main()
