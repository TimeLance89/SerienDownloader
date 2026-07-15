import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from update_checker import UpdateChecker, detect_local_commit


class UpdateCheckerTests(unittest.TestCase):
    def test_local_commit_is_read_from_git_ref(self):
        commit = "a" * 40
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".git" / "refs" / "heads").mkdir(parents=True)
            (root / ".git" / "HEAD").write_text(
                "ref: refs/heads/main\n", encoding="utf-8",
            )
            (root / ".git" / "refs" / "heads" / "main").write_text(
                commit, encoding="utf-8",
            )
            with patch.dict(os.environ, {
                "APP_COMMIT_SHA": "", "GIT_COMMIT": "", "SOURCE_COMMIT": "",
            }):
                self.assertEqual(detect_local_commit(root), commit)

    def test_identical_revision_is_current(self):
        commit = "b" * 40
        checker = UpdateChecker(app_dir=Path("."), cache_seconds=0)
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "sha": commit,
            "html_url": "https://github.com/example/commit/current",
            "commit": {"message": "Aktueller Stand"},
        }
        with (
            patch("update_checker.detect_local_commit", return_value=commit),
            patch("update_checker.requests.get", return_value=response) as get,
        ):
            result = checker.check(force=True)

        self.assertFalse(result["update_available"])
        self.assertEqual(result["comparison"], "identical")
        self.assertEqual(get.call_count, 1)

    def test_newer_main_revision_is_reported(self):
        current = "c" * 40
        latest = "d" * 40
        latest_response = Mock()
        latest_response.raise_for_status.return_value = None
        latest_response.json.return_value = {
            "sha": latest,
            "html_url": "https://github.com/example/commit/latest",
            "commit": {"message": "Neuer Stand\n\nDetails"},
        }
        compare_response = Mock()
        compare_response.raise_for_status.return_value = None
        compare_response.json.return_value = {
            "status": "ahead", "ahead_by": 3, "behind_by": 0,
        }
        checker = UpdateChecker(app_dir=Path("."), cache_seconds=0)
        with (
            patch("update_checker.detect_local_commit", return_value=current),
            patch(
                "update_checker.requests.get",
                side_effect=[latest_response, compare_response],
            ),
        ):
            result = checker.check(force=True)

        self.assertTrue(result["update_available"])
        self.assertEqual(result["ahead_by"], 3)
        self.assertEqual(result["latest_message"], "Neuer Stand")

    def test_missing_local_revision_still_reports_repository(self):
        latest = "e" * 40
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"sha": latest, "commit": {"message": "Main"}}
        checker = UpdateChecker(app_dir=Path("."), cache_seconds=0)
        with (
            patch("update_checker.detect_local_commit", return_value=""),
            patch("update_checker.requests.get", return_value=response),
        ):
            result = checker.check(force=True)

        self.assertIsNone(result["update_available"])
        self.assertEqual(result["latest_sha"], latest)
        self.assertEqual(result["comparison"], "unknown")

if __name__ == "__main__":
    unittest.main()
