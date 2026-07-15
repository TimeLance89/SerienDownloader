import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import server
from seerr_client import SeerrRequest


class _FakeSeerrClient:
    configured = True

    def __init__(self, requests):
        self.requests = requests
        self.last_error = ""

    def test_connection(self):
        return True

    def approved_requests(self):
        return list(self.requests)


class _Response:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise server.requests.HTTPError(str(self.status_code))

    def json(self):
        return self.payload


class _MoonfinSession:
    def __init__(self):
        self.posts = []

    def get(self, url, **_kwargs):
        if url.endswith("/Plugins"):
            return _Response([{"Name": "Moonfin", "Id": "moonfin-id"}])
        if url.endswith("/Plugins/moonfin-id/Configuration"):
            return _Response({"EnableSettingsSync": True, "JellyseerrEnabled": False})
        if "/Moonfin/Settings/" in url:
            return _Response({"global": {"homeRowsStyle": "v2"}, "tv": {}})
        return _Response(status_code=404)

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return _Response({}, 204)


class ServerSeerrTests(unittest.TestCase):
    def setUp(self):
        self.old_cfg = dict(server.state.seerr_cfg)
        self.old_jellyfin_cfg = dict(server.state.jellyfin_cfg)
        self.old_scan_retry = server.state.seerr_last_scan_retry
        server.state.seerr_last_scan_retry = 0
        with server.state.seerr_requests_lock:
            self.old_requests = server.state.seerr_requests
            server.state.seerr_requests = {}
        with server.state.seerr_jobs_lock:
            self.old_jobs = server.state.seerr_jobs
            server.state.seerr_jobs = {}

    def tearDown(self):
        server.state.seerr_cfg = self.old_cfg
        server.state.jellyfin_cfg = self.old_jellyfin_cfg
        server.state.seerr_last_scan_retry = self.old_scan_retry
        with server.state.seerr_requests_lock:
            server.state.seerr_requests = self.old_requests
        with server.state.seerr_jobs_lock:
            server.state.seerr_jobs = self.old_jobs

    def test_available_request_is_not_downloaded_again(self):
        request = SeerrRequest(5, "movie", 550, media_status=5)

        with patch.object(server, "_seerr_update_record") as update, patch.object(
            server, "_seerr_process_movie",
        ) as process_movie:
            server._seerr_process_request(request)

        process_movie.assert_not_called()
        self.assertEqual(update.call_args.args[0], 5)
        self.assertEqual(update.call_args.kwargs["status"], "available")

    def test_4k_request_is_not_downloaded_as_non_4k(self):
        request = SeerrRequest(6, "movie", 551, is_4k=True)
        decline_request = Mock(return_value=True)
        client = SimpleNamespace(decline_request=decline_request, last_error="")

        with patch.object(server, "_seerr_client", return_value=client), patch.object(
            server, "_seerr_update_record",
        ) as update, patch.object(
            server, "_seerr_process_movie",
        ) as process_movie:
            server._seerr_process_request(request)

        process_movie.assert_not_called()
        decline_request.assert_called_once_with(6)
        self.assertEqual(update.call_args.kwargs["status"], "unsupported")
        self.assertTrue(update.call_args.kwargs["seerr_declined"])

    def test_completed_request_retries_jellyfin_scan_with_rate_limit(self):
        server.state.seerr_requests = {
            "7": {
                "request_id": 7,
                "status": "completed",
                "last_scan_retry": 900,
                "media_type": "movie",
                "tmdb_id": 552,
                "seasons": [],
                "is_4k": False,
            },
        }
        refresh_library = Mock(return_value=True)
        jellyfin = SimpleNamespace(configured=True, refresh_library=refresh_library)
        request = SeerrRequest(7, "movie", 552)

        with patch.object(server, "get_jellyfin_client", return_value=jellyfin), patch.object(
            server, "_seerr_update_record",
        ) as update, patch.object(server.time, "time", return_value=1000):
            server._seerr_process_request(request)
        update.assert_not_called()
        refresh_library.assert_not_called()

        with patch.object(server, "get_jellyfin_client", return_value=jellyfin), patch.object(
            server, "_seerr_update_record",
        ) as update, patch.object(server.time, "time", return_value=1201):
            server._seerr_process_request(request)
        self.assertEqual(update.call_args.kwargs["status"], "completed")
        self.assertEqual(update.call_args.kwargs["last_scan_retry"], 1201)
        refresh_library.assert_called_once_with()

    def test_completed_requests_share_global_scan_throttle(self):
        refresh_library = Mock(return_value=True)
        jellyfin = SimpleNamespace(configured=True, refresh_library=refresh_library)
        base = {
            "status": "completed",
            "last_scan_retry": 0,
            "media_type": "movie",
            "seasons": [],
            "is_4k": False,
        }
        server.state.seerr_requests = {
            "31": {**base, "request_id": 31, "tmdb_id": 301},
            "32": {**base, "request_id": 32, "tmdb_id": 302},
        }

        with patch.object(server, "get_jellyfin_client", return_value=jellyfin), patch.object(
            server.appconfig, "save_seerr_requests", return_value=True,
        ), patch.object(server.time, "time", return_value=1000):
            server._seerr_process_request(SeerrRequest(31, "movie", 301))
            server._seerr_process_request(SeerrRequest(32, "movie", 302))

        refresh_library.assert_called_once_with()

    def test_job_result_is_persisted_as_completed(self):
        server.state.seerr_requests = {
            "9": {
                "request_id": 9,
                "status": "queued",
                "pending_slugs": ["movie-9"],
                "completed_slugs": [],
                "failures": [],
            },
        }
        job = {"request_id": "9", "kind": "movie"}

        with patch.object(server.appconfig, "save_seerr_requests", return_value=True):
            server._seerr_job_result(job, "movie-9", True, "ok", Path("movie.mkv"))

        record = server.state.seerr_requests["9"]
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["pending_slugs"], [])
        self.assertEqual(record["completed_slugs"], ["movie-9"])

    def test_same_slug_updates_all_seerr_requests(self):
        item = {"shared-movie": {"kind": "movie"}}
        with patch.object(server.appconfig, "save_seerr_requests", return_value=True), patch.object(
            server, "_persist_queue_state",
        ):
            server._seerr_register_request_jobs(21, item, "Film")
            server._seerr_register_request_jobs(22, item, "Film")
            self.assertEqual(len(server.state.seerr_jobs["shared-movie"]), 2)
            server._seerr_terminal_without_job(
                "shared-movie", True, "ok", Path("movie.mkv"),
            )

        self.assertEqual(server.state.seerr_requests["21"]["status"], "completed")
        self.assertEqual(server.state.seerr_requests["22"]["status"], "completed")

    def test_series_with_wrong_known_year_is_rejected(self):
        candidate = SimpleNamespace(
            title="Die Serie", year="1999", sample_slug="die-serie/staffel-1/episode-1",
        )
        with patch.object(server, "search_series_candidates", return_value=[candidate]), patch.object(
            server, "get_series_for_value",
        ) as load_series:
            with self.assertRaisesRegex(RuntimeError, "abweichendes Erscheinungsjahr"):
                server._seerr_find_series({"title": "Die Serie", "year": "2024"})
        load_series.assert_not_called()

    def test_series_without_year_requires_tmdb_id_confirmation(self):
        candidate = SimpleNamespace(
            title="Die Serie  [S.to]", year="", sample_slug="die-serie/staffel-1/episode-1",
        )
        tmdb = SimpleNamespace(series_matches_id=Mock(return_value=False))
        with patch.object(server, "search_series_candidates", return_value=[candidate]), patch.object(
            server, "get_tmdb_client", return_value=tmdb,
        ), patch.object(server, "get_series_for_value") as load_series:
            with self.assertRaisesRegex(RuntimeError, "TMDB-ID"):
                server._seerr_find_series({
                    "title": "Die Serie", "year": "2024", "tmdb_id": 123,
                })
        tmdb.series_matches_id.assert_called_once_with("Die Serie", "123", "2024")
        load_series.assert_not_called()

    def test_movie_search_loads_only_exact_title_and_skips_cjk_alias(self):
        exact = SimpleNamespace(
            title="The Furious [Moflix]", year="2026", slug="exact",
            is_movie=True,
        )
        unrelated = SimpleNamespace(
            title="The Fast and the Furious", year="2001", slug="wrong",
            is_movie=True,
        )
        movie = SimpleNamespace(
            title="The Furious [Moflix]", year="2026", url="https://source/exact",
            hosters=[SimpleNamespace(language="", is_de=False)],
        )
        tmdb = SimpleNamespace(movie_summary=Mock(return_value={"tmdb_id": 1280738}))

        with patch.object(
            server, "search_movie_candidates", return_value=[unrelated, exact],
        ) as search, patch.object(
            server, "load_movie_for_slug", return_value=movie,
        ) as load, patch.object(server, "get_tmdb_client", return_value=tmdb):
            options = server._seerr_find_movie_sources({
                "title": "The Furious", "original_title": "火遮眼", "year": "2026",
            }, 1280738)

        self.assertEqual(options[0][0].slug, "exact")
        load.assert_called_once_with("exact")
        search.assert_called_once_with("The Furious")

    def test_movie_search_continues_after_rate_limited_exact_source(self):
        first = SimpleNamespace(
            title="The Furious", year="2026", slug="limited", is_movie=True,
        )
        second = SimpleNamespace(
            title="The Furious", year="2026", slug="working", is_movie=True,
        )
        movie = SimpleNamespace(
            title="The Furious", year="2026", url="https://source/working",
            hosters=[SimpleNamespace(language="", is_de=False)],
        )
        error = server.requests.HTTPError("429 Too Many Requests")
        error.response = SimpleNamespace(status_code=429)
        tmdb = SimpleNamespace(movie_summary=Mock(return_value={"tmdb_id": 1280738}))

        with patch.object(
            server, "search_movie_candidates", return_value=[first, second],
        ), patch.object(
            server, "load_movie_for_slug", side_effect=[error, movie],
        ) as load, patch.object(server, "get_tmdb_client", return_value=tmdb):
            options = server._seerr_find_movie_sources({
                "title": "The Furious", "original_title": "", "year": "2026",
            }, 1280738)

        self.assertEqual(options[0][0].slug, "working")
        self.assertEqual(load.call_count, 2)

    def test_movie_search_rejects_explicit_non_german_audio(self):
        candidate = SimpleNamespace(
            title="The Furious", year="2026", slug="english", is_movie=True,
        )
        movie = SimpleNamespace(
            title="The Furious", year="2026", url="https://source/english",
            hosters=[SimpleNamespace(language="Englisch", is_de=False)],
        )
        tmdb = SimpleNamespace(movie_summary=Mock(return_value={"tmdb_id": 1280738}))

        with patch.object(
            server, "search_movie_candidates", return_value=[candidate],
        ), patch.object(
            server, "load_movie_for_slug", return_value=movie,
        ), patch.object(server, "get_tmdb_client", return_value=tmdb):
            with self.assertRaisesRegex(RuntimeError, "ohne deutsche Tonspur"):
                server._seerr_find_movie_sources({
                    "title": "The Furious", "original_title": "", "year": "2026",
                }, 1280738)

    def test_reused_request_id_discards_old_identity_and_jobs(self):
        server.state.seerr_requests = {
            "41": {
                "request_id": 41,
                "status": "completed",
                "media_type": "movie",
                "tmdb_id": 111,
                "seasons": [],
                "is_4k": False,
            },
        }
        server.state.seerr_jobs = {
            "old-film": [{"request_id": "41", "kind": "movie"}],
        }
        tmdb = SimpleNamespace(
            configured=True,
            movie_by_id=Mock(return_value={"tmdb_id": 222, "title": "Neu"}),
        )

        with patch.object(server, "get_tmdb_client", return_value=tmdb), patch.object(
            server, "_seerr_process_movie",
        ) as process_movie, patch.object(
            server.appconfig, "save_seerr_requests", return_value=True,
        ):
            server._seerr_process_request(SeerrRequest(41, "movie", 222))

        process_movie.assert_called_once()
        self.assertNotIn("old-film", server.state.seerr_jobs)
        self.assertEqual(server.state.seerr_requests["41"]["tmdb_id"], 222)

    def test_poll_dispatches_all_approved_requests(self):
        requests = [
            SeerrRequest(1, "movie", 10),
            SeerrRequest(2, "tv", 20, (1, 2)),
        ]
        server.state.seerr_cfg = {
            "enabled": True,
            "url": "http://seerr:5055",
            "api_key": "secret",
            "poll_interval_seconds": 60,
        }

        with patch.object(server, "_seerr_client", return_value=_FakeSeerrClient(requests)), patch.object(
            server, "_seerr_process_request",
        ) as process:
            result = server.seerr_poll_once()

        self.assertTrue(result["ok"])
        self.assertEqual(result["requests"], 2)
        self.assertEqual([call.args[0] for call in process.call_args_list], requests)

    def test_poll_does_not_report_success_when_request_list_fails(self):
        client = _FakeSeerrClient([])

        def fail_list():
            client.last_error = "401 Unauthorized"
            return []

        client.approved_requests = fail_list
        server.state.seerr_cfg = {
            "enabled": True,
            "url": "http://seerr:5055",
            "api_key": "wrong",
            "poll_interval_seconds": 60,
        }
        server.state.seerr_last_success = 123

        with patch.object(server, "_seerr_client", return_value=client):
            result = server.seerr_poll_once()

        self.assertFalse(result["ok"])
        self.assertEqual(result["detail"], "401 Unauthorized")
        self.assertEqual(server.state.seerr_last_success, 123)

    def test_moonfin_stable_config_and_tv_profile_are_updated_without_reset(self):
        session = _MoonfinSession()
        server.state.jellyfin_cfg = {
            "url": "http://jellyfin:8096",
            "api_key": "secret",
            "user_id": "user-1",
        }

        with patch.object(server.requests, "Session", return_value=session):
            result = server.configure_moonfin_seerr("http://nas:5055", True)

        self.assertTrue(result["configured"])
        settings_payload = session.posts[0][1]["json"]["settings"]
        plugin_payload = session.posts[1][1]["json"]
        self.assertTrue(plugin_payload["EnableSettingsSync"])
        self.assertTrue(plugin_payload["JellyseerrEnabled"])
        self.assertEqual(plugin_payload["JellyseerrUrl"], "http://nas:5055")
        self.assertTrue(settings_payload["global"]["jellyseerrEnabled"])
        self.assertTrue(settings_payload["tv"]["jellyseerrEnabled"])
        self.assertEqual(settings_payload["global"]["homeRowsStyle"], "v2")


if __name__ == "__main__":
    unittest.main()
