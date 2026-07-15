import json
import unittest
from unittest.mock import patch

from jellyfin_client import JellyfinClient


class FakeResponse:
    def __init__(self, payload=None):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class JellyfinClientTests(unittest.TestCase):
    def setUp(self):
        self.client = JellyfinClient("http://jellyfin", "key")

    @patch("urllib.request.urlopen")
    def test_refresh_library_posts_to_jellyfin_endpoint(self, urlopen):
        urlopen.return_value = FakeResponse()

        self.assertTrue(self.client.refresh_library())

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "http://jellyfin/Library/Refresh")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("X-emby-token"), "key")
        self.assertEqual(urlopen.call_args.kwargs["timeout"], self.client.timeout)

    @patch("urllib.request.urlopen", side_effect=OSError("offline"))
    def test_refresh_library_reports_failure(self, _urlopen):
        self.assertFalse(self.client.refresh_library())

    @patch("urllib.request.urlopen")
    def test_empty_movie_library_is_an_empty_list(self, urlopen):
        urlopen.return_value = FakeResponse({"Items": []})
        self.assertEqual(self.client.list_movies(), [])

    @patch("urllib.request.urlopen")
    def test_movie_library_is_paginated(self, urlopen):
        urlopen.side_effect = [
            FakeResponse({"Items": [{"Name": "One", "ProductionYear": 2020}], "TotalRecordCount": 2}),
            FakeResponse({"Items": [{"Name": "Two", "ProductionYear": 2021}], "TotalRecordCount": 2}),
        ]

        movies = self.client.list_movies(limit=1)

        self.assertEqual([movie["name"] for movie in movies], ["One", "Two"])
        self.assertIn("StartIndex=0", urlopen.call_args_list[0].args[0].full_url)
        self.assertIn("StartIndex=1", urlopen.call_args_list[1].args[0].full_url)

    @patch("urllib.request.urlopen")
    def test_server_capped_pages_follow_total_record_count(self, urlopen):
        urlopen.side_effect = [
            FakeResponse({"Items": [{"Name": "One"}], "TotalRecordCount": 2}),
            FakeResponse({"Items": [{"Name": "Two"}], "TotalRecordCount": 2}),
        ]

        movies = self.client.list_movies(limit=1000)

        self.assertEqual([movie["name"] for movie in movies], ["One", "Two"])
        self.assertIn("StartIndex=1", urlopen.call_args_list[1].args[0].full_url)

    @patch("urllib.request.urlopen", side_effect=OSError("offline"))
    def test_movie_fetch_failure_is_none(self, _urlopen):
        self.assertIsNone(self.client.list_movies())

    @patch("urllib.request.urlopen")
    def test_empty_episode_library_is_an_empty_list(self, urlopen):
        urlopen.return_value = FakeResponse({"Items": []})
        self.assertEqual(self.client.list_episodes(), [])

    @patch("urllib.request.urlopen")
    def test_series_provider_ids_are_exposed(self, urlopen):
        urlopen.return_value = FakeResponse({"Items": [{
            "Id": "series-1", "Name": "Dark", "ProviderIds": {"Tmdb": "70523"},
        }]})

        self.assertEqual(self.client.list_series(), [{
            "id": "series-1", "name": "Dark", "original_title": "",
            "sort_name": "", "tmdb_id": "70523",
        }])

    @patch("urllib.request.urlopen", side_effect=OSError("offline"))
    def test_episode_fetch_failure_is_none(self, _urlopen):
        self.assertIsNone(self.client.list_episodes())

    @patch("urllib.request.urlopen", side_effect=OSError("offline"))
    def test_convenience_checks_handle_fetch_failure(self, _urlopen):
        self.assertFalse(self.client.match("Dark"))
        self.assertFalse(self.client.has_episode("Dark", 1, 1))
        self.assertEqual(self.client.episodes_for_series("Dark"), set())

    def test_same_title_with_different_year_is_not_a_match(self):
        items = [{"name": "The Thing", "original_title": "", "sort_name": "", "year": 1982}]

        self.assertFalse(self.client.match("The Thing", "2011", items=items))
        self.assertTrue(self.client.match("The Thing", "1982", items=items))

    def test_requested_year_requires_a_jellyfin_year(self):
        items = [{"name": "The Thing", "original_title": "", "sort_name": "", "year": None}]

        self.assertFalse(self.client.match("The Thing", "2011", items=items))

    def test_short_installment_title_matches_long_jellyfin_title(self):
        items = [{
            "name": "Breaking Dawn - Bis(s) zum Ende der Nacht - Teil 2",
            "original_title": "", "sort_name": "", "year": 2012,
        }]

        self.assertTrue(self.client.match("Breaking Dawn Teil 2", items=items))
        self.assertFalse(self.client.match("Breaking Dawn Teil 1", items=items))

    def test_base_movie_does_not_match_a_numbered_installment(self):
        items = [{
            "name": "Dune: Part Two", "original_title": "", "sort_name": "", "year": 2024,
        }]

        self.assertFalse(self.client.match("Dune", items=items))

    def test_tmdb_movie_id_has_priority_over_localized_title(self):
        items = [{
            "name": "Drachenzähmen leicht gemacht", "original_title": "", "sort_name": "",
            "year": 2010, "tmdb_id": "10191",
        }]

        self.assertTrue(self.client.match("How to Train Your Dragon", "2010", items, tmdb_id=10191))
        self.assertFalse(self.client.match(
            "Drachenzähmen leicht gemacht", "2010", items, tmdb_id=99999,
        ))

    def test_series_alias_can_match_localized_jellyfin_title(self):
        items = [{"series": "House of the Dragon", "season": 1, "episode": 2, "played": True}]

        self.assertEqual(
            self.client.episodes_for_series("Das Haus des Drachen", items, aliases=("House of the Dragon",)),
            {(1, 2)},
        )
        self.assertEqual(
            self.client.watched_episodes_for_series(
                "Das Haus des Drachen", items, aliases=("House of the Dragon",),
            ),
            {(1, 2)},
        )

    def test_tmdb_series_id_matches_even_when_titles_differ(self):
        series = [{"id": "jf-series", "name": "Completely Different", "tmdb_id": "94997"}]
        episodes = [{
            "series": "Completely Different", "series_id": "jf-series",
            "season": 2, "episode": 3,
        }]

        ids = self.client.series_ids_for("House of the Dragon", 94997, items=series)

        self.assertEqual(ids, {"jf-series"})
        self.assertEqual(
            self.client.episodes_for_series(
                "House of the Dragon", episodes, series_ids=ids,
            ),
            {(2, 3)},
        )

    def test_same_series_title_without_provider_id_is_ambiguous(self):
        series = [
            {"id": "one", "name": "The Office", "tmdb_id": ""},
            {"id": "two", "name": "The Office", "tmdb_id": ""},
        ]
        self.assertIsNone(self.client.series_ids_for("The Office", items=series))

    def test_conflicting_tmdb_series_id_does_not_fall_back_to_title(self):
        series = [{"id": "wrong", "name": "The Office", "tmdb_id": "999"}]
        self.assertEqual(
            self.client.series_ids_for("The Office", tmdb_id="2316", items=series),
            set(),
        )

    def test_tmdb_series_id_does_not_fall_back_to_title_without_provider_id(self):
        series = [{"id": "unknown", "name": "The Office", "tmdb_id": ""}]
        self.assertIsNone(
            self.client.series_ids_for("The Office", tmdb_id="2316", items=series),
        )

    def test_explicit_empty_series_ids_do_not_fall_back_to_title(self):
        episodes = [{
            "series": "Dark", "series_id": "jf-dark", "season": 1, "episode": 1,
        }]
        self.assertEqual(
            self.client.episodes_for_series("Dark", episodes, series_ids=set()),
            set(),
        )
        self.assertFalse(
            self.client.has_episode("Dark", 1, 1, episodes, series_ids=set()),
        )


if __name__ == "__main__":
    unittest.main()
