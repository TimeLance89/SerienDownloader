import unittest
from types import SimpleNamespace
from unittest.mock import patch

import server


class _MovieProvider:
    def __init__(self, name):
        self.name = name

    def search(self, _query):
        return [self.name]


class ProviderPriorityTests(unittest.TestCase):
    def setUp(self):
        self.priority = server.state.provider_priorities
        self.fallback_override = server.SERIES_FALLBACK_PROVIDERS
        server.SERIES_FALLBACK_PROVIDERS = None

    def tearDown(self):
        server.state.provider_priorities = self.priority
        server.SERIES_FALLBACK_PROVIDERS = self.fallback_override

    def test_movie_search_uses_configured_provider_order(self):
        server.state.provider_priorities = {
            "movies": ["kinox", "moflix", "einschalten", "filmpalast"],
            "series": list(server.appconfig.SERIES_PROVIDER_DEFAULTS),
        }
        fp = _MovieProvider("filmpalast")
        with (
            patch("server.get_fp_scraper", return_value=fp),
            patch("server.MoflixScraper", return_value=_MovieProvider("moflix")),
            patch("server.EinschaltenScraper", return_value=_MovieProvider("einschalten")),
            patch("server.KinoxScraper", return_value=_MovieProvider("kinox")),
        ):
            results = server.search_movie_candidates("Dune")

        self.assertEqual(results, ["kinox", "moflix", "einschalten", "filmpalast"])

    def test_series_search_uses_configured_provider_order(self):
        server.state.provider_priorities = {
            "movies": list(server.appconfig.MOVIE_PROVIDER_DEFAULTS),
            "series": ["moflix", "filmpalast", "serienstream"],
        }

        def search(provider, _query):
            return [provider]

        with patch("server._search_series_for_provider", side_effect=search):
            results = server.search_series_candidates("Dark")

        self.assertEqual(results, ["moflix", "filmpalast", "serienstream"])

    def test_episode_fallback_skips_primary_source_and_keeps_priority(self):
        server.state.provider_priorities = {
            "movies": list(server.appconfig.MOVIE_PROVIDER_DEFAULTS),
            "series": ["moflix", "serienstream", "filmpalast"],
        }
        calls = []

        def lookup(provider, title):
            calls.append((provider, title))
            return None

        with patch("server._fallback_get_series", side_effect=lookup):
            server.find_episode_fallbacks(
                "Dark", 1, 1, source_slug="moflix:42:dark-s01e01",
            )

        self.assertEqual(calls, [
            ("serienstream", "Dark"),
            ("filmpalast", "Dark"),
        ])

    def test_episode_sources_are_reordered_for_existing_watchlist_slugs(self):
        server.state.provider_priorities = {
            "movies": list(server.appconfig.MOVIE_PROVIDER_DEFAULTS),
            "series": ["filmpalast", "moflix", "serienstream"],
        }
        sto = SimpleNamespace(url="https://serienstream.to/serie/dark/staffel-1/episode-1")
        fp = SimpleNamespace(url="https://filmpalast.to/stream/dark-s01e01")
        moflix = SimpleNamespace(url="https://moflix-stream.xyz/titles/42/dark/season/1/episode/1")

        ordered = server._ordered_episode_sources([sto, moflix, fp])

        self.assertEqual(ordered, [fp, moflix, sto])

    def test_telegram_movie_ties_keep_provider_search_order(self):
        first = SimpleNamespace(title="Titanic [Moflix]")
        second = SimpleNamespace(title="Titanic")
        ranked = server._telegram_best_result("Titanic", [first, second])
        self.assertEqual(ranked, [first, second])

    def test_telegram_series_uses_first_provider_for_same_title(self):
        first = SimpleNamespace(
            title="Dark [Moflix]", base_slug="moflix:42:dark", sample_slug="moflix:42:dark",
        )
        second = SimpleNamespace(
            title="Dark [S.to]", base_slug="serienstream:dark", sample_slug="serienstream:dark",
        )
        ranked = server._rank_telegram_series_results("Dark", [first, second])
        self.assertEqual(ranked, [first])


if __name__ == "__main__":
    unittest.main()
