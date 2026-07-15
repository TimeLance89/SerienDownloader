import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import server
from filmpalast_scraper import FilmpalastSeriesResult
from serienstream_scraper import SerienstreamScraper


def result(title, slug, cover="https://images.example/cover.jpg"):
    return FilmpalastSeriesResult(
        title=f"{title} [S.to]",
        base_slug=f"serienstream:{slug}",
        sample_slug=f"serienstream:{slug}",
        sample_url=f"https://serienstream.to/serie/{slug}",
        cover_url=cover,
    )


class FakeBot:
    def __init__(self):
        self.sent = []
        self.photos = []
        self.answers = []
        self.cleared = []
        self.next_message_id = 100

    def send(self, chat_id, text):
        self.sent.append((chat_id, text))
        self.next_message_id += 1
        return True

    def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append((chat_id, text, reply_markup))
        self.next_message_id += 1
        return self.next_message_id

    def send_photo(
        self, chat_id, photo, caption, reply_markup=None, content_type="image/jpeg",
    ):
        self.photos.append((chat_id, photo, caption, reply_markup, content_type))
        self.next_message_id += 1
        return self.next_message_id

    def answer_callback(self, callback_query_id, text=""):
        self.answers.append((callback_query_id, text))
        return True

    def clear_inline_keyboard(self, chat_id, message_id):
        self.cleared.append((chat_id, message_id))
        return True


class TelegramSeriesSelectionTests(unittest.TestCase):
    def setUp(self):
        self.old_bot = server._telegram_bot
        self.old_cfg = server.state.telegram_cfg
        self.old_choices = server.state.telegram_series_choices
        server._telegram_bot = FakeBot()
        server.state.telegram_cfg = {
            "enabled": True, "bot_token": "token", "chat_id": "123",
        }
        server.state.telegram_series_choices = {}

    def tearDown(self):
        server._telegram_bot = self.old_bot
        server.state.telegram_cfg = self.old_cfg
        server.state.telegram_series_choices = self.old_choices

    def test_search_card_exposes_absolute_cover_url(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("""
            <a class="card cover-card" href="/serie/dark">
              <img alt="Dark | Stream" data-src="/media/images/channel/dark.jpg">
            </a>
        """, "lxml")
        scraper = SerienstreamScraper.__new__(SerienstreamScraper)

        matches = scraper._parse_cards(soup)

        self.assertEqual(len(matches), 1)
        self.assertEqual(
            matches[0].cover_url,
            "https://serienstream.to/media/images/channel/dark.jpg",
        )

    def test_multiple_matches_require_selection_before_series_pipeline(self):
        request = {"title": "Office", "mode": "all", "season": None, "episode": None}
        matches = [result("The Office", "the-office-us"), result("The Office UK", "the-office-uk")]

        with (
            patch("server.get_jellyfin_client", return_value=SimpleNamespace(configured=True)),
            patch("server.search_series_candidates", return_value=matches),
            patch("server._publish_telegram_series_choices") as publish,
            patch("server._run_telegram_series_request") as run,
        ):
            server._handle_telegram_series_request("123", request)

        publish.assert_called_once_with("123", request, matches)
        run.assert_not_called()

    def test_single_match_continues_without_choice(self):
        request = {"title": "Dark", "mode": "season", "season": 2, "episode": None}
        match = result("Dark", "dark")

        with (
            patch("server.get_jellyfin_client", return_value=SimpleNamespace(configured=True)),
            patch("server.search_series_candidates", return_value=[match]),
            patch("server._run_telegram_series_request") as run,
        ):
            server._handle_telegram_series_request("123", request)

        run.assert_called_once_with("123", request, "serienstream:dark")

    def test_choice_cards_upload_posters_and_store_short_callbacks(self):
        request = {"title": "Office", "mode": "all", "season": None, "episode": None}
        matches = [result("The Office", "office-us"), result("The Office UK", "office-uk")]

        with patch("server._fetch_cover_data", return_value=(b"poster", "image/jpeg")):
            server._publish_telegram_series_choices("123", request, matches)

        self.assertEqual(len(server._telegram_bot.photos), 2)
        self.assertEqual(len(server.state.telegram_series_choices), 1)
        entry = next(iter(server.state.telegram_series_choices.values()))
        self.assertTrue(entry["ready"])
        self.assertEqual(len(entry["message_ids"]), 2)
        callbacks = [
            photo[3]["inline_keyboard"][0][0]["callback_data"]
            for photo in server._telegram_bot.photos
        ]
        self.assertTrue(all(len(value.encode("utf-8")) <= 64 for value in callbacks))
        self.assertNotEqual(callbacks[0], callbacks[1])

    def test_more_results_are_available_on_followup_page(self):
        request = {"title": "Office", "mode": "all", "season": None, "episode": None}
        matches = [result(f"Office {index}", f"office-{index}") for index in range(8)]

        with patch("server._fetch_cover_data", return_value=(b"poster", "image/jpeg")):
            server._publish_telegram_series_choices("123", request, matches)
            token, entry = next(iter(server.state.telegram_series_choices.items()))
            navigation = next(
                sent[2]["inline_keyboard"][0][0]["callback_data"]
                for sent in server._telegram_bot.sent
                if len(sent) == 3 and sent[2]
            )
            server.handle_telegram_callback("123", "cb-next", navigation)

        self.assertEqual(navigation, f"srn:{token}:6")
        self.assertEqual(len(server._telegram_bot.photos), 8)
        self.assertEqual(entry["next_index"], 8)
        self.assertTrue(entry["ready"])
        self.assertIn(token, server.state.telegram_series_choices)

    def test_title_ranking_prefers_closest_match(self):
        matches = [
            result("10-8: Officers on Duty", "officers"),
            result("The Office", "the-office"),
            result("Office Girls", "office-girls"),
        ]

        ranked = server._rank_telegram_series_results("Office", matches)

        self.assertEqual(ranked[0].sample_slug, "serienstream:the-office")

    def test_callback_consumes_choice_once_and_preserves_scope(self):
        request = {"title": "Office", "mode": "episode", "season": 3, "episode": 4}
        candidate = result("The Office UK", "office-uk")
        server.state.telegram_series_choices["abcdefgh"] = {
            "chat_id": "123",
            "request": request,
            "candidates": [candidate],
            "created_at": time.monotonic(),
            "expires_at": time.monotonic() + 600,
            "message_ids": [51, 52],
            "ready": True,
        }

        with patch("server._run_telegram_series_request") as run:
            server.handle_telegram_callback("123", "cb-1", "sr:abcdefgh:0")
            server.handle_telegram_callback("123", "cb-2", "sr:abcdefgh:0")

        run.assert_called_once_with(
            "123", request, "serienstream:office-uk", wait_for_lock=True,
        )
        self.assertEqual(server.state.telegram_series_choices, {})
        self.assertEqual(server._telegram_bot.cleared, [("123", 51), ("123", 52)])
        self.assertIn("bereits verwendet", server._telegram_bot.answers[-1][1])

    def test_foreign_chat_cannot_consume_choice(self):
        candidate = result("Dark", "dark")
        server.state.telegram_series_choices["abcdefgh"] = {
            "chat_id": "123",
            "request": {"title": "Dark", "mode": "all"},
            "candidates": [candidate],
            "created_at": time.monotonic(),
            "expires_at": time.monotonic() + 600,
            "message_ids": [],
            "ready": True,
        }

        with patch("server._run_telegram_series_request") as run:
            server.handle_telegram_callback("999", "cb", "sr:abcdefgh:0")

        run.assert_not_called()
        self.assertIn("abcdefgh", server.state.telegram_series_choices)

    def test_loading_choice_is_not_consumed(self):
        candidate = result("Dark", "dark")
        server.state.telegram_series_choices["abcdefgh"] = {
            "chat_id": "123",
            "request": {"title": "Dark", "mode": "all"},
            "candidates": [candidate],
            "created_at": time.monotonic(),
            "expires_at": time.monotonic() + 600,
            "message_ids": [1],
            "ready": False,
        }

        with patch("server._run_telegram_series_request") as run:
            server.handle_telegram_callback("123", "cb", "sr:abcdefgh:0")

        run.assert_not_called()
        self.assertIn("abcdefgh", server.state.telegram_series_choices)
        self.assertIn("noch geladen", server._telegram_bot.answers[-1][1])

    def test_full_pending_store_does_not_drop_choice_during_consume(self):
        now = time.monotonic()
        candidate = result("Dark", "dark")
        for index in range(server.TELEGRAM_SERIES_MAX_PENDING):
            token = f"token{index:03d}"
            server.state.telegram_series_choices[token] = {
                "chat_id": "123",
                "request": {"title": "Dark", "mode": "all"},
                "candidates": [candidate],
                "created_at": now + index,
                "expires_at": now + 600,
                "message_ids": [],
                "ready": True,
            }

        status, _entry, selected = server._consume_telegram_series_choice(
            "123", "token000", 0,
        )

        self.assertEqual(status, "ok")
        self.assertEqual(selected.sample_slug, "serienstream:dark")


if __name__ == "__main__":
    unittest.main()
