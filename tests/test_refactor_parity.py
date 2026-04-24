import os

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services import text_processing as svc


def test_extract_keywords_parity() -> None:
    text = "код код код деплой деплой сервер любовь"
    assert runtime.extract_keywords(text, limit=8) == svc.extract_keywords(
        text,
        limit=8,
        rus_stopwords=runtime.RUS_STOPWORDS,
        en_stopwords=runtime.EN_STOPWORDS,
    )


def test_detect_archetype_scores_parity() -> None:
    text = "код код код деплой деплой сервер любовь"
    keywords = ["код", "деплой", "сервер", "любовь"]
    assert runtime.detect_archetype_scores(text, keywords) == svc.detect_archetype_scores(
        text,
        keywords,
        archetype_lexicon=runtime.ARCHETYPE_LEXICON,
        rus_stopwords=runtime.RUS_STOPWORDS,
        en_stopwords=runtime.EN_STOPWORDS,
    )


def test_sanitize_parity() -> None:
    raw = " Тимур: ПрИвЕт 😄\nкак дела?? "
    assert runtime.sanitize_reply_text(raw) == svc.sanitize_reply_text(raw)


def test_split_chain_parity() -> None:
    text = "one.two\nthree....four"
    assert runtime.split_into_chain(text) == svc.split_into_chain(text)


def test_top_items_parity() -> None:
    counter = {"b": 2, "a": 2, "bad": "x", "c": 1}
    assert runtime._top_items(counter, n=3) == svc.top_items(counter, n=3)
