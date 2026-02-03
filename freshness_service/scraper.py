from __future__ import annotations

import requests
from bs4 import BeautifulSoup

from .config import settings


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def get_clean_text(url: str) -> str | None:
    try:
        response = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=settings.request_timeout_s,
        )
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        text = soup.get_text(separator=" ")
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        cleaned = "\n".join(chunk for chunk in chunks if chunk)
        return cleaned or None
    except Exception as exc:
        print(f"Error scraping {url}: {exc}")
        return None
