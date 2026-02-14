from __future__ import annotations

import asyncio
from bs4 import BeautifulSoup
import requests

from .config import get_settings

try:
    from playwright.async_api import async_playwright
except ImportError as exc:  # pragma: no cover - optional dependency
    async_playwright = None
    _PLAYWRIGHT_IMPORT_ERROR = exc


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

MIN_TEXT_LEN = 200


def _extract_text_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    meta_desc = None
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if meta_tag and meta_tag.get("content"):
        meta_desc = meta_tag["content"].strip()
    og_desc_tag = soup.find("meta", attrs={"property": "og:description"})
    if og_desc_tag and og_desc_tag.get("content"):
        meta_desc = og_desc_tag["content"].strip()

    text = soup.get_text(separator=" ")
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    cleaned = "\n".join(chunk for chunk in chunks if chunk)
    if meta_desc:
        cleaned = f"{meta_desc}\n{cleaned}" if cleaned else meta_desc
    return cleaned or None


async def _get_page_html(url: str, timeout_ms: int) -> str | None:
    if async_playwright is None:
        raise RuntimeError(
            "playwright is not installed. Install with `pip install playwright` "
            "and run `playwright install`."
        ) from _PLAYWRIGHT_IMPORT_ERROR

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page(extra_http_headers=DEFAULT_HEADERS)
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            return await page.content()
        finally:
            await browser.close()


async def get_clean_text(url: str) -> str | None:
    settings = get_settings()
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=settings.request_timeout_s)
        if resp.ok and resp.text:
            cleaned = _extract_text_from_html(resp.text)
            if cleaned and len(cleaned) >= MIN_TEXT_LEN:
                return cleaned

        html = await _get_page_html(url, settings.request_timeout_s * 1000)
        if not html:
            return None
        return _extract_text_from_html(html)
    except Exception as exc:
        print(f"Error scraping {url}: {exc}")
        return None
