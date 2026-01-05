import datetime
import logging
import re
import threading
import time

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

from io_utils import write_row

logger = logging.getLogger(__name__)
print_lock = threading.Lock()

# ---------- helpers ----------

def _extract(text: str, pattern: str, default="N/A"):
    if not text:
        return default
    m = re.search(pattern, text)
    return m.group(1) if m else default


def _parse_volume(text: str) -> str:
    if not text:
        return "N/A"
    m = re.search(r"(\d+\.?\d*)", text)
    if not m:
        return "N/A"

    v = float(m.group(1))
    t = text.upper()
    if "B" in t:
        v *= 1_000_000_000
    elif "M" in t:
        v *= 1_000_000
    elif "K" in t:
        v *= 1_000

    return str(int(v)) if v.is_integer() else str(v)


def _is_404(page) -> bool:
    try:
        if "404.aspx" in (page.url or ""):
            return True
        if "page requested" in page.content().lower():
            return True
    except Exception:
        pass
    return False


def _wait_for_data(page, timeout_s=20) -> bool:
    selectors = ["col_open", "col_high", "col_low", "col_ask", "col_volume"]
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        found = 0
        for cls in selectors:
            try:
                txt = page.locator(f".{cls}").first.inner_text(timeout=1000)
                if "HK$" in txt:
                    found += 1
            except Exception:
                pass
        if found >= 2:
            return True
        time.sleep(1.5)

    return False


# ---------- worker ----------

def scrape_worker(stock_codes: list[str], base_url: str) -> tuple[int, int]:
    """
    One worker thread:
    - owns its own Playwright instance
    - owns one browser
    - processes many stock codes sequentially
    """
    thread_id = threading.get_ident()
    tlog = logging.getLogger(f"Thread-{thread_id}")

    success = 0
    fail = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for stock_code in stock_codes:
            date = datetime.datetime.now().strftime("%Y%m%d")
            url = f"{base_url}?sym={stock_code}"

            data = {
                "CODE": stock_code,
                "DATE": date,
                "OPEN": "N/A",
                "INTRADAY_HIGH": "N/A",
                "INTRADAY_LOW": "N/A",
                "CLOSE": "N/A",
                "P/E": "N/A",
                "VOLUME": "N/A",
                "STATUS": "Success",
            }

            context = browser.new_context(
                user_agent=f"Mozilla/5.0 StockScraper Thread-{thread_id}",
                viewport={"width": 1280, "height": 720},
            )
            page = context.new_page()

            try:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                except PWTimeoutError:
                    raise RuntimeError("timeout")

                if _is_404(page):
                    raise RuntimeError("404")

                if not _wait_for_data(page):
                    raise RuntimeError("no data")

                data["OPEN"] = _extract(page.locator(".col_open").first.inner_text(), r"HK\$(\d+\.\d+)")
                data["INTRADAY_HIGH"] = _extract(page.locator(".col_high").first.inner_text(), r"HK\$(\d+\.\d+)")
                data["INTRADAY_LOW"] = _extract(page.locator(".col_low").first.inner_text(), r"HK\$(\d+\.\d+)")
                data["CLOSE"] = _extract(page.locator(".col_ask").first.inner_text(), r"HK\$(\d+\.\d+)")

                try:
                    pe_text = page.locator(".col_pe").first.inner_text(timeout=2000)
                except Exception:
                    pe_text = ""
                data["P/E"] = _extract(pe_text, r"(\d+\.\d+)x")

                try:
                    vol_text = page.locator(".col_volume").first.inner_text(timeout=2000)
                except Exception:
                    vol_text = ""
                data["VOLUME"] = _parse_volume(vol_text)

                with print_lock:
                    print(
                        f"{data['CODE']},{data['DATE']},{data['OPEN']},{data['INTRADAY_HIGH']},"
                        f"{data['INTRADAY_LOW']},{data['CLOSE']},{data['P/E']},{data['VOLUME']},{data['STATUS']}"
                    )

                write_row(data)
                success += 1

            except Exception as e:
                tlog.debug(f"{stock_code} failed: {e}")
                data["STATUS"] = "Error"
                write_row(data)
                fail += 1

            finally:
                page.close()
                context.close()

        browser.close()

    return success, fail