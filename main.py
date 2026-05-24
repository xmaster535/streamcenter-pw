import asyncio
from functools import partial
from selectolax.parser import HTMLParser

# আপনার প্রজেক্টের utils মডিউল যদি লোকাল ফাইল হয়, তবে এগুলো সচল থাকবে।
# যদি utils ফাইল না থাকে, তবে এই ৩টি ইম্পোর্ট লাইনের কারণে এরর আসতে পারে।
try:
    from .utils import Cache, Time, get_logger, leagues, network
except (ImportError, ValueError):
    # যদি utils ফোল্ডার বা ফাইল না থাকে, তবে স্ক্রিপ্ট সচল রাখার জন্য ডামি ইম্পোর্ট
    import logging
    logging.basicConfig(level=logging.INFO)
    class Dummy: pass
    log = logging.getLogger("STRMCNTR")
    Cache = lambda *args, **kwargs: Dummy()
    Time = Dummy()
    leagues = Dummy()
    network = Dummy()

log = get_logger(__name__) if 'get_logger' in globals() else log

urls: dict[str, dict[str, str | float]] = {}
TAG = "STRMCNTR"
CACHE_FILE = Cache(TAG, exp=86_400)
API_URL = "https://backend.streamcenter.live/api/Parties"

CATEGORIES = {
    4: "Basketball",
    9: "Football",
    13: "Baseball",
    15: "Motor Sport",
    16: "Hockey",
    17: "Fight MMA",
    18: "Boxing",
    20: "WWE",
    21: "Tennis",
}

async def process_event(url: str, url_num: int) -> str | None:
    if not (html_data := await network.request(url, log=log)):
        log.warning(f"URL {url_num}) Failed to load url.")
        return

    soup = HTMLParser(html_data.content)
    iframe = soup.css_first("iframe")

    if not iframe or not (iframe_src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) No iframe element found.")
        return

    log.info(f"URL {url_num}) Captured M3U8")
    return f"https://mainstreams.pro/hls/{iframe_src.rsplit('=', 1)[-1]}.m3u8"

async def get_events() -> list[dict[str, str]]:
    events = []
    if not (r := await network.request(API_URL, params={"pageNumber": 1, "pageSize": 500}, log=log)):
        return events

    now = Time.clean(Time.now())
    api_data: list[dict] = r.json()

    for stream_group in api_data:
        category_id: int = stream_group.get("categoryId")
        name: str = stream_group.get("gameName")
        iframe: str = stream_group.get("videoUrl")
        event_time: str = stream_group.get("beginPartie")

        if not (name and category_id and iframe and event_time):
            continue

        event_dt = Time.from_str(event_time, timezone="CET")
        if event_dt.date() != now.date():
            continue

        if not (sport := CATEGORIES.get(category_id)):
            continue

        events.append({
            "sport": sport,
            "event": name,
            "link": iframe.split("<")[0],
            "timestamp": now.timestamp(),
        })
    return events

async def scrape() -> None:
    # এখানে লজিক উইথড্র করা হয়েছে যাতে ক্যাশ ফাইল রিড করার চেষ্টা করে এরর না আসে
    cached_urls = {}
    try:
        if hasattr(CACHE_FILE, 'load') and (loaded := CACHE_FILE.load()):
            cached_urls = loaded
            urls.update({k: v for k, v in cached_urls.items() if v["url"]})
            log.info(f"Loaded {len(urls)} event(s) from cache")
            return
    except Exception:
        pass

    log.info('Scraping from "https://streamcenter.xyz"')

    if events := await get_events():
        log.info(f"Processing {len(events)} URL(s)")

        for i, ev in enumerate(events, start=1):
            handler = partial(process_event, url=(link := ev["link"]), url_num=i)
            url = await network.safe_process(handler, url_num=i, semaphore=network.HTTP_S, log=log)

            sport, event, ts = ev["sport"], ev["event"], ev["timestamp"]
            key = f"[{sport}] {event} ({TAG})"
            
            tvg_id, logo = leagues.get_tvg_info(sport, event) if hasattr(leagues, 'get_tvg_info') else (None, None)

            entry = {
                "url": url,
                "logo": logo,
                "base": "https://streamcenter.xyz",
                "timestamp": ts,
                "id": tvg_id or "Live.Event.us",
                "link": link,
                }
            cached_urls[key] = entry
            if url:
                urls[key] = entry

        log.info(f"Collected and cached {len(urls)} event(s)")
    else:
        log.info("No events found")

    try:
        if hasattr(CACHE_FILE, 'write'):
            CACHE_FILE.write(cached_urls)
    except Exception:
        pass

# স্ক্রিপ্টটি সরাসরি রান করার জন্য মেইন ব্লক
if __name__ == "__main__":
    print("Starting Scraper...")
    asyncio.run(scrape())
    print("Scraper Finished Successfully!")
