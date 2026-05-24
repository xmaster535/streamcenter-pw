import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from functools import partial
from selectolax.parser import HTMLParser
import httpx

# লগিং কনফিগারেশন (গিটহাব কনসোলে আউটপুট দেখার জন্য)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("STRMCNTR")

# নেটওয়ার্ক রিকোয়েস্ট ক্লাস (যা রিকোয়েস্ট হ্যান্ডেল করবে এবং মডিউলের অভাব পূরণ করবে)
class NetworkHandler:
    HTTP_S = asyncio.Semaphore(5) # রিকোয়েস্ট স্প্যামিং এড়াতে লিমিট ৫ করা হলো

    async def request(self, url, params=None, log=None):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        try:
            async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
                response = await client.get(url, params=params)
                if response.status_code == 200:
                    return response
                else:
                    if log:
                        log.warning(f"রিকোয়েস্ট ব্যর্থ হয়েছে, স্ট্যাটাস কোড: {response.status_code}")
        except Exception as e:
            if log:
                log.error(f"ইউআরএল {url} রিকোয়েস্ট করার সময় এরর: {e}")
        return None

    async def safe_process(self, handler, url_num, semaphore, log):
        async with semaphore:
            try:
                return await handler()
            except Exception as e:
                log.error(f"ইউআরএল {url_num} প্রসেস করার সময় এরর: {e}")
                return None

# সময় হ্যান্ডলার ক্লাস (তারিখ ও সময় ঠিক রাখার জন্য)
class TimeHandler:
    def now(self):
        return datetime.now(timezone.utc)
    
    def clean(self, dt):
        return dt
        
    def from_str(self, time_str, timezone_str="CET"):
        try:
            cleaned_time = time_str.replace("Z", "+00:00")
            return datetime.fromisoformat(cleaned_time)
        except Exception:
            return datetime.now(timezone.utc)

# গ্লোবাল অবজেক্টসমূহ ইনিশিয়ালাইজেশন
network = NetworkHandler()
Time = TimeHandler()
TAG = "STRMCNTR"
API_URL = "https://backend.streamcenter.live/api/Parties"

urls: dict[str, dict[str, str | float]] = {}

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
        log.warning(f"URL {url_num}) ইউআরএল লোড করতে ব্যর্থ: {url}")
        return None

    soup = HTMLParser(html_data.content)
    iframe = soup.css_first("iframe")

    if not iframe or not (iframe_src := iframe.attributes.get("src")):
        log.warning(f"URL {url_num}) কোনো iframe পাওয়া যায়নি।")
        return None

    log.info(f"URL {url_num}) সফলভাবে M3U8 ক্যাপচার করা হয়েছে")
    try:
        stream_id = iframe_src.rsplit("=", 1)[-1]
        return f"https://mainstreams.pro/hls/{stream_id}.m3u8"
    except Exception as e:
        log.error(f"iframe সোর্স পার্স করতে ব্যর্থ: {e}")
        return None

async def get_events() -> list[dict[str, str]]:
    events = []
    r = await network.request(API_URL, params={"pageNumber": 1, "pageSize": 500}, log=log)
    if not r:
        log.warning("StreamCenter API থেকে ডেটা আনা যায়নি।")
        return events

    now = Time.clean(Time.now())
    try:
        api_data: list[dict] = r.json()
    except Exception as e:
        log.error(f"API JSON পার্স করতে ব্যর্থ: {e}")
        return events

    for stream_group in api_data:
        category_id: int = stream_group.get("categoryId")
        name: str = stream_group.get("gameName")
        iframe: str = stream_group.get("videoUrl")
        event_time: str = stream_group.get("beginPartie")

        if not (name and category_id and iframe and event_time):
            continue

        event_dt = Time.from_str(event_time)
        
        # শুধুমাত্র আজকের খেলাগুলো ফিল্টার করা হচ্ছে
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
    cached_urls = {}
    
    # লোকাল ক্যাশ ফাইল লোড করা (যদি থাকে)
    if os.path.exists("cache.json"):
        try:
            with open("cache.json", "r", encoding="utf-8") as f:
                cached_urls = json.load(f)
                urls.update({k: v for k, v in cached_urls.items() if v.get("url")})
                log.info(f"ক্যাশ থেকে {len(urls)} টি ইভেন্ট লোড করা হয়েছে (cache.json)")
        except Exception as e:
            log.error(f"cache.json লোড করতে ব্যর্থ: {e}")

    log.info('Scraping starting from "https://streamcenter.xyz"')

    events = await get_events()
    if events:
        log.info(f"মোট {len(events)} টি ইউআরএল প্রসেস করা হচ্ছে...")

        for i, ev in enumerate(events, start=1):
            handler = partial(process_event, url=(link := ev["link"]), url_num=i)
            url = await network.safe_process(handler, url_num=i, semaphore=network.HTTP_S, log=log)

            sport, event, ts = ev["sport"], ev["event"], ev["timestamp"]
            key = f"[{sport}] {event} ({TAG})"
            
            logo = None
            tvg_id = "Live.Event.us"

            entry = {
                "url": url,
                "logo": logo,
                "base": "https://streamcenter.xyz",
                "timestamp": ts,
                "id": tvg_id,
                "link": link,
            }

            cached_urls[key] = entry
            if url:
                urls[key] = entry

        log.info(f"মোট {len(urls)} টি লাইভ ইভেন্ট সংগ্রহ করা হয়েছে।")
    else:
        log.info("আজকের জন্য কোনো লাইভ ইভেন্ট পাওয়া যায়নি।")

    # ক্যাশ আপডেট করা
    try:
        with open("cache.json", "w", encoding="utf-8") as f:
            json.dump(cached_urls, f, indent=4, ensure_ascii=False)
    except Exception as e:
         log.error(f"cache.json লিখতে ব্যর্থ: {e}")

    # প্লেলিস্ট এবং JSON আউটপুট জেনারেট করা
    save_outputs()

def save_outputs():
    # JSON ডাটা ফাইল সেভ করা
    try:
        with open("live_streams.json", "w", encoding="utf-8") as f:
            json.dump(urls, f, indent=4, ensure_ascii=False)
        log.info("সফলভাবে live_streams.json সেভ করা হয়েছে।")
    except Exception as e:
        log.error(f"JSON সেভ করতে ব্যর্থ: {e}")

    # M3U প্লেলিস্ট ফাইল সেভ করা
    try:
        with open("playlist.m3u", "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for title, info in urls.items():
                if info.get("url"):
                    logo_str = f' tvg-logo="{info["logo"]}"' if info.get("logo") else ""
                    tvg_id_str = f' tvg-id="{info["id"]}"' if info.get("id") else ""
                    f.write(f'#EXTINF:-1{tvg_id_str}{logo_str},{title}\n')
                    f.write(f'{info["url"]}\n')
        log.info("সফলভাবে playlist.m3u সেভ করা হয়েছে।")
    except Exception as e:
        log.error(f"M3U প্লেলিস্ট তৈরি করতে ব্যর্থ: {e}")

if __name__ == "__main__":
    print("StreamCenter স্ক্র্যাপার রান হচ্ছে...")
    asyncio.run(scrape())
    print("স্ক্র্যাপার রান সফলভাবে সম্পন্ন হয়েছে!")
