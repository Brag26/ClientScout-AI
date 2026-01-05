from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import time
import requests
import pycountry
import re


# =====================================================
# REGEX
# =====================================================
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+?\d[\d\s\-]{8,}\d)")
SOCIAL_REGEX = re.compile(r"(linkedin\.com|facebook\.com|instagram\.com)", re.I)


# =====================================================
# HELPERS
# =====================================================
def get_country_code(country):
    try:
        return pycountry.countries.lookup(country).alpha_2.lower()
    except Exception:
        return None


def build_region(country, state=None, city=None, postcode=None):
    if postcode:
        return f"{postcode}, {country}"
    parts = []
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    parts.append(country)
    return ", ".join(parts)


def sector_keywords(sector, keyword=None):
    if keyword:
        return [keyword]

    return {
        "Manufacturing": ["manufacturer", "factory", "industrial supplier"],
        "IT & Technology": ["software company", "IT services"],
        "Healthcare": ["hospital", "clinic"],
        "Food & Beverage": ["restaurant", "cafe"]
    }.get(sector, [sector.lower()])


def postcode_valid(item, postcode):
    if not postcode:
        return True
    return postcode.lower() in (item.get("address") or "").lower()


# =====================================================
# FIRECRAWL (STATIC SCRAPE)
# =====================================================
def firecrawl_enrich(url):
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key or not url:
        return {"status": "skipped"}

    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    try:
        resp = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={"url": url, "formats": ["markdown"], "limit": 3},
            timeout=20
        )

        if resp.status_code != 200:
            return {"status": "blocked"}

        text = resp.json().get("data", {}).get("markdown", "") or ""

        return {
            "status": "attempted",
            "emails": list(set(EMAIL_REGEX.findall(text)))[:5],
            "phones": list(set(PHONE_REGEX.findall(text)))[:3],
            "summary": text[:500]
        }

    except Exception:
        return {"status": "blocked"}


# =====================================================
# PLAYWRIGHT FALLBACK (apify/web-scraper)
# =====================================================
def playwright_enrich(client, url):
    Actor.log.info(f"🧠 Playwright fallback triggered for {url}")

    run = client.actor("apify/web-scraper").start(
        run_input={
            "startUrls": [{"url": url}],
            "renderJavaScript": True,
            "maxConcurrency": 1,
            "pageFunction": """
                async ({ page }) => {
                    await page.waitForTimeout(3000);
                    const text = document.body.innerText;
                    const links = Array.from(document.querySelectorAll('a'))
                        .map(a => a.href)
                        .filter(Boolean);
                    return { text, links };
                }
            """
        }
    )

    dataset_id = run["defaultDatasetId"]
    items = list(client.dataset(dataset_id).iterate_items())

    if not items:
        return {"status": "blocked"}

    text = items[0].get("text", "")
    links = items[0].get("links", [])

    return {
        "status": "partial",
        "emails": list(set(EMAIL_REGEX.findall(text)))[:5],
        "phones": list(set(PHONE_REGEX.findall(text)))[:3],
        "socialLinks": [l for l in links if SOCIAL_REGEX.search(l)][:5]
    }


# =====================================================
# MAIN ACTOR
# =====================================================
async def main():
    async with Actor:
        start = time.time()
        data = await Actor.get_input() or {}

        sector = data.get("sector", "")
        country = data.get("country", "")
        state = data.get("state", "")
        city = data.get("city", "")
        postcode = data.get("postcode", "")
        keyword = data.get("keyword", "")
        max_results = int(data.get("maxResults", 25))

        region = build_region(country, state, city, postcode)
        keywords = sector_keywords(sector, keyword)

        Actor.log.info(f"Region: {region}")
        Actor.log.info(f"Keywords: {keywords}")

        client = ApifyClient(os.environ["APIFY_TOKEN"])

        seen = set()
        collected = []

        # -------------------------------------------------
        # GOOGLE MAPS SEARCH
        # -------------------------------------------------
        for term in keywords:
            query = f"{term} near {region}"

            run_input = {
                "searchStringsArray": [query],
                "language": "en",
                "includeWebResults": False,
                "maxCrawledPlacesPerSearch": min(max_results * 2, 40),
                "maxConcurrency": 1
            }

            cc = get_country_code(country)
            if cc:
                run_input["countryCode"] = cc

            run = client.actor("compass/crawler-google-places").start(
                run_input=run_input
            )

            ds = run["defaultDatasetId"]
            run_id = run["id"]

            while True:
                items = list(client.dataset(ds).iterate_items())

                for item in items:
                    if not postcode_valid(item, postcode):
                        continue
                    key = f"{item.get('title')}_{item.get('address')}"
                    if key not in seen:
                        seen.add(key)
                        collected.append(item)

                if len(collected) >= max_results or time.time() - start > 60:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(2)

            if len(collected) >= max_results:
                break

        # -------------------------------------------------
        # ENRICHMENT
        # -------------------------------------------------
        output = []
        MAX_FIRECRAWL = 10
        MAX_PLAYWRIGHT = 3
        playwright_used = 0

        for item in collected[:max_results]:
            website = item.get("website")
            enrich = {"status": "skipped"}

            if website and len(output) < MAX_FIRECRAWL:
                enrich = firecrawl_enrich(website)

            if (
                enrich.get("status") == "blocked"
                and website
                and playwright_used < MAX_PLAYWRIGHT
                and sector in ["Manufacturing", "IT & Technology"]
            ):
                enrich = playwright_enrich(client, website)
                playwright_used += 1

            output.append({
                "name": item.get("title"),
                "phone": item.get("phone"),
                "website": website,
                "address": item.get("address"),
                "rating": item.get("totalScore"),
                "reviewCount": item.get("reviewsCount"),
                "category": item.get("categoryName"),
                "googleMapsUrl": item.get("url"),
                "searchQuery": keyword or sector,

                # enrichment
                "enrichmentStatus": enrich.get("status"),
                "emails": enrich.get("emails", []),
                "phones": enrich.get("phones", []),
                "socialLinks": enrich.get("socialLinks", []),
                "websiteSummary": enrich.get("summary", "")
            })

        await Actor.push_data(output)
        Actor.log.info(f"✅ Finished. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
