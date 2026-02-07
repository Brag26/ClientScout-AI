from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import time
import requests
import pycountry
import re

# =====================================================
# SAFETY: DISABLE ANY BROWSER USAGE
# =====================================================
os.environ["APIFY_DISABLE_PLAYWRIGHT"] = "1"

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


def sector_keywords(sector):
    return {
        "Manufacturing": ["manufacturer", "factory", "industrial supplier"],
        "IT & Technology": ["software company", "IT services"],
        "Healthcare": ["hospital", "clinic"],
        "Food & Beverage": ["restaurant", "cafe"],
        "Education": ["school", "tuition center", "home tutor"]
    }.get(sector, [sector.lower()])


def postcode_valid(item, postcode):
    if not postcode:
        return True
    return postcode.lower() in (item.get("address") or "").lower()

# =====================================================
# FIRECRAWL (STATIC SCRAPE ONLY)
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
            json={
                "url": url,
                "formats": ["markdown"],
                "limit": 3
            },
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
        max_results = int(data.get("maxResults", 25))

        # ---------------------------
        # KEYWORD FIX (CRITICAL)
        # ---------------------------
        raw_keywords = data.get("keywords") or data.get("keyword")

        if isinstance(raw_keywords, str):
            keywords = [k.strip() for k in raw_keywords.split(",") if k.strip()]
        elif isinstance(raw_keywords, list):
            keywords = raw_keywords
        else:
            keywords = sector_keywords(sector)

        region = build_region(country, state, city, postcode)

        Actor.log.info(f"Region: {region}")
        Actor.log.info(f"Total keywords after split: {len(keywords)}")

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

                if time.time() - start > 90:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(2)

        # -------------------------------------------------
        # ENRICHMENT (NO PLAYWRIGHT)
        # -------------------------------------------------
        output = []
        MAX_FIRECRAWL = 10

        for item in collected[:max_results]:
            website = item.get("website")
            enrich = {"status": "skipped"}

            if website and len(output) < MAX_FIRECRAWL:
                enrich = firecrawl_enrich(website)

            output.append({
                "name": item.get("title"),
                "phone": item.get("phone"),
                "website": website,
                "address": item.get("address"),
                "rating": item.get("totalScore"),
                "reviewCount": item.get("reviewsCount"),
                "category": item.get("categoryName"),
                "googleMapsUrl": item.get("url"),
                "searchQuery": item.get("searchQuery") if item.get("searchQuery") else "",

                "enrichmentStatus": enrich.get("status"),
                "emails": enrich.get("emails", []),
                "phones": enrich.get("phones", []),
                "socialLinks": [],
                "websiteSummary": enrich.get("summary", "")
            })

        await Actor.push_data(output)
        Actor.log.info(f"✅ Finished. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
