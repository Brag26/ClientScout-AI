from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import time
import requests
import pycountry
import re


# =====================================================
# COUNTRY → ISO-2 CODE
# =====================================================
def get_country_code(country_name: str):
    try:
        return pycountry.countries.lookup(country_name).alpha_2.lower()
    except Exception:
        return None


# =====================================================
# REGION BUILDER (POSTCODE > CITY/STATE > COUNTRY)
# =====================================================
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


# =====================================================
# SECTOR → GOOGLE MAPS SEARCH TERMS
# =====================================================
def sector_keywords(sector, keyword=None):
    if keyword:
        return [keyword]

    sector_map = {
        "Food & Beverage": ["restaurant", "cafe", "food supplier"],
        "Healthcare": ["hospital", "clinic", "medical centre"],
        "Manufacturing": ["manufacturer", "factory", "industrial supplier"],
        "IT & Technology": ["software company", "IT services"]
    }

    return sector_map.get(sector, [sector.lower()])


# =====================================================
# POSTCODE FILTER (OPTIONAL)
# =====================================================
def postcode_valid(item, postcode=None):
    if not postcode:
        return True
    return postcode.lower() in (item.get("address") or "").lower()


# =====================================================
# FIRECRAWL ENRICHMENT (ROBUST + DEBUGGABLE)
# =====================================================
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
WHATSAPP_REGEX = re.compile(r"(?:\+?\d[\d\s\-]{8,}\d)")
CONTACT_PAGE_REGEX = re.compile(r'href="([^"]*(contact|about)[^"]*)"', re.I)


def firecrawl_enrich(url):
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key or not url:
        return {"status": "skipped"}

    # Force HTTPS (very important)
    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    Actor.log.info(f"🔥 Firecrawl triggered for {url}")

    try:
        resp = None
        for attempt in range(2):  # retry once
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
            if resp.status_code == 200:
                break

        if resp is None or resp.status_code != 200:
            Actor.log.warning(
                f"Firecrawl failed for {url} "
                f"status={resp.status_code if resp else 'no_response'} "
                f"body={(resp.text[:200] if resp else '')}"
            )
            return {"status": f"failed_{resp.status_code if resp else 'no_response'}"}

        text = resp.json().get("data", {}).get("markdown", "") or ""

        emails = list(set(EMAIL_REGEX.findall(text)))
        whatsapp = list(set(WHATSAPP_REGEX.findall(text)))
        contacts = [c[0] for c in CONTACT_PAGE_REGEX.findall(text)]

        return {
            "status": "attempted",
            "emails": emails[:5],
            "whatsappNumbers": whatsapp[:3],
            "contactPages": contacts[:3],
            "summary": text[:500]
        }

    except Exception as e:
        Actor.log.error(f"Firecrawl exception for {url}: {e}")
        return {"status": "exception"}


# =====================================================
# MAIN ACTOR
# =====================================================
async def main():
    async with Actor:
        start_time = time.time()
        data = await Actor.get_input() or {}

        sector = data.get("sector", "")
        country = data.get("country", "")
        state = data.get("state", "")
        city = data.get("city", "")
        postcode = data.get("postcode", "")
        keyword = data.get("keyword", "")
        max_results = int(data.get("maxResults", 25))

        Actor.log.info(f"Sector: {sector}")
        Actor.log.info(f"Location: {country}, {state}, {city}, {postcode}")
        Actor.log.info(f"Max results: {max_results}")

        region = build_region(country, state, city, postcode)
        keywords = sector_keywords(sector, keyword)

        Actor.log.info(f"Region anchor: {region}")
        Actor.log.info(f"Search keywords: {keywords}")

        client = ApifyClient(os.environ["APIFY_TOKEN"])

        seen = set()
        collected = []

        # -------------------------------------------------
        # GOOGLE MAPS SEARCH
        # -------------------------------------------------
        for term in keywords:
            search_query = f"{term} near {region}"
            Actor.log.info(f"Searching: {search_query}")

            run_input = {
                "searchStringsArray": [search_query],
                "language": "en",
                "includeWebResults": False,
                "maxReviews": 0,
                "maxImages": 0,
                "maxConcurrency": 1,
                "maxCrawledPlacesPerSearch": min(max_results * 2, 40)
            }

            cc = get_country_code(country)
            if cc:
                run_input["countryCode"] = cc

            run = client.actor("compass/crawler-google-places").start(
                run_input=run_input
            )

            dataset_id = run["defaultDatasetId"]
            run_id = run["id"]

            while True:
                items = list(client.dataset(dataset_id).iterate_items())

                for item in items:
                    if not postcode_valid(item, postcode):
                        continue

                    key = f"{item.get('title')}_{item.get('address')}"
                    if key not in seen:
                        seen.add(key)
                        collected.append(item)

                if len(collected) >= max_results or time.time() - start_time > 60:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(2)

            if len(collected) >= max_results:
                break

        # -------------------------------------------------
        # FINAL OUTPUT + FIRECRAWL
        # -------------------------------------------------
        output = []
        enrich_limit = 10
        B2B_SECTORS = ["Manufacturing", "IT & Technology"]

        for item in collected[:max_results]:
            website = item.get("website")
            enrichment = {"status": "skipped"}

            if sector in B2B_SECTORS and website and len(output) < enrich_limit:
                enrichment = firecrawl_enrich(website)

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

                # 🔥 Firecrawl visibility
                "firecrawlStatus": enrichment.get("status"),
                "emails": enrichment.get("emails", []),
                "whatsappNumbers": enrichment.get("whatsappNumbers", []),
                "contactPages": enrichment.get("contactPages", []),
                "websiteSummary": enrichment.get("summary", "")
            })

        await Actor.push_data(output)
        Actor.log.info(f"Finished successfully. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
