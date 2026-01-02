from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import time
import requests
import pycountry
import re


# =====================================================
# COUNTRY → ISO-2 CODE (ALL COUNTRIES)
# =====================================================
def get_country_code(country_name: str):
    if not country_name:
        return None
    try:
        return pycountry.countries.lookup(country_name).alpha_2.lower()
    except LookupError:
        return None


# =====================================================
# BUILD REGION STRING (STRONG GEO ANCHOR)
# =====================================================
def build_region(country, state=None, city=None):
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
        "Food & Beverage": [
            "restaurant",
            "food manufacturer",
            "food supplier",
            "beverage supplier",
            "food processing company"
        ],
        "Healthcare": [
            "hospital",
            "clinic",
            "medical centre",
            "diagnostic centre"
        ],
        "Manufacturing": [
            "manufacturing company",
            "factory",
            "industrial manufacturer"
        ],
        "IT & Technology": [
            "software company",
            "IT services",
            "technology company"
        ]
    }

    return sector_map.get(sector, [sector.lower()])


# =====================================================
# STRICT FILTER: POSTCODE ONLY (OPTIONAL)
# =====================================================
def postcode_valid(item, postcode=None):
    if not postcode:
        return True
    address = (item.get("address") or "").lower()
    return postcode.lower() in address


# =====================================================
# FIRECRAWL ENRICHMENT (ROBUST + SAFE)
# =====================================================
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
)

def firecrawl_enrich(url):
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key or not url:
        return {}

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
                "limit": 3,                  # homepage + contact/about
                "includeSubdomains": False
            },
            timeout=20
        )

        if resp.status_code != 200:
            return {}

        text = resp.json().get("data", {}).get("markdown", "") or ""

        emails = list(set(EMAIL_REGEX.findall(text)))

        return {
            "emails": emails[:5],
            "websiteSummary": text[:500]
        }

    except Exception:
        return {}


# =====================================================
# MAIN ACTOR
# =====================================================
async def main():
    async with Actor:
        START = time.time()

        input_data = await Actor.get_input() or {}

        sector = input_data.get("sector", "")
        country = input_data.get("country", "").strip()   # REQUIRED
        state = input_data.get("state", "").strip()
        city = input_data.get("city", "").strip()
        postcode = input_data.get("postcode", "").strip()
        keyword = input_data.get("keyword", "").strip()
        max_results = int(input_data.get("maxResults", 25))

        Actor.log.info(f"Sector: {sector}")
        Actor.log.info(f"Location: {country}, {state}, {city}, {postcode}")
        Actor.log.info(f"Max results: {max_results}")

        # -------------------------------------------------
        # CREDIT SAFETY
        # -------------------------------------------------
        credits = Actor.get_env().get("APIFY_USER_REMAINING_CREDITS")
        if credits and float(credits) < 0.2:
            await Actor.push_data({"error": "Insufficient Apify credits"})
            return

        # -------------------------------------------------
        # REGION + KEYWORDS
        # -------------------------------------------------
        region = build_region(country, state, city)
        keywords = sector_keywords(sector, keyword)

        Actor.log.info(f"Region anchor: {region}")
        Actor.log.info(f"Search terms: {keywords}")

        client = ApifyClient(token=os.environ["APIFY_TOKEN"])

        collected = []
        seen = set()

        # -------------------------------------------------
        # GOOGLE-LIKE SEARCH LOOP
        # -------------------------------------------------
        for term in keywords:
            search_string = f"{term} near {region}"
            Actor.log.info(f"Searching: {search_string}")

            run_input = {
                "searchStringsArray": [search_string],
                "language": "en",
                "includeWebResults": False,
                "maxReviews": 0,
                "maxImages": 0,
                "maxConcurrency": 1,
                "maxCrawledPlacesPerSearch": min(max_results * 2, 40)
            }

            country_code = get_country_code(country)
            if country_code:
                run_input["countryCode"] = country_code

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
                    if key in seen:
                        continue

                    seen.add(key)
                    collected.append(item)

                if len(collected) >= max_results:
                    client.run(run_id).abort()
                    break

                if time.time() - START > 60:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(2)

            if len(collected) >= max_results:
                break

        # -------------------------------------------------
        # FINAL OUTPUT + FIRECRAWL ENRICHMENT
        # -------------------------------------------------
        output = []
        enrich_limit = 10   # protect Firecrawl credits

        for item in collected[:max_results]:
            website = item.get("website")
            enrichment = {}

            if website and len(output) < enrich_limit:
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
                "emails": enrichment.get("emails", []),
                "websiteSummary": enrichment.get("websiteSummary", "")
            })

        await Actor.push_data(output)
        Actor.log.info(f"Finished. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
