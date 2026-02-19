from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import time
import requests
import pycountry
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# =====================================================
# SAFETY
# =====================================================
os.environ["APIFY_DISABLE_PLAYWRIGHT"] = "1"

# =====================================================
# REGEX
# =====================================================
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
COMMON_PREFIXES = ["info", "sales", "contact", "admin", "support"]

COMMON_CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/contactus",
    "/Contact",
    "/Contact-Us",
    "/Contact-Us.aspx",
    "/contact.aspx"
]

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


def postcode_valid(item, postcode):
    if not postcode:
        return True
    return postcode.lower() in (item.get("address") or "").lower()


# =====================================================
# SMART FIRECRAWL (MAX 2 CALLS)
# =====================================================
def firecrawl_enrich(url):
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key or not url:
        return {"status": "skipped", "emails": []}

    # Skip social sites
    if any(x in url for x in ["facebook.com", "instagram.com", "linkedin.com"]):
        return {"status": "skipped_social", "emails": []}

    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    def scrape_page(target_url):
        try:
            resp = requests.post(
                "https://api.firecrawl.dev/v1/scrape",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "url": target_url,
                    "formats": ["html"],
                    "limit": 1
                },
                timeout=15
            )

            if resp.status_code != 200:
                return None

            return resp.json().get("data", {}).get("html", "")

        except Exception:
            return None

    def extract_mailto(html):
        soup = BeautifulSoup(html, "html.parser")
        emails = []

        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                email = a["href"].replace("mailto:", "").split("?")[0]
                emails.append(email.strip())

        return list(set(emails))[:3], soup

    # 1️⃣ Homepage
    homepage_html = scrape_page(url)
    if not homepage_html:
        return {"status": "blocked", "emails": []}

    emails, soup = extract_mailto(homepage_html)
    if emails:
        return {"status": "found_homepage", "emails": emails}

    # 2️⃣ Try linked contact page
    contact_url = None
    for a in soup.find_all("a", href=True):
        if "contact" in a["href"].lower():
            contact_url = urljoin(url, a["href"])
            break

    # 3️⃣ If not linked, try common paths
    if not contact_url:
        for path in COMMON_CONTACT_PATHS:
            possible_url = urljoin(url, path)
            contact_url = possible_url
            break

    if contact_url:
        contact_html = scrape_page(contact_url)
        if contact_html:
            emails, _ = extract_mailto(contact_html)
            if emails:
                return {"status": "found_contact", "emails": emails}

    # 4️⃣ Smart domain guess
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "")
    guessed_emails = [f"{prefix}@{domain}" for prefix in COMMON_PREFIXES]

    return {"status": "guessed_domain", "emails": guessed_emails[:3]}


# =====================================================
# MAIN ACTOR
# =====================================================
async def main():
    async with Actor:
        start_time = time.time()
        data = await Actor.get_input() or {}

        country = data.get("country", "")
        state = data.get("state", "")
        city = data.get("city", "")
        postcode = data.get("postcode", "")
        max_results = int(data.get("maxResults", 70))

        raw = data.get("keywords") or data.get("keyword") or ""

        if isinstance(raw, str):
            keywords = [k.strip() for k in raw.split(",") if k.strip()]
        elif isinstance(raw, list):
            keywords = raw
        else:
            keywords = []

        region = build_region(country, state, city, postcode)
        Actor.log.info(f"Region: {region}")
        Actor.log.info(f"Keywords: {keywords[:5]}")

        client = ApifyClient(os.environ["APIFY_TOKEN"])

        seen = set()
        collected = []

        # =================================================
        # GOOGLE MAPS SEARCH
        # =================================================
        for term in keywords:
            query = f"{term} near {region}"

            run_input = {
                "searchStringsArray": [query],
                "language": "en",
                "includeWebResults": False,
                "maxCrawledPlacesPerSearch": 80,
                "maxConcurrency": 1
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
                        item["searchQuery"] = term
                        collected.append(item)

                if time.time() - start_time > 120:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(2)

        Actor.log.info(f"Collected before cap: {len(collected)}")

        # =================================================
        # OUTPUT + ENRICHMENT
        # =================================================
        output = []
        MAX_FIRECRAWL = 15

        for item in collected[:max_results]:
            enrich = {"status": "skipped", "emails": []}

            if item.get("website") and len(output) < MAX_FIRECRAWL:
                enrich = firecrawl_enrich(item.get("website"))

            # Extract latitude & longitude
            lat = None
            lng = None

            if item.get("location"):
                lat = item["location"].get("lat")
                lng = item["location"].get("lng")
            elif item.get("gpsCoordinates"):
                lat = item["gpsCoordinates"].get("latitude")
                lng = item["gpsCoordinates"].get("longitude")

            output.append({
                "name": item.get("title"),
                "phone": item.get("phone"),
                "website": item.get("website"),
                "address": item.get("address"),
                "rating": item.get("totalScore"),
                "reviewCount": item.get("reviewsCount"),
                "category": item.get("categoryName"),
                "googleMapsUrl": item.get("url"),
                "image": (
                    item.get("imageUrl") or
                    (item.get("imageUrls")[0] if item.get("imageUrls") else None)
                ),
                "latitude": lat,
                "longitude": lng,
                "searchQuery": item.get("searchQuery"),
                "enrichmentStatus": enrich.get("status"),
                "emails": enrich.get("emails", [])
            })

        await Actor.push_data(output)
        Actor.log.info(f"✅ Finished. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
