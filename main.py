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
# CONFIG
# =====================================================
USE_FIRECRAWL = False  # 🔥 Change to True anytime
MAX_FIRECRAWL = 15

# =====================================================
# REGEX
# =====================================================
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# =====================================================
# HELPERS
# =====================================================
def get_country_code(country):
    try:
        return pycountry.countries.lookup(country).alpha_2.lower()
    except:
        return None


def build_region(country, state=None, city=None, postcode=None):
    if postcode:
        return f"{postcode}, {country}"
    parts = [x for x in [city, state, country] if x]
    return ", ".join(parts)


def postcode_valid(item, postcode):
    if not postcode:
        return True
    return postcode.lower() in (item.get("address") or "").lower()


# =====================================================
# SIMPLE WEB ENRICH (FREE)
# =====================================================
def simple_web_enrich(url):
    if not url:
        return {"status": "skipped", "emails": [], "persons": []}

    if any(x in url for x in ["facebook.com", "instagram.com", "linkedin.com"]):
        return {"status": "skipped_social", "emails": [], "persons": []}

    headers = {"User-Agent": "Mozilla/5.0"}

    def fetch(target):
        try:
            r = requests.get(target, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.text
        except:
            return None
        return None

    def extract(html):
        soup = BeautifulSoup(html, "html.parser")

        emails = []

        # mailto
        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                email = a["href"].replace("mailto:", "").split("?")[0]
                emails.append(email.strip())

        # visible
        emails.extend(EMAIL_REGEX.findall(html))
        emails = list(set(emails))[:5]

        persons = []
        for tag in soup.find_all(["h1", "h2", "h3", "strong", "b"]):
            text = tag.get_text().strip()
            if 2 <= len(text.split()) <= 4:
                if all(w[0].isupper() for w in text.split() if w.isalpha()):
                    persons.append(text)

        persons = list(set(persons))[:3]

        return emails, persons

    homepage = fetch(url)
    if not homepage:
        return {"status": "blocked", "emails": [], "persons": []}

    emails, persons = extract(homepage)
    if emails:
        return {"status": "found_homepage", "emails": emails, "persons": persons}

    # Try contact/about
    soup = BeautifulSoup(homepage, "html.parser")
    for a in soup.find_all("a", href=True):
        if "contact" in a["href"].lower() or "about" in a["href"].lower():
            contact_url = urljoin(url, a["href"])
            contact_page = fetch(contact_url)
            if contact_page:
                emails, persons = extract(contact_page)
                if emails:
                    return {"status": "found_contact", "emails": emails, "persons": persons}

    # Domain guess
    domain = urlparse(url).netloc.replace("www.", "")
    guessed = [f"info@{domain}", f"contact@{domain}"]

    return {"status": "guessed_domain", "emails": guessed, "persons": []}


# =====================================================
# FIRECRAWL ENRICH (OPTIONAL)
# =====================================================
def firecrawl_enrich(url):
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        return {"status": "firecrawl_disabled", "emails": [], "persons": []}

    try:
        resp = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={"url": url, "formats": ["html"], "limit": 1},
            timeout=15
        )

        if resp.status_code != 200:
            return {"status": "firecrawl_failed", "emails": [], "persons": []}

        html = resp.json().get("data", {}).get("html", "")
        soup = BeautifulSoup(html, "html.parser")

        emails = EMAIL_REGEX.findall(html)
        emails = list(set(emails))[:5]

        persons = []
        for tag in soup.find_all(["h1", "h2", "h3"]):
            text = tag.get_text().strip()
            if 2 <= len(text.split()) <= 4:
                persons.append(text)

        return {
            "status": "firecrawl_success",
            "emails": emails,
            "persons": persons[:3]
        }

    except:
        return {"status": "firecrawl_error", "emails": [], "persons": []}


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
        keywords = raw.split(",") if isinstance(raw, str) else raw

        region = build_region(country, state, city, postcode)

        client = ApifyClient(os.environ["APIFY_TOKEN"])

        seen = set()
        collected = []

        for term in keywords:
            query = f"{term.strip()} near {region}"

            run_input = {
                "searchStringsArray": [query],
                "language": "en",
                "maxCrawledPlacesPerSearch": 80,
            }

            cc = get_country_code(country)
            if cc:
                run_input["countryCode"] = cc

            run = client.actor("compass/crawler-google-places").start(run_input=run_input)

            dataset_id = run["defaultDatasetId"]

            items = list(client.dataset(dataset_id).iterate_items())

            for item in items:
                if not postcode_valid(item, postcode):
                    continue

                key = f"{item.get('title')}_{item.get('address')}"
                if key not in seen:
                    seen.add(key)
                    item["searchQuery"] = term
                    collected.append(item)

        output = []

        for idx, item in enumerate(collected[:max_results]):

            if item.get("website"):
                if USE_FIRECRAWL and idx < MAX_FIRECRAWL:
                    enrich = firecrawl_enrich(item["website"])
                    if not enrich.get("emails"):
                        enrich = simple_web_enrich(item["website"])
                else:
                    enrich = simple_web_enrich(item["website"])
            else:
                enrich = {"status": "no_website", "emails": [], "persons": []}

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
                "emails": enrich.get("emails", []),
                "contactPersons": enrich.get("persons", [])
            })

        await Actor.push_data(output)
        Actor.log.info(f"✅ Finished. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
