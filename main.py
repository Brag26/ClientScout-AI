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
# REGEX
# =====================================================
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

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
# SIMPLE WEBSITE ENRICHMENT (NO FIRECRAWL)
# =====================================================
def simple_web_enrich(url):
    if not url:
        return {"status": "skipped", "emails": [], "persons": []}

    if any(x in url for x in ["facebook.com", "instagram.com", "linkedin.com"]):
        return {"status": "skipped_social", "emails": [], "persons": []}

    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    def fetch_page(target_url):
        try:
            resp = requests.get(target_url, headers=headers, timeout=10)
            if resp.status_code == 200:
                return resp.text
        except:
            return None
        return None

    def extract_info(html):
        soup = BeautifulSoup(html, "html.parser")

        # 🔹 Mailto extraction
        emails = []
        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                email = a["href"].replace("mailto:", "").split("?")[0]
                emails.append(email.strip())

        # 🔹 Visible email extraction
        visible_emails = EMAIL_REGEX.findall(html)
        emails.extend(visible_emails)

        emails = list(set(emails))[:5]

        # 🔹 Extract probable person names
        persons = []
        for tag in soup.find_all(["h1", "h2", "h3", "strong", "b"]):
            text = tag.get_text().strip()
            if 2 <= len(text.split()) <= 4:
                if all(word[0].isupper() for word in text.split() if word.isalpha()):
                    persons.append(text)

        persons = list(set(persons))[:3]

        return emails, persons

    # 1️⃣ Homepage
    homepage_html = fetch_page(url)
    if not homepage_html:
        return {"status": "blocked", "emails": [], "persons": []}

    emails, persons = extract_info(homepage_html)

    if emails:
        return {"status": "found_homepage", "emails": emails, "persons": persons}

    # 2️⃣ Try contact/about page
    soup = BeautifulSoup(homepage_html, "html.parser")
    contact_url = None

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "contact" in href or "about" in href:
            contact_url = urljoin(url, a["href"])
            break

    if contact_url:
        contact_html = fetch_page(contact_url)
        if contact_html:
            emails, persons = extract_info(contact_html)
            if emails:
                return {"status": "found_contact", "emails": emails, "persons": persons}

    # 3️⃣ Domain email fallback
    parsed = urlparse(url)
    domain = parsed.netloc.replace("www.", "")
    guessed_emails = [f"info@{domain}", f"contact@{domain}"]

    return {"status": "guessed_domain", "emails": guessed_emails, "persons": []}


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

        for item in collected[:max_results]:
            enrich = {"status": "skipped", "emails": [], "persons": []}

            if item.get("website"):
                enrich = simple_web_enrich(item.get("website"))

            # Latitude & Longitude
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
