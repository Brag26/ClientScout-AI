from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
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
# WEBSITE ENRICHMENT
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

        # Mailto
        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                email = a["href"].replace("mailto:", "").split("?")[0]
                emails.append(email.strip())

        # Visible emails
        emails.extend(EMAIL_REGEX.findall(html))
        emails = list(set(emails))[:5]

        # Contact person detection
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

    soup = BeautifulSoup(homepage, "html.parser")

    for a in soup.find_all("a", href=True):
        if "contact" in a["href"].lower() or "about" in a["href"].lower():
            contact_url = urljoin(url, a["href"])
            contact_page = fetch(contact_url)
            if contact_page:
                emails, persons = extract(contact_page)
                if emails:
                    return {"status": "found_contact", "emails": emails, "persons": persons}

    # Fallback
    domain = urlparse(url).netloc.replace("www.", "")
    guessed = [f"info@{domain}", f"contact@{domain}"]

    return {"status": "guessed_domain", "emails": guessed, "persons": []}


# =====================================================
# MAIN ACTOR
# =====================================================
async def main():
    async with Actor:
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
            keywords = [k.strip() for k in raw if k.strip()]
        else:
            keywords = []

        if not keywords:
            Actor.log.error("❌ No keywords provided.")
            return

        region = build_region(country, state, city, postcode)
        search_queries = [f"{k} near {region}" for k in keywords]

        Actor.log.info(f"Search queries: {search_queries}")

        client = ApifyClient(os.environ["APIFY_TOKEN"])

        run_input = {
            "searchStringsArray": search_queries,
            "language": "en",
            "includeWebResults": False,
            "maxCrawledPlacesPerSearch": 50,
            "maxConcurrency": 1
        }

        cc = get_country_code(country)
        if cc:
            run_input["countryCode"] = cc

        try:
            # 🔥 IMPORTANT: call() waits until finished
            run = client.actor("compass/crawler-google-places").call(
                run_input=run_input
            )
        except Exception as e:
            Actor.log.error(f"Google Maps actor failed: {e}")
            return

        dataset = client.dataset(run["defaultDatasetId"])

        seen = set()
        collected = []

        for item in dataset.iterate_items():
            if not postcode_valid(item, postcode):
                continue

            key = f"{item.get('title')}_{item.get('address')}"
            if key not in seen:
                seen.add(key)
                collected.append(item)

        Actor.log.info(f"Collected raw items: {len(collected)}")

        if not collected:
            Actor.log.warning("⚠ No results found.")
            return

        output = []

        for item in collected[:max_results]:

            if item.get("website"):
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
                "enrichmentStatus": enrich.get("status"),
                "emails": enrich.get("emails", []),
                "contactPersons": enrich.get("persons", [])
            })

        await Actor.push_data(output)
        Actor.log.info(f"✅ Finished. Leads saved: {len(output)}")


if __name__ == "__main__":
    asyncio.run(main())
