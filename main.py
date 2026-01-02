from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import json
import aiohttp
import time
import pycountry


# =====================================================
# ALL-COUNTRY ISO-2 MAPPING (NO HARDCODE)
# =====================================================
def get_country_code(country_name: str):
    if not country_name:
        return None
    try:
        country = pycountry.countries.lookup(country_name.strip())
        return country.alpha_2.lower()
    except LookupError:
        return None


# =====================================================
# STATE NAME VARIATIONS (handles abbreviations)
# =====================================================
STATE_VARIATIONS = {
    "tamil nadu": ["tamil nadu", "tamilnadu", "tn", "t.n"],
    "karnataka": ["karnataka", "ka", "k.a"],
    "kerala": ["kerala", "kl", "k.l"],
    "maharashtra": ["maharashtra", "mh", "m.h"],
    "telangana": ["telangana", "ts", "t.s", "tg"],
    "andhra pradesh": ["andhra pradesh", "ap", "a.p"],
    "west bengal": ["west bengal", "wb", "w.b"],
    "gujarat": ["gujarat", "gj", "g.j"],
    "rajasthan": ["rajasthan", "rj", "r.j"],
    "madhya pradesh": ["madhya pradesh", "mp", "m.p"],
    "uttar pradesh": ["uttar pradesh", "up", "u.p"],
}


def get_state_variations(state):
    """Returns all possible variations of a state name"""
    if not state:
        return []
    
    state_lower = state.lower().strip()
    
    # Check if we have predefined variations
    if state_lower in STATE_VARIATIONS:
        return STATE_VARIATIONS[state_lower]
    
    # Otherwise return the state itself and common abbreviation patterns
    words = state_lower.split()
    abbreviations = [state_lower]
    
    # Add acronym (e.g., "West Bengal" -> "wb")
    if len(words) > 1:
        acronym = "".join(w[0] for w in words)
        abbreviations.append(acronym)
        abbreviations.append(".".join(w[0] for w in words) + ".")
    
    return abbreviations


# =====================================================
# BUILD REGION STRING (ANCHORS GOOGLE MAPS SEARCH)
# =====================================================
def build_region_string(country, state=None, city=None):
    parts = []
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    parts.append(country)
    return ", ".join(parts)


# =====================================================
# SMART GEOGRAPHIC VALIDATION
# More flexible - checks multiple address variations
# =====================================================
def is_location_valid(item, state=None, city=None, postcode=None):
    """
    Validates that the business is in the specified geographic area
    Uses flexible matching for state names and abbreviations
    """
    address = (item.get("address") or "").lower()
    
    if not address:
        return False
    
    # Check postcode if provided (strict)
    if postcode:
        if postcode.lower() not in address:
            return False
    
    # Check state if provided (flexible - checks variations)
    if state:
        state_variations = get_state_variations(state)
        # Check if ANY variation appears in the address
        if not any(variation in address for variation in state_variations):
            return False
    
    # Check city if provided (flexible)
    if city:
        city_lower = city.lower().strip()
        if city_lower not in address:
            # Try without spaces (e.g., "New Delhi" -> "newdelhi")
            city_no_space = city_lower.replace(" ", "")
            address_no_space = address.replace(" ", "")
            if city_no_space not in address_no_space:
                return False
    
    return True


# =====================================================
# AI QUERY GENERATION (MULTI-QUERY, GOOGLE-LIKE)
# =====================================================
async def generate_search_queries_with_llm(sector, keyword, region):
    prompt = f"""
Generate 6–8 Google Maps search queries for businesses related to:
Sector: {sector}
Keyword: {keyword}
Region: {region}

Rules:
- Return ONLY a JSON array
- Include category-style searches (manufacturers, suppliers, companies, services)
- No explanation
"""

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as res:
                data = await res.json()

        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        text = text.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(text)

        if not isinstance(parsed, list) or not parsed:
            raise ValueError("Invalid JSON")

        return parsed[:6]

    except Exception as e:
        Actor.log.warning(f"⚠️ LLM failed, fallback used: {e}")
        return [keyword] if keyword else [sector]


# =====================================================
# MAIN ACTOR
# =====================================================
async def main():
    async with Actor:
        START_TIME = time.time()

        input_data = await Actor.get_input() or {}

        sector = input_data.get("sector", "")
        country = input_data.get("country", "").strip()
        state = input_data.get("state", "").strip()
        city = input_data.get("city", "").strip()
        postcode = input_data.get("postcode", "").strip()
        keyword = input_data.get("keyword", "").strip()
        max_results = int(input_data.get("maxResults", 25))

        Actor.log.info(f"📋 Sector: {sector}")
        Actor.log.info(f"📍 Inputs: country={country}, state={state}, city={city}, postcode={postcode}")
        Actor.log.info(f"🔢 Max results: {max_results}")

        # Log state variations being used
        if state:
            variations = get_state_variations(state)
            Actor.log.info(f"🔍 State variations: {variations}")

        # -------------------------------------------------
        # CREDIT SAFETY
        # -------------------------------------------------
        remaining = Actor.get_env().get("APIFY_USER_REMAINING_CREDITS")
        if remaining:
            try:
                if float(remaining) < 0.2:
                    await Actor.push_data({"error": "Insufficient Apify credits."})
                    return
            except ValueError:
                pass

        # -------------------------------------------------
        # BUILD REGION
        # -------------------------------------------------
        region = build_region_string(country, state=state, city=city)
        Actor.log.info(f"🧭 Region anchor: {region}")

        # -------------------------------------------------
        # MULTI-QUERY GENERATION
        # -------------------------------------------------
        queries = await generate_search_queries_with_llm(
            sector, keyword, region
        )
        Actor.log.info(f"🔍 Queries: {queries}")

        client = ApifyClient(token=os.environ["APIFY_TOKEN"])

        all_items = []
        seen = set()
        filtered_count = 0

        # -------------------------------------------------
        # SEARCH LOOP
        # -------------------------------------------------
        for query in queries:
            search_string = f"{query} in {region}".strip()
            Actor.log.info(f"➡️ Searching: {search_string}")

            run_input = {
                "searchStringsArray": [search_string],
                "language": "en",
                "includeWebResults": False,
                "maxReviews": 0,
                "maxImages": 0,
                "maxConcurrency": 1,
                "maxCrawledPlacesPerSearch": min(max_results * 2, 50)
            }

            # Country lock
            country_code = get_country_code(country)
            if country_code:
                run_input["countryCode"] = country_code
                Actor.log.info(f"🌍 Country locked: {country_code}")

            run = client.actor("compass/crawler-google-places").start(
                run_input=run_input
            )

            run_id = run["id"]
            dataset_id = run["defaultDatasetId"]

            while True:
                items = list(client.dataset(dataset_id).iterate_items())

                for item in items:
                    # Validate location
                    if not is_location_valid(item, state=state, city=city, postcode=postcode):
                        filtered_count += 1
                        Actor.log.debug(f"❌ Filtered: {item.get('title')} - {item.get('address')}")
                        continue
                    
                    key = f"{item.get('title')}_{item.get('address')}"
                    if key not in seen:
                        seen.add(key)
                        all_items.append(item)
                        Actor.log.info(f"✅ Added: {item.get('title')} - {item.get('address')}")

                if len(all_items) >= max_results:
                    client.run(run_id).abort()
                    break

                if time.time() - START_TIME > 120:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(3)

            if len(all_items) >= max_results:
                break

        Actor.log.info(f"📊 Total filtered out: {filtered_count}")

        # -------------------------------------------------
        # FINAL OUTPUT
        # -------------------------------------------------
        final_results = []

        for item in all_items:
            final_results.append({
                "name": item.get("title"),
                "phone": item.get("phone"),
                "website": item.get("website"),
                "address": item.get("address"),
                "rating": item.get("totalScore"),
                "reviewCount": item.get("reviewsCount"),
                "category": item.get("categoryName"),
                "googleMapsUrl": item.get("url"),
                "searchQuery": keyword or sector
            })

            if len(final_results) >= max_results:
                break

        await Actor.push_data(final_results)
        Actor.log.info(f"🎉 Finished. {len(final_results)} leads saved (filtered {filtered_count}).")


if __name__ == "__main__":
    asyncio.run(main())
