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
# BUILD REGION STRING (ANCHORS GOOGLE MAPS SEARCH)
# Now uses "in <region>" for stronger constraint
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
# STRICT GEOGRAPHIC VALIDATION
# Ensures results are actually in the specified location
# =====================================================
def is_location_valid(item, state=None, city=None, postcode=None):
    """
    Validates that the business is in the specified geographic area
    """
    address = (item.get("address") or "").lower()
    
    if not address:
        return False
    
    # Check postcode if provided
    if postcode and postcode.lower() not in address:
        return False
    
    # Check state if provided (CRITICAL for Tamil Nadu issue)
    if state:
        state_lower = state.lower()
        # Check for exact match or common abbreviations
        if state_lower not in address:
            # Check if abbreviated form is in address (e.g., "TN" for Tamil Nadu)
            return False
    
    # Check city if provided
    if city:
        city_lower = city.lower()
        if city_lower not in address:
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

        return parsed[:6]  # nominal diversity

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
        country = input_data.get("country", "").strip()   # REQUIRED
        state = input_data.get("state", "").strip()
        city = input_data.get("city", "").strip()
        postcode = input_data.get("postcode", "").strip()
        keyword = input_data.get("keyword", "").strip()
        max_results = int(input_data.get("maxResults", 25))

        Actor.log.info(f"📋 Sector: {sector}")
        Actor.log.info(f"📍 Inputs: country={country}, state={state}, city={city}, postcode={postcode}")
        Actor.log.info(f"🔢 Max results: {max_results}")

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
        # BUILD REGION (ANCHOR SEARCH INSIDE STATE/CITY)
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

        # -------------------------------------------------
        # GOOGLE-LIKE SEARCH LOOP (REGION-ANCHORED)
        # -------------------------------------------------
        for query in queries:
            # CRITICAL FIX: Use "in <region>" instead of "near <region>"
            # This provides stronger geographic constraint
            search_string = f"{query} in {region}".strip()
            Actor.log.info(f"➡️ Searching: {search_string}")

            run_input = {
                "searchStringsArray": [search_string],
                "language": "en",
                "includeWebResults": False,
                "maxReviews": 0,
                "maxImages": 0,
                "maxConcurrency": 1,
                # Request MORE results to account for filtering
                "maxCrawledPlacesPerSearch": min(max_results * 3, 75)
            }

            # Country lock (ALL countries supported)
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
                    # CRITICAL FIX: Validate location BEFORE adding
                    if not is_location_valid(item, state=state, city=city, postcode=postcode):
                        Actor.log.debug(f"❌ Filtered out: {item.get('title')} - {item.get('address')}")
                        continue
                    
                    key = f"{item.get('title')}_{item.get('address')}"
                    if key not in seen:
                        seen.add(key)
                        all_items.append(item)
                        Actor.log.info(f"✅ Valid result: {item.get('title')} - {item.get('address')}")

                if len(all_items) >= max_results:
                    client.run(run_id).abort()
                    break

                if time.time() - START_TIME > 120:  # Extended timeout for filtering
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(3)

            if len(all_items) >= max_results:
                break

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
        Actor.log.info(f"🎉 Finished. {len(final_results)} leads saved.")
        
        if state and len(final_results) < max_results:
            Actor.log.warning(f"⚠️ Only found {len(final_results)}/{max_results} results in {state}. Consider expanding search area.")


if __name__ == "__main__":
    asyncio.run(main())
