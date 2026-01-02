from apify import Actor
from apify_client import ApifyClient
import asyncio
import os
import json
import aiohttp
import time


# =====================================================
# AI QUERY GENERATION (MULTI-QUERY, GOOGLE-LIKE)
# =====================================================
async def generate_search_queries_with_llm(sector, keyword, location):
    prompt = f"""
Generate 6–8 different Google Maps search queries for businesses related to:
Sector: {sector}
Keyword: {keyword}
Location: {location}

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

        return parsed[:6]  # 🔒 limit but keep diversity

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
        city = input_data.get("city", "").strip()
        state = input_data.get("state", "").strip()
        postcode = input_data.get("postcode", "").strip()
        keyword = input_data.get("keyword", "").strip()
        country = input_data.get("country", "").strip()
        max_results = int(input_data.get("maxResults", 10))

        # -------------------------------------------------
        # CREDIT SAFETY
        # -------------------------------------------------
        remaining = Actor.get_env().get("APIFY_USER_REMAINING_CREDITS")
        if remaining:
            try:
                if float(remaining) < 0.2:
                    Actor.log.error("❌ Insufficient Apify credits")
                    await Actor.push_data({
                        "error": "Insufficient Apify credits."
                    })
                    return
            except ValueError:
                pass

        # -------------------------------------------------
        # STRICT LOCATION PRIORITY (GENERIC)
        # -------------------------------------------------
        if postcode:
            base_location = postcode
        elif city:
            base_location = f"{city}, {state}" if state else city
        elif state:
            base_location = f"{state}, {country}" if country else state
        elif country:
            base_location = country
        else:
            base_location = ""

        Actor.log.info(f"📍 Base location: {base_location}")

        # -------------------------------------------------
        # MULTI QUERY GENERATION (LIKE GOOGLE)
        # -------------------------------------------------
        queries = await generate_search_queries_with_llm(
            sector, keyword, base_location
        )

        Actor.log.info(f"🔎 Queries generated: {queries}")

        client = ApifyClient(token=os.environ["APIFY_TOKEN"])

        all_items = []
        seen_keys = set()

        # -------------------------------------------------
        # GOOGLE-LIKE SEARCH LOOP
        # -------------------------------------------------
        for query in queries:
            search_string = f"{query} in {base_location}".strip()

            Actor.log.info(f"➡️ Searching: {search_string}")

            run_input = {
                "searchStringsArray": [search_string],
                "language": "en",
                "includeWebResults": False,
                "maxReviews": 0,
                "maxImages": 0,
                "maxConcurrency": 1,
                "maxCrawledPlacesPerSearch": min(max_results, 20)
            }

            # Country restriction if provided
            country_map = {
                "india": "in",
                "australia": "au",
                "united states": "us",
                "usa": "us",
                "united kingdom": "gb",
                "uk": "gb",
                "canada": "ca",
                "singapore": "sg",
                "uae": "ae"
            }

            if country:
                code = country_map.get(country.lower())
                if code:
                    run_input["countryCode"] = code

            run = client.actor("compass/crawler-google-places").start(
                run_input=run_input
            )

            run_id = run["id"]
            dataset_id = run["defaultDatasetId"]

            # Poll results
            while True:
                items = list(client.dataset(dataset_id).iterate_items())

                for item in items:
                    key = f"{item.get('title')}_{item.get('address')}"
                    if key not in seen_keys:
                        seen_keys.add(key)
                        all_items.append(item)

                if len(all_items) >= max_results:
                    client.run(run_id).abort()
                    break

                if time.time() - START_TIME > 90:
                    client.run(run_id).abort()
                    break

                await asyncio.sleep(3)

            if len(all_items) >= max_results:
                break

        # -------------------------------------------------
        # FINAL NORMALIZED OUTPUT
        # -------------------------------------------------
        final_results = []

        for item in all_items[:max_results]:
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

        await Actor.push_data(final_results)
        Actor.log.info(f"🎉 Finished. {len(final_results)} leads saved.")


if __name__ == "__main__":
    asyncio.run(main())
