from apify import Actor
import random

async def main():
    async with Actor:
        input_data = await Actor.get_input() or {}

        sector = input_data.get("sector")
        city = input_data.get("city")
        keyword = input_data.get("keyword", sector)
        max_results = input_data.get("maxResults", 10)

        results = []

        for i in range(max_results):
            results.append({
                "name": f"{sector} Lead {i+1}",
                "sector": sector,
                "keyword": keyword,
                "city": city,
                "phone": f"+91 9{random.randint(100000000,999999999)}",
                "email": f"lead{i+1}@example.com",
                "score": random.randint(60, 95)
            })

        await Actor.push_data(results)
        Actor.log.info(f"Generated {len(results)} leads for sector: {sector}")
