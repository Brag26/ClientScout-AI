from apify import Actor
from apify_client import ApifyClient
import asyncio

async def main():
    async with Actor:
        Actor.log.info("=== Starting Lead Generator ===")
        
        # Get input
        input_data = await Actor.get_input() or {}
        sector = input_data.get("sector", "Healthcare")
        city = input_data.get("city", "Mumbai")
        keyword = input_data.get("keyword", sector)
        max_results = input_data.get("maxResults", 10)
        
        Actor.log.info(f"Searching for: {keyword} in {city}")
        
        # Initialize Apify client with token
        client = ApifyClient(token=Actor.config.token)
        
        # Prepare search query for Google Maps
        search_query = f"{keyword} in {city}"
        
        Actor.log.info(f"Running Google Maps scraper for: {search_query}")
        
        # Run Google Maps Scraper
        run_input = {
            "searchStringsArray": [search_query],
            "maxCrawledPlacesPerSearch": max_results,
            "language": "en",
            "includeWebResults": False,
            "maxReviews": 0,
            "maxImages": 0
        }
        
        # Call the Google Maps Scraper actor
        run = client.actor("compass/crawler-google-places").call(run_input=run_input)
        
        Actor.log.info("Google Maps scraper finished, processing results...")
        
        # Process and format results
        results = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            lead = {
                "name": item.get("title", "N/A"),
                "sector": sector,
                "keyword": keyword,
                "city": city,
                "phone": item.get("phone", "N/A"),
                "email": item.get("email", "N/A"),
                "website": item.get("website", "N/A"),
                "address": item.get("address", "N/A"),
                "rating": item.get("totalScore", 0),
                "reviewCount": item.get("reviewsCount", 0),
                "googleMapsUrl": item.get("url", "N/A"),
                "category": item.get("categoryName", "N/A")
            }
            results.append(lead)
            Actor.log.info(f"Found: {lead['name']}")
        
        # Push to dataset
        await Actor.push_data(results)
        Actor.log.info(f"=== Successfully generated {len(results)} real leads ===")

if __name__ == '__main__':
    asyncio.run(main())
