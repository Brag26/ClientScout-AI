from apify import Actor
from apify_client import ApifyClient
import asyncio
import os

async def main():
    async with Actor:
        Actor.log.info("=== Starting Lead Generator ===")
        
        # Get input
        input_data = await Actor.get_input() or {}
        sector = input_data.get("sector", "Healthcare")
        city = input_data.get("city", "Mumbai")
        postcode = input_data.get("postcode", "")
        keyword = input_data.get("keyword", sector)
        max_results = input_data.get("maxResults", 10)
        
        # Build location string with optional postcode
        location = f"{city} {postcode}".strip() if postcode else city
        
        Actor.log.info(f"Searching for: {keyword} in {location}")
        
        # Initialize Apify client with token from environment
        token = os.environ.get('APIFY_TOKEN')
        client = ApifyClient(token=token)
        
        # Prepare search query for Google Maps
        search_query = f"{keyword} in {location}"
        
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
                "postcode": postcode,
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
