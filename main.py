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
        city = input_data.get("city", "").strip()
        postcode = input_data.get("postcode", "").strip()
        
        # Define default keywords for each sector
        sector_keywords = {
            "Healthcare": "Doctors, Clinics, Hospitals",
            "Real Estate": "Real Estate Agents, Property Developers",
            "Manufacturing": "Manufacturing Companies, Factories",
            "IT & Startups": "IT Companies, Software Companies, Startups",
            "Education": "Schools, Colleges, Training Centers"
        }
        
        # Use provided keyword, or default to sector keyword
        keyword = input_data.get("keyword", "").strip()
        if not keyword:
            keyword = sector_keywords.get(sector, sector)
        
        max_results = input_data.get("maxResults", 10)
        
        # Build location string intelligently
        if city and postcode:
            location = f"{city} {postcode}"
        elif city:
            location = city
        elif postcode:
            location = postcode
        else:
            location = ""
        
        # Build search query
        if location:
            search_query = f"{keyword} in {location}"
            Actor.log.info(f"Searching for: {keyword} in {location} (Sector: {sector})")
        else:
            search_query = keyword
            Actor.log.info(f"Searching for: {keyword} (Sector: {sector}, No location specified)")
        
        # Initialize Apify client with token from environment
        token = os.environ.get('APIFY_TOKEN')
        client = ApifyClient(token=token)
        
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
                "city": city if city else "N/A",
                "postcode": postcode if postcode else "N/A",
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
