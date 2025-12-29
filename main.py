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
        
        # Define comprehensive keywords for each sector
        sector_keywords = {
            "Healthcare": "Doctors, Clinics, Hospitals, Medical Centers, Specialists, Dentists, Physiotherapy, Diagnostic Centers",
            "Real Estate": "Real Estate Agents, Property Developers, Realtors, Real Estate Companies, Property Consultants, Builders",
            "Manufacturing": "Manufacturing Companies, Factories, Industrial Units, Production Facilities, OEM Manufacturers",
            "IT & Technology": "IT Companies, Software Companies, Tech Startups, Web Development, App Development, IT Services, Cloud Services",
            "Education & Training": "Schools, Colleges, Universities, Training Centers, Coaching Classes, Online Education, Tutors",
            "Legal Services": "Lawyers, Law Firms, Legal Consultants, Attorneys, Advocates, Legal Advisors",
            "Financial Services": "Banks, Financial Advisors, Investment Firms, Accounting Firms, Tax Consultants, Financial Planners",
            "Hospitality & Tourism": "Hotels, Resorts, Travel Agencies, Tour Operators, Restaurants, Guest Houses, Holiday Packages",
            "Retail & E-commerce": "Retail Stores, Shopping Centers, Online Stores, E-commerce, Supermarkets, Outlets",
            "Food & Beverage": "Restaurants, Cafes, Food Delivery, Catering Services, Bakeries, Cloud Kitchens, Food Manufacturers",
            "Construction": "Construction Companies, Contractors, Builders, Civil Engineers, Architecture Firms, Interior Designers",
            "Automotive": "Car Dealers, Auto Repair, Car Service Centers, Vehicle Sales, Auto Parts, Garages",
            "Marketing & Advertising": "Marketing Agencies, Advertising Firms, Digital Marketing, SEO Services, Creative Agencies, PR Firms",
            "Consulting": "Business Consultants, Management Consulting, Strategy Consulting, HR Consultants, Advisory Services",
            "Logistics & Transportation": "Logistics Companies, Freight Forwarders, Courier Services, Transportation Services, Warehousing",
            "Beauty & Wellness": "Beauty Salons, Spas, Wellness Centers, Gyms, Yoga Studios, Beauty Parlors, Cosmetics",
            "Entertainment & Media": "Event Planners, Production Houses, Media Companies, Photography Studios, Entertainment Services",
            "Agriculture": "Agricultural Services, Farming Equipment, Agro Products, Organic Farming, Agricultural Consultants",
            "Energy & Utilities": "Solar Companies, Energy Consultants, Utility Services, Renewable Energy, Power Solutions",
            "Telecommunications": "Telecom Companies, Network Providers, Internet Services, Broadband Providers, Mobile Services",
            "Insurance": "Insurance Companies, Insurance Agents, Insurance Brokers, Life Insurance, Health Insurance",
            "Professional Services": "Business Services, Corporate Services, Document Services, Translation Services, Notary Services",
            "Non-Profit & NGO": "NGOs, Charitable Organizations, Non-Profit Organizations, Foundations, Social Services",
            "Sports & Fitness": "Fitness Centers, Sports Clubs, Personal Trainers, Sports Equipment, Martial Arts, Dance Studios"
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
