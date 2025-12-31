# 🤖 ClientScout AI  
### “AI that finds your next client.”

**ClientScout AI** is an AI-powered, multi-sector business lead generator that discovers **real businesses** using intelligent search queries and Google Maps data.  
Built for **agencies, sales teams, founders, and BPOs** who need accurate, location-based leads at scale.

---

## 🌟 Key Features

- **🧠 AI-Powered Search Intelligence**  
  Uses large language models to generate highly relevant Google Maps search queries.

- **🌍 Multi-Sector Coverage**  
  Works across **24+ industries** including Healthcare, Real Estate, IT, Finance, Manufacturing, and more.

- **📍 Location-Aware Discovery**  
  Supports city, state, postcode/ZIP, and country-based searches — or global discovery.

- **🔍 Real Business Data**  
  Collects verified business information directly from Google Maps:
  - Business name
  - Address
  - Phone number
  - Website
  - Ratings & reviews
  - Google Maps URL

- **✨ Smart Deduplication**  
  Automatically removes duplicate businesses across searches.

- **💰 Cost-Safe Execution**  
  Built-in safeguards to prevent runaway crawls and unexpected credit usage.

---

## 📋 Input Parameters

### Required
- **sector**  
  Select one of the supported industry sectors.

### Optional
- **country** – Any country (e.g., India, Australia, USA)
- **state / province**
- **city / suburb**
- **postcode / ZIP**
- **keyword** – Refine searches (e.g., *Dermatologist*, *AI Startup*)
- **maxResults** – Maximum number of leads (default: 10)

---

## 🚀 How ClientScout AI Works

### 1️⃣ AI Query Generation
ClientScout AI intelligently generates 3–5 Google Maps search queries based on your sector and location.

Example:
> Sector: Healthcare  
> Keyword: Dermatologist  
> Location: Chennai

AI may generate:
- dermatologists
- skin clinics
- cosmetic dermatology
- dermatology specialists

---

### 2️⃣ Google Maps Discovery
Each query searches Google Maps and extracts **real, verified businesses**, not scraped lists or outdated databases.

---

### 3️⃣ Smart Limiting & Deduplication
- Crawling stops once the requested number of leads is collected
- Duplicate businesses are removed automatically

---

## 📊 Example Usage

### Example 1 — Local Lead Generation
```json
{
  "sector": "Healthcare",
  "city": "Chennai",
  "keyword": "Dermatologist",
  "maxResults": 10
}
```

### Example 2 — Country-Level Discovery
```json
{
  "sector": "IT & Technology",
  "country": "India",
  "keyword": "AI Startups",
  "maxResults": 20
}
```

### Example 3 — Precise Location Search
```json
{
  "sector": "Real Estate",
  "city": "Mumbai",
  "postcode": "400001",
  "keyword": "Luxury Apartments"
}
```

---

## 📤 Output Data

Each lead includes:

```json
{
  "name": "Apollo Skin Clinic",
  "phone": "+91 44 1234 5678",
  "website": "https://apolloskin.com",
  "address": "Chennai, Tamil Nadu, India",
  "rating": 4.5,
  "reviewCount": 234,
  "category": "Dermatology clinic",
  "googleMapsUrl": "https://maps.google.com/...",
  "searchQuery": "dermatologists"
}
```

---

## 🎯 Supported Sectors

Healthcare, Real Estate, Manufacturing, IT & Technology, Education & Training, Legal Services, Financial Services, Hospitality & Tourism, Retail & E-commerce, Food & Beverage, Construction, Automotive, Marketing & Advertising, Consulting, Logistics & Transportation, Beauty & Wellness, Entertainment & Media, Agriculture, Energy & Utilities, Telecommunications, Insurance, Professional Services, Non-Profit & NGO, Sports & Fitness.

---

## ⚠️ Usage & Cost Notes

- Google Maps crawling consumes Apify credits
- ClientScout AI includes **hard limits and safety guards**
- Global searches are more expensive than country-restricted searches
- For best cost control, always specify **country or city**

---

### 🚀 ClientScout AI  
**“AI that finds your next client.”**
