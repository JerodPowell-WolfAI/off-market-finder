"""
Off-Market Property Finder v2 - Wolf Intelligence
Real data scraper using Firecrawl extract + scrape APIs.
Targets Pierce County (Tacoma, WA) public records and listing sites.
"""

import os
import json
import time
from datetime import datetime
from firecrawl import Firecrawl

# Config
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "fc-9568f99ebb0f4ae1bd5d7a513da27a94")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

fc = Firecrawl(api_key=FIRECRAWL_API_KEY)

PROPERTY_SCHEMA = {
    "type": "object",
    "properties": {
        "properties": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "address": {"type": "string", "description": "Full street address"},
                    "city": {"type": "string"},
                    "state": {"type": "string"},
                    "zip_code": {"type": "string"},
                    "price": {"type": "number", "description": "Listed price, sold price, or assessed value"},
                    "bedrooms": {"type": "number"},
                    "bathrooms": {"type": "number"},
                    "square_footage": {"type": "number"},
                    "property_type": {"type": "string", "description": "House, Condo, Townhouse, Multi-Family, etc."},
                    "year_built": {"type": "number"},
                    "lot_size": {"type": "string"},
                    "sold_date": {"type": "string", "description": "Date sold if recently sold"},
                    "days_on_market": {"type": "number"},
                    "status": {"type": "string", "description": "Sold, For Sale, FSBO, Pending, etc."},
                    "owner_name": {"type": "string", "description": "Owner name if available from public records"},
                    "parcel_number": {"type": "string", "description": "Tax parcel or APN number if available"},
                }
            }
        }
    }
}


def scrape_redfin_sold(zip_code: str) -> list:
    """Scrape recently sold properties from Redfin for a zip code."""
    print(f"\n--- Redfin Recently Sold: {zip_code} ---")
    url = f"https://www.redfin.com/zipcode/{zip_code}/filter/include=sold-3mo"

    try:
        # First get markdown to see what's there
        result = fc.scrape(url, formats=["markdown"], timeout=60000)
        if not result or not result.markdown:
            print("  No data returned")
            return []

        md = result.markdown
        print(f"  Got {len(md)} chars of page content")

        # Now extract structured data
        extract_result = fc.extract(
            urls=[url],
            prompt=f"Extract ALL recently sold property listings in zip code {zip_code}, Tacoma WA area. "
                   f"For each property get: full street address, city, state, zip code, sold price, "
                   f"bedrooms, bathrooms, square footage, property type (House/Condo/Townhouse), "
                   f"sold date, and days on market. Include ALL properties visible.",
            schema=PROPERTY_SCHEMA,
            timeout=120
        )

        if extract_result and hasattr(extract_result, 'data') and extract_result.data:
            props = extract_result.data.get("properties", [])
            for p in props:
                p["source"] = "Redfin"
                p["status"] = "Recently Sold"
                if not p.get("zip_code"):
                    p["zip_code"] = zip_code
                if not p.get("city"):
                    p["city"] = "Tacoma"
                if not p.get("state"):
                    p["state"] = "WA"
            print(f"  Extracted {len(props)} properties")
            return props
        else:
            print("  Extract returned no data")
            return []

    except Exception as e:
        print(f"  Error: {e}")
        return []


def scrape_redfin_fsbo(zip_code: str) -> list:
    """Scrape FSBO / For Sale properties from Redfin."""
    print(f"\n--- Redfin For Sale: {zip_code} ---")
    url = f"https://www.redfin.com/zipcode/{zip_code}"

    try:
        extract_result = fc.extract(
            urls=[url],
            prompt=f"Extract ALL property listings for sale in zip code {zip_code}. "
                   f"For each property get: full street address, city, state, zip code, listed price, "
                   f"bedrooms, bathrooms, square footage, property type, days on market, status.",
            schema=PROPERTY_SCHEMA,
            timeout=120
        )

        if extract_result and hasattr(extract_result, 'data') and extract_result.data:
            props = extract_result.data.get("properties", [])
            for p in props:
                p["source"] = "Redfin"
                p["status"] = p.get("status", "For Sale")
                if not p.get("zip_code"):
                    p["zip_code"] = zip_code
                if not p.get("city"):
                    p["city"] = "Tacoma"
                if not p.get("state"):
                    p["state"] = "WA"
            print(f"  Extracted {len(props)} properties")
            return props
        else:
            print("  No data")
            return []

    except Exception as e:
        print(f"  Error: {e}")
        return []


def scrape_pierce_county_assessor(parcel_search: str = None) -> list:
    """Scrape Pierce County Assessor public records."""
    print(f"\n--- Pierce County Assessor ---")

    # The assessor search page
    url = "https://epip.co.pierce.wa.us/CFApps/atr/ePIP/search.cfm"

    try:
        extract_result = fc.extract(
            urls=[url],
            prompt="Extract all property records visible on this Pierce County Assessor page. "
                   "For each property get: address, city, zip code, owner name, parcel number, "
                   "assessed value (total), land value, improvement value, property type, "
                   "year built, square footage, bedrooms, bathrooms, lot size, "
                   "and last sale date and price if shown.",
            schema=PROPERTY_SCHEMA,
            timeout=120
        )

        if extract_result and hasattr(extract_result, 'data') and extract_result.data:
            props = extract_result.data.get("properties", [])
            for p in props:
                p["source"] = "Pierce County Assessor"
                if not p.get("state"):
                    p["state"] = "WA"
            print(f"  Extracted {len(props)} properties")
            return props
        else:
            print("  No data (assessor search pages often need interaction)")
            return []

    except Exception as e:
        print(f"  Error: {e}")
        return []


def scrape_realtor_com(zip_code: str) -> list:
    """Scrape realtor.com listings."""
    print(f"\n--- Realtor.com: {zip_code} ---")
    url = f"https://www.realtor.com/realestateandhomes-search/{zip_code}"

    try:
        extract_result = fc.extract(
            urls=[url],
            prompt=f"Extract ALL property listings in zip code {zip_code}. "
                   f"For each: address, city, state, zip code, price, bedrooms, bathrooms, "
                   f"square footage, property type, lot size, and listing status.",
            schema=PROPERTY_SCHEMA,
            timeout=120
        )

        if extract_result and hasattr(extract_result, 'data') and extract_result.data:
            props = extract_result.data.get("properties", [])
            for p in props:
                p["source"] = "Realtor.com"
                if not p.get("zip_code"):
                    p["zip_code"] = zip_code
                if not p.get("state"):
                    p["state"] = "WA"
            print(f"  Extracted {len(props)} properties")
            return props
        else:
            print("  No data")
            return []

    except Exception as e:
        print(f"  Error: {e}")
        return []


def calculate_opportunity_score(properties: list) -> list:
    """Score each property for off-market opportunity potential."""
    current_year = datetime.now().year

    for prop in properties:
        score = 0
        reasons = []

        # Parse years owned from sold_date
        years_owned = None
        sold_date = prop.get("sold_date", "")
        if sold_date:
            try:
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"]:
                    try:
                        parsed = datetime.strptime(sold_date.strip()[:10], fmt)
                        years_owned = current_year - parsed.year
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        price = prop.get("price", 0) or 0

        # Scoring
        if years_owned and years_owned >= 15:
            score += 30
            reasons.append(f"Long-term owner ({years_owned}yr)")
        elif years_owned and years_owned >= 10:
            score += 20
            reasons.append(f"Owner {years_owned}yr")

        if prop.get("owner_name"):
            score += 10
            reasons.append("Owner identified")

        if prop.get("status") == "Recently Sold":
            score += 15
            reasons.append("Recently sold (comp data)")

        if price and price > 0:
            score += 10
            reasons.append(f"${price:,.0f}")

        if prop.get("property_type") in ["House", "Single Family", "Multi-Family"]:
            score += 5
            reasons.append(prop["property_type"])

        if prop.get("square_footage") and prop["square_footage"] > 1500:
            score += 5
            reasons.append(f"{prop['square_footage']:,} sqft")

        prop["opportunity_score"] = min(score, 100)
        prop["opportunity_reasons"] = "; ".join(reasons) if reasons else "Low signal"
        prop["years_owned"] = years_owned
        prop["scraped_date"] = datetime.now().strftime("%Y-%m-%d")

        # Placeholder fields for compatibility with dashboard
        if "is_absentee" not in prop:
            prop["is_absentee"] = False
        if "estimated_equity" not in prop:
            prop["estimated_equity"] = None
        if "equity_percentage" not in prop:
            prop["equity_percentage"] = None
        if "purchase_price" not in prop:
            prop["purchase_price"] = prop.get("price")
        if "assessed_value" not in prop:
            prop["assessed_value"] = prop.get("price")
        if "dnc_status" not in prop:
            prop["dnc_status"] = "Clear"

    properties.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    return properties


def run_full_scrape(zip_codes: list = None):
    """Run the full scraping pipeline."""
    if zip_codes is None:
        zip_codes = ["98402", "98405", "98406"]

    print("=" * 60)
    print("OFF-MARKET PROPERTY FINDER v2 - Wolf Intelligence")
    print(f"Target: {', '.join(zip_codes)}")
    print(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    all_properties = []

    for zip_code in zip_codes:
        # Redfin recently sold (best source)
        props = scrape_redfin_sold(zip_code)
        all_properties.extend(props)
        time.sleep(3)

        # Redfin for sale
        props = scrape_redfin_fsbo(zip_code)
        all_properties.extend(props)
        time.sleep(3)

        # Realtor.com
        props = scrape_realtor_com(zip_code)
        all_properties.extend(props)
        time.sleep(3)

    # Try Pierce County Assessor
    assessor_props = scrape_pierce_county_assessor()
    all_properties.extend(assessor_props)

    # Deduplicate by address
    seen = set()
    unique = []
    for p in all_properties:
        addr = (p.get("address", "") or "").strip().lower()
        if addr and addr not in seen:
            seen.add(addr)
            unique.append(p)

    print(f"\n--- Deduplication: {len(all_properties)} -> {len(unique)} unique ---")

    # Score opportunities
    scored = calculate_opportunity_score(unique)

    # Export
    output = {
        "generated": datetime.now().isoformat(),
        "total_properties": len(scored),
        "source": f"Live scrape - Pierce County, WA ({', '.join(zip_codes)})",
        "target_zips": zip_codes,
        "properties": scored
    }

    # Save as the main data file (dashboard reads this)
    json_path = os.path.join(OUTPUT_DIR, "off_market_properties.json")
    with open(json_path, "w") as f:
        json.dump(output, f, indent=2)

    # Also save demo file so dashboard picks it up
    demo_path = os.path.join(OUTPUT_DIR, "off_market_properties_demo.json")
    with open(demo_path, "w") as f:
        json.dump(output, f, indent=2)

    # Summary
    high = [p for p in scored if p.get("opportunity_score", 0) >= 40]
    print("\n" + "=" * 60)
    print("RESULTS")
    print(f"  Total unique properties: {len(scored)}")
    print(f"  Opportunity score 40+: {len(high)}")
    print(f"  Sources: {set(p.get('source', '?') for p in scored)}")
    print(f"  Output: {json_path}")
    print("=" * 60)

    for p in scored[:10]:
        print(f"  [{p['opportunity_score']:>3}] {p.get('address', '?'):40s} | ${p.get('price', 0):>12,.0f} | {p.get('source', '?')}")

    return scored


if __name__ == "__main__":
    run_full_scrape(["98402", "98405", "98406"])
