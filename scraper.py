"""
Off-Market Property Finder - Wolf Intelligence
Scrapes county assessor public records to identify off-market property opportunities.
Uses Firecrawl for intelligent web scraping with structured data extraction.
"""

import os
import json
import csv
import time
from datetime import datetime, timedelta
from firecrawl import Firecrawl
import pandas as pd

# Config
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "fc-9568f99ebb0f4ae1bd5d7a513da27a94")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Initialize Firecrawl
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

# Property data schema for extraction
PROPERTY_SCHEMA = {
    "type": "object",
    "properties": {
        "properties": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "address": {"type": "string", "description": "Full property street address"},
                    "city": {"type": "string"},
                    "zip_code": {"type": "string"},
                    "owner_name": {"type": "string", "description": "Current property owner name"},
                    "mailing_address": {"type": "string", "description": "Owner mailing address if different from property"},
                    "purchase_date": {"type": "string", "description": "Date the current owner purchased the property"},
                    "purchase_price": {"type": "number", "description": "Price paid when purchased"},
                    "assessed_value": {"type": "number", "description": "Current assessed/appraised value"},
                    "land_value": {"type": "number", "description": "Assessed land value"},
                    "improvement_value": {"type": "number", "description": "Assessed improvement/building value"},
                    "property_type": {"type": "string", "description": "Residential, Commercial, Multi-family, etc."},
                    "bedrooms": {"type": "number"},
                    "bathrooms": {"type": "number"},
                    "square_footage": {"type": "number"},
                    "lot_size": {"type": "string"},
                    "year_built": {"type": "number"},
                    "parcel_number": {"type": "string", "description": "Tax parcel or APN number"},
                },
                "required": ["address", "owner_name"]
            }
        }
    },
    "required": ["properties"]
}


def scrape_county_assessor(url: str, prompt: str) -> list:
    """Scrape a county assessor page and extract structured property data."""
    print(f"  Scraping: {url}")
    try:
        result = firecrawl.scrape(
            url,
            formats=[{
                "type": "json",
                "prompt": prompt,
                "schema": PROPERTY_SCHEMA
            }],
            timeout=120000
        )

        if result and hasattr(result, 'json') and result.json:
            properties = result.json.get("properties", [])
            print(f"  Found {len(properties)} properties")
            return properties
        elif result and isinstance(result, dict):
            properties = result.get("json", {}).get("properties", [])
            print(f"  Found {len(properties)} properties")
            return properties
        else:
            print("  No structured data returned")
            return []
    except Exception as e:
        print(f"  Error scraping {url}: {e}")
        return []


def scrape_with_extract(urls: list, prompt: str) -> list:
    """Use Firecrawl extract endpoint for multi-URL extraction."""
    print(f"  Extracting from {len(urls)} URLs...")
    try:
        result = firecrawl.extract(
            urls=urls,
            prompt=prompt,
            schema=PROPERTY_SCHEMA
        )

        if result and hasattr(result, 'data'):
            properties = result.data.get("properties", [])
            print(f"  Extracted {len(properties)} properties")
            return properties
        elif result and isinstance(result, dict):
            properties = result.get("data", {}).get("properties", [])
            print(f"  Extracted {len(properties)} properties")
            return properties
        else:
            print("  No data extracted")
            return []
    except Exception as e:
        print(f"  Extract error: {e}")
        return []


def calculate_indicators(properties: list) -> list:
    """Add off-market indicators to each property."""
    current_year = datetime.now().year
    enriched = []

    for prop in properties:
        # Calculate years owned
        years_owned = None
        purchase_date = prop.get("purchase_date", "")
        if purchase_date:
            try:
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y", "%m-%d-%Y"]:
                    try:
                        pd_parsed = datetime.strptime(purchase_date.strip()[:10], fmt)
                        years_owned = current_year - pd_parsed.year
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        # Calculate estimated equity
        purchase_price = prop.get("purchase_price", 0) or 0
        assessed_value = prop.get("assessed_value", 0) or 0
        estimated_equity = assessed_value - purchase_price if purchase_price > 0 else None
        equity_percentage = (estimated_equity / assessed_value * 100) if estimated_equity and assessed_value > 0 else None

        # Detect absentee owner
        property_addr = (prop.get("address", "") or "").lower().strip()
        mailing_addr = (prop.get("mailing_address", "") or "").lower().strip()
        is_absentee = bool(mailing_addr and property_addr and mailing_addr != property_addr)

        # Score the opportunity (0-100)
        score = 0
        reasons = []

        if years_owned and years_owned >= 15:
            score += 30
            reasons.append(f"Long-term owner ({years_owned} years)")
        elif years_owned and years_owned >= 10:
            score += 20
            reasons.append(f"Owner {years_owned} years")

        if equity_percentage and equity_percentage >= 50:
            score += 25
            reasons.append(f"High equity ({equity_percentage:.0f}%)")

        if is_absentee:
            score += 25
            reasons.append("Absentee owner")

        if assessed_value and assessed_value > 0:
            score += 10
            reasons.append(f"Assessed at ${assessed_value:,.0f}")

        if purchase_price and purchase_price > 0 and assessed_value and assessed_value > purchase_price * 2:
            score += 10
            reasons.append("Value doubled since purchase")

        prop["years_owned"] = years_owned
        prop["estimated_equity"] = estimated_equity
        prop["equity_percentage"] = round(equity_percentage, 1) if equity_percentage else None
        prop["is_absentee"] = is_absentee
        prop["opportunity_score"] = min(score, 100)
        prop["opportunity_reasons"] = "; ".join(reasons) if reasons else "No strong indicators"
        prop["scraped_date"] = datetime.now().strftime("%Y-%m-%d")

        enriched.append(prop)

    # Sort by opportunity score descending
    enriched.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    return enriched


def export_to_csv(properties: list, filename: str = "off_market_properties.csv"):
    """Export properties to CSV."""
    filepath = os.path.join(OUTPUT_DIR, filename)

    if not properties:
        print("No properties to export.")
        return filepath

    columns = [
        "opportunity_score", "address", "city", "zip_code", "owner_name",
        "mailing_address", "is_absentee", "purchase_date", "years_owned",
        "purchase_price", "assessed_value", "estimated_equity", "equity_percentage",
        "property_type", "bedrooms", "bathrooms", "square_footage", "lot_size",
        "year_built", "parcel_number", "opportunity_reasons", "scraped_date"
    ]

    df = pd.DataFrame(properties)
    # Only include columns that exist
    available_cols = [c for c in columns if c in df.columns]
    df = df[available_cols]

    df.to_csv(filepath, index=False)
    print(f"\nExported {len(properties)} properties to {filepath}")
    return filepath


def export_to_json(properties: list, filename: str = "off_market_properties.json"):
    """Export properties to JSON for the dashboard."""
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w") as f:
        json.dump({
            "generated": datetime.now().isoformat(),
            "total_properties": len(properties),
            "properties": properties
        }, f, indent=2, default=str)

    print(f"Exported JSON to {filepath}")
    return filepath


# ============================================================
# County-specific scrapers
# ============================================================

def scrape_pierce_county(zip_codes: list = None):
    """Scrape Pierce County Assessor (Tacoma, WA area)."""
    print("\n=== Pierce County Assessor ===")

    # Pierce County property search
    base_url = "https://epip.co.pierce.wa.us/CFApps/atr/ePIP/search.cfm"

    all_properties = []

    prompt = (
        "Extract all property records visible on this page. "
        "For each property, get: the property address, city, zip code, "
        "owner name, mailing address, purchase/sale date, sale price, "
        "current assessed value (land + improvement), property type, "
        "bedrooms, bathrooms, square footage, lot size, year built, "
        "and parcel/tax account number. "
        "Return ALL properties visible on the page."
    )

    # Scrape the search results page
    properties = scrape_county_assessor(base_url, prompt)
    all_properties.extend(properties)

    return all_properties


def scrape_king_county(zip_codes: list = None):
    """Scrape King County Assessor (Seattle, WA area)."""
    print("\n=== King County Assessor ===")

    base_url = "https://blue.kingcounty.com/Assessor/eRealProperty/default.aspx"

    prompt = (
        "Extract all property records visible on this page. "
        "For each property, get: the property address, city, zip code, "
        "owner name, mailing address, purchase/sale date, sale price, "
        "current assessed value, property type, bedrooms, bathrooms, "
        "square footage, lot size, year built, and parcel number. "
        "Return ALL properties visible on the page."
    )

    properties = scrape_county_assessor(base_url, prompt)
    return properties


def scrape_zillow_fsbo(zip_code: str):
    """Scrape Zillow FSBO/coming soon listings for a zip code."""
    print(f"\n=== Zillow FSBO/Coming Soon - {zip_code} ===")

    url = f"https://www.zillow.com/homes/for_sale/{zip_code}_rb/?searchQueryState=%7B%22filterState%22%3A%7B%22fsbo%22%3A%7B%22value%22%3Atrue%7D%7D%7D"

    prompt = (
        "Extract all property listings visible on this page. "
        "For each listing get: address, city, zip code, price, "
        "property type, bedrooms, bathrooms, square footage, "
        "lot size, and any owner/agent information shown. "
        "These are For Sale By Owner listings."
    )

    properties = scrape_county_assessor(url, prompt)
    for p in properties:
        p["source"] = "Zillow FSBO"
    return properties


def scrape_redfin_sold(zip_code: str, months_back: int = 6):
    """Scrape recently sold properties to find expired/withdrawn listings."""
    print(f"\n=== Redfin Recently Sold - {zip_code} ===")

    url = f"https://www.redfin.com/zipcode/{zip_code}/filter/include=sold-{months_back}mo"

    prompt = (
        "Extract all recently sold property listings on this page. "
        "For each property get: address, city, zip code, sold price, "
        "sold date, original list price, days on market, "
        "property type, bedrooms, bathrooms, square footage, "
        "and lot size."
    )

    properties = scrape_county_assessor(url, prompt)
    for p in properties:
        p["source"] = "Redfin Sold"
    return properties


# ============================================================
# Main runner
# ============================================================

def run_demo(target_zips: list = None, sources: list = None):
    """
    Run the off-market property finder for demo.

    Args:
        target_zips: List of zip codes to search (default: Tacoma/Seattle area)
        sources: List of sources to scrape (default: all)
    """
    if target_zips is None:
        target_zips = ["98402", "98405", "98406"]  # Tacoma area

    if sources is None:
        sources = ["pierce_county", "zillow_fsbo"]

    print("=" * 60)
    print("OFF-MARKET PROPERTY FINDER - Wolf Intelligence")
    print(f"Target zip codes: {', '.join(target_zips)}")
    print(f"Sources: {', '.join(sources)}")
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    all_properties = []

    # Run scrapers based on config
    if "pierce_county" in sources:
        props = scrape_pierce_county(target_zips)
        all_properties.extend(props)

    if "king_county" in sources:
        props = scrape_king_county(target_zips)
        all_properties.extend(props)

    if "zillow_fsbo" in sources:
        for zip_code in target_zips:
            props = scrape_zillow_fsbo(zip_code)
            all_properties.extend(props)
            time.sleep(2)  # Rate limiting

    if "redfin_sold" in sources:
        for zip_code in target_zips:
            props = scrape_redfin_sold(zip_code)
            all_properties.extend(props)
            time.sleep(2)

    # Enrich with indicators
    print(f"\n--- Processing {len(all_properties)} total properties ---")
    enriched = calculate_indicators(all_properties)

    # Export
    csv_path = export_to_csv(enriched)
    json_path = export_to_json(enriched)

    # Summary
    high_score = [p for p in enriched if p.get("opportunity_score", 0) >= 50]
    absentee = [p for p in enriched if p.get("is_absentee")]
    long_term = [p for p in enriched if (p.get("years_owned") or 0) >= 10]

    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print(f"  Total properties found: {len(enriched)}")
    print(f"  High opportunity (score 50+): {len(high_score)}")
    print(f"  Absentee owners: {len(absentee)}")
    print(f"  Long-term owners (10+ years): {len(long_term)}")
    print(f"\n  CSV: {csv_path}")
    print(f"  JSON: {json_path}")
    print("=" * 60)

    return enriched


if __name__ == "__main__":
    # Demo run with Tacoma-area zip codes
    results = run_demo(
        target_zips=["98402", "98405", "98406"],
        sources=["pierce_county", "zillow_fsbo"]
    )
