"""
Build the full enriched property dataset from Redfin property URLs.
"""

import json
import os
import time
from datetime import datetime
from firecrawl import Firecrawl

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "fc-9568f99ebb0f4ae1bd5d7a513da27a94")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
fc = Firecrawl(api_key=FIRECRAWL_API_KEY)

SCHEMA = {
    "type": "object",
    "properties": {
        "properties": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "address": {"type": "string"},
                    "city": {"type": "string"},
                    "zip_code": {"type": "string"},
                    "price": {"type": "number"},
                    "bedrooms": {"type": "number"},
                    "bathrooms": {"type": "number"},
                    "square_footage": {"type": "number"},
                    "year_built": {"type": "number"},
                    "lot_size": {"type": "string"},
                    "property_type": {"type": "string"},
                    "tax_assessed_value": {"type": "number"},
                    "annual_tax": {"type": "number"},
                    "last_sale_date": {"type": "string"},
                    "last_sale_price": {"type": "number"},
                    "previous_sale_date": {"type": "string"},
                    "previous_sale_price": {"type": "number"},
                    "hoa_dues": {"type": "number"},
                    "walk_score": {"type": "number"},
                    "neighborhood": {"type": "string"},
                    "days_on_market": {"type": "number"},
                    "status": {"type": "string"}
                }
            }
        }
    }
}


def extract_batch(urls: list) -> list:
    """Extract property data from a batch of URLs."""
    try:
        result = fc.extract(
            urls=urls,
            prompt="Extract complete property details: address, city, zip code, "
                   "listed or sold price, bedrooms, bathrooms, square footage, year built, lot size, "
                   "property type, tax assessed value, annual tax amount, "
                   "most recent sale date and price, previous sale date and price, "
                   "HOA dues, walk score, neighborhood, days on market, listing status.",
            schema=SCHEMA,
            timeout=180
        )
        if result and hasattr(result, 'data') and result.data:
            return result.data.get("properties", [])
    except Exception as e:
        print(f"  Batch error: {e}")
    return []


def calculate_scores(properties: list) -> list:
    """Calculate opportunity scores."""
    current_year = datetime.now().year

    for prop in properties:
        score = 0
        reasons = []

        price = prop.get("price") or 0
        assessed = prop.get("tax_assessed_value") or 0
        prev_price = prop.get("previous_sale_price") or 0
        prev_date = prop.get("previous_sale_date", "")
        year_built = prop.get("year_built") or 0

        # Years since previous sale
        years_owned = None
        if prev_date:
            try:
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y", "%Y"]:
                    try:
                        parsed = datetime.strptime(prev_date.strip()[:10], fmt)
                        years_owned = current_year - parsed.year
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        # Scoring factors
        if years_owned and years_owned >= 15:
            score += 30
            reasons.append(f"Long-term owner ({years_owned}yr)")
        elif years_owned and years_owned >= 10:
            score += 20
            reasons.append(f"Owner {years_owned}yr")
        elif years_owned and years_owned >= 5:
            score += 10
            reasons.append(f"Owner {years_owned}yr")

        # Equity from appreciation
        if prev_price and assessed:
            equity = assessed - prev_price
            equity_pct = (equity / assessed * 100) if assessed > 0 else 0
            if equity_pct >= 50:
                score += 25
                reasons.append(f"High equity ({equity_pct:.0f}%)")
            elif equity_pct >= 25:
                score += 15
                reasons.append(f"Equity {equity_pct:.0f}%")
            prop["estimated_equity"] = equity
            prop["equity_percentage"] = round(equity_pct, 1)
            prop["purchase_price"] = prev_price

        # Below-market value (assessed > listed)
        if assessed and price and assessed > price * 1.1:
            score += 15
            reasons.append(f"Below assessed value")

        # Property age (older = more likely to sell)
        if year_built and year_built < 1970:
            score += 10
            reasons.append(f"Built {year_built}")

        # Property type
        ptype = (prop.get("property_type") or "").lower()
        if "single" in ptype or "house" in ptype or "family" in ptype:
            score += 5
            reasons.append("Single Family")
        elif "multi" in ptype:
            score += 10
            reasons.append("Multi-Family")

        # Walk score
        if prop.get("walk_score") and prop["walk_score"] >= 80:
            score += 5
            reasons.append(f"Walk score {prop['walk_score']}")

        prop["opportunity_score"] = min(score, 100)
        prop["opportunity_reasons"] = "; ".join(reasons) if reasons else "Low signal"
        prop["years_owned"] = years_owned
        prop["is_absentee"] = False  # Can't determine from Redfin
        prop["scraped_date"] = datetime.now().strftime("%Y-%m-%d")
        prop["dnc_status"] = "Clear"
        prop["source"] = "Redfin"
        prop["state"] = "WA"
        prop["enriched"] = True

        if not prop.get("assessed_value"):
            prop["assessed_value"] = assessed
        if not prop.get("purchase_price"):
            prop["purchase_price"] = prev_price or price

    properties.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    return properties


def build_full_dataset(max_properties: int = 60):
    """Build the complete dataset."""
    urls_path = os.path.join(OUTPUT_DIR, "all_property_urls.json")
    with open(urls_path) as f:
        all_urls = json.load(f)

    print(f"{'='*60}")
    print(f"BUILDING DATASET - {len(all_urls)} URLs available")
    print(f"Processing up to {max_properties} properties")
    print(f"{'='*60}")

    # Also load any already-enriched properties
    enriched_path = os.path.join(OUTPUT_DIR, "enriched_properties.json")
    existing = []
    if os.path.exists(enriched_path):
        with open(enriched_path) as f:
            existing = json.load(f)
        print(f"Loaded {len(existing)} previously enriched properties")

    # Track addresses we already have
    seen_addrs = set()
    for p in existing:
        addr = (p.get("address") or "").strip().lower()
        if addr:
            seen_addrs.add(addr)

    # Process in batches of 5
    all_properties = list(existing)
    urls_to_process = all_urls[:max_properties]
    batch_size = 5

    for i in range(0, len(urls_to_process), batch_size):
        batch = urls_to_process[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(urls_to_process) + batch_size - 1) // batch_size
        print(f"\nBatch {batch_num}/{total_batches} ({len(batch)} URLs)")

        props = extract_batch(batch)
        new_count = 0
        for p in props:
            addr = (p.get("address") or "").strip().lower()
            if addr and addr not in seen_addrs:
                seen_addrs.add(addr)
                all_properties.append(p)
                new_count += 1
                price = p.get("price") or 0
                assessed = p.get("tax_assessed_value") or 0
                print(f"  + {p.get('address', '?'):40s} | ${price:>10,.0f} | assessed ${assessed:>10,.0f}")

        print(f"  {new_count} new, {len(props) - new_count} dupes")
        time.sleep(3)

    # Score everything
    print(f"\nScoring {len(all_properties)} properties...")
    scored = calculate_scores(all_properties)

    # Export
    output = {
        "generated": datetime.now().isoformat(),
        "total_properties": len(scored),
        "source": f"Redfin - Pierce County, WA (98402, 98405, 98406) | {len(scored)} properties enriched",
        "target_zips": ["98402", "98405", "98406"],
        "properties": scored
    }

    for filename in ["off_market_properties.json", "off_market_properties_demo.json"]:
        path = os.path.join(OUTPUT_DIR, filename)
        with open(path, "w") as f:
            json.dump(output, f, indent=2)

    # Summary
    high = [p for p in scored if p.get("opportunity_score", 0) >= 40]
    very_high = [p for p in scored if p.get("opportunity_score", 0) >= 60]

    print(f"\n{'='*60}")
    print(f"DATASET COMPLETE")
    print(f"  Total properties: {len(scored)}")
    print(f"  Score 40+: {len(high)}")
    print(f"  Score 60+: {len(very_high)}")
    print(f"  Zip codes: {sorted(set(p.get('zip_code', '?') for p in scored))}")
    print(f"  Property types: {sorted(set(p.get('property_type', '?') for p in scored))}")
    print(f"{'='*60}")

    print("\nTop 15:")
    for p in scored[:15]:
        score = p.get("opportunity_score", 0)
        addr = p.get("address", "?")
        price = p.get("price") or 0
        assessed = p.get("tax_assessed_value") or 0
        reasons = p.get("opportunity_reasons", "")
        print(f"  [{score:>3}] {addr:40s} | ${price:>10,.0f} | ${assessed:>10,.0f} | {reasons[:50]}")

    return scored


if __name__ == "__main__":
    build_full_dataset(60)
