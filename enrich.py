"""
Enrichment pipeline - takes scraped properties and adds tax/sale history from detail pages.
"""

import json
import os
import time
from datetime import datetime
from firecrawl import Firecrawl

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "fc-9568f99ebb0f4ae1bd5d7a513da27a94")
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
fc = Firecrawl(api_key=FIRECRAWL_API_KEY)

DETAIL_SCHEMA = {
    "type": "object",
    "properties": {
        "address": {"type": "string"},
        "price": {"type": "number"},
        "bedrooms": {"type": "number"},
        "bathrooms": {"type": "number"},
        "square_footage": {"type": "number"},
        "year_built": {"type": "number"},
        "lot_size": {"type": "string"},
        "property_type": {"type": "string"},
        "hoa_dues": {"type": "number"},
        "tax_assessed_value": {"type": "number"},
        "tax_history": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "year": {"type": "number"},
                    "tax_amount": {"type": "number"},
                    "assessed_value": {"type": "number"}
                }
            }
        },
        "sale_history": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string"},
                    "price": {"type": "number"},
                    "event": {"type": "string"}
                }
            }
        },
        "walk_score": {"type": "number"},
        "neighborhood": {"type": "string"}
    }
}


def build_redfin_url(address: str, city: str = "Tacoma", state: str = "WA") -> str:
    """Build a Redfin search URL from an address."""
    clean = address.strip().replace(" ", "-").replace("#", "unit-").replace(",", "")
    return f"https://www.redfin.com/{state}/{city}/{clean}"


def enrich_property(prop: dict) -> dict:
    """Enrich a single property with detail page data."""
    address = prop.get("address", "")
    city = prop.get("city", "Tacoma")
    state = prop.get("state", "WA")
    zip_code = prop.get("zip_code", "")

    # Build search query for Firecrawl
    search_query = f"{address} {city} {state} {zip_code} property details redfin"

    try:
        # Use search to find the property detail page
        results = fc.search(search_query, limit=1)
        if results and hasattr(results, 'data') and results.data:
            detail_url = results.data[0].url if hasattr(results.data[0], 'url') else None

            if detail_url and 'redfin.com' in detail_url:
                # Extract detailed data
                extract_result = fc.extract(
                    urls=[detail_url],
                    prompt=f"Extract complete property details for {address}. Include: "
                           f"tax assessed value, full tax history, complete sale history with dates and prices, "
                           f"year built, lot size, HOA dues, walk score, neighborhood.",
                    schema=DETAIL_SCHEMA,
                    timeout=120
                )

                if extract_result and hasattr(extract_result, 'data') and extract_result.data:
                    detail = extract_result.data

                    # Merge enrichment data
                    if detail.get("tax_assessed_value"):
                        prop["assessed_value"] = detail["tax_assessed_value"]
                    if detail.get("tax_history"):
                        prop["tax_history"] = detail["tax_history"]
                    if detail.get("sale_history"):
                        prop["sale_history"] = detail["sale_history"]
                        # Find original purchase
                        sales = [s for s in detail["sale_history"]
                                if s.get("price") and "sold" in (s.get("event", "") or "").lower()]
                        if len(sales) >= 2:
                            prop["purchase_price"] = sales[-1]["price"]
                            prop["purchase_date"] = sales[-1]["date"]
                    if detail.get("year_built"):
                        prop["year_built"] = detail["year_built"]
                    if detail.get("lot_size"):
                        prop["lot_size"] = detail["lot_size"]
                    if detail.get("hoa_dues"):
                        prop["hoa_dues"] = detail["hoa_dues"]
                    if detail.get("walk_score"):
                        prop["walk_score"] = detail["walk_score"]
                    if detail.get("neighborhood"):
                        prop["neighborhood"] = detail["neighborhood"]

                    prop["enriched"] = True
                    print(f"  [OK] {address} - assessed: ${detail.get('tax_assessed_value', 0):,.0f}")
                    return prop

        prop["enriched"] = False
        print(f"  [--] {address} - no detail page found")
        return prop

    except Exception as e:
        prop["enriched"] = False
        print(f"  [ERR] {address} - {e}")
        return prop


def recalculate_scores(properties: list) -> list:
    """Recalculate opportunity scores with enriched data."""
    current_year = datetime.now().year

    for prop in properties:
        score = 0
        reasons = []

        # Years owned from sale history
        sale_history = prop.get("sale_history", [])
        sales = [s for s in sale_history if s.get("price") and "sold" in (s.get("event", "") or "").lower()]

        purchase_price = prop.get("purchase_price", 0) or 0
        purchase_date = prop.get("purchase_date", "")
        assessed_value = prop.get("assessed_value", 0) or 0
        price = prop.get("price", 0) or 0

        years_owned = None
        if purchase_date:
            try:
                for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
                    try:
                        parsed = datetime.strptime(purchase_date.strip()[:10], fmt)
                        years_owned = current_year - parsed.year
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        # Scoring
        if years_owned and years_owned >= 15:
            score += 30
            reasons.append(f"Long-term owner ({years_owned}yr)")
        elif years_owned and years_owned >= 10:
            score += 20
            reasons.append(f"Owner {years_owned}yr")
        elif years_owned and years_owned >= 5:
            score += 10
            reasons.append(f"Owner {years_owned}yr")

        # Equity
        if purchase_price and assessed_value:
            equity = assessed_value - purchase_price
            equity_pct = (equity / assessed_value * 100) if assessed_value > 0 else 0
            if equity_pct >= 50:
                score += 25
                reasons.append(f"High equity ({equity_pct:.0f}%)")
            elif equity_pct >= 25:
                score += 15
                reasons.append(f"Equity {equity_pct:.0f}%")
            prop["estimated_equity"] = equity
            prop["equity_percentage"] = round(equity_pct, 1)

        # Value appreciation
        if purchase_price and price and price > purchase_price * 1.5:
            score += 10
            reasons.append("Significant appreciation")

        # Property type
        if prop.get("property_type") in ["House", "Single Family", "Multi-Family"]:
            score += 5
            reasons.append(prop["property_type"])

        # Data quality bonus
        if prop.get("enriched"):
            score += 10
            reasons.append("Verified data")

        if assessed_value:
            reasons.append(f"Assessed ${assessed_value:,.0f}")

        prop["opportunity_score"] = min(score, 100)
        prop["opportunity_reasons"] = "; ".join(reasons) if reasons else "Low signal"
        prop["years_owned"] = years_owned

    properties.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    return properties


def run_enrichment(max_properties: int = 15):
    """Load scraped data, enrich top properties, recalculate scores."""
    input_path = os.path.join(OUTPUT_DIR, "off_market_properties.json")

    with open(input_path) as f:
        data = json.load(f)

    properties = data.get("properties", [])
    print(f"Loaded {len(properties)} properties")
    print(f"Enriching top {max_properties}...\n")

    # Enrich the top N by current score + all houses
    houses = [p for p in properties if p.get("property_type") in ["House", "Single Family", None, ""]]
    condos = [p for p in properties if p not in houses]
    to_enrich = (houses + condos)[:max_properties]

    enriched_count = 0
    for i, prop in enumerate(to_enrich):
        print(f"[{i+1}/{len(to_enrich)}]", end="")
        enrich_property(prop)
        if prop.get("enriched"):
            enriched_count += 1
        time.sleep(4)  # Rate limit

    # Recalculate all scores
    properties = recalculate_scores(properties)

    # Save
    data["properties"] = properties
    data["generated"] = datetime.now().isoformat()
    data["enriched_count"] = enriched_count
    data["source"] = data.get("source", "") + f" | Enriched {enriched_count}/{len(to_enrich)}"

    with open(input_path, "w") as f:
        json.dump(data, f, indent=2)

    demo_path = os.path.join(OUTPUT_DIR, "off_market_properties_demo.json")
    with open(demo_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Enriched {enriched_count}/{len(to_enrich)} properties")
    print(f"Top scored properties:")
    for p in properties[:10]:
        enriched_flag = "+" if p.get("enriched") else " "
        print(f"  [{p['opportunity_score']:>3}]{enriched_flag} {p.get('address', '?'):40s} | ${p.get('price', 0):>12,.0f}")

    return properties


if __name__ == "__main__":
    run_enrichment(15)
