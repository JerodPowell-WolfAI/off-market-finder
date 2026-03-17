"""
Generate realistic demo data for the Off-Market Property Finder.
Use this as backup if live scraping fails during the Friday demo.
"""

import json
import random
from datetime import datetime, timedelta

# Realistic Tacoma/Seattle area data
STREETS = [
    "N Alder St", "S Pine St", "E Division Ln", "Yakima Ave", "Pacific Ave",
    "S Tacoma Way", "6th Ave", "N 21st St", "S 56th St", "Orchard St",
    "N Stevens St", "S Sheridan Ave", "E 34th St", "N Proctor St", "S Union Ave",
    "Bridgeport Way", "Canyon Rd", "Steilacoom Blvd", "S 72nd St", "N Pearl St",
    "Meridian Ave", "S Alaska St", "E Portland Ave", "N Vassault St", "38th Ave"
]

FIRST_NAMES = ["Robert", "Linda", "James", "Patricia", "Michael", "Barbara", "William",
               "Elizabeth", "David", "Jennifer", "Richard", "Maria", "Thomas", "Susan",
               "Charles", "Dorothy", "Joseph", "Margaret", "Daniel", "Sandra"]

LAST_NAMES = ["Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson"]

CITIES = ["Tacoma", "Lakewood", "University Place", "Fircrest", "Ruston"]

PROPERTY_TYPES = ["Single Family", "Single Family", "Single Family", "Multi-Family",
                  "Townhouse", "Condo", "Single Family", "Single Family"]


def generate_property(idx):
    street_num = random.randint(100, 9999)
    street = random.choice(STREETS)
    city = random.choice(CITIES)
    zip_code = random.choice(["98402", "98405", "98406", "98407", "98408", "98498", "98499"])

    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    owner_name = f"{first} {last}"

    # Purchase date - bias toward long-term ownership for demo
    years_ago = random.choices(
        [random.randint(2, 5), random.randint(6, 10), random.randint(11, 20), random.randint(21, 35)],
        weights=[15, 25, 35, 25]
    )[0]
    purchase_date = datetime.now() - timedelta(days=years_ago * 365 + random.randint(0, 365))

    # Purchase price based on era
    if years_ago > 20:
        purchase_price = random.randint(80000, 180000)
    elif years_ago > 10:
        purchase_price = random.randint(150000, 350000)
    elif years_ago > 5:
        purchase_price = random.randint(250000, 450000)
    else:
        purchase_price = random.randint(350000, 600000)

    # Current assessed value (WA market appreciation)
    appreciation = 1 + (years_ago * random.uniform(0.03, 0.07))
    assessed_value = int(purchase_price * appreciation)
    land_value = int(assessed_value * random.uniform(0.3, 0.5))
    improvement_value = assessed_value - land_value

    # Absentee owner (30% chance)
    is_absentee = random.random() < 0.3
    if is_absentee:
        mailing_city = random.choice(["Phoenix, AZ", "Las Vegas, NV", "Portland, OR",
                                       "San Diego, CA", "Denver, CO", "Boise, ID"])
        mailing_address = f"{random.randint(100, 9999)} {random.choice(['Oak Dr', 'Sunset Blvd', 'Main St', 'Park Ave'])}, {mailing_city}"
    else:
        mailing_address = f"{street_num} {street}, {city}, WA {zip_code}"

    prop_type = random.choice(PROPERTY_TYPES)
    bedrooms = random.choice([2, 3, 3, 3, 4, 4, 5]) if "Family" in prop_type else random.choice([1, 2, 2, 3])
    bathrooms = random.choice([1, 1.5, 2, 2, 2.5, 3])
    sqft = random.randint(900, 3200) if "Family" in prop_type else random.randint(600, 1800)
    year_built = random.randint(1945, 2015)
    lot_size = f"{random.uniform(0.1, 0.5):.2f} acres"

    prop = {
        "address": f"{street_num} {street}",
        "city": city,
        "zip_code": zip_code,
        "owner_name": owner_name,
        "mailing_address": mailing_address,
        "purchase_date": purchase_date.strftime("%Y-%m-%d"),
        "purchase_price": purchase_price,
        "assessed_value": assessed_value,
        "land_value": land_value,
        "improvement_value": improvement_value,
        "property_type": prop_type,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "square_footage": sqft,
        "lot_size": lot_size,
        "year_built": year_built,
        "parcel_number": f"{random.randint(1000, 9999)}{random.randint(100, 999)}{random.randint(1000, 9999)}",
    }

    # Calculate indicators
    years_owned = years_ago
    estimated_equity = assessed_value - purchase_price
    equity_pct = (estimated_equity / assessed_value * 100) if assessed_value > 0 else 0

    score = 0
    reasons = []
    if years_owned >= 15:
        score += 30
        reasons.append(f"Long-term owner ({years_owned} years)")
    elif years_owned >= 10:
        score += 20
        reasons.append(f"Owner {years_owned} years")
    if equity_pct >= 50:
        score += 25
        reasons.append(f"High equity ({equity_pct:.0f}%)")
    if is_absentee:
        score += 25
        reasons.append("Absentee owner")
    if assessed_value > 0:
        score += 10
        reasons.append(f"Assessed at ${assessed_value:,.0f}")
    if assessed_value > purchase_price * 2:
        score += 10
        reasons.append("Value doubled since purchase")

    prop["years_owned"] = years_owned
    prop["estimated_equity"] = estimated_equity
    prop["equity_percentage"] = round(equity_pct, 1)
    prop["is_absentee"] = is_absentee
    prop["opportunity_score"] = min(score, 100)
    prop["opportunity_reasons"] = "; ".join(reasons) if reasons else "No strong indicators"
    prop["scraped_date"] = datetime.now().strftime("%Y-%m-%d")
    prop["dnc_status"] = random.choice(["Clear", "Clear", "Clear", "Clear", "DNC Listed"])

    return prop


def generate_demo_dataset(count=25):
    properties = [generate_property(i) for i in range(count)]
    properties.sort(key=lambda x: x["opportunity_score"], reverse=True)

    output = {
        "generated": datetime.now().isoformat(),
        "total_properties": len(properties),
        "source": "Demo Data - Pierce County, WA",
        "target_zips": ["98402", "98405", "98406"],
        "properties": properties
    }

    output_path = "off_market_properties_demo.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Generated {count} demo properties -> {output_path}")

    # Summary
    high = [p for p in properties if p["opportunity_score"] >= 50]
    absentee = [p for p in properties if p["is_absentee"]]
    longterm = [p for p in properties if p["years_owned"] >= 10]
    dnc_clear = [p for p in properties if p["dnc_status"] == "Clear"]

    print(f"\nSummary:")
    print(f"  High opportunity (50+): {len(high)}")
    print(f"  Absentee owners: {len(absentee)}")
    print(f"  Long-term owners (10+yr): {len(longterm)}")
    print(f"  DNC clear: {len(dnc_clear)}")

    return properties


if __name__ == "__main__":
    generate_demo_dataset(25)
