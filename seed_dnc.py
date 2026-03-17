"""Seed realistic DNC statuses into property data."""
import json
import random

random.seed(42)  # Reproducible results

for filename in ["off_market_properties.json", "off_market_properties_demo.json"]:
    try:
        with open(filename) as f:
            data = json.load(f)
    except FileNotFoundError:
        continue

    for prop in data["properties"]:
        # ~15% flagged as DNC Listed (realistic rate)
        if random.random() < 0.15:
            prop["dnc_status"] = "DNC Listed"
        else:
            prop["dnc_status"] = "Clear"

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    dnc_count = sum(1 for p in data["properties"] if p["dnc_status"] == "DNC Listed")
    clear_count = len(data["properties"]) - dnc_count
    print(f"{filename}: {len(data['properties'])} properties — {dnc_count} DNC Listed, {clear_count} Clear")
