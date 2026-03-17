"""
DNC (Do Not Call) Registry Checker
Checks phone numbers against the FTC Do Not Call flat file.

Setup:
1. Register at https://telemarketing.donotcall.gov (free for up to 5 area codes)
2. Download the flat file for your area codes
3. Place the file as 'dnc_list.txt' in this directory
4. Run: python dnc_checker.py

Also supports DNCscrub.com API (free 100 lookups/month):
  Set DNCSCRUB_API_KEY environment variable
"""

import json
import os
import re
import requests


def clean_phone(phone: str) -> str:
    """Strip phone number to 10 digits."""
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    return digits


def load_ftc_dnc_file(filepath: str = "dnc_list.txt") -> set:
    """Load FTC flat file into a set for O(1) lookup."""
    if not os.path.exists(filepath):
        print(f"FTC DNC file not found: {filepath}")
        print("Download from https://telemarketing.donotcall.gov")
        return set()

    dnc_numbers = set()
    with open(filepath) as f:
        for line in f:
            number = line.strip()
            if number:
                dnc_numbers.add(clean_phone(number))

    print(f"Loaded {len(dnc_numbers)} numbers from FTC DNC file")
    return dnc_numbers


def check_dnc_ftc(phone: str, dnc_set: set) -> bool:
    """Check a phone number against the FTC flat file."""
    return clean_phone(phone) in dnc_set


def check_dnc_scrub(phone: str, api_key: str = None) -> dict:
    """Check a phone number via DNCscrub.com API (free 100/month)."""
    api_key = api_key or os.environ.get("DNCSCRUB_API_KEY")
    if not api_key:
        return {"status": "unknown", "error": "No API key set"}

    try:
        resp = requests.get(
            "https://api.dncscrub.com/v1/check",
            params={"phone": clean_phone(phone), "api_key": api_key},
            timeout=10
        )
        if resp.ok:
            return resp.json()
        return {"status": "error", "code": resp.status_code}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def check_properties(properties_file: str = "off_market_properties.json",
                     phone_field: str = "phone_number"):
    """Check all properties with phone numbers against DNC list."""
    # Try FTC file first
    dnc_set = load_ftc_dnc_file()

    with open(properties_file) as f:
        data = json.load(f)

    checked = 0
    flagged = 0

    for prop in data["properties"]:
        phone = prop.get(phone_field)
        if not phone:
            continue

        cleaned = clean_phone(phone)
        if not cleaned or len(cleaned) != 10:
            continue

        checked += 1

        if dnc_set:
            # FTC flat file check
            if check_dnc_ftc(cleaned, dnc_set):
                prop["dnc_status"] = "DNC Listed"
                flagged += 1
            else:
                prop["dnc_status"] = "Clear"
        else:
            # Fall back to DNCscrub API
            result = check_dnc_scrub(cleaned)
            if result.get("dnc_status") or result.get("do_not_call"):
                prop["dnc_status"] = "DNC Listed"
                flagged += 1
            else:
                prop["dnc_status"] = "Clear"

    # Save updated data
    with open(properties_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nDNC Check Complete:")
    print(f"  Checked: {checked}")
    print(f"  Flagged: {flagged}")
    print(f"  Clear: {checked - flagged}")
    print(f"  No phone: {len(data['properties']) - checked}")

    return data


if __name__ == "__main__":
    check_properties()
