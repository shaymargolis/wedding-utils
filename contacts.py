#!/usr/bin/env python3
import csv
import sys
import subprocess
from typing import List, Dict
from tqdm import tqdm

SEPARATOR = "::"
PHONE_JOIN = " | "

def search_contacts_with_phones(fragment: str) -> List[Dict[str, List[str]]]:
    """
    Returns a list of dicts: { 'name': str, 'phones': [str, ...] }
    Matches contacts whose full name contains `fragment` (case-insensitive).
    """
    frag_esc = fragment.replace('"', '\\"')
    osa = f'''
    set frag to "{frag_esc}"
    tell application "Contacts"
        set matches to (people whose name contains frag)
        if (count of matches) is 0 then return ""
        set out to ""
        repeat with p in matches
            set theName to (name of p as string)
            set phoneValues to ""
            repeat with ph in (phones of p)
                set phoneValues to phoneValues & (value of ph) & "{PHONE_JOIN}"
            end repeat
            if phoneValues is not "" then
                set phoneValues to text 1 thru -{len(PHONE_JOIN)+1} of phoneValues
            end if
            set out to out & theName & " {SEPARATOR} " & phoneValues & linefeed
        end repeat
        return out
    end tell
    '''
    res = subprocess.run(
        ["/usr/bin/osascript", "-e", osa],
        capture_output=True, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or "osascript execution failed")

    out = []
    for line in res.stdout.splitlines():
        if not line.strip():
            continue
        if f" {SEPARATOR} " in line:
            name, phones = line.split(f" {SEPARATOR} ", 1)
            name = name.strip()
            phones = [p.strip() for p in phones.split(PHONE_JOIN)] if phones.strip() else []
            out.append({"name": name, "phones": phones})
        else:
            out.append({"name": line.strip(), "phones": []})
    return out

def process_csv(input_path: str, output_path: str) -> None:
    with open(input_path, newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        # Normalize fieldnames
        fieldnames = [fn.strip().lstrip("\ufeff") for fn in (reader.fieldnames or [])]
        reader.fieldnames = fieldnames

        if "name" not in fieldnames:
            raise ValueError(f'Input CSV must contain a "name" column, got {fieldnames}')

        # Add output columns if missing
        for col in ["phone number", "contact_name", "match_count"]:
            if col not in fieldnames:
                fieldnames.append(col)

        rows = list(reader)

    for row in tqdm(rows, desc="Processing contacts"):
        query = (row.get("name") or "").strip()
        phone_cell = ""
        contact_name_cell = ""
        match_count = 0
        if query:
            results = search_contacts_with_phones(query)
            match_count = len(results)
            if match_count == 1:
                phones = results[0]["phones"]
                phone_cell = PHONE_JOIN.join(phones) if phones else ""
                contact_name_cell = results[0]["name"]
        row["phone number"] = phone_cell
        row["contact_name"] = contact_name_cell
        row["match_count"] = str(match_count)

    with open(output_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    if len(sys.argv) != 3:
        print("Usage: python contacts_csv_lookup.py <input.csv> <output.csv>")
        sys.exit(1)
    input_csv, output_csv = sys.argv[1], sys.argv[2]
    try:
        process_csv(input_csv, output_csv)
        print(f"Wrote: {output_csv}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
