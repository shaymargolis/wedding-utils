#!/usr/bin/env python3
import csv
import os
import sys
import json
import subprocess
import time
from collections import deque
from typing import List, Dict, Callable, Optional
from urllib import request, parse, error
from tqdm import tqdm

SEPARATOR = "::"
PHONE_JOIN = " | "
MAX_CONTACTS_PER_MINUTE = 90

# Rolling window counter of contacts processed in the last 60 seconds
_CONTACT_RATE_WINDOW = deque()  # items: (timestamp, count)


def _rate_prune(now: Optional[float] = None) -> None:
    now = now if now is not None else time.time()
    cutoff = now - 60.0
    while _CONTACT_RATE_WINDOW and _CONTACT_RATE_WINDOW[0][0] <= cutoff:
        _CONTACT_RATE_WINDOW.popleft()


def _rate_current_total(now: Optional[float] = None) -> int:
    now = now if now is not None else time.time()
    _rate_prune(now)
    return sum(c for _, c in _CONTACT_RATE_WINDOW)


def _rate_expect_and_wait(expected_count: int) -> None:
    """Ensure room for expected_count contacts within 60s window, else wait."""
    now = time.time()
    _rate_prune(now)
    while _rate_current_total(now) + max(0, expected_count) > MAX_CONTACTS_PER_MINUTE:
        oldest_ts = _CONTACT_RATE_WINDOW[0][0] if _CONTACT_RATE_WINDOW else now
        sleep_for = max(1.0, (oldest_ts + 60.0) - now)
        time.sleep(sleep_for)
        now = time.time()
        _rate_prune(now)


def _rate_record(count: int) -> None:
    if count <= 0:
        return
    _CONTACT_RATE_WINDOW.append((time.time(), int(count)))


def _normalize_phone_for_compare(phone: str) -> str:
    """
    Normalize a phone string for equality comparison while ignoring country codes.

    Strategy:
    - Keep only digits
    - Compare by the last 9 digits when available (fits IL numbers: 0XXXXXXXXX vs 972XXXXXXXXX)
    - If fewer than 9 digits remain, use what's there
    """
    digits = "".join(ch for ch in phone if ch.isdigit())
    if not digits:
        return ""
    return digits[-9:] if len(digits) >= 9 else digits


def _pick_display_phone_from_candidates(phones: List[str], normalized_value: str) -> str:
    """
    Pick a representative display string for the phone, preferring a local format
    (leading 0) if present among the candidates. Falls back to reconstructing a
    local-like format from the normalized value.
    """
    # Prefer any candidate that already looks like a local IL format (starts with '0')
    for ph in phones:
        ph_stripped = ph.strip()
        if ph_stripped.startswith("0"):
            return ph_stripped
    # Otherwise, if we have the normalized last-9, reconstruct a local form
    if len(normalized_value) == 9:
        return "0" + normalized_value
    # Fallback to the first candidate as-is
    return phones[0].strip() if phones else ""

def search_contacts_with_phones_apple(fragment: str) -> List[Dict[str, List[str]]]:
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


def search_contacts_with_phones_google(fragment: str, token: str) -> List[Dict[str, List[str]]]:
    """
    Search Google Contacts using the People API, returning
    [{ 'name': str, 'phones': [str, ...] }, ...]

    Requires an OAuth access token with contacts read scope. The tool obtains
    this via OAuth Device Flow using a Google OAuth Client ID.
    """
    if not token:
        raise ValueError("Google access token is required for source 'google'")

    out: List[Dict[str, List[str]]] = []
    endpoint = "https://people.googleapis.com/v1/people:searchContacts"
    params = {
        "query": fragment,
        "pageSize": "5",
        "readMask": "names,phoneNumbers",
    }
    page_token = None
    # Cap pages to avoid runaway loops
    for _ in range(100):
        page_size = int(params.get("pageSize", 5))
        if page_token:
            params["pageToken"] = page_token
        else:
            params.pop("pageToken", None)
        url = endpoint + "?" + parse.urlencode(params)
        req = request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })
        # Rate-limit: ensure we won't exceed 90 contacts/min assuming worst-case page
        _rate_expect_and_wait(page_size)
        # Execute with retry on 429
        while True:
            try:
                with request.urlopen(req, timeout=20) as resp:
                    data = json.load(resp)
                break
            except error.HTTPError as e:
                if e.code == 429:
                    # Quota exceeded — sleep 60s and retry
                    time.sleep(60)
                    continue
                # Try to surface meaningful error
                try:
                    err_body = e.read().decode("utf-8", errors="ignore")
                except Exception:
                    err_body = str(e)
                raise RuntimeError(f"Google API error {e.code}: {err_body}")
            except Exception as e:
                raise RuntimeError(f"Failed to reach Google API: {e}")

        results = data.get("results", [])
        _rate_record(len(results))
        for item in results:
            person = item.get("person", {})
            names = person.get("names", [])
            display_name = ""
            for n in names:
                # Prefer displayName if present
                if n.get("displayName"):
                    display_name = n["displayName"].strip()
                    break
            if not display_name and names:
                display_name = (names[0].get("displayName") or "").strip()

            phones_list: List[str] = []
            for ph in person.get("phoneNumbers", []) or []:
                v = ph.get("value")
                if v:
                    phones_list.append(str(v).strip())

            if display_name or phones_list:
                out.append({"name": display_name, "phones": phones_list})

        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return out

def _get_google_access_token_via_device_flow(
    *,
    client_id: str,
    client_secret: Optional[str] = None,
    scopes: Optional[List[str]] = None,
    poll_timeout_sec: int = 600,
) -> str:
    """
    Obtain an OAuth access token using Google's Device Authorization Grant.

    Requires a Google OAuth client ID (TV and Limited Input or Desktop app).
    A client secret is optional; some client types may require it when polling
    the token endpoint.
    """
    scopes = scopes or ["https://www.googleapis.com/auth/contacts.readonly"]
    # Step 1: Get a device code
    code_url = "https://oauth2.googleapis.com/device/code"
    data = parse.urlencode({
        "client_id": client_id,
        "scope": " ".join(scopes),
    }).encode("utf-8")
    try:
        with request.urlopen(
            request.Request(
                code_url,
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
            ),
            timeout=20,
        ) as resp:
            payload = json.load(resp)
    except error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""
        # Try to extract a structured error for better diagnostics
        hint = ""
        try:
            j = json.loads(body) if body else {}
            err = j.get("error") or j.get("error_description") or ""
            if isinstance(err, dict):
                err = err.get("message") or err.get("status") or ""
            if err:
                hint = f" ({err})"
        except Exception:
            pass
        raise RuntimeError(f"Failed to start device auth: HTTP {e.code}{hint}. Response: {body}")
    except Exception as e:
        raise RuntimeError(f"Failed to start device auth: {e}")

    device_code = payload.get("device_code")
    user_code = payload.get("user_code")
    verification_url = payload.get("verification_url") or payload.get("verification_uri")
    interval = int(payload.get("interval", 5))
    expires_in = int(payload.get("expires_in", poll_timeout_sec))
    if not (device_code and user_code and verification_url):
        raise RuntimeError(f"Unexpected device code response: {payload}")

    print(
        f"To authorize, visit {verification_url} and enter code: {user_code}",
        file=sys.stderr,
    )

    # Step 2: Poll for token
    token_url = "https://oauth2.googleapis.com/token"
    grant_type = "urn:ietf:params:oauth:grant-type:device_code"
    deadline = (expires_in if expires_in > 0 else poll_timeout_sec)
    waited = 0
    while waited <= deadline:
        token_params = {
            "client_id": client_id,
            "device_code": device_code,
            "grant_type": grant_type,
        }
        if client_secret:
            token_params["client_secret"] = client_secret
        try:
            with request.urlopen(
                request.Request(
                    token_url,
                    data=parse.urlencode(token_params).encode("utf-8"),
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json",
                    },
                ),
                timeout=20,
            ) as resp:
                tok = json.load(resp)
        except error.HTTPError as e:
            # Parse error response body for polling hints
            try:
                body = e.read().decode("utf-8", errors="ignore")
                parsed = json.loads(body)
                err = parsed.get("error")
            except Exception:
                err = None
            if err in {"authorization_pending", "slow_down"}:
                sleep_for = interval + (5 if err == "slow_down" else 0)
                import time as _time
                _time.sleep(sleep_for)
                waited += sleep_for
                continue
            if err == "access_denied":
                raise RuntimeError("Authorization denied by user")
            if err == "expired_token":
                raise RuntimeError("Device code expired before authorization completed")
            if err in {"invalid_client", "unauthorized_client"}:
                raise RuntimeError(
                    "OAuth client is not permitted for Device Flow. "
                    "Use a 'TV and Limited Input' or 'Desktop' OAuth client ID and ensure the OAuth consent screen is configured."
                )
            raise RuntimeError(f"Token polling failed: HTTP {e.code}. Response: {body}")
        except Exception as e:
            raise RuntimeError(f"Failed to poll token endpoint: {e}")

        access_token = tok.get("access_token")
        if access_token:
            return access_token

        # If no token yet, follow interval
        import time as _time
        _time.sleep(interval)
        waited += interval

    raise RuntimeError("Timed out waiting for device authorization")


def process_csv(
    input_path: str,
    output_path: str,
    *,
    source: str = "apple",
    google_client_id: Optional[str] = None,
    google_client_secret: Optional[str] = None,
    google_scope: Optional[str] = None,
) -> None:
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

    # Choose search function
    if source == "apple":
        searcher: Callable[[str], List[Dict[str, List[str]]]] = search_contacts_with_phones_apple
    elif source == "google":
        # Always use Device Flow with a client ID (client secret optional)
        client_id = (google_client_id or os.environ.get("GOOGLE_CLIENT_ID", "").strip())
        client_secret = (google_client_secret or os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()) or None
        scope = (google_scope or os.environ.get("GOOGLE_SCOPE", "").strip()) or (
            "https://www.googleapis.com/auth/contacts"
        )
        if not client_id:
            raise ValueError(
                "Source 'google' requires --google-client-id or GOOGLE_CLIENT_ID for OAuth Device Flow"
            )
        token = _get_google_access_token_via_device_flow(
            client_id=client_id,
            client_secret=client_secret,
            scopes=[scope],
        )

        def _searcher(query: str) -> List[Dict[str, List[str]]]:
            return search_contacts_with_phones_google(query, token)

        searcher = _searcher
    else:
        raise ValueError("source must be 'apple' or 'google'")

    for row in tqdm(rows, desc="Processing contacts"):
        query = (row.get("name") or "").strip()
        phone_cell = ""
        contact_name_cell = ""
        match_count = 0
        if query:
            results = searcher(query)
            match_count = len(results)
            if match_count == 1:
                phones = results[0]["phones"]
                phone_cell = PHONE_JOIN.join(phones) if phones else ""
                contact_name_cell = results[0]["name"]
            elif match_count > 1:
                # If all matched contacts have the same phone number(s) ignoring country codes,
                # fill the phone. We consider all phone entries across matches; if, after
                # normalization, there is exactly one unique value, we use it.
                all_phones = []  # original strings
                norm_values = set()
                for r in results:
                    for ph in r.get("phones", []):
                        all_phones.append(ph)
                        nv = _normalize_phone_for_compare(ph)
                        if nv:
                            norm_values.add(nv)
                if len(norm_values) == 1:
                    sole_norm = next(iter(norm_values))
                    phone_cell = _pick_display_phone_from_candidates(all_phones, sole_norm)
        row["phone number"] = phone_cell
        row["contact_name"] = contact_name_cell
        row["match_count"] = str(match_count)

    with open(output_path, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Lookup phone numbers from Contacts and enrich a CSV")
    parser.add_argument("input_csv", help="Path to input CSV with a 'name' column")
    parser.add_argument("output_csv", help="Path to write the enriched CSV")
    parser.add_argument(
        "--source",
        choices=["apple", "google"],
        default="apple",
        help="Contact source: 'apple' (default) or 'google'",
    )
    parser.add_argument(
        "--google-client-id",
        dest="google_client_id",
        default=None,
        help="Google OAuth client ID for Device Flow (or set GOOGLE_CLIENT_ID)",
    )
    parser.add_argument(
        "--google-scope",
        dest="google_scope",
        default=None,
        help=(
            "OAuth scope to request (default: contacts.readonly). "
            "Override with GOOGLE_SCOPE env or this flag if needed."
        ),
    )
    parser.add_argument(
        "--google-client-secret",
        dest="google_client_secret",
        default=None,
        help="Optional Google OAuth client secret for Device Flow (or set GOOGLE_CLIENT_SECRET)",
    )
    args = parser.parse_args()

    try:
        process_csv(
            args.input_csv,
            args.output_csv,
            source=args.source,
            google_client_id=args.google_client_id,
            google_client_secret=args.google_client_secret,
            google_scope=args.google_scope,
        )
        print(f"Wrote: {args.output_csv}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
