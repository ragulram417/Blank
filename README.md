import os
import time
import requests
from requests.auth import HTTPBasicAuth

# ==============================
# CONFIGURATION (Use ENV VARS)
# ==============================

SIEBEL_USERNAME = os.getenv("SIEBEL_USERNAME")
SIEBEL_PASSWORD = os.getenv("SIEBEL_PASSWORD")

TOKEN_URL = os.getenv("SIEBEL_TOKEN_URL")      # e.g. https://dev-api.zebra.com/v2/oauth/token/access
API_URL = os.getenv("SIEBEL_API_URL")          # e.g. https://dev-api.zebra.com/rs/v1/dataservices/queryRepairConfig

TOKEN_EXPIRY_SECONDS = 3600  # 1 hour

# ==============================
# TOKEN CACHE (IN-MEMORY)
# ==============================

_token_cache = {
    "access_token": None,
    "expiry_time": 0
}

# ==============================
# TOKEN FUNCTIONS
# ==============================

def get_token():
    """
    Fetch a new access token from Siebel OAuth endpoint
    """
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "grant_type": "password"
    }

    response = requests.post(
        TOKEN_URL,
        headers=headers,
        data=payload,
        auth=HTTPBasicAuth(SIEBEL_USERNAME, SIEBEL_PASSWORD),
        timeout=30
    )

    response.raise_for_status()
    token_response = response.json()

    access_token = token_response.get("access_token")
    expires_in = token_response.get("expires_in", TOKEN_EXPIRY_SECONDS)

    if not access_token:
        raise ValueError("Access token not found in response")

    # Cache token
    _token_cache["access_token"] = access_token
    _token_cache["expiry_time"] = time.time() + expires_in - 60  # buffer of 1 min

    return access_token


def get_valid_token():
    """
    Return cached token if valid, otherwise fetch a new one
    """
    if (
        _token_cache["access_token"] is None
        or time.time() >= _token_cache["expiry_time"]
    ):
        return get_token()

    return _token_cache["access_token"]

# ==============================
# SIEBEL API CALL
# ==============================

def call_siebel_api(api_url, repair_number):
    token = get_valid_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "repairNumber": repair_number
    }

    response = requests.post(
        api_url,
        headers=headers,
        json=payload,
        timeout=30
    )

    response.raise_for_status()
    return response.json()

# ==============================
# MAIN EXECUTION
# ==============================

if __name__ == "__main__":
    try:
        result = call_siebel_api(
            api_url=API_URL,
            repair_number="200004878-1"
        )
        print(result)

    except requests.exceptions.RequestException as e:
        print(f"HTTP Error calling Siebel API: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")
