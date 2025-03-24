
# Task: Fetch logo URLs for domains using Clearbit's API.
# Approach: Direct API calls to Clearbit's Logo API for simplicity.
# Challenges: Limited coverage (not all logos exist in Clearbit's database).  ~ 84% coverage.
# Solution: Single API-based method, but coverage improved in later versions.


import pandas as pd
import requests

# Load the Parquet file
df = pd.read_parquet("logos.snappy.parquet")


# Function to fetch logo URL using Clearbit's Logo API
def fetch_logo_with_clearbit(domain):
    try:
        # Construct the Clearbit API URL
        clearbit_url = f"https://logo.clearbit.com/{domain}"

        # Send a GET request to the Clearbit API
        response = requests.get(clearbit_url, timeout=10)

        # If the request is successful (status code 200), return the logo URL
        if response.status_code == 200:
            return clearbit_url

        # If the logo is not found (status code 404), return None
        elif response.status_code == 404:
            print(f"No logo found for {domain}")
            return None

        # Handle other status codes
        else:
            print(f"Error fetching logo for {domain}: HTTP {response.status_code}")
            return None

    except Exception as e:
        print(f"Error fetching logo for {domain}: {e}")
        return None


# Apply the function to each domain in the DataFrame
df['logo_url'] = df['domain'].apply(fetch_logo_with_clearbit)

# Display the DataFrame with logo URLs
print(df.head())

# Save the DataFrame with logo URLs to a new Parquet file
df.to_parquet("logos_with_urls_clearbit.snappy.parquet", compression='snappy')