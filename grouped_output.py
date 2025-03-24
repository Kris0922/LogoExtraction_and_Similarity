
# Task: Display grouped logos and their domains after clustering.
# Approach: Iterate through grouped data and print human-readable output.
# Challenges:
#   1. Handling nested lists of domains/URLs.
#   2. Formatting output for readability.
# Solution: Use pandas aggregation and zip() for parallel iteration.

import pandas as pd

# Load the grouped logos data
grouped_logos = pd.read_parquet("grouped_logos_without_clustering.snappy.parquet")
count = 0
# Iterate through each group and print the domains and logo URLs
for _, row in grouped_logos.iterrows():
    group_id = row['group_id']
    domains = row['domain']
    logo_urls = row['logo_url']

    print(f"Group {group_id}:")
    for domain, logo_url in zip(domains, logo_urls):
        print(f"  Domain: {domain}")
        print(f"  Logo URL: {logo_url}")
        count += 1
    print("-" * 40)  # Separator between groups
print(count)