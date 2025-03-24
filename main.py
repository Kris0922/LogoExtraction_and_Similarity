# Task: Group logos by similarity without ML algorithms.
# Approach: Use image hashing (average_hash) and Hamming distance for clustering.
# Challenges:
#   1. Efficient pairwise comparison (O(n²) complexity).
#   2. Tuning similarity threshold (empirically set to 5).
#   3. Handling failed downloads.
# Solution:
#   - Precompute hashes to avoid reprocessing.
#   - Use hierarchical grouping to reduce redundant checks.

import pandas as pd
import requests
import numpy as np
from PIL import Image
from io import BytesIO
from imagehash import average_hash
from scipy.spatial.distance import hamming

# Step 1: Load the data
df = pd.read_parquet("logos_with_urls_improved.snappy.parquet")
df = df.dropna(subset=['logo_url'])  # Remove rows with no logo URL
print(f"Total logos to process: {len(df)}")

# Step 2: Download logos and preprocess them
def download_logo(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return Image.open(BytesIO(response.content)).convert('RGB')
    except Exception as e:
        print(f"Error downloading logo from {url}: {e}")
    return None

df['logo_image'] = df['logo_url'].apply(download_logo)
df = df.dropna(subset=['logo_image'])  # Remove rows where logo download failed
print(f"Logos successfully downloaded: {len(df)}")

# Step 3: Compute image hashes
def compute_image_hash(image):
    return average_hash(image)

df['logo_hash'] = df['logo_image'].apply(compute_image_hash)

# Step 4: Compute pairwise similarity and group similar logos
threshold = 5  # Hamming distance threshold for similarity
groups = []
used_indices = set()

for i in range(len(df)):
    if i in used_indices:
        continue
    group = [i]
    for j in range(i + 1, len(df)):
        if j in used_indices:
            continue
        # Compute Hamming distance between hashes
        distance = hamming(df.iloc[i]['logo_hash'].hash.flatten(), df.iloc[j]['logo_hash'].hash.flatten()) * 64
        if distance <= threshold:
            group.append(j)
            used_indices.add(j)
    groups.append(group)
    used_indices.add(i)

# Step 5: Assign group IDs to each logo
df['group_id'] = -1
for group_id, group in enumerate(groups):
    for idx in group:
        df.at[df.index[idx], 'group_id'] = group_id

# Step 6: Group similar logos and their corresponding URLs
grouped_logos = df.groupby('group_id').agg({'domain': list, 'logo_url': list}).reset_index()

# Save the results to a new Parquet file
grouped_logos.to_parquet("grouped_logos_without_clustering.snappy.parquet", compression='snappy')

# Print the results
print(grouped_logos.head())