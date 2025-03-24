# Task: Validate outputs and visualize logos for manual verification.
# Approach: Load Parquet files and perform sanity checks.
# Challenges:
#   1. Ensuring data integrity across stages.
#   2. Manual validation of logo grouping accuracy.
# Solution:
#   - Basic dataframe summaries (head(), info()).
#   - Commented-out visualization for ad-hoc checks (Matplotlib).
#   - Compare columns and shapes across dataframes to check for how many logos were extracted.

import pandas as pd

# Load the Parquet file
df = pd.read_parquet("logos_with_urls_clearbit.snappy.parquet")

# Display basic info and first few rows
print(df.head())
print(df.info())

df = pd.read_parquet("logos_with_urls_improved.snappy.parquet")
print(df.head())
print(df.info())

df = pd.read_parquet("grouped_logos_without_clustering.snappy.parquet")
print(df.head())
print(df.info())

# print("Columns in the dataframe:")
# print(df.columns)

# print("Shape of the dataframe:")
# print(df.shape)
#
# import matplotlib.pyplot as plt
# import requests
# from PIL import Image
# from io import BytesIO

# Function to display a few logos
# def display_logos(urls, num=5):
#     fig, axes = plt.subplots(1, num, figsize=(15, 5))
#     for i, url in enumerate(urls[:num]):
#         response = requests.get(url)
#         img = Image.open(BytesIO(response.content))
#         axes[i].imshow(img)
#         axes[i].axis("off")
#     plt.show()
#
# # Assuming 'logo_url' is the column with image links
# display_logos(df["logo_url"])
