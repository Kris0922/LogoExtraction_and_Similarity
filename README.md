# Logo Similarity Grouping System

This project groups websites by logo similarity **without using ML algorithms**, leveraging image hashing and clustering techniques. The system extracts logos from domains using multiple fallback sources, computes perceptual hashes, and groups visually similar logos using Hamming distance.

## 🚀 Key Features
- **High Coverage**: Extracts logos for >97% of domains using Clearbit, Google Favicon API, and direct scraping.
- **Non-ML Clustering**: Uses image hashing (`average_hash`) and Hamming distance for grouping.
- **Scalable Design**: Optimized for future parallelization (e.g., Dask/Spark).

## 📁 File Descriptions

### 1. `extracting_logos.py`
- **Purpose**: Initial logo extraction using Clearbit's API.
- **Details**:
  - Fetches logos via Clearbit's `logo.clearbit.com` endpoint.
  - Simple but limited to Clearbit's database coverage.
  - Output: `logos_with_urls_clearbit.snappy.parquet`.

### 2. `improvedExtracting.py`
- **Purpose**: Enhanced logo extraction with fallback strategies.
- **Details**:
  - **3-Step Fallback**: 
    1. Clearbit API → 
    2. Google Favicon API → 
    3. Direct website scraping (using `BeautifulSoup`).
  - Handles anti-scraping headers and retries.
  - Output: `logos_with_urls_improved.snappy.parquet`.

### 3. `main.py`
- **Purpose**: Groups logos by similarity.
- **Details**:
  - Downloads logos and computes `average_hash` for each.
  - Groups logos using **Hamming distance** (threshold = 5).
  - Avoids ML libraries, focusing on lightweight hashing.
  - Output: `grouped_logos_without_clustering.snappy.parquet`.

### 4. `grouped_output.py`
- **Purpose**: Displays grouped results in human-readable format.
- **Details**:
  - Prints domains and logo URLs for each group.
  - Example output:
    ```
    Group 0:
      Domain: example.com
      Logo URL: https://logo.clearbit.com/example.com
    ```

### 5. `visualization_and_proofing.py`
- **Purpose**: Validates outputs and visualizes logos.
- **Details**:
  - Checks data integrity across Parquet files.
  - Includes commented-out code for Matplotlib-based logo previews.
