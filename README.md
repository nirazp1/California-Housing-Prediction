# California Housing Market Analysis

This project analyzes housing market trends in California by integrating data from multiple sources including Census data, Zillow housing data, rental listings, and policy reports.

## Setup

1. Clone this repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env` file with your API keys:
   ```
   CENSUS_API_KEY=your_census_api_key_here
   ```
   Get your Census API key from: https://api.census.gov/data/key_signup.html

5. Download required data files:
   - From Zillow Research (https://www.zillow.com/research/data/):
     - State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv (Home Value Index)
     - Metro_zori_uc_sfrcondomfr_sm_month.csv (Rental Index)
   Place these files in the `data` directory.

## Project Structure

```
.
├── data/                  # Data files directory
├── output/               # Analysis output directory
├── .env                  # Environment variables
├── requirements.txt      # Project dependencies
├── README.md            # This file
└── california_housing_analysis.py  # Main analysis script
```

## Usage

Run the analysis:
```bash
python california_housing_analysis.py
```

The script will:
1. Fetch Census data for California
2. Process Zillow housing data
3. Scrape current rental listings
4. Extract policy data from PDFs
5. Clean and integrate all datasets
6. Generate analysis reports in the `output` directory

## Output

The analysis generates the following files in the `output` directory:
- `california_housing_data_final.csv`: Complete integrated dataset
- `summary_stats.csv`: Statistical summary of the data
- `summary_report.md`: Detailed analysis report

## Data Sources

- Demographics: U.S. Census Bureau API
- Housing Prices: Zillow Research Data
- Rental Listings: apartments.com
- Policy Data: HUD User portal 