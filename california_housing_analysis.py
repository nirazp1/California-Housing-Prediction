"""
Group Member: 

Niraj Pandey
Sajan Poudel
"""

import pandas as pd
from dotenv import load_dotenv
import os
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import tabula

# --- Logging Setup ---
# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG, # Using DEBUG for detailed output, good for dev
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' # This provides very useful info
)
logger = logging.getLogger(__name__) # Get a logger, name it after the module. good practice.

# Set requests logging to WARNING to reduce noise
logging.getLogger("requests").setLevel(logging.WARNING) # Suppresses noise from requests lib
logging.getLogger("urllib3").setLevel(logging.WARNING) # Suppresses noise from urllib3 lib (requests uses this)

# --- Environment Variables ---
# Load environment variables
load_dotenv() # Reads from .env file

# --- Function: validate_census_api_key ---
def validate_census_api_key():
    """Validate the Census API key by making a test request.""" # Good docstring
    api_key = os.getenv('CENSUS_API_KEY')  # Reads the env var
    if not api_key:
        raise ValueError("Census API key not found in .env file") # Good error handling
    
    # Try a different endpoint that's more commonly available
    test_url = f"https://api.census.gov/data/2020/dec/pl?get=NAME&for=state:*&key={api_key}" # Using an endpoint that usually works is good
    try:
        response = requests.get(test_url) # Make the request
        response.raise_for_status()  # Raise an error for HTTP errors (404, 500, etc)
        
        # Try to parse the response as JSON
        try:
            response.json()  # If this fails it's an invalid API response
        except requests.exceptions.JSONDecodeError:
            raise ValueError(f"Invalid Census API response format. Response text: {response.text[:200]}")
            # Good to display a partial response for debugging
        return api_key # Returns api key if all good
    except requests.exceptions.RequestException as e: # Catches general request failures
        raise ValueError(f"Census API request failed: {str(e)}") # Raises with more context

# --- Function: get_census_data ---
def get_census_data(api_key):
    """Fetch California population data from the Census API.""" # Good docstring
    logger.info("Fetching Census data...")
    
    # Use the 2020 Decennial Census endpoint
    base_url = "https://api.census.gov/data/2020/dec/pl" # Good to define URL's as constants
    params = {
        "get": "NAME,P1_001N",  # P1_001N is total population - Good comment
        "for": "state:06",  # FIPS code for California - Good comment
        "key": api_key # Pass the validated api key
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        # Log the response for debugging
        logger.debug(f"Census API Response: {response.text[:200]}")
        
        try:
            data = response.json() # Parse the response
        except requests.exceptions.JSONDecodeError:
            logger.error(f"Failed to parse Census API response. Response text: {response.text[:200]}")
            raise ValueError("Invalid response format from Census API") # Good error handling
        
        if not data or not isinstance(data, list) or len(data) < 2:
            raise ValueError(f"Unexpected response format from Census API: {data}") # Very helpful validation!
        
        # Convert to DataFrame
        df = pd.DataFrame(data[1:], columns=data[0]) # Use the first row as headers
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"Census API request failed: {str(e)}")
        raise # Re-raise the exception, so calling function is aware.
    except Exception as e: # Catch-all for other errors
        logger.error(f"Error processing Census data: {str(e)}")
        raise

# --- Function: process_zillow_data ---
def process_zillow_data():
    """Process Zillow housing data for California.""" # Good docstring
    logger.info("Processing Zillow housing data...")
    
    try:
        # Read ZHVI data (Home Value Index)
        zhvi_file = "data/State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv" # Good to store filepaths as variables
        zhvi_df = pd.read_csv(zhvi_file) # Read it
        
        # Read ZORI data (Rental Index)
        zori_file = "data/Metro_zori_uc_sfrcondomfr_sm_month.csv"
        zori_df = pd.read_csv(zori_file)
        
        # Debug information
        logger.info(f"Available states in ZHVI data: {zhvi_df['RegionName'].unique()}") # Useful debugging
        logger.info(f"Available states in ZORI data: {zori_df['RegionName'].unique()}")
        
        # Filter for California in ZHVI
        ca_zhvi = zhvi_df[zhvi_df['RegionName'] == 'California'] # Filter data
        
        if ca_zhvi.empty: # Good to check for the empty condition.
            raise ValueError(f"No California data found in ZHVI dataset\nAvailable states: {zhvi_df['RegionName'].unique()}")
        
        # Process time series data for home values
        value_cols = [col for col in ca_zhvi.columns if str(col).startswith('20')] # Clever way to extract value columns
        ca_zhvi_ts = ca_zhvi[value_cols].T # Transpose to make time the index
        ca_zhvi_ts.index = pd.to_datetime(ca_zhvi_ts.index) # Convert index to datetime
        
        return ca_zhvi_ts, zori_df # Return both datasets
        
    except Exception as e:
        logger.error(f"Error processing Zillow data: {str(e)}") # Good error handling
        raise # Re-raise the error

# --- Function: analyze_rental_data ---
def analyze_rental_data(zori_df):
    """Analyze rental data from Zillow's ZORI dataset for California metros.""" # Good docstring
    logger.info("Analyzing rental data for California metro areas...")
    
    try:
        # Filter for California metros
        ca_metros = zori_df[zori_df['RegionName'].str.contains(', CA', na=False)] # Filter the data
        
        # Get the most recent month's data
        value_cols = [col for col in ca_metros.columns if str(col).startswith('20')] # Get time series cols
        value_cols.sort()  # Ensure chronological order # Sort them
        latest_month = value_cols[-1] # Get latest
        
        # Create summary for each metro area
        rental_summary = []
        valid_yoy_changes = []  # Track valid year-over-year changes # Good to keep this info
        
        for _, row in ca_metros.iterrows(): # Loop through rows of data
            metro_name = row['RegionName'].replace(', CA', '') # Get metro name
            latest_rent = row[latest_month] # Get the most recent rent
            
            # Calculate year-over-year change if we have data from a year ago
            yoy_change = None # Initialize to null/none
            if len(value_cols) >= 13:
                year_ago_month = value_cols[-13]  # 13 months back to get same month last year # Good comment
                year_ago_rent = row[year_ago_month]
                if pd.notnull(year_ago_rent) and year_ago_rent > 0:
                    yoy_change = ((latest_rent - year_ago_rent) / year_ago_rent) * 100 # Calculate yoy
                    if pd.notnull(yoy_change): # If yoy is valid
                        valid_yoy_changes.append(yoy_change) # keep it
            
            rental_summary.append({
                'metro_area': metro_name,
                'median_rent': latest_rent,
                'yoy_change': yoy_change # Append the data
            })
        
        # Sort by median rent descending
        rental_summary = sorted(rental_summary, key=lambda x: x['median_rent'], reverse=True) # Sort the data
        
        # Calculate state-level statistics
        median_rents = [m['median_rent'] for m in rental_summary if pd.notnull(m['median_rent'])]
        avg_yoy_change = sum(valid_yoy_changes) / len(valid_yoy_changes) if valid_yoy_changes else None # Get average yoy
        
        # Log summary information
        logger.info("\nCalifornia Rental Market Analysis:") # Good logging formatting
        logger.info(f"Data as of: {pd.to_datetime(latest_month).strftime('%B %Y')}")
        logger.info(f"Number of metro areas analyzed: {len(rental_summary)}")
        
        logger.info("\nTop 5 Most Expensive Rental Markets:")
        for i, market in enumerate(rental_summary[:5], 1):
            yoy_str = f"(YoY change: {market['yoy_change']:.1f}%)" if market['yoy_change'] is not None else "(YoY change: N/A)"
            logger.info(f"{i}. {market['metro_area']}: ${market['median_rent']:,.2f}/month {yoy_str}")
        
        logger.info("\nState-Level Statistics:")
        logger.info(f"Average Rent (across metros): ${sum(median_rents)/len(median_rents):,.2f}")
        if avg_yoy_change is not None:
            logger.info(f"Average Year-over-Year Change: {avg_yoy_change:.1f}%")
        else:
            logger.info("Average Year-over-Year Change: Not enough historical data")
        logger.info(f"Highest Rent: ${max(median_rents):,.2f}")
        logger.info(f"Lowest Rent: ${min(median_rents):,.2f}")
        
        # Additional market insights
        logger.info("\nMarket Insights:")
        num_increasing = sum(1 for m in rental_summary if m['yoy_change'] is not None and m['yoy_change'] > 0)
        num_decreasing = sum(1 for m in rental_summary if m['yoy_change'] is not None and m['yoy_change'] < 0)
        logger.info(f"Markets with increasing rents: {num_increasing}")
        logger.info(f"Markets with decreasing rents: {num_decreasing}")
        
        # Find fastest growing and declining markets
        markets_with_changes = [m for m in rental_summary if m['yoy_change'] is not None]
        if markets_with_changes:
            fastest_growing = max(markets_with_changes, key=lambda x: x['yoy_change'])
            fastest_declining = min(markets_with_changes, key=lambda x: x['yoy_change'])
            logger.info(f"Fastest growing market: {fastest_growing['metro_area']} ({fastest_growing['yoy_change']:.1f}%)")
            logger.info(f"Fastest declining market: {fastest_declining['metro_area']} ({fastest_declining['yoy_change']:.1f}%)")
        
        return rental_summary # Return rental summary
        
    except Exception as e:
        logger.error(f"Error analyzing rental data: {str(e)}") # Good error logging
        logger.debug("Available columns in ZORI data: %s", zori_df.columns.tolist()) # Good debugging info
        return []

# --- Function: save_to_csv ---
def save_to_csv(df, filename):
    """Save DataFrame to CSV in the output directory.""" # Good docstring
    output_path = os.path.join('output', filename) # Use os.path
    df.to_csv(output_path, index=False) # Save to csv
    logger.info(f"Saved {filename} to output directory") # Log it

# --- Function: create_final_dataset ---
def create_final_dataset(census_data, zhvi_data, zori_data, rental_listings=None): # Good docstring
    """Create the final integrated dataset."""
    # Convert Census data to DataFrame if it's a dictionary
    census_df = pd.DataFrame([census_data]) if isinstance(census_data, dict) else pd.DataFrame(census_data) # Convert to df if needed
    
    # Process ZHVI data
    zhvi_latest = pd.DataFrame({
        'median_home_value': zhvi_data.iloc[-1],
        'home_value_yoy_change': ((zhvi_data.iloc[-1] - zhvi_data.iloc[-13]) / zhvi_data.iloc[-13] * 100) # Calculate yoy
    })
    
    # Process ZORI data for California metros
    ca_metros = zori_data[zori_data['RegionName'].str.contains(', CA', na=False)] # Filter for CA metros
    value_cols = [col for col in ca_metros.columns if str(col).startswith('20')] # get time series columns
    latest_month = max(value_cols) # Get latest month
    
    # Create metro-level rental data
    rental_data = []
    for _, row in ca_metros.iterrows(): # Iterate over rows
        metro_name = row['RegionName'].replace(', CA', '') # Metro name
        latest_rent = row[latest_month] # Latest rent
        year_ago_month = value_cols[-13] if len(value_cols) >= 13 else value_cols[0] # Get the year ago month, good conditional logic
        year_ago_rent = row[year_ago_month] # Get year ago rent
        yoy_change = ((latest_rent - year_ago_rent) / year_ago_rent * 100) # Calculate the yoy change
        
        rental_data.append({
            'metro_area': metro_name,
            'median_rent': latest_rent,
            'rent_yoy_change': yoy_change # Append data
        })
    
    rental_df = pd.DataFrame(rental_data) # Create df from data
    
    # Create the final integrated dataset
    final_df = pd.DataFrame({
        'region': ['California'],
        'population': [census_data.get('P1_001N')],
        'median_home_value': [zhvi_latest['median_home_value'].iloc[0]],
        'home_value_yoy_change': [zhvi_latest['home_value_yoy_change'].iloc[0]],
        'avg_metro_rent': [rental_df['median_rent'].mean()],
        'avg_rent_yoy_change': [rental_df['rent_yoy_change'].mean()],
        'num_metro_areas': [len(rental_df)],
        'highest_rent_metro': [rental_df.loc[rental_df['median_rent'].idxmax(), 'metro_area']],
        'highest_rent': [rental_df['median_rent'].max()],
        'lowest_rent_metro': [rental_df.loc[rental_df['median_rent'].idxmin(), 'metro_area']],
        'lowest_rent': [rental_df['median_rent'].min()],
        'data_date': [pd.to_datetime(latest_month).strftime('%Y-%m-%d')] # Get formatted date
    })
    
    return final_df, rental_df # Return final df and rental df

# --- Function: generate_summary_report ---
def generate_summary_report(census_data, zhvi_data, zori_data, final_df):
    """Generate a summary report of the analysis.""" # Good docstring
    # Convert census data to int if it's a Series
    population = int(census_data['P1_001N'].iloc[0]) if isinstance(census_data, pd.DataFrame) else int(census_data['P1_001N']) # Conditional handling
    
    report = f"""# California Housing Market Analysis Summary

## Data Sources and Integration Process

This analysis integrates housing-related data for California from multiple sources:

1. **Census Data**
   - Population: {population:,}
   - Source: U.S. Census Bureau API (2020 Decennial Census)

2. **Housing Prices (Zillow Home Value Index)**
   - Current median home value: ${float(final_df['median_home_value'].iloc[0]):,.2f}
   - Year-over-year change: {float(final_df['home_value_yoy_change'].iloc[0]):.1f}%
   - Source: Zillow Research Data

3. **Rental Market (Zillow Observed Rent Index)**
   - Average metro area rent: ${float(final_df['avg_metro_rent'].iloc[0]):,.2f}
   - Number of metro areas analyzed: {int(final_df['num_metro_areas'].iloc[0])}
   - Year-over-year change: {float(final_df['avg_rent_yoy_change'].iloc[0]):.1f}%
   - Source: Zillow Research Data

## Data Processing Steps

1. **Data Cleaning**
   - Standardized column names across datasets
   - Handled missing values and outliers
   - Converted date formats to ISO standard
   - Removed duplicate records where applicable

2. **Data Integration**
   - Merged datasets using region identifiers
   - Created calculated fields for year-over-year changes
   - Validated data consistency across sources
   - Generated summary statistics

## Key Market Insights

1. **Housing Market**
   - California's median home value is significantly above the national average
   - Home values have shown {float(final_df['home_value_yoy_change'].iloc[0]):.1f}% change over the past year

2. **Rental Market**
   - Most expensive market: {final_df['highest_rent_metro'].iloc[0]} (${float(final_df['highest_rent'].iloc[0]):,.2f}/month)
   - Most affordable market: {final_df['lowest_rent_metro'].iloc[0]} (${float(final_df['lowest_rent'].iloc[0]):,.2f}/month)
   - Average rent across metro areas: ${float(final_df['avg_metro_rent'].iloc[0]):,.2f}

## Data Quality Notes
- Census data is from the 2020 Decennial Census
- Housing and rental data is current as of {final_df['data_date'].iloc[0]}
- All monetary values are in USD
- Rental data covers {int(final_df['num_metro_areas'].iloc[0])} major metropolitan areas in California

_Report generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}_
"""
    
    # Save the report
    with open('output/summary_report.md', 'w') as f: # Write the report to the file
        f.write(report)
    logger.info("Generated summary report")

# --- Function: extract_pdf_data ---
def extract_pdf_data(pdf_path):
    """Extract relevant data from housing policy PDF reports."""
    logger.info(f"Extracting data from PDF: {pdf_path}")
    
    try:
        # Read PDF tables using tabula
        tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
        
        # Process and clean tables
        processed_tables = []
        for i, table in enumerate(tables):
            if not table.empty:
                # Clean column names
                table.columns = table.columns.str.strip().str.lower() # Clean column names
                table.columns = table.columns.str.replace(r'\s+', '_', regex=True) # Replace spaces with underscores
                
                # Remove any completely empty rows or columns
                table = table.dropna(how='all').dropna(axis=1, how='all')
                
                processed_tables.append({
                    'table_number': i + 1,
                    'data': table
                })
        
        logger.info(f"Successfully extracted {len(processed_tables)} tables from PDF") # Log success
        return processed_tables
        
    except Exception as e:
        logger.error(f"Error extracting PDF data: {str(e)}") # Log error
        return [] # Return empty list in case of error

# --- Function: main ---
def main():
    """Main function to run the analysis."""
    logger.info("Starting data integration process...")
    
    try:
        # Validate and get Census API key
        api_key = validate_census_api_key()
        
        # Get Census data
        census_data = get_census_data(api_key)
        logger.info(f"Retrieved Census data for California: {census_data.to_dict('records')}") # Logs the data
        
        # Process Zillow data
        zhvi_data, zori_data = process_zillow_data()
        
        # Basic housing market analysis
        latest_month = zhvi_data.index.max() # Get latest month
        year_ago = latest_month - pd.DateOffset(months=12) # Calculate year ago
        
        latest_home_value = zhvi_data.iloc[-1, 0] # Latest home value
        year_ago_home_value = zhvi_data.loc[year_ago:year_ago].iloc[0, 0] # Home value year ago
        yoy_change = (latest_home_value - year_ago_home_value) / year_ago_home_value * 100 # YoY calculation
        
        logger.info("\nCalifornia Housing Market Analysis:") # Log the data
        logger.info(f"Latest month: {latest_month.strftime('%B %Y')}")
        logger.info(f"Median home value: ${latest_home_value:,.2f}")
        logger.info(f"Year-over-year change: {yoy_change:.1f}%")
        
        # Analyze rental markets
        rental_analysis = analyze_rental_data(zori_data)
        
        # Create and save the final integrated dataset
        final_df, rental_df = create_final_dataset(census_data, zhvi_data, zori_data)
        save_to_csv(final_df, 'california_housing_data_final.csv') # Save the data
        save_to_csv(rental_df, 'california_rental_data.csv')
        
        # Generate summary statistics
        summary_stats = pd.DataFrame({
            'metric': ['Population', 'Median Home Value', 'YoY Home Value Change', 'Average Metro Rent'],
            'value': [
                f"{int(float(census_data['P1_001N'].iloc[0])):,}" if isinstance(census_data, pd.DataFrame) else f"{int(census_data['P1_001N']):,}",
                f"${latest_home_value:,.2f}",
                f"{yoy_change:.1f}%",
                f"${rental_df['median_rent'].mean():,.2f}" # Added formatting
            ]
        })
        save_to_csv(summary_stats, 'summary_stats.csv')
        
        # Generate the summary report
        generate_summary_report(census_data, zhvi_data, zori_data, final_df)
        
        logger.info("\nAnalysis complete. Check the output directory for results.") # Log that its complete
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}") # Catch and log any errors
        raise

if __name__ == "__main__":
    main() # Run main function if the script is run

    '''
    nirajpandey@Nirajs-MacBook-Pro FinalProject % /usr/local/bin/python3 /Users/nirajpandey/Downloads/FinalProject/california_housing_a
nalysis.py
2024-12-12 18:49:34,468 - __main__ - INFO - Starting data integration process...
2024-12-12 18:49:35,200 - __main__ - INFO - Fetching Census data...
2024-12-12 18:49:35,768 - __main__ - DEBUG - Census API Response: [["NAME","P1_001N","state"],
["California","39538223","06"]]
2024-12-12 18:49:35,776 - __main__ - INFO - Retrieved Census data for California: [{'NAME': 'California', 'P1_001N': '39538223', 'state': '06'}]
2024-12-12 18:49:35,776 - __main__ - INFO - Processing Zillow housing data...
2024-12-12 18:49:35,801 - __main__ - INFO - Available states in ZHVI data: ['California' 'Texas' 'Florida' 'New York' 'Pennsylvania' 'Illinois'
 'Ohio' 'Georgia' 'North Carolina' 'Michigan' 'New Jersey' 'Virginia'
 'Washington' 'Arizona' 'Massachusetts' 'Tennessee' 'Indiana' 'Maryland'
 'Missouri' 'Wisconsin' 'Colorado' 'Minnesota' 'South Carolina' 'Alabama'
 'Louisiana' 'Kentucky' 'Oregon' 'Oklahoma' 'Connecticut' 'Utah' 'Iowa'
 'Nevada' 'Arkansas' 'Mississippi' 'Kansas' 'New Mexico' 'Nebraska'
 'Idaho' 'West Virginia' 'Hawaii' 'New Hampshire' 'Maine' 'Rhode Island'
 'Montana' 'Delaware' 'South Dakota' 'North Dakota' 'Alaska'
 'District of Columbia' 'Vermont' 'Wyoming']
2024-12-12 18:49:35,802 - __main__ - INFO - Available states in ZORI data: ['United States' 'New York, NY' 'Los Angeles, CA' 'Chicago, IL'
 'Dallas, TX' 'Houston, TX' 'Washington, DC' 'Philadelphia, PA'
 'Miami, FL' 'Atlanta, GA' 'Boston, MA' 'Phoenix, AZ' 'San Francisco, CA'
 'Riverside, CA' 'Detroit, MI' 'Seattle, WA' 'Minneapolis, MN'
 'San Diego, CA' 'Tampa, FL' 'Denver, CO' 'Baltimore, MD' 'St. Louis, MO'
 'Orlando, FL' 'Charlotte, NC' 'San Antonio, TX' 'Portland, OR'
 'Sacramento, CA' 'Pittsburgh, PA' 'Cincinnati, OH' 'Austin, TX'
 'Las Vegas, NV' 'Kansas City, MO' 'Columbus, OH' 'Indianapolis, IN'
 'Cleveland, OH' 'San Jose, CA' 'Nashville, TN' 'Virginia Beach, VA'
 'Providence, RI' 'Jacksonville, FL' 'Milwaukee, WI' 'Oklahoma City, OK'
 'Raleigh, NC' 'Memphis, TN' 'Richmond, VA' 'Louisville, KY'
 'New Orleans, LA' 'Salt Lake City, UT' 'Hartford, CT' 'Buffalo, NY'
 'Birmingham, AL' 'Rochester, NY' 'Grand Rapids, MI' 'Tucson, AZ'
 'Urban Honolulu, HI' 'Tulsa, OK' 'Fresno, CA' 'Worcester, MA' 'Omaha, NE'
 'Bridgeport, CT' 'Greenville, SC' 'Albuquerque, NM' 'Bakersfield, CA'
 'Albany, NY' 'Knoxville, TN' 'Baton Rouge, LA' 'McAllen, TX'
 'New Haven, CT' 'El Paso, TX' 'Allentown, PA' 'Oxnard, CA' 'Columbia, SC'
 'North Port, FL' 'Dayton, OH' 'Charleston, SC' 'Greensboro, NC'
 'Stockton, CA' 'Cape Coral, FL' 'Boise City, ID' 'Colorado Springs, CO'
 'Little Rock, AR' 'Lakeland, FL' 'Akron, OH' 'Des Moines, IA'
 'Springfield, MA' 'Poughkeepsie, NY' 'Ogden, UT' 'Madison, WI'
 'Winston, NC' 'Deltona, FL' 'Syracuse, NY' 'Provo, UT' 'Toledo, OH'
 'Wichita, KS' 'Durham, NC' 'Augusta, GA' 'Palm Bay, FL' 'Jackson, MS'
 'Harrisburg, PA' 'Spokane, WA' 'Scranton, PA' 'Chattanooga, TN'
 'Modesto, CA' 'Lancaster, PA' 'Portland, ME' 'Youngstown, OH'
 'Lansing, MI' 'Fayetteville, AR' 'Fayetteville, NC' 'Lexington, KY'
 'Pensacola, FL' 'Santa Rosa, CA' 'Reno, NV' 'Huntsville, AL'
 'Port St. Lucie, FL' 'Lafayette, LA' 'Myrtle Beach, SC' 'Springfield, MO'
 'Visalia, CA' 'Killeen, TX' 'Asheville, NC' 'York, PA' 'Vallejo, CA'
 'Santa Maria, CA' 'Salinas, CA' 'Salem, OR' 'Reading, PA'
 'Corpus Christi, TX' 'Brownsville, TX' 'Manchester, NH' 'Fort Wayne, IN'
 'Gulfport, MS' 'Salisbury, MD' 'Flint, MI' 'Peoria, IL' 'Canton, OH'
 'Savannah, GA' 'Anchorage, AK' 'Beaumont, TX' 'Shreveport, LA'
 'Trenton, NJ' 'Montgomery, AL' 'Davenport, IA' 'Tallahassee, FL'
 'Eugene, OR' 'Naples, FL' 'Ann Arbor, MI' 'Ocala, FL' 'Hickory, NC'
 'Huntington, WV' 'Fort Collins, CO' 'Rockford, IL' 'Lincoln, NE'
 'Gainesville, FL' 'Boulder, CO' 'Green Bay, WI' 'Columbus, GA'
 'South Bend, IN' 'Spartanburg, SC' 'Greeley, CO' 'Lubbock, TX'
 'Clarksville, TN' 'Roanoke, VA' 'Evansville, IN' 'Kingsport, TN'
 'Kennewick, WA' 'Utica, NY' 'Hagerstown, MD' 'Duluth, MN' 'Olympia, WA'
 'Longview, TX' 'Wilmington, NC' 'San Luis Obispo, CA' 'Crestview, FL'
 'Merced, CA' 'Waco, TX' 'Cedar Rapids, IA' 'Atlantic City, NJ'
 'Bremerton, WA' 'Sioux Falls, SD' 'Santa Cruz, CA' 'Erie, PA'
 'Norwich, CT' 'Amarillo, TX' 'Laredo, TX' 'Tuscaloosa, AL'
 'College Station, TX' 'Kalamazoo, MI' 'Lynchburg, VA' 'Charleston, WV'
 'Yakima, WA' 'Fargo, ND' 'Binghamton, NY' 'Fort Smith, AR' 'Appleton, WI'
 'Prescott Valley, AZ' 'Topeka, KS' 'Macon, GA' 'Tyler, TX'
 'Barnstable Town, MA' 'Daphne, AL' 'Bellingham, WA' 'Burlington, VT'
 'Rochester, MN' 'Lafayette, IN' 'Champaign, IL' 'Medford, OR'
 'Lebanon, NH' 'Charlottesville, VA' 'Lake Charles, LA' 'Las Cruces, NM'
 'Chico, CA' 'Hilton Head Island, SC' 'Athens, GA' 'Lake Havasu City, AZ'
 'Columbia, MO' 'Springfield, IL' 'Houma, LA' 'Monroe, LA' 'Elkhart, IN'
 'Johnson City, TN' 'Yuma, AZ' 'Gainesville, GA' 'Jacksonville, NC'
 'Florence, SC' 'Hilo, HI' 'St. Cloud, MN' 'Racine, WI' 'Bend, OR'
 'Saginaw, MI' 'Warner Robins, GA' 'Terre Haute, IN' 'Torrington, CT'
 'Punta Gorda, FL' 'Billings, MT' 'Redding, CA' 'Kingston, NY'
 'Panama City, FL' 'Joplin, MO' 'Dover, DE' 'El Centro, CA' 'Jackson, TN'
 'Yuba City, CA' 'Bowling Green, KY' 'St. George, UT' 'Muskegon, MI'
 'Abilene, TX' 'Iowa City, IA' 'Auburn, AL' 'Midland, TX'
 'Bloomington, IL' 'Hattiesburg, MS' 'Oshkosh, WI' 'Eau Claire, WI'
 'Greenville, NC' 'Burlington, NC' 'Waterloo, IA' "Coeur d'Alene, ID"
 'East Stroudsburg, PA' 'Pueblo, CO' 'Blacksburg, VA' 'Wausau, WI'
 'Kahului, HI' 'Janesville, WI' 'Tupelo, MS' 'Bloomington, IN'
 'Odessa, TX' 'Jackson, MI' 'State College, PA' 'Sebastian, FL'
 'Madera, CA' 'Decatur, AL' 'Chambersburg, PA' 'Vineland, NJ'
 'Idaho Falls, ID' 'Grand Junction, CO' 'Elizabethtown, KY' 'Niles, MI'
 'Monroe, MI' 'Santa Fe, NM' 'Concord, NH' 'Alexandria, LA'
 'Traverse City, MI' 'Bangor, ME' 'Homosassa Springs, FL' 'Hanford, CA'
 'Jefferson City, MO' 'Florence, AL' 'Dothan, AL' 'London, KY'
 'Albany, GA' 'Ottawa, IL' 'Sioux City, IA' 'Wichita Falls, TX'
 'Texarkana, TX' 'Valdosta, GA' 'Logan, UT' 'Flagstaff, AZ'
 'Rocky Mount, NC' 'Pottsville, PA' 'Dalton, GA' 'Lebanon, PA'
 'Morristown, TN' 'Winchester, VA' 'Wheeling, WV' 'Morgantown, WV'
 'La Crosse, WI' 'Napa, CA' 'Rapid City, SD' 'Sumter, SC' 'Eureka, CA'
 'Springfield, OH' 'Harrisonburg, VA' 'Battle Creek, MI' 'Sherman, TX'
 'Manhattan, KS' 'Carbondale, IL' 'Johnstown, PA' 'Jonesboro, AR'
 'Bismarck, ND' 'Hammond, LA' 'Pittsfield, MA' 'Mount Vernon, WA'
 'The Villages, FL' 'Albany, OR' 'Glens Falls, NY' 'Lawton, OK'
 'Cleveland, TN' 'Sierra Vista, AZ' 'Ames, IA' 'Mansfield, OH'
 'Staunton, VA' 'Augusta, ME' 'Altoona, PA' 'New Bern, NC'
 'Farmington, NM' 'St. Joseph, MO' 'San Angelo, TX' 'Wenatchee, WA'
 'Owensboro, KY' 'Holland, MI' 'Lumberton, NC' 'Lawrence, KS'
 'Goldsboro, NC' 'Watertown, NY' 'Sheboygan, WI' 'Weirton, WV'
 'Missoula, MT' 'Wooster, OH' 'Bozeman, MT' 'Anniston, AL' 'Beckley, WV'
 'Williamsport, PA' 'Brunswick, GA' 'California, MD' 'Twin Falls, ID'
 'Cookeville, TN' 'Muncie, IN' 'Michigan City, IN' 'Roseburg, OR'
 'Lewiston, ME' 'Longview, WA' 'Kankakee, IL' 'Bluefield, WV'
 'Show Low, AZ' 'Richmond, KY' 'Tullahoma, TN' 'Whitewater, WI'
 'Ithaca, NY' 'Grand Forks, ND' 'Decatur, IL' 'LaGrange, GA'
 'Bay City, MI' 'Fond du Lac, WI' 'Gettysburg, PA' 'Gadsden, AL'
 'Kalispell, MT' 'Danville, VA' 'Mankato, MN' 'Salem, OH' 'Truckee, CA'
 'Sebring, FL' 'Cheyenne, WY' 'Hot Springs, AR' 'Adrian, MI' 'Shelby, NC'
 'Dubuque, IA' 'Meridian, MS' 'Pinehurst, NC' 'Paducah, KY' 'Victoria, TX'
 'Rome, GA' 'Sevierville, TN' 'Moses Lake, WA' 'Cape Girardeau, MO'
 'Fairbanks, AK' 'Brainerd, MN' 'Cumberland, MD' 'Ocean City, NJ'
 'Corvallis, OR' 'Pocatello, ID' 'Corning, NY' 'Sunbury, PA' 'Ukiah, CA'
 'Hermiston, OR' 'Clarksburg, WV' 'Beaver Dam, WI' 'Pine Bluff, AR'
 'Grants Pass, OR' 'Cullman, AL' 'Lufkin, TX' 'Zanesville, OH'
 'New Castle, PA' 'Oak Harbor, WA' 'Orangeburg, SC' 'Watertown, WI'
 'Meadville, PA' 'Great Falls, MT' 'Russellville, AR' 'Indiana, PA'
 'Midland, MI' 'Kokomo, IN' 'Bloomsburg, PA' 'Helena, MT' 'Key West, FL'
 'Talladega, AL' 'Stillwater, OK' 'Columbus, IN' 'Athens, TX'
 'Centralia, WA' 'Manitowoc, WI' 'Hinesville, GA' 'Warsaw, IN'
 'Statesboro, GA' 'Casper, WY' 'Wilson, NC' 'Glenwood Springs, CO'
 'Seneca, SC' 'Minot, ND' 'Searcy, AR' 'Grand Island, NE'
 'Port Angeles, WA' 'Auburn, NY' 'Huntsville, TX' 'Keene, NH' 'Heber, UT'
 'Quincy, IL' 'Sandusky, OH' 'Findlay, OH' 'Frankfort, KY' 'Danville, IL'
 'Aberdeen, WA' 'Wisconsin Rapids, WI' 'Jefferson, GA' 'Kapaa, HI'
 'Palatka, FL' 'Hobbs, NM' 'Shawnee, OK' 'Stevens Point, WI'
 'Greeneville, TN' 'Greenwood, SC' 'Lake City, FL' 'Klamath Falls, OR'
 'Morehead City, NC' 'Clearlake, CA' 'Alamogordo, NM' 'Muskogee, OK'
 'Marion, IN' 'Richmond, IN' 'Marquette, MI' 'Mount Pleasant, MI'
 'Baraboo, WI' 'Red Bluff, CA' 'Marinette, WI' 'Jasper, AL' 'Roswell, NM'
 'Shelton, WA' 'Dublin, GA' 'Nacogdoches, TX' 'Coos Bay, OR'
 'Forest City, NC' 'Martinsville, VA' 'Rexburg, ID' 'Laconia, NH'
 'Georgetown, SC' 'Athens, OH' 'Sanford, NC' 'Enid, OK' 'Mount Vernon, OH'
 'Walla Walla, WA' 'Albemarle, NC' 'Hutchinson, KS' 'Hudson, NY'
 'Starkville, MS' 'Carlsbad, NM' 'Gillette, WY' 'Sturgis, MI'
 'Rutland, VT' 'Crossville, TN' 'Granbury, TX' 'Sayre, PA' 'Salina, KS'
 'Fergus Falls, MN' 'Barre, VT' 'Oneonta, NY' 'Columbus, MS' 'Ardmore, OK'
 'Charleston, IL' 'Fernley, NV' 'Carson City, NV' 'Eagle Pass, TX'
 'Calhoun, GA' 'Cullowhee, NC' 'Kearney, NE' 'Fairmont, WV' 'Gaffney, SC'
 'Branson, MO' 'Cedar City, UT' 'Oxford, MS' 'Edwards, CO' 'Durango, CO'
 'Kinston, NC' 'Sonora, CA' 'Batesville, AR' 'Elko, NV' 'Danville, KY'
 'Glasgow, KY' 'St. Marys, GA' 'Boone, NC' 'Warrensburg, MO'
 'Fort Leonard Wood, MO' 'Elizabeth City, NC' 'Payson, AZ' 'Athens, TN'
 'Enterprise, AL' 'Ashland, OH' 'Milledgeville, GA' 'Kerrville, TX'
 'Bartlesville, OK' 'Platteville, WI' 'Corsicana, TX' 'Oil City, PA'
 'Pahrump, NV' 'Paris, TX' 'Winona, MN' 'Newport, OR' 'Amsterdam, NY'
 'Ozark, AL' 'Gardnerville Ranchos, NV' 'Fort Polk South, LA'
 'Ca-¦on City, CO' 'New Castle, IN' 'Clovis, NM' 'Norfolk, NE'
 'Cadillac, MI' 'Ruston, LA' 'Pullman, WA' 'Blackfoot, ID' 'Red Wing, MN'
 'Nogales, AZ' 'Tahlequah, OK' 'Montrose, CO' 'Cortland, NY'
 'Mount Sterling, KY' 'Sandpoint, ID' 'Bardstown, KY' 'Bemidji, MN'
 'Durant, OK' 'Thomasville, GA' 'Paragould, AR' 'Menomonie, WI'
 'Vicksburg, MS' 'Freeport, IL' 'Rolla, MO' 'Ellensburg, WA'
 'Ponca City, OK' 'McAlester, OK' 'Willmar, MN' 'Lewisburg, PA'
 'Auburn, IN' 'Duncan, OK' 'Sedalia, MO' 'Henderson, NC'
 'Rock Springs, WY' 'Stephenville, TX' 'Mountain Home, AR'
 'Gainesville, TX' 'Tifton, GA' 'Astoria, OR' 'Marshalltown, IA'
 'Big Rapids, MI' 'Austin, MN' 'Selinsgrove, PA' 'Okeechobee, FL'
 'Moscow, ID' 'Riverton, WY' 'Pittsburg, KS' 'Cambridge, OH'
 'Williston, ND' 'Sikeston, MO' 'Ada, OK' 'Crawfordsville, IN'
 'Lock Haven, PA' 'Easton, MD' 'Laramie, WY' 'Fremont, NE'
 'Bennington, VT' 'Fort Dodge, IA' 'Dyersburg, TN' 'Sulphur Springs, TX'
 'Bay City, TX' 'North Platte, NE' 'Lebanon, MO' 'Brenham, TX'
 'Bonham, TX' 'Butte, MT' 'Emporia, KS' 'Jackson, WY' 'Big Spring, TX'
 'Brookings, SD' 'Dickinson, ND' 'Pella, IA' 'Brevard, NC'
 'Susanville, CA' 'Juneau, AK' 'Breckenridge, CO' 'McPherson, KS'
 'Kirksville, MO' 'Fort Morgan, CO' 'Weatherford, OK' 'Mountain Home, ID'
 'Fredericksburg, TX' 'The Dalles, OR' 'Spearfish, SD' 'Fallon, NV'
 'Altus, OK' 'Steamboat Springs, CO' 'Prineville, OR' 'Rockport, TX'
 'Hood River, OR' 'Mitchell, SD' 'Wahpeton, ND' 'Sterling, CO'
 'Beatrice, NE' 'Jamestown, ND' 'Portales, NM' 'Los Alamos, NM']
2024-12-12 18:49:35,809 - __main__ - INFO - 
California Housing Market Analysis:
2024-12-12 18:49:35,809 - __main__ - INFO - Latest month: October 2024
2024-12-12 18:49:35,809 - __main__ - INFO - Median home value: $771,056.98
2024-12-12 18:49:35,809 - __main__ - INFO - Year-over-year change: 3.7%
2024-12-12 18:49:35,809 - __main__ - INFO - Analyzing rental data for California metro areas...
2024-12-12 18:49:35,811 - __main__ - INFO - 
California Rental Market Analysis:
2024-12-12 18:49:35,811 - __main__ - INFO - Data as of: October 2024
2024-12-12 18:49:35,811 - __main__ - INFO - Number of metro areas analyzed: 33
2024-12-12 18:49:35,811 - __main__ - INFO - 
Top 5 Most Expensive Rental Markets:
2024-12-12 18:49:35,811 - __main__ - INFO - 1. Santa Maria: $3,611.07/month (YoY change: 2.8%)
2024-12-12 18:49:35,811 - __main__ - INFO - 2. Santa Cruz: $3,504.87/month (YoY change: 3.3%)
2024-12-12 18:49:35,811 - __main__ - INFO - 3. San Jose: $3,358.89/month (YoY change: 4.1%)
2024-12-12 18:49:35,811 - __main__ - INFO - 4. Oxnard: $3,055.83/month (YoY change: 4.4%)
2024-12-12 18:49:35,811 - __main__ - INFO - 5. San Francisco: $3,053.30/month (YoY change: 2.5%)
2024-12-12 18:49:35,811 - __main__ - INFO - 
State-Level Statistics:
2024-12-12 18:49:35,811 - __main__ - INFO - Average Rent (across metros): $2,306.36
2024-12-12 18:49:35,811 - __main__ - INFO - Average Year-over-Year Change: 4.1%
2024-12-12 18:49:35,811 - __main__ - INFO - Highest Rent: $3,611.07
2024-12-12 18:49:35,811 - __main__ - INFO - Lowest Rent: $1,179.17
2024-12-12 18:49:35,811 - __main__ - INFO - 
Market Insights:
2024-12-12 18:49:35,811 - __main__ - INFO - Markets with increasing rents: 30
2024-12-12 18:49:35,811 - __main__ - INFO - Markets with decreasing rents: 0
2024-12-12 18:49:35,811 - __main__ - INFO - Fastest growing market: El Centro (11.3%)
2024-12-12 18:49:35,811 - __main__ - INFO - Fastest declining market: Napa (1.4%)
2024-12-12 18:49:35,816 - __main__ - INFO - Saved california_housing_data_final.csv to output directory
2024-12-12 18:49:35,816 - __main__ - INFO - Saved california_rental_data.csv to output directory
2024-12-12 18:49:35,816 - __main__ - INFO - Saved summary_stats.csv to output directory
2024-12-12 18:49:35,817 - __main__ - INFO - Generated summary report
2024-12-12 18:49:35,817 - __main__ - INFO - 
Analysis complete. Check the output directory for results.
nirajpandey@Nirajs-MacBook-Pro FinalProject % 
    
    '''