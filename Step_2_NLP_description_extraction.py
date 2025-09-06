#!/usr/bin/env python3
"""
Description Extraction Script
Uses the 'Details Link' column to scrape full property descriptions
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PropertyDescriptionScraper:
    def __init__(self, delay=1):
        self.delay = delay  # Delay between requests (be respectful)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def extract_description_from_url(self, url):
        """Extract property description from a real estate listing URL"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Common selectors for property descriptions on major sites
            description_selectors = [
                # Zillow
                '.Text-c11n-8-84-3__sc-aiai24-0.dpf__sc-1me6wg9-0.jHtYqh.hBiUwn',
                '[data-testid="description-text"]',
                '.ds-overview-section',
                
                # Redfin  
                '.remarks',
                '.listing-description',
                
                # Realtor.com
                '.ldp-property-description',
                '.property-description',
                
                # Generic fallbacks
                '.description',
                '.property-details',
                '.listing-details',
                '[class*="description"]',
                '[class*="remarks"]',
                '[class*="details"]'
            ]
            
            description = ""
            for selector in description_selectors:
                elements = soup.select(selector)
                if elements:
                    # Take the longest text found
                    texts = [elem.get_text(strip=True) for elem in elements]
                    longest_text = max(texts, key=len) if texts else ""
                    if len(longest_text) > len(description):
                        description = longest_text
            
            # Clean up the description
            description = self.clean_description(description)
            
            return description if len(description) > 50 else "No description found"
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return "Error extracting description"
    
    def clean_description(self, text):
        """Clean and standardize the extracted description"""
        if not text:
            return ""
        
        # Remove extra whitespace and newlines
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common prefixes/suffixes
        prefixes_to_remove = [
            "Property Description:", "Description:", "About this property:",
            "Property Details:", "Listed by:", "Listing provided by:"
        ]
        
        for prefix in prefixes_to_remove:
            if text.startswith(prefix):
                text = text[len(prefix):].strip()
        
        # Truncate very long descriptions (keep first 2000 characters)
        if len(text) > 2000:
            text = text[:2000] + "..."
        
        return text.strip()

def extract_nlp_features(description):
    """Extract NLP features from property description"""
    if not description or description == "No description found":
        return {
            'has_luxury_keywords': 0,
            'has_renovation_keywords': 0,
            'has_view_keywords': 0,
            'has_outdoor_keywords': 0,
            'has_modern_keywords': 0,
            'has_location_keywords': 0
        }
    
    desc_lower = description.lower()
    
    # Define keyword categories
    luxury_keywords = ['luxury', 'luxurious', 'high-end', 'premium', 'upscale', 'elegant', 'sophisticated', 'custom', 'designer']
    renovation_keywords = ['renovated', 'updated', 'remodeled', 'new', 'fresh', 'modern', 'contemporary', 'upgraded']
    view_keywords = ['view', 'views', 'overlook', 'scenic', 'panoramic', 'mountain', 'ocean', 'city lights']
    outdoor_keywords = ['yard', 'garden', 'patio', 'deck', 'pool', 'outdoor', 'landscaped', 'backyard']
    modern_keywords = ['smart home', 'stainless steel', 'granite', 'hardwood', 'marble', 'quartz', 'tile']
    location_keywords = ['walking distance', 'close to', 'near', 'convenient', 'accessible', 'commute', 'downtown']
    
    return {
        'has_luxury_keywords': 1 if any(keyword in desc_lower for keyword in luxury_keywords) else 0,
        'has_renovation_keywords': 1 if any(keyword in desc_lower for keyword in renovation_keywords) else 0,
        'has_view_keywords': 1 if any(keyword in desc_lower for keyword in view_keywords) else 0,
        'has_outdoor_keywords': 1 if any(keyword in desc_lower for keyword in outdoor_keywords) else 0,
        'has_modern_keywords': 1 if any(keyword in desc_lower for keyword in modern_keywords) else 0,
        'has_location_keywords': 1 if any(keyword in desc_lower for keyword in location_keywords) else 0
    }

def main():
    print("=== Property Description Extraction Script ===")
    
    # Load the dataset
    try:
        df = pd.read_csv('/Users/tiffanyellis/Documents/my_projects/Housing Prices Project/data/dataset2.csv')
        print(f"Loaded dataset with {len(df)} properties")
    except FileNotFoundError:
        print("Error: Could not find dataset2.csv")
        return
    
    # Check if Details Link column exists
    if 'Details Link' not in df.columns:
        print("Error: 'Details Link' column not found in dataset")
        print(f"Available columns: {list(df.columns)}")
        return
    
    # Filter out rows with missing URLs
    df_with_links = df.dropna(subset=['Details Link']).copy()
    print(f"Found {len(df_with_links)} properties with detail links")
    
    if len(df_with_links) == 0:
        print("No properties with detail links found!")
        return
    
    # Initialize scraper
    scraper = PropertyDescriptionScraper(delay=2)  # 2 second delay between requests
    
    # Extract descriptions
    print("\nExtracting property descriptions...")
    descriptions = []
    nlp_features_list = []
    
    for i, (idx, row) in enumerate(df_with_links.iterrows()):
        url = row['Details Link']
        
        print(f"Processing {i+1}/{len(df_with_links)}: {row.get('Address', 'Unknown Address')}")
        
        # Extract description
        description = scraper.extract_description_from_url(url)
        descriptions.append(description)
        
        # Extract NLP features
        nlp_features = extract_nlp_features(description)
        nlp_features_list.append(nlp_features)
        
        # Progress update
        if (i + 1) % 10 == 0:
            print(f"Processed {i+1} properties...")
        
        # Be respectful with delays
        if i < len(df_with_links) - 1:  # Don't delay after the last request
            time.sleep(scraper.delay)
    
    # Add descriptions to dataframe
    df_with_links['property_description'] = descriptions
    
    # Add NLP features
    nlp_df = pd.DataFrame(nlp_features_list)
    for col in nlp_df.columns:
        df_with_links[col] = nlp_df[col]
    
    # Show results
    print(f"\n=== Extraction Results ===")
    print(f"Successfully extracted {len(descriptions)} descriptions")
    
    # Count descriptions by quality
    valid_descriptions = [d for d in descriptions if d != "No description found" and d != "Error extracting description"]
    error_descriptions = [d for d in descriptions if d == "Error extracting description"]
    missing_descriptions = [d for d in descriptions if d == "No description found"]
    
    print(f"Valid descriptions: {len(valid_descriptions)}")
    print(f"Missing descriptions: {len(missing_descriptions)}")
    print(f"Error descriptions: {len(error_descriptions)}")
    
    # Show NLP feature stats
    print(f"\n=== NLP Feature Stats ===")
    for col in nlp_df.columns:
        count = nlp_df[col].sum()
        pct = (count / len(nlp_df)) * 100
        print(f"{col}: {count}/{len(nlp_df)} ({pct:.1f}%)")
    
    # Show sample descriptions
    print(f"\n=== Sample Descriptions ===")
    for i, desc in enumerate(valid_descriptions[:3]):
        address = df_with_links.iloc[i]['Address'] if i < len(df_with_links) else 'Unknown'
        print(f"\n{i+1}. {address}")
        print(f"Description: {desc[:200]}{'...' if len(desc) > 200 else ''}")
    
    # Save enhanced dataset
    output_file = '/Users/tiffanyellis/Documents/my_projects/Housing Prices Project/data/dataset_with_descriptions.csv'
    df_with_links.to_csv(output_file, index=False)
    print(f"\n=== Saved enhanced dataset to: {output_file} ===")
    
    # Merge back with original dataset (for properties without links)
    print("\nMerging with original dataset...")
    
    # Create description columns in original dataset
    df['property_description'] = ""
    for col in nlp_df.columns:
        df[col] = 0
    
    # Update with extracted data
    for idx in df_with_links.index:
        if idx in df.index:
            df.loc[idx, 'property_description'] = df_with_links.loc[idx, 'property_description']
            for col in nlp_df.columns:
                df.loc[idx, col] = df_with_links.loc[idx, col]
    
    # Save complete dataset
    complete_output_file = '/Users/tiffanyellis/Documents/my_projects/Housing Prices Project/data/dataset2_with_descriptions.csv'
    df.to_csv(complete_output_file, index=False)
    print(f"Saved complete dataset to: {complete_output_file}")
    
    print("\n=== Description Extraction Complete! ===")
    print("Next step: Run the location enhancement script")

if __name__ == "__main__":
    main()