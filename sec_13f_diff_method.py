import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import json
from typing import List, Dict, Optional
import re


class Form13FFetcher:
    def __init__(self, user_agent: str = 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.69 Safari/537.36'): #"email@company.com"):
        """
        Initialize the Form 13F fetcher

        Args:
            user_agent: Required by SEC - should include company name and email
        """
        self.base_url = "https://www.sec.gov/Archives/edgar/data"
        self.search_url = "https://efts.sec.gov/LATEST/search-index"
        # self.headers = {
        #     'User-Agent': user_agent,
        #     'Accept-Encoding': 'gzip, deflate',
        #     'Host': 'www.sec.gov'
        # }
        self.headers = {'Accept': '*/*',
           'Accept-Language': 'en-US,en;q=0.5',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.69 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'}
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def search_company_filings(self, cik: str, form_type: str = "13F-HR",
                               date_before: Optional[str] = None,
                               count: int = 10) -> List[Dict]:
        """
        Search for company filings using SEC's search API

        Args:
            cik: Central Index Key (company identifier)
            form_type: Type of form (13F-HR for holdings, 13F-NT for notice)
            date_before: Date in YYYY-MM-DD format
            count: Number of filings to retrieve

        Returns:
            List of filing information dictionaries
        """
        # Ensure CIK is 10 digits with leading zeros
        cik = str(cik).zfill(10)

        params = {
            'category': 'custom',
            'ciks': cik,
            'forms': form_type,
            'count': count
        }

        if date_before:
            params['dateb'] = date_before

        try:
            response = self.session.get(self.search_url, params=params)
            response.raise_for_status()

            data = response.json()
            return data.get('hits', {}).get('hits', [])

        except Exception as e:
            print(f"Error searching filings: {e}")
            return []

    def get_filing_documents(self, accession_number: str, cik: str) -> Dict:
        """
        Get the list of documents for a specific filing

        Args:
            accession_number: SEC accession number (e.g., "0001326801-23-000006")
            cik: Central Index Key

        Returns:
            Dictionary containing filing information and document URLs
        """
        cik = str(cik).zfill(10)
        # Remove hyphens from accession number for URL
        acc_no_clean = accession_number.replace('-', '')

        filing_url = f"{self.base_url}/{cik}/{acc_no_clean}/{accession_number}-index.json"

        try:
            response = self.session.get(filing_url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching filing documents: {e}")
            return {}

    def parse_13f_xml(self, xml_content: str) -> pd.DataFrame:
        """
        Parse 13F XML content and extract holdings information

        Args:
            xml_content: Raw XML content from 13F filing

        Returns:
            DataFrame containing holdings information
        """
        try:
            root = ET.fromstring(xml_content)

            # Find the information table
            holdings = []

            # Different XML structures exist, try common patterns
            info_table = root.find('.//infoTable') or root.find('.//informationTable')

            if info_table is not None:
                for entry in info_table.findall('.//infoTable') or info_table.findall('.//holding'):
                    holding = {}

                    # Extract common fields
                    name_elem = entry.find('.//nameOfIssuer') or entry.find('.//issuerName')
                    if name_elem is not None:
                        holding['issuer_name'] = name_elem.text

                    cusip_elem = entry.find('.//cusip')
                    if cusip_elem is not None:
                        holding['cusip'] = cusip_elem.text

                    shares_elem = entry.find('.//sshPrnamt') or entry.find('.//sharesOrPrincipalAmount')
                    if shares_elem is not None:
                        holding['shares'] = shares_elem.text

                    value_elem = entry.find('.//value')
                    if value_elem is not None:
                        holding['value'] = int(value_elem.text) * 1000  # SEC reports in thousands

                    if holding:  # Only add if we found some data
                        holdings.append(holding)

            return pd.DataFrame(holdings)

        except Exception as e:
            print(f"Error parsing XML: {e}")
            return pd.DataFrame()

    def fetch_13f_data(self, cik: str, num_filings: int = 1) -> List[pd.DataFrame]:
        """
        Fetch and parse 13F data for a company

        Args:
            cik: Central Index Key
            num_filings: Number of recent filings to fetch

        Returns:
            List of DataFrames containing holdings data
        """
        print(f"Searching for 13F filings for CIK: {cik}")

        # Search for filings
        filings = self.search_company_filings(cik, count=num_filings)

        if not filings:
            print("No filings found")
            return []

        results = []

        for filing in filings:
            try:
                source = filing.get('_source', {})
                accession_num = source.get('accession_number')
                file_date = source.get('file_date')

                print(f"Processing filing: {accession_num} from {file_date}")

                # Get filing documents
                docs = self.get_filing_documents(accession_num, cik)

                if not docs:
                    continue

                # Find the primary 13F document (usually ends with .xml)
                primary_doc = None
                for doc in docs.get('directory', {}).get('item', []):
                    if doc.get('name', '').endswith('.xml') and '13F' in doc.get('description', ''):
                        primary_doc = doc.get('name')
                        break

                if primary_doc:
                    # Construct document URL
                    acc_no_clean = accession_num.replace('-', '')
                    doc_url = f"{self.base_url}/{str(cik).zfill(10)}/{acc_no_clean}/{primary_doc}"

                    # Fetch and parse the document
                    response = self.session.get(doc_url)
                    response.raise_for_status()

                    df = self.parse_13f_xml(response.text)
                    if not df.empty:
                        df['filing_date'] = file_date
                        df['accession_number'] = accession_num
                        results.append(df)

                # Be respectful to SEC servers
                time.sleep(0.1)

            except Exception as e:
                print(f"Error processing filing {accession_num}: {e}")
                continue

        return results


# Example usage and utility functions
def get_company_cik(company_name: str) -> Optional[str]:
    """
    Helper function to search for a company's CIK
    Note: This is a simplified search - in practice you might want to use
    the SEC's company tickers file for more accurate matching
    """
    search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    params = {
        'company': company_name,
        'match': 'contains',
        'action': 'getcompany'
    }

    try:
        response = requests.get(search_url, params=params)
        # This would require HTML parsing to extract CIK
        # For now, return None and suggest manual CIK lookup
        return None
    except:
        return None


# Main execution example
if __name__ == "__main__":
    # Initialize fetcher with your contact info (required by SEC)
    fetcher = Form13FFetcher("YourCompany contact@yourcompany.com")

    # Example CIKs for well-known institutions:
    # Berkshire Hathaway: 1067983
    # Vanguard: 102909
    # BlackRock: 1364742

    # Fetch Berkshire Hathaway's recent 13F filings
    cik = "1067983"  # Berkshire Hathaway

    try:
        holdings_data = fetcher.fetch_13f_data(cik, num_filings=2)

        if holdings_data:
            print(f"\nFound {len(holdings_data)} filings")

            for i, df in enumerate(holdings_data):
                print(f"\nFiling {i + 1}:")
                print(f"Date: {df['filing_date'].iloc[0] if not df.empty else 'Unknown'}")
                print(f"Holdings count: {len(df)}")

                if not df.empty:
                    # Show top 10 holdings by value
                    if 'value' in df.columns:
                        top_holdings = df.nlargest(10, 'value')[['issuer_name', 'value', 'shares']]
                        print("\nTop 10 holdings by value:")
                        print(top_holdings.to_string(index=False))
                    else:
                        print("\nSample holdings:")
                        print(df.head().to_string(index=False))

                # Save to CSV
                filename = f"13f_holdings_{cik}_filing_{i + 1}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved to {filename}")
        else:
            print("No 13F data found")

    except Exception as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure you have an internet connection")
        print("2. Update the User-Agent with your actual contact information")
        print("3. Check if the CIK is correct")
        print("4. The SEC may be experiencing high traffic - try again later")
