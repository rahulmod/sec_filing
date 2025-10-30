import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import re
from typing import List, Dict, Optional


class Form13DFetcher:
    def __init__(self, user_agent: str = "Your Name your.email@example.com"):
        """
        Initialize the Form 13D fetcher.

        Args:
            user_agent: Required by SEC - should include your name and email
        """
        self.base_url = "https://data.sec.gov"
        # self.headers = {
        #     "User-Agent": user_agent,
        #     "Accept-Encoding": "gzip, deflate",
        #     "Host": "data.sec.gov"
        # }

        self.headers = {'Accept': '*/*',
           'Accept-Language': 'en-US,en;q=0.5',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.69 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'}

        # Known institutional investor patterns
        self.institutional_keywords = [
            'fund', 'capital', 'management', 'partners', 'holdings', 'investment',
            'advisors', 'asset', 'trust', 'group', 'llc', 'lp', 'corp', 'inc',
            'pension', 'endowment', 'foundation', 'insurance', 'bank'
        ]

    def search_all_institutional_filings(self,
                                         start_date: Optional[str] = None,
                                         end_date: Optional[str] = None,
                                         max_results: int = 1000) -> List[Dict]:
        """
        Search for all Form 13D filings from institutional investors using the RSS feed.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_results: Maximum number of results to return

        Returns:
            List of filing information dictionaries
        """
        filings = []

        # Use the daily index files to get comprehensive data
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        print(f"Searching for 13D filings from {start_date} to {end_date}")

        # Generate date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        current_dt = start_dt

        while current_dt <= end_dt and len(filings) < max_results:
            date_str = current_dt.strftime('%Y%m%d')
            year = current_dt.strftime('%Y')
            quarter = f"QTR{((current_dt.month - 1) // 3) + 1}"

            # Try to get daily index
            index_url = f"{self.base_url}/Archives/edgar/daily-index/{year}/{quarter}/master.{date_str}.idx"

            try:
                time.sleep(0.1)  # Rate limiting
                response = requests.get(index_url, headers=self.headers)

                if response.status_code == 200:
                    daily_filings = self._parse_daily_index(response.text, current_dt.strftime('%Y-%m-%d'))
                    filings.extend(daily_filings)

                    if len(daily_filings) > 0:
                        print(f"Found {len(daily_filings)} 13D filings on {current_dt.strftime('%Y-%m-%d')}")

            except requests.exceptions.RequestException as e:
                print(f"Error fetching index for {date_str}: {e}")

            current_dt += timedelta(days=1)

            # Limit total results
            if len(filings) >= max_results:
                filings = filings[:max_results]
                break

        # Filter for institutional investors
        institutional_filings = self._filter_institutional_investors(filings)

        return institutional_filings

    def search_filings(self,
                       ticker: Optional[str] = None,
                       cik: Optional[str] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       max_results: int = 100) -> List[Dict]:
        """
        Search for Form 13D filings by specific company.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            cik: Central Index Key of the company
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_results: Maximum number of results to return

        Returns:
            List of filing information dictionaries
        """

        # If ticker provided, get CIK first
        if ticker and not cik:
            cik = self._get_cik_from_ticker(ticker)
            if not cik:
                print(f"Could not find CIK for ticker: {ticker}")
                return []

        # Alternative approach using submissions endpoint
        if cik:
            submissions_url = f"{self.base_url}/submissions/CIK{cik:0>10}.json"
        else:
            print("Either ticker or CIK must be provided")
            return []

        try:
            # Rate limiting - SEC requires 10 requests per second max
            time.sleep(0.1)

            response = requests.get(submissions_url, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            filings = []

            # Extract recent filings
            recent_filings = data.get('filings', {}).get('recent', {})
            forms = recent_filings.get('form', [])
            filing_dates = recent_filings.get('filingDate', [])
            accession_numbers = recent_filings.get('accessionNumber', [])

            for i, form in enumerate(forms):
                if form in ['13D', '13D/A']:  # Include amendments
                    filing_date = filing_dates[i]

                    # Filter by date range if provided
                    if start_date and filing_date < start_date:
                        continue
                    if end_date and filing_date > end_date:
                        continue

                    filing_info = {
                        'form': form,
                        'filing_date': filing_date,
                        'accession_number': accession_numbers[i],
                        'cik': cik,
                        'ticker': ticker or 'Unknown'
                    }
                    filings.append(filing_info)

                    if len(filings) >= max_results:
                        break

            return filings

        except requests.exceptions.RequestException as e:
            print(f"Error fetching filings: {e}")
            return []

    def _parse_daily_index(self, index_content: str, date: str) -> List[Dict]:
        """Parse SEC daily index file for 13D filings."""
        filings = []
        lines = index_content.split('\n')

        # Skip header lines
        start_parsing = False
        for line in lines:
            if line.startswith('Form Type'):
                start_parsing = True
                continue
            if not start_parsing or line.strip() == '':
                continue

            # Parse line (pipe-delimited)
            parts = line.split('|')
            if len(parts) >= 5:
                form_type = parts[0].strip()
                company_name = parts[1].strip()
                cik = parts[2].strip()
                date_filed = parts[3].strip()
                filename = parts[4].strip()

                if form_type in ['13D', '13D/A']:
                    # Extract accession number from filename
                    # Format: edgar/data/CIK/accession-number/primary-document.txt
                    acc_match = re.search(r'/(\d{10}-\d{2}-\d{6})/', filename)
                    accession_number = acc_match.group(1) if acc_match else ''

                    filing_info = {
                        'form': form_type,
                        'filing_date': date,
                        'company_name': company_name,
                        'cik': cik,
                        'accession_number': accession_number,
                        'filename': filename,
                        'filer_type': 'Unknown'  # Will be determined later
                    }
                    filings.append(filing_info)

        return filings

    def _filter_institutional_investors(self, filings: List[Dict]) -> List[Dict]:
        """Filter filings to identify institutional investors."""
        institutional_filings = []

        for filing in filings:
            company_name = filing.get('company_name', '').lower()

            # Check if company name contains institutional keywords
            is_institutional = any(keyword in company_name for keyword in self.institutional_keywords)

            if is_institutional:
                filing['filer_type'] = 'Institutional'
                filing['institutional_indicators'] = [
                    keyword for keyword in self.institutional_keywords
                    if keyword in company_name
                ]
                institutional_filings.append(filing)
            else:
                # Additional checks for common institutional patterns
                institutional_patterns = [
                    r'[a-z]+ capital',
                    r'[a-z]+ fund',
                    r'[a-z]+ management',
                    r'[a-z]+ partners',
                    r'[a-z]+ advisors',
                    r'[a-z]+ asset',
                    r'pension fund',
                    r'investment company',
                    r'mutual fund',
                    r'hedge fund',
                    r'private equity'
                ]

                for pattern in institutional_patterns:
                    if re.search(pattern, company_name):
                        filing['filer_type'] = 'Institutional'
                        filing['institutional_indicators'] = [pattern]
                        institutional_filings.append(filing)
                        break

        return institutional_filings

    def get_major_institutional_investors(self) -> List[Dict]:
        """Get list of known major institutional investors with their CIKs."""
        major_investors = [
            {'name': 'Berkshire Hathaway Inc', 'cik': '1067983'},
            {'name': 'Vanguard Group Inc', 'cik': '102909'},
            {'name': 'BlackRock Inc', 'cik': '1364742'},
            {'name': 'State Street Corp', 'cik': '93751'},
            {'name': 'Fidelity Management & Research Company LLC', 'cik': '315066'},
            {'name': 'Capital Research Global Investors', 'cik': '1067983'},
            {'name': 'JPMorgan Chase & Co', 'cik': '19617'},
            {'name': 'Wellington Management Group LLP', 'cik': '1633917'},
            {'name': 'T. Rowe Price Associates Inc', 'cik': '1113169'},
            {'name': 'Geode Capital Management LLC', 'cik': '1235067'},
            {'name': 'Northern Trust Corp', 'cik': '73015'},
            {'name': 'Bank of America Corp', 'cik': '70858'},
            {'name': 'Charles Schwab Corp', 'cik': '316709'},
            {'name': 'Invesco Ltd', 'cik': '914208'},
            {'name': 'Goldman Sachs Group Inc', 'cik': '886982'},
            {'name': 'Morgan Stanley', 'cik': '895421'},
            {'name': 'UBS Group AG', 'cik': '1114446'},
            {'name': 'Credit Suisse Group AG', 'cik': '1053092'},
            {'name': 'Citadel Advisors LLC', 'cik': '1423053'},
            {'name': 'Bridgewater Associates LP', 'cik': '1350694'}
        ]
        return major_investors

    def search_major_institutional_filings(self,
                                           start_date: Optional[str] = None,
                                           end_date: Optional[str] = None,
                                           max_results_per_investor: int = 50) -> List[Dict]:
        """
        Search for 13D filings from major institutional investors.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_results_per_investor: Maximum results per institutional investor

        Returns:
            List of filing information dictionaries
        """
        all_filings = []
        major_investors = self.get_major_institutional_investors()

        print(f"Searching filings for {len(major_investors)} major institutional investors...")

        for investor in major_investors:
            print(f"Searching {investor['name']}...")

            try:
                filings = self._search_by_filer_cik(
                    investor['cik'],
                    investor['name'],
                    start_date,
                    end_date,
                    max_results_per_investor
                )

                if filings:
                    print(f"  Found {len(filings)} filings")
                    all_filings.extend(filings)
                else:
                    print(f"  No filings found")

                time.sleep(0.2)  # Rate limiting

            except Exception as e:
                print(f"  Error searching {investor['name']}: {e}")
                continue

        return all_filings

    def _search_by_filer_cik(self, filer_cik: str, filer_name: str,
                             start_date: Optional[str], end_date: Optional[str],
                             max_results: int) -> List[Dict]:
        """Search for 13D filings by filer CIK."""
        submissions_url = f"{self.base_url}/submissions/CIK{filer_cik:0>10}.json"

        try:
            time.sleep(0.1)
            response = requests.get(submissions_url, headers=self.headers)
            response.raise_for_status()

            data = response.json()
            filings = []

            # Extract recent filings
            recent_filings = data.get('filings', {}).get('recent', {})
            forms = recent_filings.get('form', [])
            filing_dates = recent_filings.get('filingDate', [])
            accession_numbers = recent_filings.get('accessionNumber', [])
            primary_documents = recent_filings.get('primaryDocument', [])

            for i, form in enumerate(forms):
                if form in ['13D', '13D/A']:
                    filing_date = filing_dates[i]

                    # Filter by date range if provided
                    if start_date and filing_date < start_date:
                        continue
                    if end_date and filing_date > end_date:
                        continue

                    filing_info = {
                        'form': form,
                        'filing_date': filing_date,
                        'accession_number': accession_numbers[i],
                        'filer_cik': filer_cik,
                        'filer_name': filer_name,
                        'filer_type': 'Institutional',
                        'primary_document': primary_documents[i] if i < len(primary_documents) else '',
                    }
                    filings.append(filing_info)

                    if len(filings) >= max_results:
                        break

            return filings

        except requests.exceptions.RequestException as e:
            print(f"Error fetching filings for {filer_name}: {e}")
            return []
        """
        Get detailed information about a specific 13D filing.

        Args:
            accession_number: SEC accession number
            filer_cik: CIK of the filer (institutional investor)
            target_cik: CIK of the target company (optional)

        Returns:
            Dictionary with filing details
        """
        # Use filer_cik as primary, fallback to target_cik
        cik_to_use = filer_cik or target_cik
        if not cik_to_use:
            print("Either filer_cik or target_cik must be provided")
            return {}

        # Format accession number for URL (remove dashes)
        acc_no_formatted = accession_number.replace('-', '')

        # Get filing index
        index_url = f"{self.base_url}/Archives/edgar/data/{int(cik_to_use)}/{acc_no_formatted}/{accession_number}-index.json"

        try:
            time.sleep(0.1)  # Rate limiting

            response = requests.get(index_url, headers=self.headers)
            response.raise_for_status()

            index_data = response.json()

            # Find the main 13D document
            main_doc = None
            for item in index_data.get('directory', {}).get('item', []):
                if item.get('name', '').endswith('.txt') and '13d' in item.get('name', '').lower():
                    main_doc = item.get('name')
                    break

            if not main_doc:
                # Fallback - look for first .txt file
                for item in index_data.get('directory', {}).get('item', []):
                    if item.get('name', '').endswith('.txt'):
                        main_doc = item.get('name')
                        break

            # Get document content
            doc_url = f"{self.base_url}/Archives/edgar/data/{int(cik_to_use)}/{acc_no_formatted}/{main_doc}"

            time.sleep(0.1)  # Rate limiting
            doc_response = requests.get(doc_url, headers=self.headers)
            doc_response.raise_for_status()

            return {
                'accession_number': accession_number,
                'document_url': doc_url,
                'content': doc_response.text,
                'index_data': index_data
            }

        except requests.exceptions.RequestException as e:
            print(f"Error fetching filing details: {e}")
            return {}

    def _get_cik_from_ticker(self, ticker: str) -> Optional[str]:
        """Get CIK from ticker symbol using SEC company tickers JSON."""
        try:
            time.sleep(0.1)  # Rate limiting

            tickers_url = f"{self.base_url}/files/company_tickers.json"
            response = requests.get(tickers_url, headers=self.headers)
            response.raise_for_status()

            data = response.json()

            for entry in data.values():
                if entry.get('ticker', '').upper() == ticker.upper():
                    return str(entry.get('cik_str'))

            return None

        except requests.exceptions.RequestException as e:
            print(f"Error fetching ticker data: {e}")
            return None

    def save_filings_to_csv(self, filings: List[Dict], filename: str = "form_13d_filings.csv"):
        """Save filings data to CSV file."""
        if not filings:
            print("No filings to save")
            return

        df = pd.DataFrame(filings)
        df.to_csv(filename, index=False)
        print(f"Saved {len(filings)} filings to {filename}")


def main():
    # Example usage - Updated for institutional investor search

    # IMPORTANT: Replace with your actual name and email
    fetcher = Form13DFetcher("Your Name your.email@example.com")

    # Example 1: Search all institutional 13D filings (comprehensive but slower)
    print("=== Searching All Institutional 13D Filings (Last 30 days) ===")
    all_institutional = fetcher.search_all_institutional_filings(
        start_date="2024-11-01",
        end_date="2024-12-01",
        max_results=100
    )

    print(f"Found {len(all_institutional)} institutional 13D filings")
    for filing in all_institutional[:5]:  # Show first 5
        print(f"- {filing['form']} by {filing['company_name']} filed on {filing['filing_date']}")
        print(f"  Institutional indicators: {filing.get('institutional_indicators', [])}")

    # Example 2: Search major institutional investors (faster, more targeted)
    print("\n=== Searching Major Institutional Investors ===")
    major_institutional = fetcher.search_major_institutional_filings(
        start_date="2024-01-01",
        max_results_per_investor=10
    )

    print(f"Found {len(major_institutional)} filings from major institutional investors")

    # Group by investor
    from collections import defaultdict
    by_investor = defaultdict(list)
    for filing in major_institutional:
        by_investor[filing['filer_name']].append(filing)

    print("\nFilings by investor:")
    for investor, filings in list(by_investor.items())[:5]:
        print(f"- {investor}: {len(filings)} filings")
        if filings:
            print(f"  Latest: {filings[0]['filing_date']} ({filings[0]['form']})")

    # Example 3: Get detailed information about a filing
    if major_institutional:
        print(f"\n=== Getting Details for Filing ===")
        sample_filing = major_institutional[0]
        print(f"Analyzing filing: {sample_filing['accession_number']} by {sample_filing['filer_name']}")

        details = fetcher.get_filing_details(
            sample_filing['accession_number'],
            filer_cik=sample_filing['filer_cik']
        )

        if details:
            print(f"Document URL: {details['document_url']}")
            print(f"Content preview: {details['content'][:300]}...")

    # Example 4: Save all results to CSV
    print(f"\n=== Saving Results ===")
    if all_institutional:
        fetcher.save_filings_to_csv(all_institutional, "all_institutional_13d_filings.csv")

    if major_institutional:
        fetcher.save_filings_to_csv(major_institutional, "major_institutional_13d_filings.csv")

    # Example 5: Analyze filing patterns
    print(f"\n=== Analysis Summary ===")
    all_filings = all_institutional + major_institutional

    if all_filings:
        # Most active institutional investors
        investor_counts = defaultdict(int)
        for filing in all_filings:
            investor_name = filing.get('filer_name') or filing.get('company_name', 'Unknown')
            investor_counts[investor_name] += 1

        print("Most active institutional investors:")
        sorted_investors = sorted(investor_counts.items(), key=lambda x: x[1], reverse=True)
        for investor, count in sorted_investors[:10]:
            print(f"- {investor}: {count} filings")

        # Filing trends by month
        from collections import Counter
        filing_months = [filing['filing_date'][:7] for filing in all_filings]  # YYYY-MM
        month_counts = Counter(filing_months)

        print(f"\nFiling activity by month:")
        for month, count in sorted(month_counts.items()):
            print(f"- {month}: {count} filings")

    print(f"\nTotal unique institutional filings found: {len(set(f['accession_number'] for f in all_filings))}")


if __name__ == "__main__":
    main()
