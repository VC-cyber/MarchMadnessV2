"""
NCAA Men's College Basketball Rankings Scraper.

This module scrapes team rankings from ESPN and other sources for men's college basketball
and saves the data as CSV files.
"""
import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import sys

# Add the parent directory to the path so we can import espn_stats_scraper
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from espn_stats_scraper import ESPNStatsScraperBase, logger


class ESPNMensBasketballRankingsScraperMCB(ESPNStatsScraperBase):
    """Scraper for ESPN men's college basketball rankings."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save the scraped data
        """
        super().__init__(
            base_url="https://www.espn.com/mens-college-basketball/rankings",
            output_dir=output_dir
        )
    
    def scrape_ap_rankings(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape AP poll rankings for a specific year.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing AP rankings
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons (note: this might change)
            url = f"{self.base_url}_/season/{year}"
        
        logger.info(f"Scraping AP rankings for {'current season' if year is None else year}")
        
        # Get the page content
        soup = self._get_page(url)
        if not soup:
            logger.error("Failed to retrieve page content")
            return pd.DataFrame()
        
        # Find the rankings table - AP Poll is typically the first table
        tables = soup.select("section.Rankings > div.tabs__content")
        if not tables:
            logger.error("Could not find rankings tables on the page")
            return pd.DataFrame()
        
        # AP Poll table
        ap_table = tables[0]
        
        # Extract teams and rankings
        rankings_data = []
        team_rows = ap_table.select("tbody > tr")
        
        for row in team_rows:
            try:
                # Extract rank
                rank_cell = row.select_one("td:nth-of-type(1) span")
                if not rank_cell:
                    continue
                rank = rank_cell.get_text(strip=True)
                
                # Extract team name
                team_cell = row.select_one("td:nth-of-type(1) span.ml4")
                if not team_cell:
                    continue
                team = team_cell.get_text(strip=True)
                
                # Extract record
                record_cell = row.select_one("td:nth-of-type(2)")
                record = record_cell.get_text(strip=True) if record_cell else ""
                
                # Extract points
                points_cell = row.select_one("td:nth-of-type(3)")
                points = points_cell.get_text(strip=True) if points_cell else ""
                
                rankings_data.append({
                    "Rank": rank,
                    "Team": team,
                    "Record": record,
                    "Points": points
                })
            except Exception as e:
                logger.error(f"Error extracting team data: {e}")
        
        # Create DataFrame
        if not rankings_data:
            logger.error("Failed to extract rankings data")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(rankings_data)
        result_df['Year'] = year if year else datetime.now().year
        result_df['Poll'] = 'AP'
        
        return result_df
    
    def scrape_coaches_rankings(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape Coaches poll rankings for a specific year.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing Coaches poll rankings
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons (note: this might change)
            url = f"{self.base_url}/_/week/1/year/{year}/seasontype/2"
        
        logger.info(f"Scraping Coaches rankings for {'current season' if year is None else year}")
        
        # Get the page content
        soup = self._get_page(url)
        if not soup:
            logger.error("Failed to retrieve page content")
            return pd.DataFrame()
        
        # Find the rankings table - Coaches Poll is typically the second table
        tables = soup.select("section.Rankings > div.tabs__content")
        if len(tables) < 2:
            logger.error("Could not find Coaches poll table on the page")
            return pd.DataFrame()
        
        # Coaches Poll table
        coaches_table = tables[1]
        
        # Extract teams and rankings
        rankings_data = []
        team_rows = coaches_table.select("tbody > tr")
        
        for row in team_rows:
            try:
                # Extract rank
                rank_cell = row.select_one("td:nth-of-type(1) span")
                if not rank_cell:
                    continue
                rank = rank_cell.get_text(strip=True)
                
                # Extract team name
                team_cell = row.select_one("td:nth-of-type(1) span.ml4")
                if not team_cell:
                    continue
                team = team_cell.get_text(strip=True)
                
                # Extract record
                record_cell = row.select_one("td:nth-of-type(2)")
                record = record_cell.get_text(strip=True) if record_cell else ""
                
                # Extract points
                points_cell = row.select_one("td:nth-of-type(3)")
                points = points_cell.get_text(strip=True) if points_cell else ""
                
                rankings_data.append({
                    "Rank": rank,
                    "Team": team,
                    "Record": record,
                    "Points": points
                })
            except Exception as e:
                logger.error(f"Error extracting team data: {e}")
        
        # Create DataFrame
        if not rankings_data:
            logger.error("Failed to extract rankings data")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(rankings_data)
        result_df['Year'] = year if year else datetime.now().year
        result_df['Poll'] = 'Coaches'
        
        return result_df
    
    def scrape_all_rankings(self, year: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Scrape all available rankings for a specific year.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            Dictionary mapping poll names to DataFrames containing rankings
        """
        results = {}
        
        # Scrape AP Poll rankings
        ap_df = self.scrape_ap_rankings(year)
        if not ap_df.empty:
            results['AP'] = ap_df
        
        # Scrape Coaches Poll rankings
        coaches_df = self.scrape_coaches_rankings(year)
        if not coaches_df.empty:
            results['Coaches'] = coaches_df
        
        # Combine all rankings into a single DataFrame
        if results:
            combined_df = pd.concat(results.values())
            filename = f"mens_basketball_rankings_{year if year else 'current'}.csv"
            self._save_to_csv(combined_df, filename)
        else:
            logger.warning(f"No rankings data retrieved for {year if year else 'current season'}")
        
        return results
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> Dict[int, Dict[str, pd.DataFrame]]:
        """
        Scrape rankings for multiple years.
        
        Args:
            start_year: First year to scrape data for
            end_year: Last year to scrape data for
            
        Returns:
            Dictionary mapping years to dictionaries of poll names to DataFrames
        """
        results = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping rankings for {year}")
            year_results = self.scrape_all_rankings(year)
            
            if year_results:
                results[year] = year_results
            else:
                logger.warning(f"No rankings data retrieved for {year}")
            
            # Wait between requests to avoid overloading the server
            self._wait(2.0)
        
        return results


class ESPNMensBasketballRankingsScraperSelenium(ESPNStatsScraperBase):
    """Scraper for ESPN men's college basketball rankings using Selenium."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save the scraped data
        """
        super().__init__(
            base_url="https://www.espn.com/mens-college-basketball/rankings",
            output_dir=output_dir
        )
        # Import Selenium here so it's only required when this class is used
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        
        # Set up Selenium
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.wait = WebDriverWait(self.driver, 10)
        self.By = By
        self.EC = EC
    
    def __del__(self):
        """Clean up Selenium driver when the object is destroyed."""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
        except Exception as e:
            logger.error(f"Error closing Selenium driver: {e}")
    
    def _get_ranking_data_from_table(self, table_index: int, poll_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Extract ranking data from a table.
        
        Args:
            table_index: Index of the table to extract data from (0 for AP, 1 for Coaches)
            poll_name: Name of the poll (e.g., 'AP', 'Coaches')
            year: Year of the rankings
            
        Returns:
            DataFrame containing rankings data
        """
        try:
            # Find the rankings table by using the appropriate tab
            self.wait.until(self.EC.presence_of_all_elements_located((self.By.CSS_SELECTOR, "section.Rankings ul.tabs__navigation li")))
            tabs = self.driver.find_elements(self.By.CSS_SELECTOR, "section.Rankings ul.tabs__navigation li")
            
            if len(tabs) <= table_index:
                logger.error(f"Could not find tab for {poll_name} rankings")
                return pd.DataFrame()
            
            # Click on the appropriate tab
            tabs[table_index].click()
            time.sleep(1)  # Wait for content to load
            
            # Find the rankings table
            self.wait.until(self.EC.presence_of_element_located((self.By.CSS_SELECTOR, "section.Rankings div.tabs__content")))
            tables = self.driver.find_elements(self.By.CSS_SELECTOR, "section.Rankings div.tabs__content")
            
            if len(tables) <= table_index:
                logger.error(f"Could not find {poll_name} rankings table")
                return pd.DataFrame()
            
            rankings_table = tables[table_index]
            team_rows = rankings_table.find_elements(self.By.CSS_SELECTOR, "tbody > tr")
            
            rankings_data = []
            for row in team_rows:
                try:
                    # Extract rank
                    rank_cell = row.find_element(self.By.CSS_SELECTOR, "td:nth-of-type(1) span")
                    rank = rank_cell.text.strip()
                    
                    # Extract team name
                    team_cell = row.find_element(self.By.CSS_SELECTOR, "td:nth-of-type(1) span.ml4")
                    team = team_cell.text.strip()
                    
                    # Extract record
                    record_cell = row.find_element(self.By.CSS_SELECTOR, "td:nth-of-type(2)")
                    record = record_cell.text.strip()
                    
                    # Extract points
                    points_cell = row.find_element(self.By.CSS_SELECTOR, "td:nth-of-type(3)")
                    points = points_cell.text.strip()
                    
                    rankings_data.append({
                        "Rank": rank,
                        "Team": team,
                        "Record": record,
                        "Points": points
                    })
                except Exception as e:
                    logger.error(f"Error extracting team data: {e}")
            
            # Create DataFrame
            if not rankings_data:
                logger.error(f"Failed to extract {poll_name} rankings data")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(rankings_data)
            result_df['Year'] = year if year else datetime.now().year
            result_df['Poll'] = poll_name
            
            return result_df
        except Exception as e:
            logger.error(f"Error getting {poll_name} rankings data: {e}")
            return pd.DataFrame()
    
    def scrape_ap_rankings(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape AP poll rankings for a specific year using Selenium.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing AP rankings
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons
            url = f"{self.base_url}/_/week/1/year/{year}/seasontype/2"
        
        logger.info(f"Scraping AP rankings for {'current season' if year is None else year}")
        
        try:
            # Navigate to the page
            self.driver.get(url)
            
            # Wait for the page to load
            self.wait.until(self.EC.presence_of_element_located((self.By.CSS_SELECTOR, "section.Rankings")))
            
            # Extract data from AP Poll table (index 0)
            return self._get_ranking_data_from_table(0, 'AP', year)
        except Exception as e:
            logger.error(f"Error scraping AP rankings: {e}")
            return pd.DataFrame()
    
    def scrape_coaches_rankings(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape Coaches poll rankings for a specific year using Selenium.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing Coaches poll rankings
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons
            url = f"{self.base_url}/_/week/1/year/{year}/seasontype/2"
        
        logger.info(f"Scraping Coaches rankings for {'current season' if year is None else year}")
        
        try:
            # Navigate to the page if we're not already there
            if self.driver.current_url != url:
                self.driver.get(url)
                
                # Wait for the page to load
                self.wait.until(self.EC.presence_of_element_located((self.By.CSS_SELECTOR, "section.Rankings")))
            
            # Extract data from Coaches Poll table (index 1)
            return self._get_ranking_data_from_table(1, 'Coaches', year)
        except Exception as e:
            logger.error(f"Error scraping Coaches rankings: {e}")
            return pd.DataFrame()
    
    def scrape_all_rankings(self, year: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """
        Scrape all available rankings for a specific year using Selenium.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            Dictionary mapping poll names to DataFrames containing rankings
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons
            url = f"{self.base_url}/_/week/1/year/{year}/seasontype/2"
        
        logger.info(f"Scraping all rankings for {'current season' if year is None else year}")
        
        results = {}
        
        try:
            # Navigate to the page
            self.driver.get(url)
            
            # Wait for the page to load
            self.wait.until(self.EC.presence_of_element_located((self.By.CSS_SELECTOR, "section.Rankings")))
            
            # Scrape AP Poll rankings
            ap_df = self._get_ranking_data_from_table(0, 'AP', year)
            if not ap_df.empty:
                results['AP'] = ap_df
            
            # Scrape Coaches Poll rankings
            coaches_df = self._get_ranking_data_from_table(1, 'Coaches', year)
            if not coaches_df.empty:
                results['Coaches'] = coaches_df
            
            # Combine all rankings into a single DataFrame
            if results:
                combined_df = pd.concat(results.values())
                filename = f"mens_basketball_rankings_{year if year else 'current'}.csv"
                self._save_to_csv(combined_df, filename)
            else:
                logger.warning(f"No rankings data retrieved for {year if year else 'current season'}")
            
            return results
        except Exception as e:
            logger.error(f"Error scraping all rankings: {e}")
            return {}
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> Dict[int, Dict[str, pd.DataFrame]]:
        """
        Scrape rankings for multiple years.
        
        Args:
            start_year: First year to scrape data for
            end_year: Last year to scrape data for
            
        Returns:
            Dictionary mapping years to dictionaries of poll names to DataFrames
        """
        results = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping rankings for {year}")
            year_results = self.scrape_all_rankings(year)
            
            if year_results:
                results[year] = year_results
            else:
                logger.warning(f"No rankings data retrieved for {year}")
            
            # Wait between requests to avoid overloading the server
            self._wait(2.0)
        
        return results


def main():
    """Main function to run the rankings scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape NCAA men\'s college basketball rankings.')
    parser.add_argument('--start-year', type=int, default=datetime.now().year - 1,
                        help='First year to scrape data for (e.g., 2015)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                        help='Last year to scrape data for (e.g., 2023)')
    parser.add_argument('--output-dir', type=str, default='data',
                        help='Directory to save the scraped data')
    parser.add_argument('--use-selenium', action='store_true',
                        help='Use Selenium for scraping (required for JavaScript-rendered content)')
    
    args = parser.parse_args()
    
    if args.use_selenium:
        logger.info("Using Selenium scraper for rankings")
        scraper = ESPNMensBasketballRankingsScraperSelenium(output_dir=args.output_dir)
    else:
        logger.info("Using requests/BeautifulSoup scraper for rankings")
        scraper = ESPNMensBasketballRankingsScraperMCB(output_dir=args.output_dir)
    
    scraper.scrape_multiple_years(args.start_year, args.end_year)


if __name__ == "__main__":
    main() 