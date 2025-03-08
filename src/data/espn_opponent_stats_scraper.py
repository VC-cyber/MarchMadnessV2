"""
ESPN Men's College Basketball Opponent Stats Scraper.

This module scrapes opponent statistics from ESPN's website for men's college basketball
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
import os

# Add the parent directory to the path so we can import espn_stats_scraper
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from espn_stats_scraper import ESPNStatsScraperBase, logger


class ESPNMensBasketballOpponentStatsScraperMCB(ESPNStatsScraperBase):
    """Scraper for ESPN men's college basketball opponent stats."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save the scraped data
        """
        super().__init__(
            base_url="https://www.espn.com/mens-college-basketball/stats/team/_/view/opponent",
            output_dir=output_dir
        )
    
    def scrape_opponent_stats(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape opponent statistics for a specific year.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing the opponent statistics
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons (note: this might change)
            url = f"{self.base_url}/season/{year}/seasontype/2"
        
        logger.info(f"Scraping opponent stats for {'current season' if year is None else year}")
        
        # Get the page content
        soup = self._get_page(url)
        if not soup:
            logger.error("Failed to retrieve page content")
            return pd.DataFrame()
        
        # Find the stats tables - ESPN typically has two tables: one for teams and one for stats
        tables = soup.select("div.Wrapper > div.ResponsiveTable")
        if len(tables) < 2:
            logger.error("Could not find stats tables on the page")
            return pd.DataFrame()
        
        # Team names table
        team_table = tables[0]
        # Stats table
        stats_table = tables[1]
        
        # Extract team names
        teams = []
        team_rows = team_table.select("tbody > tr")
        for row in team_rows:
            team_cell = row.select_one("td:nth-of-type(1)")
            if team_cell:
                teams.append(team_cell.get_text(strip=True))
        
        # Extract stats
        stats_rows = stats_table.select("tbody > tr")
        headers = [th.get_text(strip=True) for th in stats_table.select("thead > tr > th")]
        
        stats_data = []
        for row in stats_rows:
            row_data = [td.get_text(strip=True) for td in row.select("td")]
            stats_data.append(row_data)
        
        # Create stats DataFrame
        if not stats_data or not headers:
            logger.error("Failed to extract stats data")
            return pd.DataFrame()
        
        stats_df = pd.DataFrame(stats_data, columns=headers)
        
        # Create teams DataFrame
        teams_df = pd.DataFrame({"Team": teams})
        
        # Combine teams and stats
        if len(teams_df) == len(stats_df):
            result_df = pd.concat([teams_df, stats_df], axis=1)
            # Add a column to indicate these are opponent stats
            result_df['StatsType'] = 'Opponent'
            return result_df
        else:
            logger.error(f"Team count ({len(teams_df)}) does not match stats count ({len(stats_df)})")
            return pd.DataFrame()
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> Dict[int, pd.DataFrame]:
        """
        Scrape opponent statistics for multiple years.
        
        Args:
            start_year: First year to scrape data for
            end_year: Last year to scrape data for
            
        Returns:
            Dictionary mapping years to DataFrames containing opponent statistics
        """
        results = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping opponent data for {year}")
            df = self.scrape_opponent_stats(year)
            
            if not df.empty:
                results[year] = df
                filename = f"mens_basketball_opponent_stats_{year}.csv"
                self._save_to_csv(df, filename)
            else:
                logger.warning(f"No opponent data retrieved for {year}")
            
            # Wait between requests to avoid overloading the server
            self._wait(2.0)
        
        return results


class ESPNMensBasketballOpponentStatsScraperSelenium(ESPNStatsScraperBase):
    """Scraper for ESPN men's college basketball opponent stats using Selenium."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save the scraped data
        """
        super().__init__(
            base_url="https://www.espn.com/mens-college-basketball/stats/team/_/view/opponent",
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
    
    def scrape_opponent_stats(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape opponent statistics for a specific year using Selenium.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing the opponent statistics
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons
            url = f"{self.base_url}/season/{year}/seasontype/2"
        
        logger.info(f"Scraping opponent stats for {'current season' if year is None else year}")
        
        try:
            # Navigate to the page
            self.driver.get(url)
            
            # Wait for the tables to load
            self.wait.until(self.EC.presence_of_element_located((self.By.CSS_SELECTOR, "div.ResponsiveTable")))
            
            # If we need to navigate through pagination, do it here
            # Find the pagination control
            try:
                pagination = self.driver.find_element(self.By.CSS_SELECTOR, "div.Pagination__Controls")
                if pagination:
                    # Get all data from all pages
                    all_teams = []
                    all_stats = []
                    headers = []
                    
                    while True:
                        # Extract team names from current page
                        team_table = self.driver.find_element(self.By.CSS_SELECTOR, "div.ResponsiveTable:nth-of-type(1)")
                        team_rows = team_table.find_elements(self.By.CSS_SELECTOR, "tbody > tr")
                        
                        for row in team_rows:
                            team_cell = row.find_element(self.By.CSS_SELECTOR, "td:nth-of-type(1)")
                            all_teams.append(team_cell.text.strip())
                        
                        # Extract stats from current page
                        stats_table = self.driver.find_element(self.By.CSS_SELECTOR, "div.ResponsiveTable:nth-of-type(2)")
                        stats_rows = stats_table.find_elements(self.By.CSS_SELECTOR, "tbody > tr")
                        
                        # Get headers if this is the first page
                        if not headers:
                            headers = [th.text.strip() for th in stats_table.find_elements(self.By.CSS_SELECTOR, "thead > tr > th")]
                        
                        for row in stats_rows:
                            row_data = [td.text.strip() for td in row.find_elements(self.By.CSS_SELECTOR, "td")]
                            all_stats.append(row_data)
                        
                        # Try to go to the next page
                        next_button = pagination.find_element(self.By.CSS_SELECTOR, "button[data-track='click:next']")
                        if next_button and next_button.is_enabled():
                            next_button.click()
                            # Wait for the table to update
                            time.sleep(1)
                        else:
                            break
                    
                    # Create DataFrame
                    stats_df = pd.DataFrame(all_stats, columns=headers)
                    teams_df = pd.DataFrame({"Team": all_teams})
                    
                    # Combine teams and stats
                    if len(teams_df) == len(stats_df):
                        result_df = pd.concat([teams_df, stats_df], axis=1)
                        # Add a column to indicate these are opponent stats
                        result_df['StatsType'] = 'Opponent'
                        return result_df
                    else:
                        logger.error(f"Team count ({len(teams_df)}) does not match stats count ({len(stats_df)})")
                        return pd.DataFrame()
            except Exception as e:
                # If pagination element is not found, just extract the data from the single page
                logger.info(f"No pagination found or error navigating pages: {e}")
                
                # Extract data using Selenium from the current page
                # Extract team names
                team_table = self.driver.find_element(self.By.CSS_SELECTOR, "div.ResponsiveTable:nth-of-type(1)")
                team_rows = team_table.find_elements(self.By.CSS_SELECTOR, "tbody > tr")
                teams = [row.find_element(self.By.CSS_SELECTOR, "td:nth-of-type(1)").text.strip() for row in team_rows]
                
                # Extract stats
                stats_table = self.driver.find_element(self.By.CSS_SELECTOR, "div.ResponsiveTable:nth-of-type(2)")
                headers = [th.text.strip() for th in stats_table.find_elements(self.By.CSS_SELECTOR, "thead > tr > th")]
                stats_rows = stats_table.find_elements(self.By.CSS_SELECTOR, "tbody > tr")
                stats_data = []
                for row in stats_rows:
                    row_data = [td.text.strip() for td in row.find_elements(self.By.CSS_SELECTOR, "td")]
                    stats_data.append(row_data)
                
                # Create DataFrames
                stats_df = pd.DataFrame(stats_data, columns=headers)
                teams_df = pd.DataFrame({"Team": teams})
                
                # Combine teams and stats
                if len(teams_df) == len(stats_df):
                    result_df = pd.concat([teams_df, stats_df], axis=1)
                    # Add a column to indicate these are opponent stats
                    result_df['StatsType'] = 'Opponent'
                    return result_df
                else:
                    logger.error(f"Team count ({len(teams_df)}) does not match stats count ({len(stats_df)})")
                    return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Error scraping opponent stats: {e}")
            return pd.DataFrame()
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> Dict[int, pd.DataFrame]:
        """
        Scrape opponent statistics for multiple years.
        
        Args:
            start_year: First year to scrape data for
            end_year: Last year to scrape data for
            
        Returns:
            Dictionary mapping years to DataFrames containing opponent statistics
        """
        results = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping opponent data for {year}")
            df = self.scrape_opponent_stats(year)
            
            if not df.empty:
                results[year] = df
                filename = f"mens_basketball_opponent_stats_{year}.csv"
                self._save_to_csv(df, filename)
            else:
                logger.warning(f"No opponent data retrieved for {year}")
            
            # Wait between requests to avoid overloading the server
            self._wait(2.0)
        
        return results


def main():
    """Main function to run the opponent stats scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape ESPN men\'s college basketball opponent stats.')
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
        logger.info("Using Selenium scraper for opponent stats")
        scraper = ESPNMensBasketballOpponentStatsScraperSelenium(output_dir=args.output_dir)
    else:
        logger.info("Using requests/BeautifulSoup scraper for opponent stats")
        scraper = ESPNMensBasketballOpponentStatsScraperMCB(output_dir=args.output_dir)
    
    scraper.scrape_multiple_years(args.start_year, args.end_year)


if __name__ == "__main__":
    main() 