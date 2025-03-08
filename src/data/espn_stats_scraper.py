"""
ESPN Men's College Basketball Team Stats Scraper.

This module scrapes team statistics from ESPN's website for men's college basketball
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


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('espn_stats_scraper')


class ESPNStatsScraperBase:
    """Base class for ESPN stats scrapers."""
    
    def __init__(self, base_url: str, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            base_url: Base URL for the ESPN stats page
            output_dir: Directory to save the scraped data
        """
        self.base_url = base_url
        self.output_dir = output_dir
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.espn.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def _get_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Get the HTML content of a page and parse it with BeautifulSoup.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object or None if the request failed
        """
        try:
            logger.info(f"Fetching URL: {url}")
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}")
            return None
    
    def _save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Name of the file to save to
        """
        file_path = os.path.join(self.output_dir, filename)
        try:
            logger.info(f"Saving data to {file_path}")
            df.to_csv(file_path, index=False)
            logger.info(f"Successfully saved data to {file_path}")
        except Exception as e:
            logger.error(f"Error saving data to {file_path}: {e}")
    
    def _wait(self, seconds: float = 1.0) -> None:
        """
        Wait for a specified number of seconds between requests.
        
        Args:
            seconds: Number of seconds to wait
        """
        time.sleep(seconds)


class ESPNMensBasketballTeamStatsScraperMCB(ESPNStatsScraperBase):
    """Scraper for ESPN men's college basketball team stats."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save the scraped data
        """
        super().__init__(
            base_url="https://www.espn.com/mens-college-basketball/stats/team",
            output_dir=output_dir
        )
    
    def scrape_team_stats(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape team statistics for a specific year.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing the team statistics
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons (note: this might change)
            url = f"{self.base_url}/_/season/{year}/seasontype/2"
        
        logger.info(f"Scraping team stats for {'current season' if year is None else year}")
        
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
            return result_df
        else:
            logger.error(f"Team count ({len(teams_df)}) does not match stats count ({len(stats_df)})")
            return pd.DataFrame()
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> Dict[int, pd.DataFrame]:
        """
        Scrape team statistics for multiple years.
        
        Args:
            start_year: First year to scrape data for
            end_year: Last year to scrape data for
            
        Returns:
            Dictionary mapping years to DataFrames containing team statistics
        """
        results = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping data for {year}")
            df = self.scrape_team_stats(year)
            
            if not df.empty:
                results[year] = df
                filename = f"mens_basketball_team_stats_{year}.csv"
                self._save_to_csv(df, filename)
            else:
                logger.warning(f"No data retrieved for {year}")
            
            # Wait between requests to avoid overloading the server
            self._wait(2.0)
        
        return results


class ESPNMensBasketballTeamStatsScraperSelenium(ESPNStatsScraperBase):
    """Scraper for ESPN men's college basketball team stats using Selenium for JavaScript-rendered content."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize the scraper.
        
        Args:
            output_dir: Directory to save the scraped data
        """
        super().__init__(
            base_url="https://www.espn.com/mens-college-basketball/stats/team",
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
    
    def _get_selector_dropdown_options(self) -> Dict[str, str]:
        """
        Get the available season options from the season selector dropdown.
        
        Returns:
            Dictionary mapping display text to value
        """
        try:
            # Wait for the season selector to be present
            season_selector = self.wait.until(
                self.EC.presence_of_element_located((self.By.CSS_SELECTOR, "div.dropdown__select"))
            )
            season_selector.click()
            
            # Get all options
            option_elements = self.driver.find_elements(self.By.CSS_SELECTOR, "ul.dropdown__options li")
            options = {}
            
            for option in option_elements:
                text = option.text.strip()
                value = option.get_attribute("data-value")
                options[text] = value
            
            return options
        except Exception as e:
            logger.error(f"Error getting season options: {e}")
            return {}
    
    def scrape_team_stats(self, year: Optional[int] = None) -> pd.DataFrame:
        """
        Scrape team statistics for a specific year using Selenium.
        
        Args:
            year: Year to scrape data for (e.g., 2023). If None, gets current season.
            
        Returns:
            DataFrame containing the team statistics
        """
        url = self.base_url
        if year is not None:
            # The URL format for specific seasons
            url = f"{self.base_url}/_/season/{year}/seasontype/2"
        
        logger.info(f"Scraping team stats for {'current season' if year is None else year}")
        
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
                        if not all_stats:
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
                    return result_df
                else:
                    logger.error(f"Team count ({len(teams_df)}) does not match stats count ({len(stats_df)})")
                    return pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Error scraping team stats: {e}")
            return pd.DataFrame()
    
    def scrape_multiple_years(self, start_year: int, end_year: int) -> Dict[int, pd.DataFrame]:
        """
        Scrape team statistics for multiple years.
        
        Args:
            start_year: First year to scrape data for
            end_year: Last year to scrape data for
            
        Returns:
            Dictionary mapping years to DataFrames containing team statistics
        """
        results = {}
        
        for year in range(start_year, end_year + 1):
            logger.info(f"Scraping data for {year}")
            df = self.scrape_team_stats(year)
            
            if not df.empty:
                results[year] = df
                filename = f"mens_basketball_team_stats_{year}.csv"
                self._save_to_csv(df, filename)
            else:
                logger.warning(f"No data retrieved for {year}")
            
            # Wait between requests to avoid overloading the server
            self._wait(2.0)
        
        return results


def main():
    """Main function to run the scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape ESPN men\'s college basketball team stats.')
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
        logger.info("Using Selenium scraper")
        scraper = ESPNMensBasketballTeamStatsScraperSelenium(output_dir=args.output_dir)
    else:
        logger.info("Using requests/BeautifulSoup scraper")
        scraper = ESPNMensBasketballTeamStatsScraperMCB(output_dir=args.output_dir)
    
    scraper.scrape_multiple_years(args.start_year, args.end_year)


if __name__ == "__main__":
    main() 