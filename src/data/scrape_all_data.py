"""
Comprehensive ESPN NCAA Men's College Basketball Data Scraper.

This script runs all scrapers to collect team stats, opponent stats, and rankings
for the March Madness prediction model.
"""
import os
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Any
import pandas as pd
from tqdm import tqdm
import time
import sys

# Add current directory to path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import our scrapers
from espn_stats_scraper import (
    ESPNMensBasketballTeamStatsScraperMCB, 
    ESPNMensBasketballTeamStatsScraperSelenium, 
    logger
)
from espn_opponent_stats_scraper import (
    ESPNMensBasketballOpponentStatsScraperMCB,
    ESPNMensBasketballOpponentStatsScraperSelenium
)
from espn_rankings_scraper import (
    ESPNMensBasketballRankingsScraperMCB,
    ESPNMensBasketballRankingsScraperSelenium
)


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"espn_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
            logging.StreamHandler()
        ]
    )


def scrape_all_data(
    start_year: int,
    end_year: int, 
    output_dir: str = "data", 
    use_selenium: bool = False,
    scrape_team_stats: bool = True,
    scrape_opponent_stats: bool = True,
    scrape_rankings: bool = True,
    wait_time: float = 2.0
) -> Dict[str, Dict[int, pd.DataFrame]]:
    """
    Scrape all types of data for the specified years.
    
    Args:
        start_year: First year to scrape data for
        end_year: Last year to scrape data for
        output_dir: Directory to save the scraped data
        use_selenium: Whether to use Selenium for scraping
        scrape_team_stats: Whether to scrape team stats
        scrape_opponent_stats: Whether to scrape opponent stats
        scrape_rankings: Whether to scrape rankings
        wait_time: Time to wait between different scraper runs
        
    Returns:
        Dictionary mapping data types to dictionaries mapping years to DataFrames
    """
    results = {}
    
    # Create data directory structure by year
    for year in range(start_year, end_year + 1):
        year_dir = os.path.join(output_dir, str(year))
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
    
    # Scrape team stats
    if scrape_team_stats:
        logger.info("Scraping team stats...")
        
        if use_selenium:
            team_stats_scraper = ESPNMensBasketballTeamStatsScraperSelenium(output_dir=output_dir)
        else:
            team_stats_scraper = ESPNMensBasketballTeamStatsScraperMCB(output_dir=output_dir)
        
        team_stats_results = team_stats_scraper.scrape_multiple_years(start_year, end_year)
        results['team_stats'] = team_stats_results
        
        # Organize files by year
        for year, df in team_stats_results.items():
            year_file = os.path.join(output_dir, str(year), "team_stats.csv")
            df.to_csv(year_file, index=False)
            logger.info(f"Saved team stats for {year} to {year_file}")
        
        # Wait between different scrapers
        if scrape_opponent_stats or scrape_rankings:
            time.sleep(wait_time)
    
    # Scrape opponent stats
    if scrape_opponent_stats:
        logger.info("Scraping opponent stats...")
        
        if use_selenium:
            opponent_stats_scraper = ESPNMensBasketballOpponentStatsScraperSelenium(output_dir=output_dir)
        else:
            opponent_stats_scraper = ESPNMensBasketballOpponentStatsScraperMCB(output_dir=output_dir)
        
        opponent_stats_results = opponent_stats_scraper.scrape_multiple_years(start_year, end_year)
        results['opponent_stats'] = opponent_stats_results
        
        # Organize files by year
        for year, df in opponent_stats_results.items():
            year_file = os.path.join(output_dir, str(year), "opponent_stats.csv")
            df.to_csv(year_file, index=False)
            logger.info(f"Saved opponent stats for {year} to {year_file}")
        
        # Wait between different scrapers
        if scrape_rankings:
            time.sleep(wait_time)
    
    # Scrape rankings
    if scrape_rankings:
        logger.info("Scraping rankings...")
        
        if use_selenium:
            rankings_scraper = ESPNMensBasketballRankingsScraperSelenium(output_dir=output_dir)
        else:
            rankings_scraper = ESPNMensBasketballRankingsScraperMCB(output_dir=output_dir)
        
        rankings_results = rankings_scraper.scrape_multiple_years(start_year, end_year)
        results['rankings'] = rankings_results
        
        # Organize files by year
        for year, poll_dfs in rankings_results.items():
            # Combine all polls into a single DataFrame
            if poll_dfs:
                combined_df = pd.concat(poll_dfs.values())
                year_file = os.path.join(output_dir, str(year), "rankings.csv")
                combined_df.to_csv(year_file, index=False)
                logger.info(f"Saved rankings for {year} to {year_file}")
    
    logger.info("All data scraping completed!")
    return results


def main():
    """Main function to run all scrapers."""
    parser = argparse.ArgumentParser(description="Scrape all NCAA men's basketball data for March Madness predictions.")
    parser.add_argument('--start-year', type=int, default=datetime.now().year - 5,
                        help='First year to scrape data for (e.g., 2018)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                        help='Last year to scrape data for (e.g., 2023)')
    parser.add_argument('--output-dir', type=str, default='data',
                        help='Directory to save the scraped data')
    parser.add_argument('--use-selenium', action='store_true',
                        help='Use Selenium for scraping (required for JavaScript-rendered content)')
    parser.add_argument('--no-team-stats', action='store_true',
                        help='Skip team stats scraping')
    parser.add_argument('--no-opponent-stats', action='store_true',
                        help='Skip opponent stats scraping')
    parser.add_argument('--no-rankings', action='store_true',
                        help='Skip rankings scraping')
    parser.add_argument('--wait-time', type=float, default=2.0,
                        help='Wait time between different scraper runs in seconds')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Run all scrapers
    scrape_all_data(
        start_year=args.start_year,
        end_year=args.end_year,
        output_dir=args.output_dir,
        use_selenium=args.use_selenium,
        scrape_team_stats=not args.no_team_stats,
        scrape_opponent_stats=not args.no_opponent_stats,
        scrape_rankings=not args.no_rankings,
        wait_time=args.wait_time
    )


if __name__ == "__main__":
    main() 