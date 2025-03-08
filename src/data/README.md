# ESPN NCAA Basketball Data Scrapers

This directory contains several modules for scraping men's college basketball data from ESPN.com. These scrapers are designed to collect historical data to be used for March Madness bracket predictions.

## Available Scrapers

- **Team Stats Scraper**: Collects team offensive statistics
- **Opponent Stats Scraper**: Collects team defensive statistics (stats by opponents)
- **Rankings Scraper**: Collects AP and Coaches Poll rankings

## Usage

### Prerequisites

Before using the scrapers, make sure you have installed all the required dependencies:

```bash
pip install -r ../../requirements.txt
```

### Running Individual Scrapers

Each scraper can be run independently:

#### Team Stats Scraper

```bash
python espn_stats_scraper.py --start-year 2018 --end-year 2023 --output-dir ../../data
```

#### Opponent Stats Scraper

```bash
python espn_opponent_stats_scraper.py --start-year 2018 --end-year 2023 --output-dir ../../data
```

#### Rankings Scraper

```bash
python espn_rankings_scraper.py --start-year 2018 --end-year 2023 --output-dir ../../data
```

### Running All Scrapers at Once

To collect all types of data at once, use the comprehensive scraping script:

```bash
python scrape_all_data.py --start-year 2018 --end-year 2023 --output-dir ../../data
```

### Options

All scrapers support the following options:

- `--start-year`: First year to scrape data for (e.g., 2018)
- `--end-year`: Last year to scrape data for (e.g., 2023)
- `--output-dir`: Directory to save the scraped data
- `--use-selenium`: Use Selenium for scraping (required for JavaScript-rendered content)

The comprehensive scraper also supports:

- `--no-team-stats`: Skip team stats scraping
- `--no-opponent-stats`: Skip opponent stats scraping
- `--no-rankings`: Skip rankings scraping
- `--wait-time`: Wait time between different scraper runs in seconds

## Output

The scrapers will save data in CSV format with the following organization:

```
data/
├── 2018/
│   ├── team_stats.csv
│   ├── opponent_stats.csv
│   └── rankings.csv
├── 2019/
│   ├── team_stats.csv
...
```

Each file contains the following data:

- **team_stats.csv**: Team name and offensive statistics
- **opponent_stats.csv**: Team name and defensive statistics (stats by opponents)
- **rankings.csv**: Team rankings from AP and Coaches polls

## Data Integration

This data can be used with the feature engineering modules in this project to create input features for the March Madness prediction models.

## Notes

- The scrapers include both BeautifulSoup-based and Selenium-based implementations
- For reliable scraping of ESPN's JavaScript-rendered content, use the `--use-selenium` flag
- Be respectful with scraping frequency to avoid being blocked by ESPN
- Data structures on ESPN might change over time, requiring scraper updates 