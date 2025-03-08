"""
Data loading utilities for March Madness prediction.

This module provides functions to load different types of basketball data,
including team stats, rankings, and tournament results.
"""
import pandas as pd
import os
from typing import Dict, Optional, Union, List


def load_team_stats(year: int, data_dir: str = "data") -> pd.DataFrame:
    """
    Load team statistics for a specific year.
    
    Args:
        year: The year to load data for
        data_dir: Base directory for data
        
    Returns:
        DataFrame containing team statistics
    """
    # Placeholder - implementation will depend on actual data structure
    file_path = os.path.join(data_dir, f"{year}", "team_stats.csv")
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        raise FileNotFoundError(f"Team stats file not found for year {year}")


def load_rankings(year: int, data_dir: str = "data") -> pd.DataFrame:
    """
    Load team rankings for a specific year.
    
    Args:
        year: The year to load rankings for
        data_dir: Base directory for data
        
    Returns:
        DataFrame containing team rankings
    """
    # Placeholder - implementation will depend on actual data structure
    file_path = os.path.join(data_dir, f"{year}", "rankings.csv")
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        raise FileNotFoundError(f"Rankings file not found for year {year}")


def load_game_results(year: int, data_dir: str = "data") -> pd.DataFrame:
    """
    Load game results for a specific year.
    
    Args:
        year: The year to load game results for
        data_dir: Base directory for data
        
    Returns:
        DataFrame containing game results
    """
    # Placeholder - implementation will depend on actual data structure
    file_path = os.path.join(data_dir, f"{year}", "games.csv")
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        raise FileNotFoundError(f"Game results file not found for year {year}")


def load_tournament_data(year: int, data_dir: str = "data") -> pd.DataFrame:
    """
    Load tournament data for a specific year.
    
    Args:
        year: The year to load tournament data for
        data_dir: Base directory for data
        
    Returns:
        DataFrame containing tournament data
    """
    # Placeholder - implementation will depend on actual data structure
    file_path = os.path.join(data_dir, f"{year}", "tournament.csv")
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        raise FileNotFoundError(f"Tournament file not found for year {year}")


def load_multi_year_data(years: List[int], data_type: str, data_dir: str = "data") -> Dict[int, pd.DataFrame]:
    """
    Load same type of data for multiple years.
    
    Args:
        years: List of years to load data for
        data_type: Type of data to load ('team_stats', 'rankings', 'games', 'tournament')
        data_dir: Base directory for data
        
    Returns:
        Dictionary mapping years to DataFrames
    """
    loaders = {
        'team_stats': load_team_stats,
        'rankings': load_rankings,
        'games': load_game_results,
        'tournament': load_tournament_data
    }
    
    if data_type not in loaders:
        raise ValueError(f"Unknown data type: {data_type}. Must be one of {list(loaders.keys())}")
    
    result = {}
    for year in years:
        try:
            result[year] = loaders[data_type](year, data_dir)
        except FileNotFoundError as e:
            print(f"Warning: {e}")
    
    return result 