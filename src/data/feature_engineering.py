"""
Feature engineering for March Madness prediction models.

This module handles creating features from raw data, with special emphasis
on weighting recent years more heavily as specified in the requirements.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Union


def weight_by_recency(data_by_year: Dict[int, pd.DataFrame], 
                      emphasis_years: int = 2,
                      base_weight: float = 1.0,
                      emphasis_weight: float = 2.0) -> pd.DataFrame:
    """
    Weight data by recency, with higher weights for more recent years.
    
    Args:
        data_by_year: Dictionary mapping years to DataFrames
        emphasis_years: Number of most recent years to weight more heavily
        base_weight: Weight for older years
        emphasis_weight: Weight for recent years
    
    Returns:
        Weighted and combined DataFrame
    """
    if not data_by_year:
        raise ValueError("No data provided")
    
    # Sort years in ascending order
    years = sorted(data_by_year.keys())
    
    # Identify years to emphasize
    recent_years = years[-emphasis_years:] if len(years) >= emphasis_years else years
    
    weighted_dfs = []
    for year, df in data_by_year.items():
        if not df.empty:
            # Add year as a column
            year_df = df.copy()
            year_df['year'] = year
            
            # Apply weight based on recency
            weight = emphasis_weight if year in recent_years else base_weight
            year_df['weight'] = weight
            
            weighted_dfs.append(year_df)
    
    if not weighted_dfs:
        raise ValueError("No valid data frames to combine")
    
    # Combine all years into a single DataFrame
    return pd.concat(weighted_dfs, ignore_index=True)


def create_team_features(team_stats_by_year: Dict[int, pd.DataFrame],
                         rankings_by_year: Optional[Dict[int, pd.DataFrame]] = None,
                         emphasis_years: int = 2) -> pd.DataFrame:
    """
    Create team features from historical team statistics and rankings.
    
    Args:
        team_stats_by_year: Dictionary mapping years to team stats DataFrames
        rankings_by_year: Dictionary mapping years to team rankings DataFrames
        emphasis_years: Number of recent years to emphasize in weighting
    
    Returns:
        DataFrame with engineered team features
    """
    # Weight team stats by recency
    weighted_stats = weight_by_recency(team_stats_by_year, emphasis_years)
    
    # Add rankings if available
    if rankings_by_year:
        weighted_rankings = weight_by_recency(rankings_by_year, emphasis_years)
        # Merge rankings with team stats (implementation depends on actual data structure)
        # This is a placeholder - actual implementation will depend on data structure
        pass
    
    # Additional feature engineering would go here
    # This is a placeholder - would need to be tailored to actual data
    
    return weighted_stats


def create_matchup_features(team_features: pd.DataFrame, 
                            games_by_year: Dict[int, pd.DataFrame],
                            emphasis_years: int = 2) -> pd.DataFrame:
    """
    Create features for game matchups based on team features and historical games.
    
    Args:
        team_features: DataFrame with team features
        games_by_year: Dictionary mapping years to game results DataFrames
        emphasis_years: Number of recent years to emphasize
    
    Returns:
        DataFrame with matchup features for modeling
    """
    # Weight game results by recency
    weighted_games = weight_by_recency(games_by_year, emphasis_years)
    
    # Create matchup features
    # This is a placeholder - actual implementation will depend on data structure
    
    return weighted_games  # Placeholder return


def prepare_training_data(team_stats_by_year: Dict[int, pd.DataFrame],
                          rankings_by_year: Dict[int, pd.DataFrame],
                          games_by_year: Dict[int, pd.DataFrame],
                          target_years: List[int]) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare training data for model training.
    
    Args:
        team_stats_by_year: Dictionary mapping years to team stats DataFrames
        rankings_by_year: Dictionary mapping years to rankings DataFrames
        games_by_year: Dictionary mapping years to game results DataFrames
        target_years: Years to include in training data
    
    Returns:
        Tuple of (X, y) where X is feature DataFrame and y is target Series
    """
    # Filter to only include target years
    filtered_stats = {year: df for year, df in team_stats_by_year.items() if year in target_years}
    filtered_rankings = {year: df for year, df in rankings_by_year.items() if year in target_years}
    filtered_games = {year: df for year, df in games_by_year.items() if year in target_years}
    
    # Create team features
    team_features = create_team_features(filtered_stats, filtered_rankings)
    
    # Create matchup features
    matchup_features = create_matchup_features(team_features, filtered_games)
    
    # Extract features (X) and target (y)
    # This is a placeholder - actual implementation will depend on data structure
    X = matchup_features.drop('outcome', axis=1)  # Assuming 'outcome' is the target column
    y = matchup_features['outcome']
    
    return X, y


def prepare_tournament_predictions(team_stats_by_year: Dict[int, pd.DataFrame],
                                   rankings_by_year: Dict[int, pd.DataFrame],
                                   tournament_matchups: pd.DataFrame,
                                   emphasis_years: int = 2) -> pd.DataFrame:
    """
    Prepare prediction data for tournament matchups.
    
    Args:
        team_stats_by_year: Dictionary mapping years to team stats DataFrames
        rankings_by_year: Dictionary mapping years to rankings DataFrames
        tournament_matchups: DataFrame with tournament matchups to predict
        emphasis_years: Number of recent years to emphasize
        
    Returns:
        DataFrame with features for tournament matchup predictions
    """
    # Create team features with emphasis on recent years
    team_features = create_team_features(team_stats_by_year, rankings_by_year, emphasis_years)
    
    # Create features for tournament matchups
    # This is a placeholder - actual implementation will depend on data structure
    
    return tournament_matchups  # Placeholder return 