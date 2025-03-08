# March Madness Prediction Model

A machine learning project to predict NCAA March Madness tournament outcomes.

## Project Overview

This project aims to build prediction models for the NCAA March Madness basketball tournament. It focuses on:

- Using historical team and game data
- Emphasizing recent performance (especially last 2 years)
- Incorporating team statistics, opponent statistics, and current rankings
- Testing multiple modeling approaches

## Project Structure

```
MarchMadnessV2/
├── config/             # Configuration files
├── data/               # Data storage (organized by year and data type)
├── models/             # Model implementations
│   ├── baseline/       # Simple baseline models
│   ├── ml/             # Traditional machine learning models
│   ├── deep_learning/  # Neural network models
│   └── ensemble/       # Ensemble methods combining multiple models
├── notebooks/          # Jupyter notebooks for exploration and analysis
├── src/                # Source code
│   ├── data/           # Code for data collection and processing
│   ├── models/         # Model implementation code
│   ├── visualization/  # Visualization tools and code
│   └── utils/          # Utility functions
└── tests/              # Unit tests
```

## Feature Engineering

The model prioritizes:
- Historical team performance with higher weights for recent years (last 2 years)
- Team and opponent statistics
- Current team rankings
- Tournament performance

## Getting Started

[Instructions to be added as project develops]

## Dependencies

[Dependencies to be added]
