import numpy as np
import pandas as pd
from typing import List
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

def get_freq_time_series_data(
    data: pd.DataFrame | pd.Series,
    columns_to_show: List[str],
    date_col: str,
    freq: str
) -> pd.DataFrame | pd.Series:
    """
    Obtain time series data up to today, specified by frequency (weekly or monthly)

    Parameters
    ----------
        data: pd.DataFrame | pd.Series
            specified data, with `date_col` inside the columns

        columns_to_show: List[str]
            specified columns to show in the end

        date_col: str
            date columns contained in `data`
        
        freq: str
            frequency strings

    Returns
    ----------
        freq_data: pd.DataFrame
            frequency-based time series data
    """
    # Set a data
    data: pd.DataFrame = data[columns_to_show].set_index(date_col)

    # Resampling the data in weekly basis
    freq_data: pd.DataFrame = data.resample(freq).sum()
    
    # Initialize start and end date
    start_dt: date = freq_data.index[0].date()
    end_dt: date = datetime.today().date()

    # Check the months of difference between two dates
    month_diff: int = relativedelta(end_dt, start_dt).months
    if month_diff <= 6:
        start_dt: date = end_dt - relativedelta(months=6)

    # Assign indexes for later
    freq_data_indexes: pd.DatetimeIndex = pd.date_range(
        start=start_dt,
        end=end_dt,
        freq=freq
    )

    # Get whole data up to recent week
    freq_data: pd.DataFrame = pd.DataFrame(
        data=freq_data,
        index=freq_data_indexes
    )

    # Missing values = no transaction
    freq_data.fillna(0, inplace=True)
    return freq_data

def special_monte_carlo_simulation(
    time_series_data: pd.Series,
    n_size: int,
    n_simulation: int,
    param_scale: float = 1
) -> List[float]:
    """
    Generate samples from Monte Carlo Simulation on Time-Series Indexes, with prior distribution of Exponential.

    Parameters
    ----------
        time_series_data: pd.Series
            specified time-series data, with corresponding frequency. It must be pandas.Series
        
        n_size: int
            number of sample size

        n_simulation:
            number of simulation

        param_scale: float
            The scale parameter. Must be non-negative

    Returns
    ----------
        mean_samples: List[float]
            samples generated from 'Special' Monte Carlo Simulation
    """
    # Save the sample mean in here
    mean_samples: List[float] = []


    for _ in range(n_simulation):

        # We will chose prior distribution to be Exponential
        random_samples: np.ndarray = np.random.exponential(scale=param_scale, size=n_size)

        # Standardize the samples to have a range of (0, 1)
        min_max_scaling = lambda x: (x - random_samples.min()) / (random_samples.max() - random_samples.min())
        standardized_samples: np.ndarray = np.vectorize(min_max_scaling)(random_samples)

        # Give a magnitude of length to standardized_samples
        standardized_samples: np.ndarray = np.floor(standardized_samples * (n_size - 1)) + 1
        standardized_samples = standardized_samples.astype(int)

        # Get real samples, corresponding to sampled labels
        converted_samples: List[float] = [time_series_data.iloc[-num_loc] for num_loc in standardized_samples]
        
        # Calculate mean and variance from the sample
        converted_samples_mean: float = np.mean(converted_samples)
        converted_samples_std: float = np.std(converted_samples)

        # Generate random mean sample (Central Limit Theorem)
        mean_sample: float = np.random.normal(
            loc=converted_samples_mean,
            scale=converted_samples_std
        )

        mean_samples.append(mean_sample)

    return mean_samples

def calculate_purchase_prob(
    # PARAMETERS OF `get_freq_time_series_data`
    data: pd.DataFrame,
    columns_to_show: List[str],
    date_col: str,
    freq: str,
    
    # PARAMETERS OF `special_monte_carlo_simulation`
    column_to_observe: str,
    n_simulation: int,
    param_scale: float = 1
) -> float:
    """
    Calculate purchase probability, started from: 
        1. Obtain frequency-based time series data
        2. Do monte carlo simulation
        3. Calculate purchase probability, based on specified threshold

    Parameters
    ----------
        data: pd.DataFrame | pd.Series
            specified data, with `date_col` inside the columns

        columns_to_show: List[str]
            specified columns to show in the end

        date_col: str
            date columns contained in `data`
        
        freq: str
            frequency strings

        columns_to_observe: str
            specified column, which contained in `columns_to_show` as important factor to calculate purchase probability

        n_simulation:
            number of simulation

        param_scale: float
            The scale parameter. Must be non-negative

        epsilon: float
            give lower bound to our threshold

    Returns
    ----------
        purchase_prob: float
            probability of purchase, ranged from 0 to 1
    """
    # Obtain data with frequency-based index
    freq_data: pd.DataFrame = get_freq_time_series_data(
        data=data,
        columns_to_show=columns_to_show,
        date_col=date_col,
        freq=freq
    )

    # Obtain samples from Monte Carlo Simulation
    n_size: int = len(freq_data)
    mean_samples: List[float] = special_monte_carlo_simulation(
        time_series_data=freq_data[column_to_observe],
        n_size=n_size,
        n_simulation=n_simulation,
        param_scale=param_scale
    )

    # Calculate the Purchase probability
    purchase_prob: float = np.mean(np.array(mean_samples) > 0)
    return purchase_prob