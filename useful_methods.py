import pandas as pd
import pytz
import glob
from itertools import chain
from datetime import datetime
import requests

DEFAULT_TIMEZONE = 'America/Chicago'


def find_installations_with_solar_in_2023_or_2024(installationIds):
    """
    This method is responsible for finding and returning all the installations of the initial list that contain solar
    measurements in 2023 or 2024.

    :param   installationIds: the initial list of installation ids

    :return: the list of installations with solar in 2023 or 2024
    """

    # Initialize an empty list to store the files
    installations = []

    # Iterate over the installation ids
    for installationId in installationIds:

        # Retrieve the solar files of 2023 or 2024
        solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installationId}/*_SOLAR_{2023}*.parquet',
                                 recursive=True) +
                       glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installationId}/*_SOLAR_{2024}*.parquet',
                                 recursive=True))
        # If this list is not empty
        if solar_files:

            # Add the installation under examination to the list
            installations.append(installationId)

    # Return the list with installations
    return installations


def handle_timezones(df, timezone):
    """
    This method handles the timezones of the dataframes. More precisely, it accepts as inputs the dataframe and the
    desired timezone, and it moves it to this timezone.

    :param df:       The dataframe under examination
    :param timezone: The desired timezone for this dataset

    :return:         The dataset after moving it to the desired timezone.
    """

    # Preprocess time index
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='last')]

    # Get the default timezone
    tz = pytz.timezone(DEFAULT_TIMEZONE)

    # Handle timezone issues of NonExistentTimeError and AmbiguousTimeError
    try:
        # Localize and convert index to timezone
        df.index = df.index.tz_localize(DEFAULT_TIMEZONE).tz_convert(timezone)

    except (pytz.NonExistentTimeError, pytz.AmbiguousTimeError):

        # Store original time index
        original_index = df.index.copy()

        # Fill ambiguous and non-existent values with 'NaT'
        df.index = df.index.tz_localize(DEFAULT_TIMEZONE, ambiguous='NaT', nonexistent='NaT')

        # Find days with issues
        nat_datetimes = original_index[df.index.date != original_index.date]

        # Localize with pytz
        df.reset_index(inplace=True)
        df.loc[df['localminute'].isnull(), 'localminute'] = [tz.localize(dt) for dt in nat_datetimes]
        df.set_index('localminute', inplace=True)

        # Convert index to timezone
        df.index = df.index.tz_convert(timezone)
    return df

# Function to get sunrise and sunset times from the Sunrise-Sunset API
def get_sunrise_sunset(date, timezone_str):
    # Convert the date to string format YYYY-MM-DD
    date_str = date.strftime('%Y-%m-%d')

    # API URL to get sunrise and sunset for a date in a given timezone
    url = f"https://api.sunrise-sunset.org/json?lat=41.8781&lng=-87.6298&date={date_str}&formatted=0"

    # Make the API request
    response = requests.get(url)

    # Check if the response is valid
    if response.status_code == 200:
        data = response.json()
        if 'results' in data:
            # Extract the sunrise and sunset times in UTC
            sunrise_utc = data['results']['sunrise']
            sunset_utc = data['results']['sunset']

            # Convert the string timezone to a pytz timezone object
            timezone = pytz.timezone(timezone_str)

            # Convert UTC times to the desired timezone
            sunrise = datetime.fromisoformat(sunrise_utc).astimezone(timezone)
            sunset = datetime.fromisoformat(sunset_utc).astimezone(timezone)

            return sunrise, sunset
        else:
            print(f"Error: Unable to retrieve sunrise/sunset times for {date_str}")
            return None, None
    else:
        print(f"Error: Unable to reach API for {date_str}")
        return None, None


# Function to zero out solar values between sunrise and sunset
def zero_out_solar_between_sunrise_sunset(data, sunrise, sunset):
    # Iterate over the unique days in the dataframe

    if sunrise and sunset:
        # Mask the data for the time range between sunrise and sunset
        mask = (data.index <= sunrise) & (data.index >= sunset)

        # Set the solar values to 0 for the time range between sunrise and sunset
        data.loc[mask, 'SOLAR'] = 0

        '''
        # Create a mask for the time range between sunrise and sunset
        mask = (data.index >= sunrise) & (data.index <= sunset)

        # Get only the data between sunrise and sunset
        filtered_data = data.loc[mask]

        # Iterate over the rows where 'SOLAR' is zero
        for idx in filtered_data.index[filtered_data['SOLAR'] == 0]:
            # Get the previous and next non-zero values
            prev_val = data.loc[:idx, 'SOLAR'].iloc[:-1].replace(0, pd.NA).dropna().iloc[-1]
            next_val = data.loc[idx:, 'SOLAR'].iloc[1:].replace(0, pd.NA).dropna().iloc[0]

            # Replace the zero value with the mean of the previous and next values
            data.at[idx, 'SOLAR'] = (prev_val + next_val) / 2
        '''

    return data

def zero_out_solar_between_sunrise_sunset(data, sunrise, sunset):
    # Iterate over the unique days in the dataframe

    if sunrise and sunset:
        # Mask the data for the time range between sunrise and sunset
        mask = (data.index <= sunrise) & (data.index >= sunset)

        # Set the solar values to 0 for the time range between sunrise and sunset
        data.loc[mask, 'SOLAR'] = 0

        '''
        # Create a mask for the time range between sunrise and sunset
        mask = (data.index >= sunrise) & (data.index <= sunset)

        # Get only the data between sunrise and sunset
        filtered_data = data.loc[mask]

        # Iterate over the rows where 'SOLAR' is zero
        for idx in filtered_data.index[filtered_data['SOLAR'] == 0]:
            # Get the previous and next non-zero values
            prev_val = data.loc[:idx, 'SOLAR'].iloc[:-1].replace(0, pd.NA).dropna().iloc[-1]
            next_val = data.loc[idx:, 'SOLAR'].iloc[1:].replace(0, pd.NA).dropna().iloc[0]

            # Replace the zero value with the mean of the previous and next values
            data.at[idx, 'SOLAR'] = (prev_val + next_val) / 2
        '''

    return data

def zero_out_negative_values_between_sunrise_sunset(data, sunrise, sunset):
    # Iterate over the unique days in the dataframe

    if sunrise and sunset:
        # Mask the data for the time range between sunrise and sunset
        mask = (data.index <= sunrise) & (data.index >= sunset)

        # Set the solar values to 0 for the time range between sunrise and sunset
        data.loc[mask, 'MAINS'] < 0

        '''
        # Create a mask for the time range between sunrise and sunset
        mask = (data.index >= sunrise) & (data.index <= sunset)

        # Get only the data between sunrise and sunset
        filtered_data = data.loc[mask]

        # Iterate over the rows where 'SOLAR' is zero
        for idx in filtered_data.index[filtered_data['SOLAR'] == 0]:
            # Get the previous and next non-zero values
            prev_val = data.loc[:idx, 'SOLAR'].iloc[:-1].replace(0, pd.NA).dropna().iloc[-1]
            next_val = data.loc[idx:, 'SOLAR'].iloc[1:].replace(0, pd.NA).dropna().iloc[0]

            # Replace the zero value with the mean of the previous and next values
            data.at[idx, 'SOLAR'] = (prev_val + next_val) / 2
        '''

    return data

def correct_solar_zeros_between_sunrise_sunset(data, sunrise, sunset):
    """
    Correct zero values in the 'SOLAR' column between sunrise and sunset
    by replacing them with the mean of the previous and next non-zero values.

    Parameters:
        data (pd.DataFrame): DataFrame with a datetime index and a 'SOLAR' column.
        sunrise (str): The sunrise time as a string.
        sunset (str): The sunset time as a string.

    Returns:
        pd.DataFrame: The updated DataFrame with corrected solar values.
    """
    # Create a mask for the time range between sunrise and sunset
    mask = (data.index >= sunrise) & (data.index <= sunset)

    # Get the indices of rows with zero 'SOLAR' values within the mask
    zero_indices = data.loc[mask & (data['SOLAR'] == 0)].index

    for idx in zero_indices:
        # Get the previous non-zero value
        prev_val = data.loc[:idx, 'SOLAR'].replace(0, pd.NA).dropna().iloc[-1] \
            if not data.loc[:idx, 'SOLAR'].replace(0, pd.NA).dropna().empty else 0

        # Get the next non-zero value
        next_val = data.loc[idx:, 'SOLAR'].replace(0, pd.NA).dropna().iloc[0] \
            if not data.loc[idx:, 'SOLAR'].replace(0, pd.NA).dropna().empty else 0

        # Replace the zero value with the mean of the previous and next values
        data.at[idx, 'SOLAR'] = (prev_val + next_val) / 2 if prev_val and next_val else prev_val or next_val

    return data

