import pandas as pd
import glob
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import requests
import pytz

from useful_methods import find_installations_with_solar_in_2023_or_2024, handle_timezones, get_sunrise_sunset, \
    zero_out_solar_between_sunrise_sunset, correct_solar_zeros_between_sunrise_sunset, \
    zero_out_negative_values_between_sunrise_sunset

# Disable the future downcasting warning
pd.set_option('future.no_silent_downcasting', True)

#Import the time zones
timezones = pd.read_csv('timezones.csv', names=['installationId', 'timezone']).drop(0)

# Set index in the timezones
timezones.set_index('installationId', inplace=True)

"""
#Import the time zones
timezones = pd.read_csv('timezones.csv', names=['installationId', 'timezone']).drop(0)

# Find the installation for which the analysis need to be implemented
installationIds = timezones.installationId.values.tolist()

# Set index in the timezones
timezones.set_index('installationId', inplace=True)

# Find the installations that have solar measurements in 2023 or 2024
installations = find_installations_with_solar_in_2023_or_2024(installationIds)

# Convert list to DataFrame
df_installations = pd.DataFrame(installations, columns=["installations"])

# Save to CSV
df_installations.to_csv("data//installations_with_solar_in_2023_or_2024.csv", index=False)
"""


# Retrieve the installations with solar measurements in 2023 or 2024
installations = pd.read_csv('data//installations_with_solar_in_2023_or_2024.csv')


"""
# Iterate over the installation ids
for installationId in installations.id:

    print(installationId)
    # plot_evs_with_mains(installationId, timezones)

    # installationId = 'installation11'

    # mains_files = glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installationId}/*_IDD*.parquet', recursive=True)

    solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installationId}/*_SOLAR_{2023}*.parquet',
                             recursive=True) +
                   glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installationId}/*_SOLAR_{2024}*.parquet',
                            recursive=True))
"""

# Iterate over all the installations under examination
for installation in installations.id:
#for installation in installations.id:

    # installation = installations.id.loc[0]

    solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2023}*.parquet',
                             recursive=True) +
                   glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2024}*.parquet',
                             recursive=True))

    #file = solar_files[0]

    # Create a list to store all the corrected dataframes of all files for the installation under examination
    dataframes_list = []

    # Iterate over the solar files
    for file in solar_files:

        # Read the parquet filed
        data = pd.read_parquet(file)

        # Fill NaN values with 0 and convert to float
        data['SOLAR'] = pd.to_numeric(data['SOLAR'].fillna(0), errors='coerce')

        # Zero out negative values
        data['SOLAR'] = data['SOLAR'].apply(lambda x: x if x >= 0 else 0)

        # Retrieve the timezone of the installation under examination
        timezone = timezones.loc[installation].values[0]

        # Handle the timezones of the data
        data = handle_timezones(data, timezone)

        # Create a dictionary based on the date
        data_dict = {date: group for date, group in data.groupby(data.index.date)}

        # Iterate over the days and zero out the values that are between sunrise and sunset, and correct the zero values
        for day in data_dict.keys():

            # Get the sunrise and the sunset for the day under examination
            sunrise, sunset = get_sunrise_sunset(day, timezone)

            # Zero out the values before and after sunset
            data_dict[day] = zero_out_solar_between_sunrise_sunset(data_dict[day], sunrise, sunset)

            # Correct the zero values between sunrise and sunset
            data_dict[day] = correct_solar_zeros_between_sunrise_sunset(data_dict[day], sunrise, sunset)

        # Concatenate the daily dataframes in the dictionary into one dataframe that contains info for 30 days
        combined_data = pd.concat(data_dict.values())

        # Store the dataframes to the list
        dataframes_list.append(combined_data)

    # Concatenate the dataframes of the list to one final dataframe
    data_installation = pd.concat(dataframes_list)

    #Store the dataframe
    data_installation.to_csv(f'data//solar//{installation}.csv')

    print(installation)

# Change the file ------------------------ HERE
# file = mains_files[0]





