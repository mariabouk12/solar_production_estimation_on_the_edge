import pandas as pd
import glob
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


#Import the time zones
timezones = pd.read_csv('timezones.csv', names=['installationId', 'timezone']).drop(0)

# Set index in the timezones
timezones.set_index('installationId', inplace=True)

# Load data
installations = pd.read_csv('data//installations_with_solar_in_2023_or_2024.csv')
installations_with_small_capacities = pd.read_csv('data//small_capacities.csv')


# Filter installations where id is not in installations_with_small_capacities
filtered_installations = installations[
    ~installations['id'].isin(installations_with_small_capacities['installation_id'])
]



# Iterate over all the installations under examination
for installation in filtered_installations.id:

    print(installation)

    # installation = filtered_installations['id'].loc[0]

    solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2023}*.parquet',
                             recursive=True) +
                   glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2024}*.parquet',
                             recursive=True))

    # Create a list to store all the main files
    main_files = []

    # Iterate over the solar file
    for file in solar_files:

        # Extract the start and stop date of the solar file
        start_date = file.split('_')[3]
        stop_date = file.split('_')[4].split('.')[0]

        time_period = start_date + '_' + stop_date

        # Retrieve the mains file corresponding to this time period
        main_file = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_{time_period}_IDD**.parquet',
                               recursive=True))

        # Add the main file to the list
        if main_file:  # Append only if a match is found
            main_files.extend(main_file)


# --------------------------------------- FOR ONE INSTALLATION --------------------------------------------------------

# Execution for one installation

installation = filtered_installations['id'].loc[0]

solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2023}*.parquet',
                             recursive=True) +
                glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2024}*.parquet',
                          recursive=True))

# Create a list to store all the main files
main_files = []

# Iterate over the solar file
for file in solar_files:
    # Extract the start and stop date of the solar file
    start_date = file.split('_')[3]
    stop_date = file.split('_')[4].split('.')[0]

    time_period = start_date + '_' + stop_date

    # Retrieve the mains file corresponding to this time period
    main_file = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_{time_period}_IDD**.parquet',
                           recursive=True))

    # Add the main file to the list
    if main_file:  # Append only if a match is found

        # Read the parquet filed
        data = pd.read_parquet(main_file)

        # Fill NaN values with 0 and convert to float
        data['MAINS'] = pd.to_numeric(data['MAINS'].fillna(0), errors='coerce')

        main_files.extend(main_file)


for file in main_files:

    file = main_files[0]

    # Read the parquet filed
    data = pd.read_parquet(file)

    #
    data.sort_index()

    # Fill NaN values with 0 and convert to float
    data['MAINS'] = pd.to_numeric(data['MAINS'].fillna(0), errors='coerce')

    # Replace values that are greater than 50kW with zeros.
    data['MAINS'] = data['MAINS'].apply(lambda x: x if x < 50 else 0)

    # Zero out negative values
    # data['SOLAR'] = data['SOLAR'].apply(lambda x: x if x >= 0 else 0)

    # Retrieve the timezone of the installation under examination
    timezone = timezones.loc[installation].values[0]

    # Handle the timezones of the data
    data = handle_timezones(data, timezone)

    # Create a dictionary based on the date
    data_dict = {date: group for date, group in data.groupby(data.index.date)}

    # Iterate over the days and zero out the values that are between sunrise and sunset, and correct the zero values
    for day in data_dict.keys():

        print(day)
        # Get the sunrise and the sunset for the day under examination
        sunrise, sunset = get_sunrise_sunset(day, timezone)

        # Zero out the negative values before and after sunset
        # data_dict[day] = zero_out_negative_values_between_sunrise_sunset(data_dict[day], sunrise, sunset)

        # Correct the zero values between sunrise and sunset
        # data_dict[day] = correct_solar_zeros_between_sunrise_sunset(data_dict[day], sunrise, sunset)

    # Concatenate the daily dataframes in the dictionary into one dataframe that contains info for 30 days
    combined_data = pd.concat(data_dict.values())

