import pandas as pd
import glob
from sklearn.cluster import KMeans



# Retrieve the installations with solar measurements in 2023 or 2024
installations = pd.read_csv('data//installations_with_solar_in_2023_or_2024.csv')

# Create a dataframe to store the capacities
capacities = pd.DataFrame(columns=['installation_id', 'capacity'])

for installation in installations.id.head(3):

    #installation = installations.id.loc[0]

    data = pd.read_csv(f'data//solar//{installation}.csv')

    capacity = data[data['SOLAR'] > 0]['SOLAR'].quantile(0.98)

    # capacities.append({'installation_id': 'installation1', 'capacity': capacity}, ignore_index=True)

    # Create a new row as a DataFrame
    new_row = pd.DataFrame([{'installation_id': installation, 'capacity': capacity}])

    # Add the new row to the existing DataFrame using pd.concat
    capacities = pd.concat([capacities, new_row], ignore_index=True)


capacities = pd.DataFrame(columns=['installation_id', 'capacity'])

# Iterate over the installation ids
for installation in installations.id:

    # Retrieve the solar fiels of the installation under examination
    solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2023}*.parquet',
                             recursive=True) +
                   glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2024}*.parquet',
                             recursive=True))

    # Filter files based on the conditions
    matching_files = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '08'
    ]

    matching_files_2 = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '07'
    ]

    matching_files_3 = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '06'
    ]

    matching_files_4 = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '05'
    ]

    matching_files_5 = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '09'
    ]

    matching_files_6 = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '04'
    ]

    matching_files_7 = [
        file for file in solar_files
        if file.split('.')[0].split('_')[4][4:6] == '10'
    ]

    # Set the file as the first solar file available
    file = solar_files[0]

    if matching_files:
        file = matching_files[0]
    elif matching_files_2:
        file = matching_files_2[0]
    elif matching_files_3:
        file = matching_files_3[0]
    elif matching_files_4:
        file = matching_files_4[0]
    elif matching_files_5:
        file = matching_files_5[0]
    elif matching_files_6:
        file = matching_files_6[0]
    elif matching_files_7:
        file = matching_files_7[0]

    # Read the data
    data = pd.read_parquet(file)

    # Fill nas with zero values
    data['SOLAR'] = pd.to_numeric(data['SOLAR'].fillna(0), errors='coerce')

    # Retrieve the capacity as 98th percentile of the positive values
    capacity = data[data['SOLAR'] > 0]['SOLAR'].quantile(0.98)

    # Create a new row as a DataFrame
    new_row = pd.DataFrame([{'installation_id': installation, 'capacity': capacity}])

    # Add the new row to the existing DataFrame using pd.concat
    capacities = pd.concat([capacities, new_row], ignore_index=True)

# Exclude the installations with capacity lower that 1kWatt
capacities = capacities[capacities.capacity > 1]

'''
capacities['capacity_rounded'] = capacities['capacity'].round()
capacity_counts = rounded.groupby('capacity').size()

# Create a bar plot
plt.figure(figsize=(8, 6))  # Optional: Set the size of the figure
sns.barplot(x=capacity_counts.index, y=capacity_counts.values)

# Adding labels and title
plt.title('Number of Installations by Capacity')
plt.xlabel('Capacity')
plt.ylabel('Number of Installations')

# Show the plot
plt.tight_layout()  # Adjust layout to prevent clipping of labels
plt.show()
'''

# Number of clusters
n_clusters = 3

kmeans = KMeans(n_clusters=n_clusters, random_state=42)

capacities['cluster'] = kmeans.fit_predict(capacities[['capacity']])

clusters_info = pd.DataFrame()

clusters_info['median_values'] = capacities.groupby('cluster')['capacity'].median()
clusters_info['cluster'] = capacities.groupby('cluster').size()


capacities.to_csv('data//capacities_clustering.csv')



'''
-----------------------------  Capacities' analysis  ------------------------------------------
'''
capacities = pd.read_csv('data//capacities_clustering.csv')


# Create a group number within each cluster
# capacities['group'] = capacities.groupby('cluster').cumcount() % 2 + 1

for index in capacities.index:

    installation = capacities.loc[index].installation_id

    solar_files = (glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2023}*.parquet',
                             recursive=True) +
                   glob.glob(f'/mnt/12TB/tasos/us_dataset/1min/{installation}/*_SOLAR_{2024}*.parquet',
                             recursive=True))

    capacities.at[index, 'number_of_files'] = len(solar_files)


# Cluster zero
cluster_zero = capacities[capacities['cluster'] == 0]

# Step 1: Count the unique installation_id for each number_of_files
file_counts_zero = (cluster_zero
    .groupby('number_of_files')['installation_id']
    .nunique()  # Count unique installation IDs
    .reset_index()
)

# Step 2: Rename columns for clarity
file_counts_zero.columns = ['number_of_files', 'installation_count']

cluster_one = capacities[capacities['cluster'] == 1]

# Step 1: Count the unique installation_id for each number_of_files
file_counts_one = (cluster_one
    .groupby('number_of_files')['installation_id']
    .nunique()  # Count unique installation IDs
    .reset_index()
)

# Step 2: Rename columns for clarity
file_counts_one.columns = ['number_of_files', 'installation_count']

cluster_two = capacities[capacities['cluster'] == 2]

# Step 1: Count the unique installation_id for each number_of_files
file_counts_two = (cluster_two
    .groupby('number_of_files')['installation_id']
    .nunique()  # Count unique installation IDs
    .reset_index()
)

# Step 2: Rename columns for clarity
file_counts_two.columns = ['number_of_files', 'installation_count']


