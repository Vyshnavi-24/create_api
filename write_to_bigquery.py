import requests
from google.cloud import bigquery


# Replace with your project ID, dataset ID, and table ID
project_id = "ubertestproject-397515"
dataset_id = "Uver_dataset"
table_id = "apidataset"

# Set up BigQuery client
client = bigquery.Client(project_id)


# Define the BigQuery table reference
table_ref = client.dataset(dataset_id).table(table_id)


def fetch_and_insert_data(api_url):
    try:
        # Fetch JSON data from the API URL
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        json_data = response.json()

        try:
            client.get_table(table_ref)
        except Exception:
            raise ValueError(f'Table {table_id} does not exist in dataset {dataset_id}')

        # Insert the JSON data into BigQuery
        errors = client.insert_rows_json(table_ref, json_data)

        # Check for errors during insertion
        if errors:
            print(f'Failed to insert data into BigQuery: {errors}')
        else:
            print('Data inserted successfully')

    except requests.exceptions.RequestException as req_error:
        print(f'Request error: {req_error}')
    except Exception as e:
        print(f'Error: {e}')


if __name__ == '__main__':
    # Replace this URL with the actual API URL

    for i in range(0,100):
        print(i)

        api_url = "https://ubertestproject-397515.uc.r.appspot.com/get_data/"+str(i)+"?api_key=vyshnavi"

        fetch_and_insert_data(api_url)
