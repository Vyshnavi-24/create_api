from flask import Flask, jsonify,request
from google.cloud import bigquery

app = Flask(__name__)

api_key = "vyshnvai"


# Set up BigQuery client
client = bigquery.Client()

# Replace with your project ID and dataset ID
project_id = "ubertestproject-397515"
dataset_id = "Uver_dataset"
table_id = "RatecodeID_dim"

def authenticate_api_key(api_key):
    # Replace this function with your actual API key validation logic
    return api_key == "vyshnavi"

# Endpoint to fetch data from BigQuery
@app.route('/get_data/<int:data_id>', methods=['GET'])
def get_data(data_id):

    # Get the API key from request headers or query parameters
    api_key_from_request = request.headers.get('API-Key') or request.args.get('api_key')

    # Validate the API key
    if not authenticate_api_key(api_key_from_request):
        return jsonify({'error': 'Unauthorized'}), 401
    
    # Construct the SQL query
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` where Ratecode_id={data_id}"

    # Run the query
    query_job = client.query(query)

    # Fetch results
    results = query_job.result()

    # Convert results to a list of dictionaries
    data = [dict(row) for row in results]

    if not data:
        return jsonify({'error': 'No data found for the provided ID'}), 404

    # Return JSON response
    return jsonify(data)

if __name__ == '__main__':
    app.run()
