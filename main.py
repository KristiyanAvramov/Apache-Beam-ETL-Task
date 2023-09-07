import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import os
import logging
from google.cloud import logging as cloud_logging
from google.cloud import monitoring_v3

# Initialize Google Cloud Logging client
logging_client = cloud_logging.Client()

# Define the pipeline options
options = PipelineOptions()
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DirectRunner'

# Create a pipeline
with beam.Pipeline(options=options) as p:

    # Set up a logger for pipeline execution
    logger = logging.getLogger(__name__)

    # Check if the CSV file exists
    input_file = 'partners_data.csv'
    if not os.path.exists(input_file):
        logger.error(f"CSV file '{input_file}' does not exist.")
        raise ValueError(f"CSV file '{input_file}' does not exist.")

    # Read data from the CSV file
    data = p | 'Read CSV' >> beam.io.ReadFromText(input_file)

    # Transformation function
    def transform_data(row):
        try:
            # transformations 
            columns = row.split(',')
            transformed_data = {
                'clicks': int(columns[0]),
                'registrations': int(columns[1]),
                'deposits': float(columns[2]),
                'earnings': float(columns[3])
                # Add more transformations as needed
            }
            return transformed_data
        except Exception as e:
            # Handle malformed data gracefully
            logger.error(f"Error transforming data: {e}")
            return None

    # Apply the transformation function
    transformed_data = data | 'Transform Data' >> beam.Map(transform_data)

    # Filter data (keep records with clicks >= 10)
    filtered_data = transformed_data | 'Filter Data' >> beam.Filter(lambda x: x and x['clicks'] >= 10)

    # Check if any data records passed the filter
    if not filtered_data:
        logger.warning("No data passed the filter criteria.")
        raise ValueError("No data passed the filter criteria.")

    # Aggregate data (sum earnings by partner)
    aggregated_data = (
        filtered_data
        | 'Group by Partner' >> beam.GroupBy(lambda x: 'Total')
        | 'Sum Earnings' >> beam.Map(lambda (key, values): {'partner': key, 'total_earnings': sum(v['earnings'] for v in values)})
    )

    # Write the aggregated data to PostgreSQL
    aggregated_data | 'Write to PostgreSQL' >> beam.io.WriteToText(
        'output.txt', file_name_suffix='.csv', num_shards=1)

    # Set up monitoring client
    monitoring_client = monitoring_v3.MetricServiceClient()
    project_id = 'your-project-id'  # Replace with your GCP project ID

    # Define a custom metric for tracking successful pipeline execution
    metric_type = 'custom.googleapis.com/pipeline_execution'
    resource = f"projects/{project_id}"
    metric = monitoring_v3.MetricServiceClient.metric_descriptor_path(
        project_id, metric_type)

    # Create a time series entry for successful execution
    time_series = monitoring_v3.TimeSeries()
    time_series.metric.type = metric_type
    time_series.resource.type = resource
    time_series.resource.labels['project_id'] = project_id

    point = time_series.points.add()
    point.value.int64_value = 1  # 1 indicates successful execution

    # Write the time series data to Stackdriver Monitoring
    monitoring_client.create_time_series(
        name=f"projects/{project_id}", time_series=[time_series])
    

# Define a function to load data into PostgreSQL
def load_to_postgresql(input_file):
    try:
        conn = psycopg2.connect(
            dbname='database',
            user='user',
            password='password',
            host='host',
            port='port'
        )
        cursor = conn.cursor()
        
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS partner_data (
            clicks INT,
            registrations INT,
            deposits FLOAT,
            earnings FLOAT
        )
        """
        cursor.execute(create_table_query)
        
        # Load data from the CSV file into the table
        copy_query = f"COPY partner_data FROM '{input_file}' DELIMITER ',' CSV HEADER"
        cursor.execute(copy_query)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Log successful data loading
        logger.info("Data loaded into PostgreSQL successfully.")
    except psycopg2.Error as e:
        # Handle database connection errors
        logger.error(f"Database error: {e}")
        raise

# Load the aggregated data into PostgreSQL
try:
    load_to_postgresql('output.txt-00000-of-00001.csv')
except Exception as e:
    # Handle any other errors during data loading
    logger.error(f"Error loading data to PostgreSQL: {e}")


