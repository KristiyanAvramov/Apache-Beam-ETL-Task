import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2

# Define the pipeline options
options = PipelineOptions()
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DirectRunner'

# Create a pipeline
with beam.Pipeline(options=options) as p:

    # Read data 
    data = p | 'Read CSV' >> beam.io.ReadFromText('partners_data.csv')

    # Define a transformation function
    def transform_data(row):
        columns = row.split(',')
        transformed_data = {
            'clicks': int(columns[0]),
            'registrations': int(columns[1]),
            'deposits': float(columns[2]),
            'earnings': float(columns[3],
        }
        return transformed_data

    # Transformation function
    transformed_data = data | 'Transform Data' >> beam.Map(transform_data)

    # Filter data (keep records with clicks >= 10)
    filtered_data = transformed_data | 'Filter Data' >> beam.Filter(lambda x: x['clicks'] >= 10)

    # Aggregate data (sum earnings by partner)
    aggregated_data = (
        filtered_data
        | 'Group by Partner' >> beam.GroupBy(lambda x: 'Total')
        | 'Sum Earnings' >> beam.Map(lambda (key, values): {'partner': key, 'total_earnings': sum(v['earnings'] for v in values)})
    )

    # Write the aggregated data to PostgreSQL
    aggregated_data | 'Write to PostgreSQL' >> beam.io.WriteToText(
        'output.txt', file_name_suffix='.csv', num_shards=1)

# Function to load data into PostgreSQL
def load_to_postgresql(input_file):
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

# Load the aggregated data into PostgreSQL
load_to_postgresql('output.txt-00000-of-00001.csv')
