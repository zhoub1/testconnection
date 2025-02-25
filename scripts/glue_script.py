import sys
import logging
import pandas as pd
from datetime import datetime
from dateutil import parser
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import trim, col, udf, when, regexp_replace, isnan
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from typing import Tuple, Dict, Any

# Authors: Bill Zhou & Justin Miles

# Setup logging
# Test
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def initialize_glue_context() -> Tuple[GlueContext, Any, Job, Dict[str, Any]]:
    """
    Initialize Glue context and job.

    Returns:
        glueContext (GlueContext): The Glue context.
        spark (SparkSession): The Spark session.
        job (Job): The Glue job.
        args (dict): The job arguments.
    """
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'object_key', 'output_path'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    return glueContext, spark, job, args

def detect_delimiter(spark, full_input_path, sample_size=10):
    """
    Detects the delimiter used in a CSV file by analyzing the header and sample lines.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input CSV file.
        sample_size (int): Number of lines to sample for delimiter detection.

    Returns:
        str: The detected delimiter.
    """
    logger.info("Detecting delimiter...")
    # Read the first 'sample_size' lines from the file
    sc = spark.sparkContext
    sample_rdd = sc.textFile(full_input_path, minPartitions=1).zipWithIndex().filter(lambda x: x[1] < sample_size)
    sample_lines = sample_rdd.map(lambda x: x[0]).collect()

    # Possible delimiters to check
    possible_delimiters = [',', '\t', ';', '|']

    delimiter_counts = {delimiter: 0 for delimiter in possible_delimiters}
    for line in sample_lines:
        for delimiter in possible_delimiters:
            # Count delimiters outside quotes
            count = count_delimiter_in_line(line, delimiter)
            delimiter_counts[delimiter] += count

    # Compute average counts per line
    average_counts = {delimiter: count / len(sample_lines) for delimiter, count in delimiter_counts.items()}

    # Choose the delimiter with the highest average count
    detected_delimiter = max(average_counts, key=average_counts.get)
    logger.info(f"Detected delimiter: '{detected_delimiter}'")
    return detected_delimiter

def count_delimiter_in_line(line, delimiter):
    """
    Counts the number of delimiters in a line, ignoring delimiters inside quoted strings.

    Args:
        line (str): The line of text.
        delimiter (str): The delimiter to count.

    Returns:
        int: The count of delimiters outside of quotes.
    """
    in_quote = False
    count = 0
    i = 0
    while i < len(line):
        if line[i] == '"':
            in_quote = not in_quote
        elif line[i] == delimiter and not in_quote:
            count += 1
        i += 1
    return count

def read_csv(spark, full_input_path):
    """
    Read CSV file with proper handling of different delimiters and schema inference.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input CSV file.

    Returns:
        DataFrame: The read DataFrame.
    """
    # Detect the delimiter
    delimiter = detect_delimiter(spark, full_input_path)

    logger.info(f"Reading CSV file with delimiter '{delimiter}'")

    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .option("quote", '"') \
                   .option("escape", '"') \
                   .option("multiLine", "true") \
                   .option("delimiter", delimiter) \
                   .csv(full_input_path)
    return df

def read_txt(spark, full_input_path):
    """
    Read TXT file.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input TXT file.

    Returns:
        DataFrame: The read DataFrame.
    """
    return spark.read.text(full_input_path)

def read_xlsx(spark, full_input_path):
    """
    Read Excel file without converting all data to strings.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input Excel file.

    Returns:
        DataFrame: The read DataFrame.
    """
    # Read the Excel file into a Pandas DataFrame
    pandas_df = pd.read_excel(full_input_path, engine='openpyxl')

    # Convert Pandas DataFrame to Spark DataFrame with schema inference
    spark_df = spark.createDataFrame(pandas_df)
    return spark_df

def read_json(spark, full_input_path):
    """
    Read JSON file.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input JSON file.

    Returns:
        DataFrame: The read DataFrame.
    """
    return spark.read.option("multiLine", "true").json(full_input_path)

def read_parquet(spark, full_input_path):
    """
    Read Parquet file.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input Parquet file.

    Returns:
        DataFrame: The read DataFrame.
    """
    return spark.read.parquet(full_input_path)

def read_xml(spark, full_input_path):
    """
    Read XML file.

    Args:
        spark (SparkSession): The Spark session.
        full_input_path (str): The full S3 path to the input XML file.

    Returns:
        DataFrame: The read DataFrame.
    """
    # Replace 'yourRowTag' with the actual row tag in your XML file
    return spark.read.format("com.databricks.spark.xml") \
                     .option("rowTag", "yourRowTag") \
                     .load(full_input_path)

def read_input_data(spark, input_path, object_key) -> Tuple[DataFrame, str]:
    """
    Read data from S3 based on file type.

    Args:
        spark (SparkSession): The Spark session.
        input_path (str): The S3 input path.
        object_key (str): The S3 object key.

    Returns:
        DataFrame: The read DataFrame.
        str: The file type.
    """
    # Correct path concatenation
    full_input_path = f"{input_path.rstrip('/')}/{object_key.lstrip('/')}"
    logger.info(f"Reading data from {full_input_path}")

    # Ensure the full path refers to S3, not local
    if not full_input_path.startswith('s3://'):
        raise ValueError(f"Invalid S3 path: {full_input_path}")

    # Mapping file extensions to corresponding read functions
    file_readers = {
        '.csv': read_csv,
        '.txt': read_txt,
        '.xlsx': read_xlsx,
        '.json': read_json,
        '.parquet': read_parquet,
        '.xml': read_xml
    }

    # Determine the file type and read accordingly
    file_extension = '.' + full_input_path.split('.')[-1].lower()
    if file_extension in file_readers:
        read_function = file_readers[file_extension]
        df = read_function(spark, full_input_path)
        file_type = file_extension.strip('.')
    else:
        raise ValueError(f"Unsupported file format for {full_input_path}. Supported formats are CSV, TXT, XLSX, JSON, Parquet, and XML.")

    logger.info(f"Successfully read data from {full_input_path}.")
    return df, file_type

def standardize_date(column, formats=None):
    """
    UDF to standardize date formats in a column.

    Args:
        column (Column): The column containing date strings.
        formats (list, optional): List of date formats to try.

    Returns:
        Column: The column with standardized dates.
    """
    if formats is None:
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%Y.%m.%d',
            '%d-%b-%Y',
            '%b %d, %Y',
            '%Y%m%d'
            # Add more formats as needed
        ]

    def parse_date(date_str):
        if date_str is None or date_str == '':
            return None
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        try:
            # Fallback to dateutil parser
            return parser.parse(date_str)
        except Exception:
            return None

    return udf(parse_date, TimestampType())(column)

def transform_data(df: DataFrame) -> DataFrame:
    """
    Perform necessary transformations on the DataFrame to ensure data types are preserved and data is clean.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The transformed DataFrame.
    """
    logger.info("Starting data transformation...")

    # Print schema for debugging
    df.printSchema()

    # Check for columns with only null or NaN values
    null_columns = []
    for col_name, dtype in df.dtypes:
        if dtype in ['double', 'float']:
            # For numeric columns, check for NaN and null
            if df.filter(col(col_name).isNotNull() & ~isnan(col(col_name))).count() == 0:
                null_columns.append(col_name)
        else:
            # For non-numeric columns, check for null
            if df.filter(col(col_name).isNotNull()).count() == 0:
                null_columns.append(col_name)
    if null_columns:
        logger.warning(f"The following columns contain only null or NaN values: {null_columns}")

    # Remove leading and trailing whitespaces from string columns
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(col(col_name)))

    # Handle inconsistent date formats
    date_cols = [field.name for field in df.schema.fields if (isinstance(field.dataType, StringType) or isinstance(field.dataType, DateType)) and 'date' in field.name.lower()]
    for col_name in date_cols:
        logger.info(f"Standardizing date column: {col_name}")
        df = df.withColumn(col_name, standardize_date(col(col_name)))

    # Handle mixed data types in numeric columns
    numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType) and 'id' not in field.name.lower()]
    for col_name in numeric_cols:
        # Attempt to cast to DoubleType
        df = df.withColumn(col_name, when(col(col_name).rlike('^-?\\d+(\\.\\d+)?$'), col(col_name).cast(DoubleType())).otherwise(col(col_name)))
        # Handle percentages
        df = df.withColumn(col_name, when(col(col_name).rlike('^-?\\d+(\\.\\d+)?%$'), (regexp_replace(col(col_name), '%', '').cast(DoubleType()) / 100)).otherwise(col(col_name)))

    # Handle special characters and encoding
    for col_name in string_cols:
        df = df.withColumn(col_name, regexp_replace(col(col_name), '[^\x00-\x7F]+', ' '))

    # Replace common null representations with actual nulls
    null_values = ['', 'NULL', 'N/A', 'na', 'null']
    for null_value in null_values:
        df = df.replace(null_value, None)

    logger.info("Data transformation completed.")
    return df

def write_output_data(glueContext, df: DataFrame, output_path: str, object_key: str, file_type: str):
    """
    Write the DataFrame to S3 in Parquet format, ensuring efficient handling of datasets.

    Args:
        glueContext (GlueContext): The Glue context.
        df (DataFrame): The DataFrame to write.
        output_path (str): The S3 output path.
        object_key (str): The S3 object key.
        file_type (str): The input file type.
    """
    timestamp = datetime.utcnow().strftime('%Y-%m-%d')
    base_name = object_key.split('/')[-1].split('.')[0]

    # Custom folder name indicating the content, source file type, and processing date
    custom_folder_name = f"{base_name}_{file_type}_processed_{timestamp}"
    final_output_path = f"{output_path.rstrip('/')}/{custom_folder_name}/"

    logger.info(f"Writing transformed data to {final_output_path}...")

    # Adjust the number of partitions based on data size
    data_size = df.count()
    logger.info(f"Dataset size (number of rows): {data_size}")

    if data_size <= 1000000:
        # For small datasets, reduce the number of partitions to 1 to avoid small files
        df = df.coalesce(1)
        logger.info("Data size is small. Coalesced to 1 partition to avoid small files.")
    else:
        # For large datasets, use the default number of partitions or adjust as needed
        logger.info("Data size is large. Using default partitioning.")

    # Convert DataFrame to DynamicFrame for Glue compatibility
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # Write the DynamicFrame to S3 in Parquet format
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": final_output_path
        },
        format="parquet"
    )
    logger.info(f"Data successfully written to {final_output_path}.")

def main():
    """
    The main function that orchestrates reading, transforming, and writing data.
    """
    try:
        glueContext, spark, job, args = initialize_glue_context()

        logger.info(f"Starting job: {args['JOB_NAME']} with input: {args['input_path']}{args['object_key']}")

        # Read input data
        df, file_type = read_input_data(spark, args['input_path'], args['object_key'])

        # Transform data
        transformed_df = transform_data(df)

        # Write output data
        write_output_data(
            glueContext,
            transformed_df,
            args['output_path'],
            args['object_key'],
            file_type
        )

        logger.info("Job completed successfully.")

    except Exception as e:
        logger.error(f"Job failed due to error: {e}")
        raise e

    finally:
        job.commit()
        logger.info("Job committed successfully.")

if __name__ == "__main__":
    main()
