import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# read data from csv file
sourcedb_customers = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, 
    connection_type="s3", format="csv", connection_options={"paths": ["s3://test-gluespark/source-db/customers/"], 
    "recurse": True}
    )

# # Script generated for node Amazon S3
# EvaluateDataQuality().process_rows(
#     frame=sourcedb_customers, 
#     ruleset=DEFAULT_DATA_QUALITY_RULESET, 
#     publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749436542347", "enableDataQualityResultsPublishing": True}, 
#     additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})


# write data to S3
targetdb_customers = glueContext.write_dynamic_frame.from_options(
    frame=sourcedb_customers, connection_type="s3", format="glueparquet", 
    connection_options={"path": "s3://test-gluespark/target-db/customers/", "partitionKeys": []}, 
    format_options={"compression": "uncompressed"}
    )

job.commit()