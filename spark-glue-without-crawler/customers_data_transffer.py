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



# read data from csv file
sourcedb_customers = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, 
    connection_type="s3", format="csv", connection_options={"paths": ["s3://test-gluespark/source-db/customers/"], 
    "recurse": True}
    )


# write data to S3
targetdb_customers = glueContext.write_dynamic_frame.from_options(
    frame=sourcedb_customers, connection_type="s3", format="glueparquet", 
    connection_options={"path": "s3://test-gluespark/target-db/customers/", "partitionKeys": []}, 
    format_options={"compression": "snappy"}
    )

job.commit()