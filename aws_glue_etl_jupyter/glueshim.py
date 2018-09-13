import sys
import os.path
import shutil
import glob
import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from pprint import pprint

def _load_data(filePaths, dataset_name, spark_context):
    sqlContext = SQLContext(spark_context)
    return sqlContext.read.json(filePaths)

def _write_csv(dataframe, bucket, location, dataset_name, spark_context):
    output_path = '/data/' + bucket + '/' + location

    shutil.rmtree(output_path, True)
    dataframe.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(output_path)

def _write_parquet(dataframe, bucket, location, partition_column, dataset_name, spark_context):

    output_path = '/data/' + bucket + '/' + location

    shutil.rmtree(output_path, True)

    if partition_column != None and len(partition_column) > 0:
        dataframe.write.partitionBy(partition_column).parquet(output_path)
    else:
        dataframe.write.parquet(output_path)

def _get_spark_context():
    return pyspark.SparkContext.getOrCreate()

def _get_all_files_with_prefix(bucket, prefix, spark_context):
    pathToWalk = '/data/' + bucket + '/' + prefix +'**/*.*'   
    return glob.glob(pathToWalk,recursive=True)

def _is_in_aws():
    return False

def _get_arguments(default):
    return default

try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    import boto3
    
    def _load_data(file_paths, dataset_name, context):
        glue0 = context.create_dynamic_frame.from_options(connection_type='s3',
                                                      connection_options={'paths': file_paths},
                                                      format='json',
                                                      transformation_ctx=dataset_name)

        return glue0.toDF()
    
    def _write_csv(dataframe, bucket, location, dataset_name, spark_context):
        output_path = "s3://" + bucket + "/" + location
        df_tmp = DynamicFrame.fromDF(dataframe.repartition(1), spark_context, dataset_name)
        spark_context.write_dynamic_frame.from_options(frame = df_tmp, connection_type = "s3", connection_options = {"path": output_path}, format = "csv")


    def _delete_files_with_prefix(bucket, prefix):
        if not prefix.endswith('/'):
            prefix = prefix + "/"

        delete_keys = {'Objects' : []}
        s3 = boto3.client('s3')

        paginator = s3.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if page.get('Contents'):
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/'):
                        delete_keys['Objects'].append({'Key': str(obj['Key'])})
                  
                s3.delete_objects(Bucket=bucket, Delete=delete_keys)
                delete_keys = {'Objects' : []}

                
    def _write_parquet(dataframe, bucket, location, partition_column, dataset_name, spark_context):
        if "job-bookmark-disable" in sys.argv:
            _delete_files_with_prefix(bucket, location)

        output_path = "s3://" + bucket + "/" + location

        df_tmp = DynamicFrame.fromDF(dataframe, spark_context, dataset_name)
        if partition_column != None and len(partition_column) > 0:
            spark_context.write_dynamic_frame.from_options(frame = df_tmp, connection_type = "s3", connection_options = {"path": output_path, "partitionKeys": [partition_column] }, format = "parquet")
        else:
            spark_context.write_dynamic_frame.from_options(frame = df_tmp, connection_type = "s3", connection_options = {"path": output_path }, format = "parquet")



    def _get_spark_context():
        spark_context = GlueContext(SparkContext.getOrCreate())
        job = Job(spark_context)
        args = _get_arguments({})
        job.init(args['JOB_NAME'], args)
        
        return spark_context

    def _get_all_files_with_prefix(bucket, prefix, spark_context):
        prefixes = set()
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if page['Contents']:
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/') and '/' in obj['Key']:
                        idx = obj['Key'].rfind('/')
                        prefixes.add('s3://{}/{}'.format(bucket, obj['Key'][0:idx]))
                        
        return list(prefixes)
   
    def _get_arguments(defaults):
        return getResolvedOptions(sys.argv, ['JOB_NAME'] + defaults.keys())  

    def _is_in_aws():
        return True
    
except Exception as e:
    print('local dev')
    
class GlueShim:    
    def __init__(self):
        self.spark_context = _get_spark_context()
        
    def arguments(self, defaults):
        return _get_arguments(defaults)    
        
    def load_data(self, file_paths, dataset_name):
        return _load_data(file_paths, dataset_name, self.spark_context)
    
    def get_all_files_with_prefix(self, bucket, prefix):
        return _get_all_files_with_prefix(bucket, prefix, self.spark_context)

    def write_parquet(self, dataframe, bucket, location, partition_column, dataset_name):
        _write_parquet(dataframe, bucket, location, partition_column, dataset_name, self.spark_context)
        
    def write_csv(self, dataframe, bucket, location, dataset_name):
        _write_csv(dataframe, bucket, location, dataset_name, self.spark_context)
            
    def get_spark_context(self):
        return self.context

 