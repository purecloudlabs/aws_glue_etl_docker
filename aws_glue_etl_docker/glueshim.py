import sys
import os.path
import shutil
import glob
import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from pprint import pprint

def _load_data(filePaths, dataset_name, spark_context, groupfiles, groupsize):
    sqlContext = SQLContext(spark_context)
    return sqlContext.read.json(filePaths)


def _load_data_from_catalog(database, table_name, spark_context):
    sqlContext = SQLContext(spark_context)
    return (sqlContext
            .read
            .option("header", "true")
            .option("mode", "DROPMALFORMED")
            .csv(database))

def _write_csv(dataframe, bucket, location, dataset_name, spark_context):
    output_path = '/data/' + bucket + '/' + location

    shutil.rmtree(output_path, True)
    dataframe.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save(output_path)

def _write_parquet(dataframe, bucket, location, partition_columns, dataset_name, spark_context):

    output_path = '/data/' + bucket + '/' + location

    shutil.rmtree(output_path, True)

    if partition_columns != None and len(partition_columns) > 0:
        dataframe.write.partitionBy(partition_columns).parquet(output_path)
    else:
        dataframe.write.parquet(output_path)

def _get_spark_context():
    return (pyspark.SparkContext.getOrCreate(), None)

def _get_all_files_with_prefix(bucket, prefix, spark_context):
    pathToWalk = '/data/' + bucket + '/' + prefix +'**/*.*'
    return glob.glob(pathToWalk,recursive=True)

def _is_in_aws():
    return False

def _get_arguments(default):
    return default

def _finish(self):
    return None

try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    import boto3

    def _load_data(file_paths, dataset_name, context, groupfiles, groupsize):

        connection_options = {'paths': file_paths}

        if groupfiles != None:
            connection_options["groupFiles"] = groupfiles

        if groupsize != None:
            connection_options["groupSize"] = groupsize

        glue0 = context.create_dynamic_frame.from_options(connection_type='s3',
                                                      connection_options=connection_options,
                                                      format='json',
                                                      transformation_ctx=dataset_name)

        return glue0.toDF()

    def _load_data_from_catalog(database, table_name, context):
        dynamic_frame = context.create_dynamic_frame.from_catalog(
            database=database, table_name=table_name)
        return dynamic_frame.toDF()

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


    def _write_parquet(dataframe, bucket, location, partition_columns, dataset_name, spark_context):
        if "job-bookmark-disable" in sys.argv:
            _delete_files_with_prefix(bucket, location)

        output_path = "s3://" + bucket + "/" + location

        df_tmp = DynamicFrame.fromDF(dataframe, spark_context, dataset_name)

        print("Writing to {} ".format(output_path))

        if partition_columns != None and len(partition_columns) > 0:
            spark_context.write_dynamic_frame.from_options(frame = df_tmp, connection_type = "s3", connection_options = {"path": output_path, "partitionKeys": partition_columns }, format = "parquet")
        else:
            spark_context.write_dynamic_frame.from_options(frame = df_tmp, connection_type = "s3", connection_options = {"path": output_path }, format = "parquet")



    def _get_spark_context():
        spark_context = GlueContext(SparkContext.getOrCreate())
        job = Job(spark_context)
        args = _get_arguments({})
        job.init(args['JOB_NAME'], args)

        return (spark_context, job)

    def _get_all_files_with_prefix(bucket, prefix, spark_context):
        prefixes = set()
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if 'Contents' in page and page['Contents']:
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/') and '/' in obj['Key']:
                        idx = obj['Key'].rfind('/')
                        prefixes.add('s3://{}/{}'.format(bucket, obj['Key'][0:idx]))

        return list(prefixes)

    def _get_arguments(defaults):
        return getResolvedOptions(sys.argv, ['JOB_NAME'] + defaults.keys())

    def _is_in_aws():
        return True

    def _finish(self):
        if self.job:
            try:
                self.job.commit()
            except NameError:
                print("unable to commit job")


except Exception as e:
    print('local dev')

class GlueShim:
    def __init__(self):
        c = _get_spark_context()
        self.spark_context = c[0]
        self.job = c[1]
        self._groupfiles = None
        self._groupsize = None

    def arguments(self, defaults):
        """Gets the arguments for a job.  When running in glue, the response is pulled form sys.argv

        Keyword arguments:
        defaults -- default dictionary of options
        """
        return _get_arguments(defaults)

    def load_data(self, file_paths, dataset_name):
        """Loads data into a dataframe

        Keyword arguments:
        file_paths -- list of file paths to pull from, either absolute paths or s3:// uris
        dataset_name -- name of this dataset, used for glue bookmarking
        """
        return _load_data(file_paths, dataset_name, self.spark_context, self._groupfiles, self._groupsize)

    def load_data_from_catalog(self, database, table_name):
        """Loads data into a dataframe from the glue catalog

        Keyword arguments:
        database -- the glue database to read from
        table_name -- the table name to read
        """
        return _load_data_from_catalog(database, table_name, self.spark_context)

    def get_all_files_with_prefix(self, bucket, prefix):
        """Given a bucket and file prefix, this method will return a list of all files with that prefix

        Keyword arguments:
        bucket -- bucket name
        prefix -- filename prefix
        """
        return _get_all_files_with_prefix(bucket, prefix, self.spark_context)

    def write_parquet(self, dataframe, bucket, location, partition_columns, dataset_name):
        """Writes a dataframe in parquet format

        Keyword arguments:
        dataframe -- dataframe to write out
        bucket -- Output bucket name
        location -- Output filename prefix
        partition_columns -- list of strings to partition by, None for default partitions
        dataset_name - dataset name, will be appended to location

        """
        _write_parquet(dataframe, bucket, location, partition_columns, dataset_name, self.spark_context)

    def write_csv(self, dataframe, bucket, location, dataset_name):
        """Writes a dataframe in csv format with a partition count of 1

        Keyword arguments:
        dataframe -- dataframe to write out
        bucket -- Output bucket name
        location -- Output filename prefix
        dataset_name - dataset name, will be appended to location

        """
        _write_csv(dataframe, bucket, location, dataset_name, self.spark_context)

    def get_spark_context(self):
        """ Gets the spark context """
        return self.context

    def finish(self):
        """ Should be run at the end, will set Glue bookmarks """
        _finish(self)

    def set_group_files(self, groupfiles):
        """ Sets extra options used with glue https://docs.aws.amazon.com/glue/latest/dg/grouping-input-files.html """
        self._groupfiles = groupfiles

    def set_group_size(self, groupsize):
        """ Sets extra options used with glue https://docs.aws.amazon.com/glue/latest/dg/grouping-input-files.html """
        self._groupsize = groupsize
