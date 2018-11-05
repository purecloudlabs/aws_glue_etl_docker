# AWS Glue ETL in Docker and Jupyter
This project is a helper for creating scripts that run in both [AWS Glue](https://aws.amazon.com/glue/), [Jupyter](http://jupyter.org/) notebooks, and in docker containers with spark-submit.  Glue supports running [Zepplin notebooks](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-EC2-notebook.html) against a dev endpoint, but for quick dev sometimes you just want to run locally against a subset of data and don't want to have to pay to keep the dev endpoints running.

## Glue Shim
Glue has specific methods to load and save data to s3 which won't work when running in a jupyter notebook.  The glueshim provides a higher level api to work in both scenarios.  

```python
from aws_glue_etl_docker import glueshim
shim = glueshim.GlueShim()

params = shim.arguments({'data_bucket': "examples"})
pprint(params)


files = shim.get_all_files_with_prefix(params['data_bucket'], "data/")
print(files)

data = shim.load_data(files, 'example_data')
data.printSchema()
data.show()

shim.write_parquet(data, params['data_bucket'], "parquet", None, 'parquetdata' )
shim.write_parquet(data, params['data_bucket'], "parquetpartition", "car", 'partitioneddata' )

shim.write_csv(data, params['data_bucket'],"csv", 'csvdata')

shim.finish()
```

## Local environment
Running locally is easiest in a docker container

1. Copy data locally, and map that folder to your docker container to the /data/<bucket>/<files> path.
2. Start docker container, map your local notebook directory to ```/home/jovyan/work```

*Example Docker command*
```docker run -p 8888:8888 -v "$PWD/examples":/home/jovyan/work -v "$PWD":/data jupyter/pyspark-notebook```

### Installing package in Jupyter

```python
import sys
!{sys.executable} -m pip install git+https://github.com/purecloudlabs/aws_glue_etl_docker
```

## AWS Deployment
For deployment to AWS, this library must be packaged and put into S3. You can use the helper script deploytos3.sh to package and copy.  

Usage ```./deploytos3.sh s3://example-bucket/myprefix/aws-glue-etl-jupyter.zip

Then when starting the glue job, use your S3 zip path in the _Python library path_ configuration

## Bookmarks
The shim is currently setup to delete any data in the output folder so that if you run with bookmarks enabled and then need to reprocess the entire dataset and 

## Converting Workbook to Python Script

aws_glue_etl_docker can also be used as a cli tool to clean up Jupyter metadata from a workbook or convert it to a python script.

## Clean

The clean command will open all workbooks in a given path and remove any metadata, output and execution information. This keeps the workbooks cleaner in source control

``` aws_glue_etl_docker clean --path /dir/to/workbooks  ```

## Build

The build command will open all workbooks in a given path and convert them to python scripts.  Build will convert any markdown cells to multiline comments.  This command will not convert any cells that contain ```#LOCALDEV``` or lines that start with ```!``` as in ```!{sys.executable} -m pip install git+https://github.com/purecloudlabs/aws_glue_etl_docker```

``` aws_glue_etl_docker build --path /dir/to/workbooks  ```