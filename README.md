# AWS Glue ETL in Jupyter
This project is a helper for creating scripts that run in both [AWS Glue](https://aws.amazon.com/glue/) and [Jupyter](http://jupyter.org/) notebooks.  Glue supports running [Zepplin notebooks](https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-tutorial-EC2-notebook.html) against a dev endpoint, but for quick dev sometimes you just want to run locally against a subset of data and don't want to have to pay to keep the dev endpoints running.

## Glue Shim
Glue has specific methods to load and save data to s3 which won't work 

```python
from aws_glue_etl_jupyter import glueshim
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
!{sys.executable} -m pip install git+https://github.com/purecloudlabs/aws_glue_etl_jupyter
```

## AWS Deployment
For deployment to AWS, this library must be packaged and put into S3. You can use the helper script deploytos3.sh to package and copy.  

Usage ```./deploytos3.sh s3://example-bucket/myprefix/aws-glue-etl-jupyter.zip

Then when starting the glue job, use your S3 zip path in the _Python library path_ configuration

## Bookmarks
The shim is currently setup to delete any data in the output folder so that if you run with bookmarks enabled and then need to reprocess the entire dataset and 
