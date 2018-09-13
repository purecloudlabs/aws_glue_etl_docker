from setuptools import setup
setup(
    name = 'aws_glue_etl_jupyter',
    version = '0.1.0',
    packages = ['aws_glue_etl_jupyter'],
    license='mit',
    install_requires = ['boto3'],
    entry_points = {
        'console_scripts': [
            'aws_glue_etl_jupyter = aws_glue_etl_jupyter.__main__:main'
        ]
    })