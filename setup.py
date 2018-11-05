from setuptools import setup
setup(
    name = 'aws_glue_etl_docker',
    version = '0.4.0',
    packages = ['aws_glue_etl_docker'],
    license='mit',
    install_requires = ['boto3'],
    entry_points = {
        'console_scripts': [
            'aws_glue_etl_docker = aws_glue_etl_docker.__main__:main'
        ]
    })
