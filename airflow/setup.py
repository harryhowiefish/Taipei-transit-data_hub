from setuptools import setup, find_packages

setup(
    name='utils',
    version='0.1',
    # Include only specific packages
    packages=find_packages(include=['utils.gcp']),
    py_modules=['utils.gcp.bq', 'utils.gcp.gcs'],

    install_requires=[
        # List your package dependencies here, e.g.,
        'pandas',
        'google-cloud-storage',
        'google-cloud-bigquery'
    ],
    description='A custom utils package for etl and gcp',
    python_requires='>=3.11',
)
