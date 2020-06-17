#!/usr/bin/env python3
from setuptools import setup, find_packages
import versioneer

setup(
    name='simple-s3fs',
    author='Alexander Veit',
    author_email='alexander_veit@hms.harvard.edu',
    packages=['simple_s3fs'],
    entry_points={
      'console_scripts': [
          'simple-s3fs = simple_s3fs.__main__:main'
      ]
    },
    url='https://github.com/4dn-dcic/simple-s3fs',
    description='A simple FUSE filesystem for reading files in S3 buckets. Forked from https://github.com/higlass/simple-httpfs and has been adapted to the needs of the 4DN data portal.',
    license='MIT',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        "fusepy",
        "requests",
        "diskcache",
        "numpy",
        "boto3",
        "slugid"
    ],
    python_requires='>=3.6',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass()
)
