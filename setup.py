#!/usr/bin/env python

from setuptools import setup

setup(
    name="target-parquet",
    version="0.1.1",
    description="Singer.io target for writing into parquet files",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_parquet"],
    install_requires=[
        "jsonschema==2.6.0",
        "singer-python==5.10",
        "pandas==1.2",
        "pyarrow==3.0",
        "psutil==5.8",
    ],
    entry_points="""
          [console_scripts]
          target-parquet=target_parquet:main
      """,
    packages=["target_parquet"],
)
