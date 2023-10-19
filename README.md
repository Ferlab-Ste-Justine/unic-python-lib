# PyUniC: Python Utilities for UniC

A collection of helper methods and utilities for working with data in python
in the UniC.

## In this README :point_down:

- [Setup and Configuration](#setup-and-configuration)
- [Features](#features)
- [Usage](#usage)

## Setup and Configuration

This package is designed to work from within the UniC's Posit Workbench environment, within the "Python 3.11" preconfigured virtual environment.  Notably, some code uses the switch/case syntax from python 3.10, and the package also depends on pandas 2.1, neither of which are part of the "Python 3.9" venv.

Features that depend on browsing minio (notably the Project.from_minio() constructor) depend on having the following environment variables set: "S3ACCESSKEY", "S3SECRETKEY", and "S3ENDPOINT".  In addition, a properly setup SparkSession will need to be passed as an argument (see test.py for a sample SparkConf)

## Features

- Additional convenience helper methods monkeypatched onto PySpark dataframes for easier table inspection.
- Easily browse minio and get lists of table path/names with make_path_list()
- Use the Project class to form a collection of tables.
 - Create a Project by specifying a minio bucket and project name, and all tables are loaded automatically!
 - Alternatively, provide a dict of table names: pyspark tables if you need to do some preprocessing.
 - Add and remove tables as needed.
 - Direct accessors for each table in the project, or access via table name for dynamic/iterative queries
 - Several convenient QA methods.  Call each individually or use run_all_tests() for an quick automated QA check on a new project!
 - Generate the base of a project data dictionary (table names, variable names, and variable types), or
 - Compare the project tables to an existing data dictionary and check for discrepencies.

## Usage

### PySpark helper methods

Several methods have been monkeypatched onto pyspark's dataframe class.  This means you can pipe them onto an existing pyspark chain, for example:
```
>import pyunic as pu
>df.select(...) \
    .filter(...) \
    .countNullPercent()  # <- importing pyunic adds this method to pyspark.sql.DataFrame
```
These helper methods do all the dataprocessing in pyspark, and lastly convert to a Pandas dataframe for clearer visual output.

An important helper method is .toPandas2() which is required when converting pyspark timestamp and boolean columns to pandas.  This method sidesteps a mismatch in the pandas conversion code included in pyspark==3.3.1 (based on older pandas 1.5.3 and older numpy) and the current pandas==2.1.0.  (FYI, the Python 3.9 venv which uses pandas 1.5.3 also has to downgrade it's numpy version to resolve a similar problem).  .toPandas2 converts timestamps to string format and reconverts those columns to datetime64\[ns\] in pandas.  Booleans are similarly converted via a cast to integer.  Note that the ns precision used in numpy/pandas means that date ranges are limited to between 1677-09-21 and 2262-04-11.  Currently the conversion will fail loudly (out of bounds error) if there are any dates outside this range (as can happen with typos or "magic dates" like 9999).  In a future update this behaviour may change to coersion with NaT and a warning.

### Minio table listings

In order to facilitate loading all of a project's tables at once, or simply to get a easily copy-pastable path for a table in a notebook, use the make_path_list() method.  This will recursively browse a minio bucket anchored on the "path" argument down to a specified maximum depth (-1 for no limits, default).  The idea is that the list of paths returned by this method can be pasted into a spark.read.load().  Currently both delta and parquet yellow and green-released tables are supported, but not published-csv tables.

```
>import pyunic as pu
>#What is that table called again?
>pu.make_path_list(pu.get_s3fs(), "yellow-prd/anonymized/some_source_system")
```

### Using the Project class to perform QA

Once an ETL has produced tables for a project (suppose either in yellow enriched or green released), easily load all the project's tables like so:
```
>import pyunic as pu
>data = pu.Project.from_minio("released", "myprojectname")
```
This will print out the list of tables as they are loaded, but in case you forget, or if you want to iterate through the tables by name:
```
>data.table_names
```
Each table is accessible in two ways:
```
>#by name lookup, useful for iterations or other dynamic lookups:
>data.table("mytable")
>#by direct accessor
>data.mytable
```
Now you can run your tests, most of which return or print a pandas-formatted dataframe.
```
># run a single test
>data.show_date_ranges()
># run all tests
>data.run_all_tests()
```

### Data dictionnary validation

If you have created a dataset and you then wish to create the accompanying data dictionary (ex: a new warehouse table), you can dp this:
```
>#suppose we have created the table "mynewwarehouse" in the warehouse
>import pyunic as pu
>#what is the path for the warehouse again?  I rembmer it's in yellow...
>pu.make_path_list(pu.get_s3fs(), "yellow-prd", 2)
...
># ahh there it is!
>pu.make_path_list(pu.get_s3fs(), "yellow-prd"/warehouse/unic", 1)
...
>data = pu.Project({"mynewwarehouse":spark.read.load("s3a://copy-paste-the-path-from-above")}
>data.write_data_dictionary("my_dd.xlsx")
```
This will create an .xlsx file with a worksheet named Variable with 3 columns "tableName", "variableName" and "variableType" whicih you can use as the starting point for the fully fleshed out data dictionary.

Alternatively, if you are working on a dataset whose dictionary has already been specified, but you want to make sure that the dictionary is up to date with the project, simply copy the dictionary .xlsx to the current working directory and:
```
>data.check_data_dictionary("my_dd.xlsx")
```