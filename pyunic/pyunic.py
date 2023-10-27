"""
pyunic: A collection of helper methods and utilities for working with data in python
in the UniC.

This module uses monkey patching to add methods to pyspark.sql.DataFrame

Some methods require passing an active SparkSession as an argument.

Some methods require that the environment variables  "S3ACCESSKEY", "S3SECRETKEY", and "S3ENDPOINT"
be set to provide access to minio browsing capabilities.
"""

import os
import pyspark
from pyspark.sql.types import TimestampType, StringType, BooleanType, IntegerType, DecimalType, DateType
import pyspark.sql.functions as f
import pandas as pd
import s3fs
from IPython.display import clear_output


# A collection of helper methods to be monkey patched onto pyspark.sql.Dataframe

def toPandas2(self):
    """
    Alternative to pyspark's toPandas() which cirvumvents bool and datetime64[ns] conversion errors
    when using pandas >= 2.0 and pyspark 3.3.1.

    Note: pandas uses datetime64[ns] which is limited to a range between 1677-09-21 and 2262-04-11.
    If the source data contains typos or placeholder dates beyond this range, pandas will throw an
    OutOfBounds error.

    Returns
    -------
    Dataframe
        a pandas Dataframe version of the original pyspark.DataFrame
    """
    listOfTimestampColumns = []
    listOfBooleanColumns = []
    
    for c in self.schema:
        if c.dataType == TimestampType():
            listOfTimestampColumns.append(c.name)
            self = self.withColumn(
                c.name,
                f.date_format(c.name, "yyyy-MM-dd HH:mm:ss"))
        if c.dataType == BooleanType():
            listOfBooleanColumns.append(c.name)
            self = self.withColumn(
                c.name,
                f.col(c.name).cast("integer"))
    
    df = self.toPandas() \
        .astype({c: "datetime64[s]" for c in listOfTimestampColumns} | {c: "bool" for c in listOfBooleanColumns})
    
    return df

def countNulls(self):
    """
    Displays the number of null values in each column of a dataframe.

    Returns
    -------
    Dataframe
        A pandas dataframe with all the columns of the original pyspark dataframe and
        a single row with the count of null values per column.

    """    
    return self.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in self.columns]).toPandas()

def countNotNulls(self):
    """
    Displays the number of non-null values in each column of a dataframe.

    Returns
    -------
    Dataframe
        A pandas dataframe with all the columns of the original pyspark dataframe and
        a single row with the count of non-null values per column.

    """    
    
    return self.select([f.count(f.when(f.col(c).isNotNull(), c)).alias(c) for c in self.columns]).toPandas()

def countNullPercent(self):
    """
    Displays the ratio of null values to total values in each column of a dataframe.

    Returns
    -------
    Dataframe
        A pandas dataframe with all the columns of the original pyspark dataframe and
        a single row with the ratio of null to total values per column.  These values are
        between 0 and 1.

    """    

    return self.select([(f.count(f.when(f.col(c).isNull(), c))/f.count(f.lit(1))).alias(c) for c in self.columns]).toPandas()

def valueCounts(self, c):
    """
    Analog to pandas' value_counts().  Shows the number of occurences of each value in the given column.

    Parameters
    ----------
    c : str
        Column name whose values we wish to count

    Returns
    -------
    Dataframe
        Pandas dataframe (for better visuals compared to pyspark's .show()) with two columns.
        The first column is named as the value of the "c" argument and contains the distinct values
        of that column.  The second column is named "count" and contains the number of occurences.
    """
    return self.groupby(c).agg(f.count(c).alias("count")).sort("count", ascending = False).toPandas2()

def nunique(self, batch_size = 50):
    """
    Analog to pandas' nunique().  Shows the number of unique (aka pyspark.distinct()) values in each column.

    Parameters
    ----------
    batch_size : int, optional
        Number of columns to process at a time.  Too many columns and the query will struggle to complete,
        too few and there will be too much overhead from multiple queries.  Testing suggests that 50 is a good
        default value.

    Returns
    -------
    Dataframe
        Pandas dataframe with all the columns of the original pyspark dataframe and
        a single row with the count of unique values per column.    
    """

    dfs = []

    for i in range(0, len(self.columns), batch_size):
        clear_output(wait=True)
        print("Processing columns " + self.columns[i] + " through " + self.columns[min(i+batch_size, len(self.columns)-1)])
        df = self.select(self.columns[i:i+batch_size])
        pdf = df.agg(*(f.countDistinct(f.col(c)).alias(c) for c in df.columns)).toPandas()
        dfs.append(pdf)

    return pd.concat(dfs, axis=1)

def listAllNullColumns(self):
    """
    Creates a list of column names for columns containing all null values.  Uses the monkey patched countNotNulls() followed
    by pandas operations to select columns with a count of 0.

    Returns
    -------
    list(string)
        A list of column names for columns containing all null values.
    """
    df = self.countNotNulls()

    return list(df.loc[:, [(df[c] == 0).all() for c in df.columns]].columns)

def listNDistinctValueColumns(self, n):
    """
    Creates a list of column names for columns which contain exactly n distinct values.  Uses the monkey patched nunique() followed
    by pandas operations to select the appropriate columns.

    Parameters
    ----------
    n : int
        number of distinct values

    Returns
    -------
    list(string)
        A list of column names for columns which contain exactly n distinct values
    """
    df = self.nunique()

    return list(df.loc[:, [(df[c] == n).all() for c in df.columns]].columns)

def listSingleDistinctValueColumns(self):
    """
    Creates a list of column names for columns which contain exactly 1 distinct value (ex: all False).  Simply calls
    listNDistinctValueColumns(1)

    Returns
    -------
    list(string)
        A list of column names for columns which contain exactly 1 distinct value
    """
    return self.listNDistinctValueColumns(1)


# Register or "monkey patch" the methods onto pyspark.sql.DataFrame,
# which allows method piping:
# df.select(mycolumns).countNulls()
pyspark.sql.DataFrame.toPandas2 = toPandas2
pyspark.sql.DataFrame.countNulls = countNulls
pyspark.sql.DataFrame.countNotNulls = countNotNulls
pyspark.sql.DataFrame.countNullPercent = countNullPercent
pyspark.sql.DataFrame.valueCounts = valueCounts
pyspark.sql.DataFrame.nunique = nunique
pyspark.sql.DataFrame.listAllNullColumns = listAllNullColumns
pyspark.sql.DataFrame.listNDistinctValueColumns = listNDistinctValueColumns
pyspark.sql.DataFrame.listSingleDistinctValueColumns = listSingleDistinctValueColumns

# Tools to help browse minio

def get_s3fs():
    """
    Open a s3fs filesystem using credentials stored in the environment variables  "S3ACCESSKEY", "S3SECRETKEY", and "S3ENDPOINT"

    Returns
    -------
    s3fs.S3FileSystem
        A s3fs filesystem that can be used to browse minio buckets.
    """
    return s3fs.S3FileSystem(
        key = os.getenv("S3ACCESSKEY"),
        secret = os.getenv("S3SECRETKEY"),
        endpoint_url = os.getenv("S3ENDPOINT")
    )

def make_path_list(fs, path, max_depth = -1):
    """
    Recursively browse the minio file system starting at "path" up to a depth of "max_depth" and return a
    list of full path names to each table.

    Note: This method relies on the environment variables "S3ACCESSKEY", "S3SECRETKEY", and "S3ENDPOINT" which must be set
    to open a connection to the s3 file system (minio).  Which paths are visible is determined by the priviliges of the user account.

    Parameters
    ----------
    fs : s3fs.S3FileSystem
        The S3 filesystem to browse (use get_s3fs())
    path : str
        Initial path to search from (ex: "yellow-prd/enriched/mfm")
    max_depth : int, optional
        Maximum depth to search.  A depth of 1 lists the tables found in the directory
        specified in "path".  Use -1 (default) perform a full search. Defaults to -1.

    Returns
    -------
    list(str)
        list of full paths to all tables found.
    """

    pathlist = []

    paths = [ p for p in fs.ls(path) if not (p.endswith("_delta_log") or p.endswith("_SUCCESS") or (".parquet" in p) or (".csv" in p)) ]

    if (paths == []) or (max_depth == 0):
        pathlist.append(path)
    else:
        for p in paths:
            pathlist.extend(make_path_list(fs, p, max_depth - 1))
    return pathlist
    
# Tools to help investigate datasets and perform QA.
class Project():
    """
    A data structure for easily reading and performing operations on all the tables in a dataset.

    The default constructor takes a dict of table_name: pyspark_table.  This should be used if you
    have already read some tables from minio and wish to load them into a Project to make use of the
    QA methods, for example.
    Alternatively, using from_minio(bucket, name, spark) will browse minio and retrieve all relevant 
    tables for you.
    
    In both cases, the list of read table names is available using the accessor ".tables".
    Each individual table gets its own accessor based on it's name, or it can be accessed with
    .table(name).  For example:
    > import pyunic as pu
    > data = pu.Project("mybucket", "myproject", spark)
    > data.tables
    ['mytable']
    > data.table("mytable").count()
    10
    > data.mytable.count()
    10

    A number of QA and other useful methods are defined within this class, see individual method docstrings
    for details.  Currently all methods start with "show" or "check" and can be explored with autocompletion.

    Parameters
    ----------
    table_dict : dict(str:pyspark.sql.DataFrame)
        A dict of table_names : pyspark_tables.

    Notes
    -----
    See from_minio() for alternate constructor.
    """

    def __init__(self, table_dict):
        """
        Constructor for a Project which accepts a dict of table_names: pyspark_tables.  Used if the 
        pyspark tables have already been read and modified/filtered before you want to use Project methods
        on them.

        Parameters
        ----------
        table_dict : dict(str:pyspark.sql.DataFrame)
            A dict of table_names : pyspark_tables.
        """

        self.table_names = []
        self.tables = []

        for name, table in table_dict.items():
            self.add_table(name, table)

    @classmethod
    def from_minio(cls, bucket, name, spark):
        """
        Constructor for a Project which reads all tables with a specified project name and bucket.

        Parameters
        ----------
        bucket : str
            The type of minio bucket the project is located in.  This should be for example "enriched", "released" etc.
        name : str
            The name of a project, which corresponds to the name of the directory in the minio bucket (ex: "mfm")
        spark : SparkSession
            The spark session used in the notebook needs to be passed along to read the tables.

        Returns
        -------
        Project
            An instance of Project with loaded tables for the specified project.
        """
        path = cls._get_path_from_bucket(bucket, name)
        table_names_with_path = make_path_list(get_s3fs(), path, 1)
        print("Found the following minio tables:")
        for t in table_names_with_path:
            print(t)

        table_dict = {}
        for table_name_with_path in table_names_with_path:
            if cls._get_bucket_colour(bucket) == "yellow":
                table_dict[table_name_with_path.removeprefix(path)] = spark.read.format("delta").load("s3a://" + table_name_with_path)
            else:
                table_dict[table_name_with_path.removeprefix(path)] = spark.read.load("s3a://" + table_name_with_path)

        return cls(table_dict)

    @classmethod
    def _get_bucket_colour(cls, bucket):
        return "green" if (bucket in ["released", "published"]) else "yellow"

    @classmethod
    def _get_path_from_bucket(cls, bucket, name):
        return cls._get_bucket_colour(bucket) + "-prd/" + bucket + "/" + name + "/" + ("latest/" if (bucket == "released") else "")


    def table(self, name):
        return getattr(self, name)
        
    def add_table(self, name, table):
        self.table_names.append(name)
        self.tables.append(table)
        setattr(self, name, table)

    def remove_table(self, name):
        self.tables = [t for t in self.tables if t != self.table(name)]
        self.table_names = [n for n in table_names if n != name]
        delattr(self, name)

    def show_date_ranges(self):
        """
        Browses the project tables for columns of the Timestamp type, and for each column calculates
        the minimum and maximum date.  The results are presented in a multi-index pandas dataframe for
        visual clarity

        Returns
        -------
        Dataframe
            A pandas dataframe with a two level index (table, column) and two columns containing the
            min and max dates of each Timestamp column in the dataset.
        """
        dfs = []
        
        for t in self.table_names:
            for c in self.table(t).schema:
                if c.dataType == TimestampType():
                    
                    df = self.table(t).agg(f.min(c.name), f.max(c.name)).toPandas2()
                    df.columns = ["min", "max"]
                    df["column"] = c.name
                    df["table"] = t

                    dfs.append(df)
        
        if dfs == []:
            return []
        else:
            return pd.concat(dfs).set_index(["table", "column"])
    
    def show_duplicated_rows(self):
        """
        Browses the project tables and detects any duplicate rows.  Any duplicate rows are displayed using
        a pandas dataframe.
        """
        for t in self.table_names:
            
            idcols = [c for c in self.table(t).columns if "Id" in c]
            print(t + ":")
            df = self.table(t)
            display(df.groupBy(df.columns).agg((f.count("*")).alias("count")) \
                .filter(f.col("count") > 1).sort(idcols) \
                .toPandas2())
        
    def show_counts_and_unique_ids(self):
        """
        Browses the project tables and identifies columns which correspond to entity IDs.  These are assumed to be
        columns whose names end with "Id".  For each table and each ID, the number of unique IDs is calculated.
        Additionally the number of unique combinations of all Ids is calculated, and the number of total unique rows
        in the table is also provided for easy comparison.

        Returns
        -------
        Dataframe
            A multi-index pandas dataframe (table, column) with a single column "unique ids" showing the unique counts
        """
        dfs = []
        
        for t in self.table_names:
            
            idcols = [c for c in self.table(t).columns if "Id" in c]
            
            

            idcountsdf = self.table(t).agg(*(f.countDistinct(f.col(c)).alias(c) for c in idcols)).toPandas2()
            idcountsdf["All Ids Combined"] = self.table(t).agg(f.countDistinct(*idcols).alias("All Ids Combined")).toPandas2()["All Ids Combined"]
            idcountsdf["Total Unique Rows"] = self.table(t).distinct().count()
            
            
            df = idcountsdf.transpose().reset_index()
            df.columns = ["column", "unique ids"]
            
            df["table"] = t
            
            dfs.append(df)
        
        return pd.concat(dfs).set_index(["table", "column"])
            
            
    def show_all_value_counts(self, max_rows = 15):
        """
        Displays content from each string, boolean, integer, or decimal column in the dataset.  ID columsn (column names endinc with "Id")
        are excluded.  This uses the valueCounts function to display the top "max_rows" most common values in the retained columns.

        Parameters
        ----------
        max_rows : int, optional
            Cutoff value for the number of unique row values to display per column, by default 15
        """
        for t in self.table_names:
            
            targetcols = [c for c in self.table(t).columns if (c[-2:] != "Id") and (self.table(t).schema[c].dataType in [StringType(), BooleanType(), IntegerType(), DecimalType()])]
            
            print(t + ":")
            
            for c in targetcols:
                count = self.table(t).select(c).distinct().count()
                if count > max_rows:
                    print(c + " has " + str(count) + " distinct values, showing only the top " + str(max_rows) + " value counts")
                if count == 0:
                    print(c + " has no countable values?!")
                display(self.table(t).valueCounts(c).loc[lambda df: df.index < max_rows])
            
    def check_for_all_null_columns(self):
        """
        Checks every column in the project to make sure no columns are entirely composed of null values.

        Returns
        -------
        Dataframe
            A multi-index (table, column) pandas dataframe with a single column ("All nulls?")
            which contains either the value "ALL NULLS!" or "Some data"
        """
        dfs = []
        
        for t in self.table_names:
            
            
            rowcount = self.table(t).count()
            
            nullcountsdf = self.table(t).countNulls()
            
            df = nullcountsdf.transpose().reset_index()
            df.columns = ["column", "nullcount"]
            df["table"] = t

            df["All nulls?"] = df["nullcount"].apply(lambda x: "ALL NULLS!" if (x == rowcount) else "Some data")
            
            dfs.append(df.drop(columns = ["nullcount"]))
            
        return pd.concat(dfs).set_index(["table", "column"])
            
            
    def check_missing_data_percentages(self):
        """
        Computes the percentage of missing (null) values in each column of the project.  Percentage of non-null values
        and absolute counts of non-null values are also provided.

        Returns
        -------
        Dataframe
            A multi-index (table, column) pandas dataframe with the following columns: "% null", "count non null" and "% non null"
        """
        dfs = []
        
        for t in self.table_names:
            
            percentsdf = self.table(t).countNullPercent()
            nonnullcountdf = self.table(t).countNotNulls()
            
            df = percentsdf.transpose().merge(nonnullcountdf.transpose(), left_index=True, right_index=True).reset_index()
            df.columns = ["column", "% null", "count non null"]
            df["table"] = t

            df["% non null"] = df["% null"].apply(lambda x: round((1-x) * 100, 2))
            df["% null"] = df["% null"].apply(lambda x: round(x * 100, 2))
            
            dfs.append(df)
            
        return pd.concat(dfs).set_index(["table", "column"])

    def run_all_tests(self):
        """
        Runs all QA tests on the project.
        """
        print("Checking for duplicate rows.")
        self.show_duplicated_rows()

        print("Finding date ranges.")        
        display(self.show_date_ranges())

        print("Counting unique Ids.")
        display(self.show_counts_and_unique_ids())

        print("Checking for all null columns.")
        display(self.check_for_all_null_columns())

        print("Showing missing data.")
        display(self.check_missing_data_percentages())

        print("Showing a sample of most common values.")
        self.show_all_value_counts()

    # Methods used for data dictionary authoring and verificaiton

    def _get_dd_type(self, table, column):
        """
        Converts the column type (pyspark.sql.types) to the string representation
        used in our data dictionaries.

        The only computation is a check to see if a TimestampType() column might in
        fact be a Date and not a DateTime.

        Parameters
        ----------
        table : str
            Table name
        column : str
            Column name

        Returns
        -------
        str
            Type label for use in our data dictionaries
        """
        match self.table(table).schema[column].dataType:
            case StringType():
                return "Text"
            case BooleanType():
                return "Boolean"
            case IntegerType():
                return "Integer"
            case DecimalType():
                return "Decimal"
            case DateType():
                return "Date"
            case TimestampType():
                nondates = self.table(table) \
                    .filter(f.col(column).isNotNull()) \
                    .select(f.hour(f.col(column)).alias("h"), f.minute(f.col(column)).alias("m"), f.second(f.col(column)).alias("s")) \
                    .filter((f.col("h") != 0) | (f.col("m") != 0) | (f.col("s") != 0)) \
                    .count()
                if nondates > 0:
                    return "Datetime"
                else:
                    return "Date"
                

    def create_data_dictionary_df(self):
        """
        Creates the 3 core columns of the Variable sheet of our data dictionaries:
        "tableName", "variableName", and "valueType".

        Returns
        -------
        Pandas.DataFrame
            A pandas dataframe with the full listing of tables, columns, and their types
            in the dd format.
        """
        list_of_row_dicts = []
        for t in self.table_names:
            for c in self.table(t).columns:
                row = {}
                row["tableName"] = t
                row["variableName"] = c
                row["valueType"] = self._get_dd_type(t, c)

                list_of_row_dicts.append(row)
                
        return pd.DataFrame(list_of_row_dicts)

    def write_data_dictionary(self, filename):
        """
        Writes to the specified filename a .xlsx file with a single sheet named "Variable"
        containing the 3 columns to start fleshing out a data dictionary:
        "tableName", "variableName", "valueType"

        Parameters
        ----------
        filename : str
            Desired output file name.
        """
        self.create_data_dictionary_df().to_excel(filename, sheet_name="Variable")

    def check_data_dictionary(self, filename):
        """
        Performs a series of checks comparing an existing data dictionary (specified in filename)
        and the tables in the Project instance.

        Using tableName and variableName, checks for columns present in one but not the other.

        Then, for those columns that match, compares the valueTypes and identifies any mismatches.

        Parameters
        ----------
        filename : str
            Filename of .xlsx data dictionary to check.
        """
        dd = pd.read_excel(filename, sheet_name="Variable", usecols=["tableName", "variableName", "valueType"])

        df = self.create_data_dictionary_df()

        outer = dd[["tableName", "variableName"]] \
            .merge(df[["tableName", "variableName"]],
            on=["tableName", "variableName"],
            how="outer",
            indicator = True)
        
        print("Rows in dd which don't match any column in the dataset:")
        display(outer.loc[outer._merge == "left_only"].drop(columns=["_merge"]))

        print("Columns in the dataset with no matching dd entry:")
        display(outer.loc[outer._merge == "right_only"].drop(columns=["_merge"]))

        inner = df.rename(columns={"valueType": "valueTypeFromDataset"}) \
            .merge(dd.rename(columns={"valueType": "valueTypeFromDictionary"}), on=["tableName", "variableName"], how="inner")
        print("Variables with mismatched types:")
        display(inner.loc[inner.valueTypeFromDictionary != inner.valueTypeFromDataset])