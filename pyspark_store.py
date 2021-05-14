# -*- coding: UTF-8 -*-
class StoreTable():
    """
    Class responsible for store Spark DataFrames in Hive as external table, spark-sql and
    hadoop file system [hdfs].
    """
    # Constants
    BASE_PATH = ""
    PROYECTO = None
    BASE_SPARK_LAYER = ""
    BASE_HIVE_LAYER = ""
    CSV_FORMAT = "csv"
    PARQUET_FORMAT = "parquet"
    URL_HDFS = None
    DATAFRAME = None

    def __init__(self, sparkSession, hiveConecction, HDFS_URL, log, project):
        """
        Initialize the class with the SparkSession, Hive and hdfs url where the
        DataFrame is going to be store.
        """
        project = str(project).lower()
        self.spark = sparkSession
        self.hive = hiveConecction
        self.project = project
        self.hdfs_url = HDFS_URL
        self.log = log

        self.log.info("Class initialized")

    def _save_df_to_HDFS(self, filename, partition=False, partition_by_cols=None, mode='overwrite'):
        """
        Store the DataFrame in the hdfs path.
        Args:
            filename (str): Path with the name of the file where the DataFrame is going to be store.
            partition (bool): Flag that represents if DataFrame is partitioned.
            partition_by_cols (str): Name of the columns by which the DataFrame is partitioned
            mode (str): Specifies the behavior when data or table already exists.

        Returns:
            None
        """
        if partition is not False:
            self.DATAFRAME.write.partitionBy(partition_by_cols).mode(mode).parquet(filename)
            return filename
        self.DATAFRAME.write.mode(mode).parquet(filename)
        return filename

    def _save_df_as_external_to_hive(self, path_to_table, name, layer, table, partition_by=None):
        working_table = "{db}.{tb}".format(db=layer, tb=name)
        types_dict = dict(self.DATAFRAME.dtypes)

        if partition_by is not None:
            partition_type = get_partition_type(types_dict, partition_by)
            self.hive.executeUpdate(
            """
                CREATE EXTERNAL TABLE IF NOT EXISTS {table}({schema})
                PARTITIONED BY ({partitionBy})
                ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                STORED AS
                INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                LOCATION '{path_to_file}'
            """.format(
                table=working_table,
                schema=format_schema_to_string(types_dict, partition_by),
                partitionBy=get_partition_type(types_dict, partition_by),
                path_to_file=path_to_table))
            self.hive.executeUpdate("MSCK REPAIR TABLE {table}".format(table=working_table))
            return

        self.hive.executeUpdate(
            """
                CREATE EXTERNAL TABLE {table}({schema})
                ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                STORED AS
                INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                LOCATION '{path_to_file}'
            """.format(
                table=working_table,
                schema=format_schema_to_string(types_dict,partition_column=None),
                path_to_file=path_to_table))
        self.hive.executeUpdate("MSCK REPAIR TABLE {table}".format(table=working_table))

    def _save_df_as_external_to_sparksql(self, path_to_table, name, layer, table, partition_by=None):

        working_table = "{db}.{tb}".format(db=layer, tb=name)
        types_dict = dict(self.DATAFRAME.dtypes)

        if partition_by is not None:
            partition_type = get_partition_type(types_dict, partition_by)
            self.spark.sql(
            """
                CREATE EXTERNAL TABLE IF NOT EXISTS {table}({schema})
                PARTITIONED BY ({partitionBy})
                ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                STORED AS
                INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                LOCATION '{path_to_file}'
            """.format(
                table=working_table,
                schema=format_schema_to_string(types_dict, partition_by),
                partitionBy=get_partition_type(types_dict, partition_by),
                path_to_file=path_to_table))
            self.spark.sql("MSCK REPAIR TABLE {table}".format(table=working_table))
            return

        self.spark.sql(
            """
                CREATE EXTERNAL TABLE {table}({schema})
                ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                STORED AS
                INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                LOCATION '{path_to_file}'
            """.format(
                table=working_table,
                schema=format_schema_to_string(types_dict,partition_column=None),
                path_to_file=path_to_table))
        self.spark.sql("MSCK REPAIR TABLE {table}".format(table=working_table))

    def store_table(self, output_table, name, path_to_hdfs, partition=None, partition_by=None, layer=None, mode="overwrite"):
        self.DATAFRAME = output_table
        filename = str(str(path_to_hdfs) + str(filename))
        path_to_table_in_hdfs = self._save_df_to_HDFS(filename, partition=partition, partition_by_cols=partition_by, mode=mode)
        self._save_df_as_external_to_sparksql(path_to_table_in_hdfs, name, layer, table, partition_by=partition_by)
        self._save_df_as_external_to_hive(path_to_table, name, layer, table, partition_by=partition_by)

    def store_single_csv(self, output_file, name, layer=None, sublayer=None, partitionBy=None, mode="overwrite"):

        # Genera la ruta y nombre del archivo csv a guardar.
        csv_filename = "{base}/{capa}/{sublayer}/{nombre}/{part}".format(
            base=self.BASE_PATH,
            capa=layer,
            nombre=name,
            part=partitionBy,
            sublayer=sublayer
        )
        # Reduce la cantidad de particiones a 1 y escribe el archivo csv.
        output_file.repartition(1).write.csv(
            csv_filename,
            mode=mode,
            header=None)

        self.log.info("Creacion de archivo {} Ok".format(name))


def format_schema_to_string(values, partition_column=None):
    """
    Returns a string formatted schema of the DataFrame.
    Args:
        values (dict): Dicctionary with data from the DataFrame.
        partition_column (str): Name of the column to ignore in the definition
        of the schema for create external partitioned tables in Hive and spark-sql.
        Default value=None

    Returns:
        (str): Schema of the DataFrame.
        Format: ['colName colType, colName colType, ..., colName colType']
    """
    if ((partition_column is not None) and (partition_column in values)):
        del values[partition_column]
    return ", ".join(["{key} {value}".format(key=key, value=value) for key, value in values.items()])

def get_partition_type(values, column_name):
    """
    Returns column's type in the DataFrame passed to the funtion.
    Args:
        values (dict)
        column_name (str)

    Returns:
        colType (str): string representing the column's type in the DataFrame
    """
    value = values[str(column_name)]
    return "{key} {value}".format(key=column_name, value=value)
