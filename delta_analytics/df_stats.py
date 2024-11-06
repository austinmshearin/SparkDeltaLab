"""
Module to calculate statistics of spark dataframes
"""
# Imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def descriptive_statistics(df: DataFrame):
    """
    Calculates descriptive statistics of a spark dataframe

    Parameters
    ----------
    df: pyspark.sql.DataFrame
        The pyspark dataframe

    Returns
    -------
    tuple(dict, spark_schema)
        The dictionary is the descriptive statistics
        The spark schema can be used to create the statistics spark dataframe

    Examples
    -------
    stats, schema = calculate_dataframe_statistics(df)
    df_stats = spark.createDataFrame(stats, schema)

    Notes
    -----
    Each column in the dataframe will have a row in the statistics dataframe with various calculated statistics:
    * All data types
        * total_rows: Total number of rows in the column (should be the same for all columns)
        * non_null_count: Total number of non-null rows
    * Boolean columns
        * true_count: Total number of rows that are True
    * Numerical columns (both integer and float)
        * mean: The average
        * min: The minimum
        * max: The maximum
        * percentile_5: The 5% percentile
        * percentile_25: The 25% percentile
        * percentile_40: The 40% percentile
        * median: The 50% percentile
        * percentile_60: The 60% percentile
        * percentile_75: The 75% percentile
        * percentile_95: The 95% percentile
    * Float columns
        * decimal_resolution: The maximum number of decimal places
    * String columns
        * distinct_count: The number of distinct strings (not including nulls or empty strings)
        * empty_string_count: The number of empty strings
        * contains_space: The number of strings that contain a space
        * contains_only_letters: The number of strings that contain only letters ("^[a-zA-Z]+$")
        * contains_only_lowercase_letters: The number of strings that contain only lowercase letters ("^[a-z]+$")
        * contains_only_numbers: The number of strings that contain only numbers ("^[0-9]+$")
        * contains_only_alphanumeric: The number of strings that contain only alphanumeric ("^[a-zA-Z0-9]+$")
        * contains_only_alphanumeric_space: The number of strings that contain only alphanumeric and spaces ("^[a-zA-Z ]+$")
        * contains_hyphen: The number of strings that contain a hyphen ("\\-")
        * contains_tabs_returns_newlines: The number of strings that contain tabs, carriage returns, or newlines ("[\t\r\n]")
        * contains_messy_chars: The number of strings that contain messy characters ("[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?`~]")
        * value_count_1: The count of the most occurring string
        * value_count_2: The sum of the counts of the two most occurring strings
        * value_count_5: The sum of the counts of the five most occurring strings
    """
    def calculate_numerical_stats(df: DataFrame, stats: dict, col_name: str) -> dict:
        if stats["non_null_count"] > 0:
            percentiles = df.approxQuantile(col_name, [0.05, 0.25, 0.4, 0.5, 0.6, 0.75, 0.95], 0.01)
            numerical_stats = {
                "mean": df.agg(F.mean(col_name)).first()[0],
                "min": df.agg(F.min(col_name)).first()[0],
                "max": df.agg(F.max(col_name)).first()[0],
                "percentile_5": percentiles[0],
                "percentile_25": percentiles[1],
                "percentile_40": percentiles[2],
                "median": percentiles[3],
                "percentile_60": percentiles[4],
                "percentile_75": percentiles[5],
                "percentile_95": percentiles[6]
            }
            numerical_stats = {key: float(item) for key, item in numerical_stats.items() if item is not None}
            stats.update(**numerical_stats)
        return stats

    def calculate_boolean_stats(df: DataFrame, stats: dict, col_name: str) -> dict:
        true_count = df.filter(F.col(col_name) == True).count()
        stats["true_count"] = true_count
        return stats

    def calculate_integer_stats(df: DataFrame, stats: dict, col_name: str) -> dict:
        stats = calculate_numerical_stats(df, stats, col_name)
        return stats

    def calculate_float_stats(df: DataFrame, stats: dict, col_name: str) -> dict:
        stats = calculate_numerical_stats(df, stats, col_name)
        df_resolution = df.withColumn("decimal_places", F.length(F.split(F.col(col_name).cast(T.StringType()), '\.')[1]))
        stats["decimal_resolution"] = df_resolution.agg(F.max("decimal_places")).collect()[0][0]
        return stats

    def calculate_string_stats(df: DataFrame, stats: dict, col_name: str) -> dict:
        stats["distinct_count"] = df.filter(F.col(col_name).isNotNull()).filter(F.col(col_name) != "").select(col_name).distinct().count()
        stats["empty_string_count"] = df.filter(F.col(col_name) == "").count()
        stats["contains_space"] = df.filter(F.col(col_name).contains(" ")).count()
        stats["contains_only_letters"] = df.filter(F.col(col_name).rlike("^[a-zA-Z]+$")).count()
        stats["contains_only_lowercase_letters"] = df.filter(F.col(col_name).rlike("^[a-z]+$")).count()
        stats["contains_only_numbers"] = df.filter(F.col(col_name).rlike("^[0-9]+$")).count()
        stats["contains_only_alphanumeric"] = df.filter(F.col(col_name).rlike("^[a-zA-Z0-9]+$")).count()
        stats["contains_only_alphanumeric_space"] = df.filter(F.col(col_name).rlike("^[a-zA-Z0-9 ]+$")).count()
        stats["contains_hyphen"] = df.filter(F.col(col_name).rlike("\\-")).count()
        stats["contains_tabs_returns_newlines"] = df.filter(F.col(col_name).rlike("[\t\r\n]")).count()
        stats["contains_messy_chars"] = df.filter(F.col(col_name).rlike("[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?`~]")).count()
        counts_df = (
            df.filter(F.col(col_name).isNotNull())
            .groupBy(col_name)
            .count()
            .orderBy(F.col("count").desc())
        )
        stats["value_count_1"] = counts_df.limit(1).agg({"count": "sum"}).first()["sum(count)"]
        stats["value_count_2"] = counts_df.limit(2).agg({"count": "sum"}).first()["sum(count)"]
        stats["value_count_5"] = counts_df.limit(5).agg({"count": "sum"}).first()["sum(count)"]
        return stats

    stats_data = []

    stats_schema = T.StructType([
        T.StructField("column", T.StringType(), True),
        T.StructField("total_rows", T.IntegerType(), True),
        T.StructField("non_null_count", T.IntegerType(), True),
        T.StructField("true_count", T.IntegerType(), True),
        T.StructField("mean", T.DoubleType(), True),
        T.StructField("median", T.DoubleType(), True),
        T.StructField("min", T.DoubleType(), True),
        T.StructField("max", T.DoubleType(), True),
        T.StructField("percentile_5", T.DoubleType(), True),
        T.StructField("percentile_25", T.DoubleType(), True),
        T.StructField("percentile_40", T.DoubleType(), True),
        T.StructField("percentile_60", T.DoubleType(), True),
        T.StructField("percentile_75", T.DoubleType(), True),
        T.StructField("percentile_95", T.DoubleType(), True),
        T.StructField("decimal_resolution", T.IntegerType(), True),
        T.StructField("distinct_count", T.IntegerType(), True),
        T.StructField("empty_string_count", T.IntegerType(), True),
        T.StructField("contains_space", T.IntegerType(), True),
        T.StructField("contains_only_letters", T.IntegerType(), True),
        T.StructField("contains_only_lowercase_letters", T.IntegerType(), True),
        T.StructField("contains_only_numbers", T.IntegerType(), True),
        T.StructField("contains_only_alphanumeric", T.IntegerType(), True),
        T.StructField("contains_only_alphanumeric_space", T.IntegerType(), True),
        T.StructField("contains_hyphen", T.IntegerType(), True),
        T.StructField("contains_tabs_returns_newlines", T.IntegerType(), True),
        T.StructField("contains_messy_chars", T.IntegerType(), True),
        T.StructField("value_count_1", T.IntegerType(), True),
        T.StructField("value_count_2", T.IntegerType(), True),
        T.StructField("value_count_5", T.IntegerType(), True),
    ])

    for field in df.schema.fields:
        column_name = field.name
        stats = {"column": column_name}
        stats["total_rows"] = df.count()
        stats["non_null_count"] = df.filter(F.col(column_name).isNotNull()).count()
        if isinstance(field.dataType, T.BooleanType):
            stats_data.append(calculate_boolean_stats(df, stats, column_name))
        if isinstance(field.dataType, (T.ShortType, T.IntegerType, T.LongType)):
            stats_data.append(calculate_integer_stats(df, stats, column_name))
        if isinstance(field.dataType, (T.FloatType, T.DoubleType)):
            stats_data.append(calculate_float_stats(df, stats, column_name))
        if isinstance(field.dataType, T.StringType):
            stats_data.append(calculate_string_stats(df, stats, column_name))

    return (stats_data, stats_schema)
