package demo.spark.sql.data.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadSaveFunctions {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Creating DataFrames")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> peopleDF =
                spark.read().format("json").load("src/main/resources/people.json");
        peopleDF.show();
        peopleDF.select("name", "age").write().format("parquet").save("/tmp/namesAndAges.parquet");
    }
}
