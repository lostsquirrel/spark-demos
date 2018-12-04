package demo.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UntypedUserDefinedAggregateFunctions {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Creating DataFrames")
                .master("local[2]")
                .getOrCreate();

        // Register the function to access it
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = spark.read().json("src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
    }
}
