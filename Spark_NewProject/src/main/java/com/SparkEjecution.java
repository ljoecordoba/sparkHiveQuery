package com;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import com.databricks.spark.csv.*;

public class SparkEjecution {
    public static void main(String[] args) {
    	SparkConf conf = new SparkConf().setAppName("Simple Application");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	HiveContext sqlContext = new HiveContext(sc);
    	//HiveContext sqlContext = new HiveContext(sc);
    	DataFrame df = sqlContext.sql("select * from sandbox.tabla_test_spark");
    	df.write().format("com.databricks.spark.csv").save("hdfs:///user/bigdata/luciano/export2");
	}
}
