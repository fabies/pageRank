package app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {


    public static void main(String[] args) {

        // Setup Spark config
        SparkConf conf = new SparkConf()
                .setAppName("PageRankApplication")
                .setMaster("local[*]"); // Run Spark locally with as many worker threads as logical cores in the machine

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Processing
        System.out.println("Calculating PageRank");

        // Stop Spark
        sc.close();
        sc.stop();
    }
    
}


