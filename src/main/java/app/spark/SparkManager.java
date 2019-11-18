package app.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkManager {

    private static SparkManager INSTANCE;
    private SparkConf conf;
    private JavaSparkContext context;

    private SparkManager() {
    }

    public static SparkManager getInstance() {
        if(INSTANCE == null) {
            INSTANCE = new SparkManager();
        }
        return INSTANCE;
    }

    public JavaSparkContext getSparkContext() {
        return context;
    }

    public void initSpark() {
        if (this.conf == null) {
            this.conf = new SparkConf()
                    .setAppName("PageRankApp")
                    .setMaster("local[*]"); // Run Spark locally with as many worker threads as logical cores in the machine
        }
        if (this.context == null) {
            this.context = new JavaSparkContext(conf);
        }
    }

    public void stopSpark() {
        this.context.close();
        this.context.stop();
    }
}
