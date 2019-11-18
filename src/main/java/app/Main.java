package app;

import app.pagerank.PageRank;
import app.spark.SparkManager;
import app.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;

public class Main {

    public static void main(String[] args) {
        final String FILE_PATH = "/home/fabiola/IdeaProjects/pagerank/src/main/java/app/data/input_data.txt"; // ToDo: cambiar para que acepte path relativo
        SparkManager sparkManager = SparkManager.getInstance();
        Utils utils = new Utils();

        // Init spark config
        sparkManager.initSpark();

        // Generate graph structure
        JavaPairRDD<String, Iterable<String>> inputGraph = utils.buildGraph(FILE_PATH);
        System.out.println(inputGraph.collect());

        // Processing
        System.out.println("Calculating PageRank");
        PageRank.calculatePageRank(0.85);

        // Stop Spark
        sparkManager.stopSpark();
    }

}


