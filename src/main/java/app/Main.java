package app;

import app.pagerank.PageRank;
import app.spark.SparkManager;
import app.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.awt.*;
import java.io.*;
import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

        final String FILE_PATH = "C:/Users/barnu/Desktop/pagerank/src/main/java/app/data/input_data.txt"; // ToDo: cambiar para que acepte path relativo


        SparkManager sparkManager = SparkManager.getInstance();
        Utils utils = new Utils();

        // Init spark config
        sparkManager.initSpark();

        // Generate graph structure
        JavaPairRDD<String, Iterable<String>> inputGraph = utils.buildGraph(FILE_PATH);

        // Reads the dampening factor
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.println("Please enter the dampening factor (default=0.85): ");
        String dFactor = myObj.nextLine();  // Read user input
        double dampeningFactor = 0.85;
        try {
            dampeningFactor = new Double(dFactor);
        } catch (Exception e) {
            // Keep the default value
        }

        // Processing
        System.out.println("Calculating PageRank");
        JavaPairRDD<String, Double> ranksMapping = PageRank.calculatePageRank(inputGraph, dampeningFactor);



        // Displays and saves the result
        System.out.println("PageRank Result");
        List<Tuple2<String, Double>> ranksList = ranksMapping.collect();
        System.out.println(ranksList);

        PrintWriter writer = new PrintWriter("results.txt", "UTF-8");
        writer.print("Nodo\tPage Rank\n");
        for (Tuple2<String, Double> t:ranksList) {
            writer.println(t._1 + "\t"+t._2);
        }
        writer.close();

        // Stop Spark
        sparkManager.stopSpark();
    }

}


