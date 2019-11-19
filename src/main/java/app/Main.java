package app;

import app.pagerank.PageRank;
import app.spark.SparkManager;
import app.utils.Utils;
import org.apache.spark.api.java.JavaPairRDD;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

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
        PageRank.calculatePageRank(inputGraph, dampeningFactor);

        // Stop Spark
        sparkManager.stopSpark();
    }

}


