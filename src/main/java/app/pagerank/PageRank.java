package app.pagerank;

import app.spark.SparkManager;

public class PageRank {
    SparkManager sparkManager = SparkManager.getInstance();
    private final double DEFAULT_PR = 0.15; // every page has at least a PR of 0.15 to share out

    public static void calculatePageRank(double dampeningFactor) {

    }
}
