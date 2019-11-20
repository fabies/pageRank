package app.utils;

import app.spark.SparkManager;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Utils {
    SparkManager sparkManager = SparkManager.getInstance();

    private JavaRDD<String> readFromFile(String filePath) {
        return sparkManager.getSparkContext().textFile(filePath);
    }

    // ToDo: Hay que cambiar esta funcion para que procese el archivo con el formato correcto
    /**
     * Generates graph object to use as PageRank input.
     * @param filePath
     * @return RDD object of form: [(siteId, [link1, link2, ...]), (siteId, [link1, link2, ...]), ... ]
     *          Example: [(1, [3, 4]), (2, [1]), (3, [2, 4]), (4, [1, 2, 3])]
     */
    public JavaPairRDD<String, Iterable<String>> buildGraph(String filePath) {
        Pattern SPACES = Pattern.compile("\\s+");
        JavaRDD<String> rows = readFromFile(filePath);

        List<String> rowsList = rows.collect();

        // Gets the first line, the number of nodes
        int nodesNumber = Integer.parseInt(rowsList.get(0));

        // Associates the index of the line with the info
        List<Tuple2<Integer, Iterable<String>>> results = new ArrayList<>();
        for (int i = 1; i <= nodesNumber; i++) {
            try {
                results.add(new Tuple2<Integer, Iterable<String>>(i, Arrays.asList(rowsList.get(i).split(" "))));
            } catch (Exception e) {
                // It catches a behavior of readFromFile(), which doesn't takes the last line if it is a ""
                results.add(new Tuple2<Integer, Iterable<String>>(i, new ArrayList<>()));
            }

        }

        // Translates the variable type to the one needed for pagerank
        JavaPairRDD<String, Iterable<String>> graph = sparkManager.getSparkContext().parallelize(results).mapToPair(t -> {
            return new Tuple2(Integer.toString(t._1), t._2);
        });


        return graph;
    }

}
