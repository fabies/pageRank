package app.utils;

import app.spark.SparkManager;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

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

        return rows.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();
    }
}