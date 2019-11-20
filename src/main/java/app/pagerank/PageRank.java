package app.pagerank;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PageRank {
    private static final double DEFAULT_PR = 0.15; // every page has at least a PR of 0.15 to share out

    public static JavaPairRDD<String, Double> calculatePageRank(JavaPairRDD<String, Iterable<String>> sitesGraph, double dampeningFactor) {
        System.out.println("Graph Matrix");
        System.out.println(sitesGraph.collect());

        // Initialize ranks
        Double initialRankVal =  (double)1/sitesGraph.count();
        JavaPairRDD<String, Double> ranksMapping = sitesGraph.mapValues(rs -> initialRankVal);
        System.out.println("Inital Ranks Mapping");
        System.out.println(ranksMapping.collect());

        // Calculate new ranks
        JavaPairRDD<String, Tuple2<Iterable<String>, Double>> linkRankAggregationMap = sitesGraph.join(ranksMapping);
        JavaPairRDD<String, Double> ranking = linkRankAggregationMap.values()
                .flatMapToPair(linksRankPair -> {
                    int linkListSize = Iterables.size(linksRankPair._1());
                    List<Tuple2<String, Double>> results = new ArrayList<>(); // stores ranks calculated for the link's list
                    for (String link : linksRankPair._1) { // iterates through the link's list of each site
                        results.add(new Tuple2<>(link, linksRankPair._2() / linkListSize));
                    }
                    return results.iterator();
                });

        // Apply dampening factor and default rank as required
        ranksMapping = ranking.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double x, Double y) { return x + y; }
        }).mapValues(sum -> DEFAULT_PR + sum * dampeningFactor);

        return ranksMapping;
    }
}
