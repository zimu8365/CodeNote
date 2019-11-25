package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * demo
 */
public class demo {

    static SparkConf conf = null;
    static JavaSparkContext sc = null;
    static {
        conf = new SparkConf();
        conf.setMaster("local").setAppName("TA");
        sc = new JavaSparkContext(conf);
    }

    public static void main(final String[] args) {
        // map();
        // flatmap();
        // mapPartitions();
        // mapPartitionsWithIndex();
        // reduce();
        reduceByKey();
    }

    // transformation
    public static void map() {
        final String[] names = { "AA", "BB", "CC" };
        final List<String> list = Arrays.asList(names);
        final JavaRDD<String> listRdd = sc.parallelize(list);
        final JavaRDD<String> newRdd = listRdd.map(name -> {
            return "Hello " + name;
        });
        newRdd.foreach(name -> System.out.println(name));
    }

    public static void flatmap() {
        final String[] names = { "AA DD", "BB EE", "CC FF" };
        final List<String> list = Arrays.asList(names);
        final JavaRDD<String> listRdd = sc.parallelize(list);
        final JavaRDD<String> newRdd = listRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(name -> {
            return "Hello " + name;
        });
        newRdd.foreach(name -> System.out.println(name));
    }

    public static void mapPartitions() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> listRdd = sc.parallelize(list, 2);
        listRdd.mapPartitions(iterator -> {
            final List<String> aList = new ArrayList<>();
            while (iterator.hasNext()) {
                aList.add("Hello " + iterator.next());
            }
            return aList.iterator();
        }).foreach(name -> System.out.println(name));
    }

    public static void mapPartitionsWithIndex() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> listRdd = sc.parallelize(list, 2);
        listRdd.mapPartitionsWithIndex((index, iterator) -> {
            final List<String> aList = new ArrayList<>();
            while (iterator.hasNext()) {
                aList.add(index + "_" + iterator.next());
            }
            return aList.iterator();
        }, true).foreach(str -> System.out.println(str));
        ;
    }

    // Action
    public static void reduce() {
        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        final JavaRDD<Integer> listRdd = sc.parallelize(list, 2);
        final Integer result = listRdd.reduce((x, y) -> x + y);
        System.out.println(result);
    }

    public static void reduceByKey() {
        final List<Tuple2<String, Integer>> list = Arrays.asList(
            new Tuple2<String, Integer>("武当", 99),
            new Tuple2<String, Integer>("少林", 97), 
            new Tuple2<String, Integer>("武当", 89),
            new Tuple2<String, Integer>("少林", 77));
        final JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        final JavaPairRDD<String, Integer> rPairRDD = listRDD.reduceByKey((x, y) -> x + y);
        rPairRDD.foreach(tuple -> System.out.println("门派: " + tuple._1 + "->" + tuple._2));
    }
    
}