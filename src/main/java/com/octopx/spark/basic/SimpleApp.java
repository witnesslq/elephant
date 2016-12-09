package com.octopx.spark.basic;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public class SimpleApp {
	private final static String SPARK_HOME = "/Users/yuyang/Workspace/Binary/spark-1.6.1-bin-hadoop2.6";
	private final static String HDFS_HOME = "hdfs://192.168.1.90:8020";
	
	public static void main(String[] args) {
		String logFile = HDFS_HOME + "/user/yuyang/README.md";
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> distData = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		long sum = distData.reduce((a, b) -> a + b);
		System.out.println("Sum of 1, 2, 3, 4, 5: " + sum);
		
		JavaRDD<String> logData = sc.textFile(logFile).cache();

//		long numAs = logData.filter(new Function<String, Boolean>() {
//			public Boolean call(String s) {
//				return s.contains("a");
//			}
//		}).count();
//		long numCs = logData.filter(new Function<String, Boolean>() {
//			public Boolean call(String s) {
//				return s.contains("c");
//			}
//		}).count();

		long numAs = logData.filter(s -> s.contains("a")).count();
		long numCs = logData.filter(s -> s.contains("c")).count();
		System.out.println("Lines with a: " + numAs + ", lines with c: " + numCs);
		
		JavaRDD<Integer> lineLengths = logData.map(s -> s.length());
		lineLengths.persist(StorageLevel.MEMORY_ONLY());
		
		JavaPairRDD<String, Integer> pairs = logData.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
	}
}
