package com.hana.spark;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class WordCount {
	public static final Pattern wordRe = Pattern.compile("^\\w+$");
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Uage WordCount <File>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("WordCount");
		// conf.setMaster("local[24]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		lines.cache();

		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String s) {
						return Arrays.asList(SPACE.split(s));
					}

				});

		JavaRDD<String> filterWords = words
				.filter(new Function<String, Boolean>() {
					@Override
					public Boolean call(String s) {
						return wordRe.matcher(s).find();
					}
				});

		JavaPairRDD<String, Integer> ones = filterWords
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer num1, Integer num2) {
						return num1 + num2;
					}
				}

				);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ":" + tuple._2());
		}

		ctx.stop();

	}

}