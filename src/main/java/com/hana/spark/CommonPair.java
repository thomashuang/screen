package com.hana.spark;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class CommonPair {

	private static class Sum implements Function2<Integer, Integer, Integer> {
		@Override
		public Integer call(Integer a, Integer b) {
			return a + b;
		}
	}

	public static List<Tuple2<String, String>> combinations(List<String> caIds) {

		Collections.sort(caIds);
		List<Tuple2<String, String>> sortedCaIds = new ArrayList<Tuple2<String, String>>();
		for (String caId1 : caIds) {
			for (String caId2 : caIds) {
				if (caId1.compareTo(caId2) < 0) {
					sortedCaIds.add(new Tuple2<String, String>(caId1, caId2));

				}
			}
		}
		return sortedCaIds;
	}

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("CommonPair");
		// conf.setMaster("local[2]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0], 6);

		// Step 1: split to ca_id clients->list
		JavaPairRDD<String, Iterable<String>> cas = lines
				.mapToPair(new PairFunction<String, String, Iterable<String>>() {
					@Override
					public Tuple2<String, Iterable<String>> call(String line) {
						String[] parts = line.split("\\|");
						String ca_id = parts[0];
						String[] clients_ = parts[1].split("\\t");
						List<String> clients = new ArrayList<String>();
						for (String s : clients_) {
							clients.add(s);
						}
						return new Tuple2<String, Iterable<String>>(ca_id,
								clients);

					}
				});

		// Print result
		// List<Tuple2<String, Iterable<String>>> output = cas.collect();
		// for(Tuple2<?, ?> iter : output){

		// System.out.println( "CaId: " +iter._1);
		// List<String> clients = (List<String>) iter._2;
		// for (String c : clients) {
		// System.out.println("client: " + c);

		// }

		// }

		// Step 2: To ca_id, client pair
		JavaPairRDD<String, String> caPairs = cas
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> s) {
						String ca = s._1;
						List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
						List<String> clients = (List<String>) s._2;
						for (String c : clients) {
							results.add(new Tuple2<String, String>(c, ca));
						}
						return results;
					}
				});

		// Print caPairs
		// List<Tuple2<String, String>> caPairsOutput = caPairs.collect();
		// for(Tuple2<?, ?> iter : caPairsOutput){
		// System.out.println( "CaId: " + iter._1 + " client: " + iter._2);
		// }

		JavaPairRDD<String, Iterable<String>> clientCas = caPairs
				.groupByKey(256);

		JavaPairRDD<Tuple2<String, String>, Integer> userCommonPairs = clientCas
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, Tuple2<String, String>, Integer>() {

					@Override
					public Iterable<Tuple2<Tuple2<String, String>, Integer>> call(
							Tuple2<String, Iterable<String>> s) {
						List<String> lists = toList(s._2);
						List<Tuple2<String, String>> sortedCaIds = combinations(lists);
						List<Tuple2<Tuple2<String, String>, Integer>> results = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
						for (Tuple2<String, String> pair : sortedCaIds) {
							results.add(new Tuple2<Tuple2<String, String>, Integer>(
									pair, 1));
						}
						return results;
					}
				});

		JavaPairRDD<Tuple2<String, String>, Integer> commonUsersCount = userCommonPairs
				.reduceByKey(new Sum(), 256);

		JavaRDD<String> outLines = commonUsersCount
				.map(new Function<Tuple2<Tuple2<String, String>, Integer>, String>() {

					@Override
					public String call(
							Tuple2<Tuple2<String, String>, Integer> pair) {
						Tuple2<String, String> key = pair._1;
						return key._1 + "," + key._2 + "," + pair._2;
					}
				});



		outLines.saveAsTextFile("epgrex");

		// PrintWriter writer = null;
		// try {

		// writer = new PrintWriter("results.txt", "UTF-8");
		// JavaPairRDD< Tuple2<String, String>, Integer> commonUsersCount =
		// userCommonPairs.reduceByKey(new Sum());
		// List<Tuple2< Tuple2<String, String>, Integer>> caIdRexs =
		// commonUsersCount.collect();
		// for(Tuple2<Tuple2<String, String>, Integer> data : caIdRexs){
		// Tuple2<String, String> pairs = data._1;
		// writer.println( pairs._1 + "-" + pairs._2 + ": " + data._2 );

		// }

		// } catch (IOException ex) {

		// }finally {
		// if (writer != null)
		// {
		// writer.close();
		// }

		// }
		sc.stop();
	}

	public static List<String> toList(Iterable<String> inIters) {
		List<String> outList = new ArrayList<String>();
		for (String s : inIters) {
			outList.add(s);
		}

		return outList;
	}
}
