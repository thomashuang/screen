package com.hana.spark.sql;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class SQLDemo {

	public static class LogInfo implements Serializable {

		private String created;
		private String provider;
		private String category;
		private int channel;

		public String getCreated() {
			return created;
		}

		public void setCreated(String created) {
			this.created = created;
		}

		public String getProvider() {
			return provider;
		}

		public void setProvider(String provider) {
			this.provider = provider;
		}

		public String getCategory() {
			return category;
		}

		public void setCategory(String category) {
			this.category = category;
		}

		public int getChannel() {
			return channel;
		}

		public void setChannel(int channel) {
			this.channel = channel;
		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Uage SQLDemo <File>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("SQLDemo");
		conf.setMaster("local[20]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(args[0], 1);

		JavaRDD<LogInfo> logs = lines.map(new Function<String, LogInfo>() {
			public LogInfo call(String line) throws Exception {
				String[] parts = line.split("\\|");
				LogInfo log = new LogInfo();
				log.setCreated(parts[0]);
				log.setProvider(parts[3]);
				log.setCategory(parts[4]);
				log.setChannel(Integer.parseInt(parts[5].trim()));
				return log;
			}
		});
		logs.persist(StorageLevels.MEMORY_AND_DISK);

		SQLContext sqlCtx = new SQLContext(sc);
		DataFrame dataFrame = sqlCtx.createDataFrame(logs, LogInfo.class);
		// Register the DataFrame as a table.
		dataFrame.registerTempTable("stat_logs");

		sqlCtx.udf().register("to_day_with_hour", new UDF1<String, String>() {
			@Override
			public String call(String str) throws Exception {
				return str.substring(0, 13);
			}
		}, DataTypes.StringType);

		DataFrame results = sqlCtx
				.sql(String
						.format("SELECT channel, COUNT(*) AS total FROM stat_logs GROUP BY channel ORDER BY total DESC LIMIT %d",
								1000));
		List<String> data = results.toJavaRDD()
				.map(new Function<Row, String>() {
					@Override
					public String call(Row row) {
						return row.getInt(0) + "," + row.getLong(1);
					}
				}).collect();

		System.out.println("Channel     ,    total  ");
		for (String line : data) {
			System.out.println(line);
		}
		sc.stop();
	}
}
