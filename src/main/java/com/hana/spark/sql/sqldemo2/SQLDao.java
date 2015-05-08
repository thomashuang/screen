package com.hana.spark.sql.sqldemo2;

import java.util.List;
import java.util.ArrayList;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.api.java.UDF1;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import org.apache.spark.sql.Row;

public class SQLDao {

	private SQLContext sqlCtx;

	// public EpgPvDao(JavaSparkContext sc, JavaRDD<String> data)
	// {
	// 	sqlCtx = new SQLContext(sc);


	// 	JavaRDD<Row> rowRDD = data.map(
	// 	  new Function<String, Row>() {
	// 	  	@Override
	// 	    public Row call(String record) throws Exception {
	// 	      String[] fields = record.split("|");
	// 	      return Row.create(fields[0], fields[3].trim(), fields[4], Integer.parseInt(fields[5]));
	// 	    }
	// 	});

	// 	DataFrame dataFrame = sqlCtx.createDataFrame(rowRDD, schema());
	// 	// Register the DataFrame as a table.
	// 	dataFrame.registerTempTable("stat_logs");

	// 	registerFunction();
	// }


	public SQLDao(JavaSparkContext sc, JavaRDD<LogInfo> data)
	{
		sqlCtx = new SQLContext(sc);
		DataFrame dataFrame = sqlCtx.createDataFrame(data, LogInfo.class);
		// Register the DataFrame as a table.
		dataFrame.registerTempTable("stat_logs");

		registerFunction();

	}
	
	private void registerFunction()
	{

		sqlCtx.udf().register("to_day_with_hour", 
			new UDF1<String, String>() {
      			@Override
      			public String call(String str) throws Exception {
        		return str.substring(0, 13);
      	}
    	}, DataTypes.StringType);

	}

	private StructType schema() {
		String schemaString = "created provider category";
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
  			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}

		fields.add(DataTypes.createStructField("channel", DataTypes.IntegerType, true));
		return DataTypes.createStructType(fields);
	}


    public Row getCount() {
        return sqlCtx.sql("SELECT COUNT(*) as total FROM stat_logs").first();
    }
	public DataFrame getGroupByChannel(int limit)
	{
		return sqlCtx.sql(String.format("SELECT channel, COUNT(*) AS total FROM stat_logs GROUP BY channel ORDER BY total DESC LIMIT %d", limit));
	}

	public DataFrame getGroupByHour(int limit) {

		return sqlCtx.sql(String.format("SELECT to_day_with_hour(created), COUNT(*) AS total FROM stat_logs GROUP BY to_day_with_hour(created) ORDER BY total DESC LIMIT %d", limit));
	}

}
