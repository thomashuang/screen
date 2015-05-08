package com.hana.spark.sql.sqldemo2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.StorageLevels;
import java.io.Serializable;

public class SQLMain {

    
    public static void main(String[] args) {
        if (args.length < 1)
        {
            System.err.println("Uage EpgPv <File>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("EpgPv");
        //conf.setMaster("local[20]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLReporter epgpvReport = new SQLReporter(sc, args[0]);
        epgpvReport.report();

        sc.stop();
    }
}

