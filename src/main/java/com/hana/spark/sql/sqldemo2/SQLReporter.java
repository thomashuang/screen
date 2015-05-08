package com.hana.spark.sql.sqldemo2;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

 public  class SQLReporter implements Serializable{

        private transient SQLDao dao;

        public SQLReporter(JavaSparkContext sc, String path){
   
            JavaRDD<String> lines = sc.textFile(path, 1);

            //Other way
            JavaRDD<LogInfo> logs = lines.map(
                new Function<String, LogInfo>() {
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

            dao = new SQLDao(sc, logs);
        }

        public DataFrame getGroupByChannel(int limit)
        {
            return dao.getGroupByChannel(limit);
        }


        public DataFrame getGroupByHour(int limit) {

            return dao.getGroupByHour(limit);
        }

        public  Row  getCount() {
             return dao.getCount(); 
        }

        public void report()
        {
            int limit = 1000;
            Row r = getCount();
            System.out.println(r.getLong(0));
            DataFrame results = getGroupByChannel(limit);
            List<String> lines = results.toJavaRDD().map(
                new Function<Row, String>() {
                   // @Override
                    public String call(Row row) {
                        return row.getInt(0) + "," + row.getLong(1);
                }
            }).collect();

            System.out.println("Channel     ,    total  ");
            for(String line : lines)
            {
                System.out.println(line);
            }
            

            // DataFrame results2 = getGroupByHour(limit);
            //   List<String> lines2 = results2.toJavaRDD().map(
            //       new Function<Row, String>() {
            //          // @Override
            //           public String call(Row row) {
            //               return row.getString(0) + "," + row.getLong(1);
            //       }
            //   }).collect();
  
            // System.out.println("group by hour     ,    total  ");
            // for(String line : lines2)
            // {
            //       System.out.println(line);
            // }
        }

    }
