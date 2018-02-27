package com.chandan.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Use below command on Terminal in spark home, to run this program:
 * bin/spark-submit --class "com.chandan.example.countAlphabet" --master local[4] /var/www/spark-example/target/spark-example-1.0-SNAPSHOT.jar README.md z
 *
 * Created by chandan.bansal on 27/02/18.
 */
public class countAlphabet {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: countAlphabets <file> <alphabet>");
            System.exit(1);
        }

        String logFile = args[0]; // Should be some file on your system
        String alphabet = args[1];

//        String logFile = "/usr/local/Cellar/apache-spark/2.2.1/README.md";
//        String alphabet = "a";

//        SparkConf conf =
//                new SparkConf()
//                        .setAppName("Simple Application")
//                        .setMaster("spark://10.3.50.139:7077");

        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
//                .config(conf)
                .getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains(alphabet)).count();
//        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with " + alphabet +": " + numAs);
        spark.stop();
    }
}
