package mg.lahatra3;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SparkMain {
    public static void main(String[] args) {
        log.info("Hello lahatrad...");

        List<Double> inputData = new ArrayList<>();
        inputData.add(3.11);
        inputData.add(12.235);
        inputData.add(13.897);
        inputData.add(87.25578);
        inputData.add(11.3658);

        log.info("INPUT: {}", inputData);

        SparkConf sparkConf = new SparkConf()
                .setAppName("lahatrad")
                .setMaster("local[*]");

        try(JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<Double> myRdd = sparkContext.parallelize(inputData);
        }
    }
}