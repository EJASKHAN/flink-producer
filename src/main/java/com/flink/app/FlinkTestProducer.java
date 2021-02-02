/*
  author: Ejaskhan
  This is a sample flink application for running in Intellij,which will produce data to EH.
*/
package com.flink.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

public class FlinkTestProducer {

    private static final String TOPIC = "test_source";
    private static final String FILE_PATH = "src/main/resources/producer.config";

    public static void main(String... args) {
        try {
            Properties properties = new Properties();
            properties.load(new FileReader(FILE_PATH));

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
             for(int iter=0;iter<3;iter++) {
                 DataStream<String> stream = createStream(env,iter);
                 FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                         TOPIC,
                         new SimpleStringSchema(),
                         properties);

                 stream.addSink(myProducer);
                 env.execute("Flink Producer");
             }

        } catch(FileNotFoundException e){
            System.out.println("FileNotFoundException: " + e);
        } catch (Exception e) {
            System.out.println("Failed with exception:: " + e);
        }
    }

    public static DataStream<String> createStream(StreamExecutionEnvironment env,int offset){
        return env.fromSequence(0, 10)
                .map((MapFunction<Long, String>) in -> "EK-" + in +" "+ offset);
    }
}
