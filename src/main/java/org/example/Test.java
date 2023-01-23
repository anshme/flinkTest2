package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.example.kafka.KafkaConsumer;
import org.example.kafka.KafkaProducer;
import org.example.modeldto.TransactionDTO;
import org.example.service.ReadAndWriteFiles;
import org.example.service.TransactionStreamingLogic;

public class Test {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaConsumer.getKafkaSource("localhost:9092", "TOPIC-IN", "flink consumer");
        DataStream<String> allKafkaMessages = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        ReadAndWriteFiles.writeDataStreamStringToTextFile(allKafkaMessages,
                "file:///mnt/c/Users/mpurn/IntellijProjects/src/main/allKafkaMessages.txt");

        String filePath="/mnt/c/tools/flink-1.16.0/sample.json";
        final DataStream<String> jsonDataStream= ReadAndWriteFiles.readDataFromJson(env,filePath);

        DataStream<String> abc = jsonDataStream.flatMap(new TransactionStreamingLogic.JsonSplitter());
        ReadAndWriteFiles.writeDataStreamStringToTextFile(abc,
                "file:///mnt/c/Users/mpurn/IntellijProjects/src/main/abc.txt");

        DataStream<TransactionDTO> transactionDTODataSet = abc.map(TransactionStreamingLogic::convertToTransactionPojo)
                .map(TransactionStreamingLogic::convertToDTOClass);
        ReadAndWriteFiles.writePojoToTextFile1(transactionDTODataSet,
                "file:///mnt/c/Users/mpurn/IntellijProjects/src/main/transactionDTODataSet.txt");
//        transactionDTODataSet.print();
//        DataStream<Integer> map = transactionDTODataSet.map(e -> e.getAge());
//        map.print();
        DataStream<String> transactionDTOStringMapOperator= transactionDTODataSet.map(TransactionStreamingLogic::convertObjToJsonString);
        ReadAndWriteFiles.writeDataStreamStringToTextFile(transactionDTOStringMapOperator,
                "file:///mnt/c/Users/mpurn/IntellijProjects/src/main/transactionDTOStringMapOperator.txt");

        KafkaSink<String> kafkaSink = KafkaProducer.getKafkaSink("localhost:9092", "TOPIC-OUT");
        transactionDTOStringMapOperator.sinkTo(kafkaSink);
        env.execute("test");
        //txnDtoDS.writeAsCsv("file:///mnt/c/Users/anshm/IntellijProjects/flinkTest/sampleDto.csv");
    }
}
