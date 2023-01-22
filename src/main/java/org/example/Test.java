package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.StreamEndEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.file.src.FileSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.flink.core.fs.FileSystem.WriteMode;
public class Test {
    static ObjectMapper objectMapper = new ObjectMapper().configure((DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),false);

    public static void main(String[] args) throws Exception {

        //final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaConsumer.getKafkaSource("", "", "");
        DataStream<String> allKafkaMessages = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

//        DataSet<String> text = env.fromElements(
//                "[Hello, My Dataset API Flink Program, This is also a string]");
//
////        DataSet<Tuple2<String, Integer>> wordCounts = text
////                .flatMap(new LineSplitter())
////                .groupBy(0)
////                .sum(1);
////        wordCounts.print();
//
//        DataSet<String> abc = text.flatMap(new JsonSplitter());
//        abc.print();

//        DataStream<String> jsonDataset = env.readFile("file:////mnt/c/Users/mpurn/IntellijProjects/src/main/resources/sample.json");
//        FileSource.forRecordStreamFormat(new TextLineInputFormat(),
//                "file:////mnt/c/Users/mpurn/IntellijProjects/src/main/resources/sample.json").build();
//file:///mnt/c/Users/mpurn/IntellijProjects/src/main/resources/
        File file= new File("sample.json");
        System.out.println(file.getAbsolutePath());
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(),Path.fromLocalFile(file))
                        .build();
        final DataStream<String> jsonDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        DataStream<String> abc = jsonDataStream.flatMap(new JsonSplitter());
        abc.writeAsText("file:///mnt/c/Users/mpurn/IntellijProjects/src/main/abc.txt",WriteMode.OVERWRITE);
//        jsonDataset.print();
//        abc.print();

//        DataStream<TransactionDTO> transactionDTODataSet = jsonDataStream.map(Test::convertToTransactionPojo)
//                .map(Test::convertToDTOClass);
//        transactionDTODataSet.print();
//        DataStream<Integer> map = transactionDTODataSet.map(e -> e.getAge());
//        map.print();
//        DataStream<String> transactionDTOStringMapOperator= transactionDTODataSet.map(Test::convertObjToJsonString);
//        KafkaSink<String> kafkaSink = KafkaProducer.getKafkaSink("localhost:9092", "TOPIC-OUT");
//        transactionDTOStringMapOperator.sinkTo(kafkaSink);
        env.execute("test");
        //txnDtoDS.writeAsCsv("file:///mnt/c/Users/anshm/IntellijProjects/flinkTest/sampleDto.csv");

    }

    public static Transaction convertToTransactionPojo(String json) throws IOException {
        return parsejson(json, Transaction.class);
    }

    public static TransactionDTO convertToDTOClass(Transaction transaction) throws IOException {
        return fetchRequiredData(transaction);
    }

    public static String convertObjToJsonString(TransactionDTO txnDTO) throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(txnDTO);
    }

    public static TransactionDTO fetchRequiredData(Transaction tr){
        String txn_id = tr.getTxn_id();
        String txn_amount = tr.getAmount();
        String tr_time = tr.getTime();
        String payer_name = tr.getPayer().getName();
        String enc_payer_name = payer_name.substring(0,1) + String.join("", Collections.nCopies(payer_name.length()-2, "*")) + payer_name.substring(payer_name.length()-1);
        int payer_age = tr.payer.getAge();
        return new TransactionDTO(txn_id,txn_amount,tr_time,enc_payer_name,payer_age);
    }

    public static <T> T parsejson(String json, Class<T> clazz) throws IOException {
        if(json != null){
            return objectMapper.readValue(json, clazz);
        }
        return null;
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public static class JsonSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) {
            collector.collect(s);
        }
    }

    public static String transform(String json) throws IOException {
        Transaction tr = parsejson(json, Transaction.class);
        TransactionDTO txnDTO = fetchRequiredData(tr);
        return convertObjToJsonString(txnDTO);
    }
}
