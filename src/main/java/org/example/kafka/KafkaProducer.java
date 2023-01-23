package org.example.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

public class KafkaProducer {

  //  public  static void  kafkaConnect(DataStream<String> kafkaMessage){
//        String outputTopic="TOPIC-OUT";
//        String bootstrapServers="localhost:9092";
//
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(bootstrapServers)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic(outputTopic)
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//
//        kafkaMessage.sinkTo(sink);
//
//        KafkaRecordSerializationSchema.builder()
//                .setValueSerializationSchema(new SimpleStringSchema())
//                .setKeySerializationSchema(new SimpleStringSchema())
//                .setPartitioner(new FlinkFixedPartitioner<>())
//                .build();

//        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
//                .setValueSerializationSchema(new SimpleStringSchema())
//                .setTopic(outputTopic)
//                .build();
//
//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(bootstrapServers)
//                .setRecordSerializer(serializer)
//                .build();
//
//        kafkaMessage.sinkTo(sink);
  //  }
    public static KafkaSink<String> getKafkaSink(String brokers, String topic){
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        return sink;
    }
}
