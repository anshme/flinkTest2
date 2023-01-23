package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.example.model.Transaction;
import org.example.modeldto.TransactionDTO;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;

import static org.apache.commons.lang3.StringUtils.split;

public class TransactionStreamingLogic {
    static ObjectMapper objectMapper = new ObjectMapper().configure((DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES),false);

    public static Transaction convertToTransactionPojo(String json) throws IOException {
        return parsejson(json, Transaction.class);
    }

    public static TransactionDTO convertToDTOClass(Transaction transaction) throws IOException {
        return fetchRequiredData(transaction);
    }
    public static class JsonSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) {
            System.out.println("jsonarray"+s);

            JSONArray jsonarray = new JSONArray(s);
            System.out.println("jsonarray length :"+jsonarray.length());
            for(int i=0; i<jsonarray.length(); i++) {
                JSONObject obj = jsonarray.getJSONObject(i);
                System.out.println(obj.toString());
                collector.collect(obj.toString());
            }
        }
    }

    public static String convertObjToJsonString(TransactionDTO txnDTO) throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        return ow.writeValueAsString(txnDTO);
    }

    public static TransactionDTO fetchRequiredData(Transaction tr){
        String txn_id = tr.getTxn_id();
        String txn_amount = tr.getAmount();
        String tr_time = split(tr.getTime())[0];
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

//    public static class JsonSplitter implements FlatMapFunction<String, String> {
//        @Override
//        public void flatMap(String s, Collector<String> collector) {
//            collector.collect(s);
//        }
//    }

    public static String transform(String json) throws IOException {
        Transaction tr = parsejson(json, Transaction.class);
        TransactionDTO txnDTO = fetchRequiredData(tr);
        return convertObjToJsonString(txnDTO);
    }
}
