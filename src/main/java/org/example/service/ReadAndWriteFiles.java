package org.example.service;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.modeldto.TransactionDTO;

import java.io.File;

public class ReadAndWriteFiles {
    public static DataStream<String> readDataFromJson(StreamExecutionEnvironment env, String filePath) {
        File file= new File(filePath);
        System.out.println(file.getAbsolutePath());
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), Path.fromLocalFile(file))
                        .build();
        final DataStream<String> jsonDataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
    }

    public static void writeDataStreamStringToTextFile(DataStream<String> FileContent, String filePath) {
        FileContent.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
    }

    public static void writePojoToTextFile(DataStream<TransactionDTO> transactionDTODataSet, String filePath) {
        transactionDTODataSet.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
    }
    public static <T> void writePojoToTextFile1(DataStream<T> t, String filePath) {
        t.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
    }
}
