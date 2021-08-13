package io.hspx.standaloneReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Runner {
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);

        java.nio.file.Path inputDir = java.nio.file.Path.of("sample-data/parquet/");
        java.nio.file.Path outputDir = java.nio.file.Path.of("sample-data/parquet-out/");

        DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(inputDir);

        for (java.nio.file.Path path : stream) {
            if (!Files.isDirectory(path)) {
                var out = readParquet(conf, path.toString());
                writeParquet(conf, out.getFirst(), out.getSecond(), outputDir.toString() + "/" + path.getFileName());
            }
        }

        stream = Files.newDirectoryStream(outputDir);
        for (java.nio.file.Path path : stream) {
            if (!Files.isDirectory(path) && path.getFileName().toString().endsWith(".crc")) {
                Files.delete(path);
            }
        }
    }

    public static Pair<Schema, List<GenericData.Record>> readParquet(final Configuration conf, String pathOfFile) throws IOException {
        System.out.println(String.format("Reading %s", pathOfFile));
        ParquetReader<GenericData.Record> reader = null;
        Path path = new Path(pathOfFile);
        reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(conf)
                .build();
        GenericData.Record record;
        List<GenericData.Record> recordList = new ArrayList<>();
        Schema schema = null;
        int count = 0;
        while ((record = reader.read()) != null) {
            recordList.add(record);
            schema = record.getSchema();
        }

        System.out.println("Count: " + recordList.size());

        return new Pair<>(schema, recordList);
    }

    public static void writeParquet(final Configuration conf, Schema schema, List<GenericData.Record> recordList, String pathOfFile) throws IOException {
        System.out.println(String.format("Writing file %s with %d records", pathOfFile, recordList.size()));
        Path path = new Path(pathOfFile);
        ParquetWriter<GenericData.Record> writer = null;
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();

            for (GenericData.Record record : recordList) {
                writer.write(record);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
