package io.hspx.standaloneReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.simple.Int96Value;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Runner {
    public static void main(String[] args) throws IOException {
//        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);

        java.nio.file.Path inputDir = java.nio.file.Path.of("sample-data/parquet/");
        java.nio.file.Path outputDir = java.nio.file.Path.of("sample-data/parquet-out/");

        DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(inputDir);

        for (java.nio.file.Path path : stream) {
            if (!Files.isDirectory(path)) {
                var out = readParquet(conf, path.toString());
                writeParquet(conf, out, outputDir + "/" + path.getFileName());
            }
        }

        stream = Files.newDirectoryStream(outputDir);
        for (java.nio.file.Path path : stream) {
            if (!Files.isDirectory(path) && path.getFileName().toString().endsWith(".crc")) {
                Files.delete(path);
            }
        }
    }

    public static void logRecord(GenericData.Record record) {
        var schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            var value = record.get(field.name());
            if (value != null && value.getClass().getName().equals("org.apache.avro.generic.GenericData$Fixed")) {
                var x = (GenericData.Fixed) value;
                value = getDateTimeValueFromBinary(x);//new Int96Value(Binary.fromConstantByteArray(x.bytes()));//convertBinaryToDecimal(x, 10, 0);
            }

            System.out.println(field.name() + " (" + (value != null ? value.getClass().getName() : null) + ")" + " = " + value);
        }
    }

    public static List<GenericData.Record> readParquet(final Configuration conf, String pathOfFile) throws IOException {
        System.out.println(String.format("Reading %s", pathOfFile));
        ParquetReader<GenericData.Record> reader = null;
        Path path = new Path(pathOfFile);
        reader = AvroParquetReader
                .<GenericData.Record>builder(path)
                .withConf(conf)
                .build();
        GenericData.Record record;
        List<GenericData.Record> recordList = new ArrayList<>();
        while ((record = reader.read()) != null) {
            recordList.add(record);
            System.out.println("\n\n========Record=======");
            logRecord(record);
        }

        System.out.println("Count: " + recordList.size());

        return recordList;
    }

    public static void writeParquet(final Configuration conf, List<GenericData.Record> recordList, String pathOfFile) throws IOException {
        System.out.println(String.format("Writing file %s with %d records", pathOfFile, recordList.size()));
        Path path = new Path(pathOfFile);
        ParquetWriter<GenericData.Record> writer = null;
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(recordList.get(0).getSchema())
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

    private static BigDecimal convertBinaryToDecimal(GenericData.Fixed value, int precision, int scale) {
        // based on parquet-mr pig conversion which is based on spark conversion... yo dawg?
        if (precision <= 18) {
            ByteBuffer buffer = ByteBuffer.wrap(value.bytes());
            byte[] bytes = buffer.array();
            int start = buffer.arrayOffset() + buffer.position();
            int end = buffer.arrayOffset() + buffer.limit();
            long unscaled = 0L;
            int i = start;
            while (i < end) {
                unscaled = (unscaled << 8 | bytes[i] & 0xff);
                i++;
            }
            int bits = 8 * (end - start);
            long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
            if (unscaledNew <= -Math.pow(10, 18) || unscaledNew >= Math.pow(10, 18)) {
                return new BigDecimal(unscaledNew);
            } else {
                return BigDecimal.valueOf(unscaledNew / Math.pow(10, scale));
            }
        } else {
            return new BigDecimal(new BigInteger(value.bytes()), scale);
        }
    }

    public static long getDateTimeValueFromBinary(GenericData.Fixed binaryTimeStampValue) {
        // This method represents binaryTimeStampValue as ByteBuffer, where timestamp is stored as sum of
        // julian day number (32-bit) and nanos of day (64-bit)

        NanoTime nt = NanoTime.fromBinary(Binary.fromConstantByteArray(binaryTimeStampValue.bytes()));
        int julianDay = nt.getJulianDay();
        long nanosOfDay = nt.getTimeOfDayNanos();
        return (julianDay - 2440588) * TimeUnit.HOURS.toMillis(24)
                + nanosOfDay / TimeUnit.MILLISECONDS.toNanos(1);
    }
}
