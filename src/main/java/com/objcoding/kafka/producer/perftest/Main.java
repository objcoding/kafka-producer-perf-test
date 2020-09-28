package com.objcoding.kafka.producer.perftest;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @author zhangchenghui.dev@gmail.com
 * @since 1.0.0
 */
public class Main {

    private static final String TOPIC = "topic";
    private static final String MSG_FILE = "msgfile";
    private static final String ACKS = "acks";
    private static final String BOOTSTRAP_SERVERS = "bootstrapservers";
    private static final String LINGER_MS = "lingerms";
    private static final String BATCH_SIZE = "batchsize";
    private static final String BUFFER_MEMORY = "buffermemory";
    private static final String MAX_REQUEST_SIZE = "maxrequestsize";
    private static final String COMPRESSION_TYPE = "compressiontype";

    public static void main(String[] args) throws ParseException, IOException {

        Options options = new Options();
        options.addOption(TOPIC, true, "topic name.");
        options.addOption(MSG_FILE, true, "msg file.");
        options.addOption(ACKS, true, "ask config.");
        options.addOption(BOOTSTRAP_SERVERS, true, "bootstrap servers id.");
        options.addOption(LINGER_MS, true, "linger ms config.");
        options.addOption(BATCH_SIZE, true, "batch size config.");
        options.addOption(BUFFER_MEMORY, true, "buffer memory config.");
        options.addOption(MAX_REQUEST_SIZE, true, "max request size config.");
        options.addOption(COMPRESSION_TYPE, true, "compression type config.");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cl.getOptionValue(BOOTSTRAP_SERVERS));
        if (cl.hasOption(ACKS)) {
            properties.put(ProducerConfig.ACKS_CONFIG, cl.getOptionValue(ACKS));
        }
        if (cl.hasOption(LINGER_MS)) {
            properties.put(ProducerConfig.LINGER_MS_CONFIG, cl.getOptionValue(LINGER_MS));
        }
        if (cl.hasOption(BATCH_SIZE)) {
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, cl.getOptionValue(BATCH_SIZE));
        }
        if (cl.hasOption(BUFFER_MEMORY)) {
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, cl.getOptionValue(BUFFER_MEMORY));
        }
        if (cl.hasOption(MAX_REQUEST_SIZE)) {
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, cl.getOptionValue(MAX_REQUEST_SIZE));
        }
        if (cl.hasOption(COMPRESSION_TYPE)) {
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cl.getOptionValue(COMPRESSION_TYPE));
        }
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        List<byte[]> msgBytesList = new ArrayList<>();
        Random random = new Random();
        if (cl.hasOption(MSG_FILE)) {
            FileReader file = new FileReader(cl.getOptionValue(MSG_FILE));
            BufferedReader buff = new BufferedReader(file);
            String line;
            while (buff.ready()) {
                line = buff.readLine();
                System.out.println("msgSize:" + line.getBytes().length);
                msgBytesList.add(line.getBytes());
            }
            file.close();
            buff.close();
        } else {
            String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            for (int j = 0; j < 1024; j++) {
                int i1 = random.nextInt(10);
                if (i1 == 0) {
                    i1 = 1;
                }
                byte[] bytes = new byte[1024 * i1];
                for (int i = 0; i < bytes.length - 1; i++) {
                    bytes[i] = (byte) str.charAt(random.nextInt(62));
                }
                msgBytesList.add(bytes);
            }
        }

        int size = msgBytesList.size();
        while (true) {
            long start = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(cl.getOptionValue(TOPIC), msgBytesList.get(random.nextInt(size - 1))));
            long end = System.currentTimeMillis() - start;
            if (end > 100) {
                System.out.println("发送耗时:" + end);
            }
        }
    }

}
