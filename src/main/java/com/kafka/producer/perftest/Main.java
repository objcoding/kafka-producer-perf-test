package com.kafka.producer.perftest;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @author zhangchenghui.dev@gmail.com
 * @since 1.0.0
 */
public class Main {

    public static void main(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption("topic", true, "topic name.");
        options.addOption(ProducerConfig.ACKS_CONFIG.replace(".", ""), true, "ask config.");
        options.addOption(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG.replace(".", ""), true, "bootstrap servers id.");
        options.addOption(ProducerConfig.LINGER_MS_CONFIG.replace(".", ""), true, "linger ms config.");
        options.addOption(ProducerConfig.BATCH_SIZE_CONFIG.replace(".", ""), true, "batch size config.");
        options.addOption(ProducerConfig.BUFFER_MEMORY_CONFIG.replace(".", ""), true, "buffer memory config.");
        options.addOption(ProducerConfig.MAX_REQUEST_SIZE_CONFIG.replace(".", ""), true, "max request size config.");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cl.getOptionValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG.replace(".", "")));
        if (cl.hasOption(ProducerConfig.ACKS_CONFIG.replace(".", ""))) {
            properties.put(ProducerConfig.ACKS_CONFIG, cl.getOptionValue(ProducerConfig.ACKS_CONFIG.replace(".", "")));
        }
        if (cl.hasOption(ProducerConfig.LINGER_MS_CONFIG.replace(".", ""))) {
            properties.put(ProducerConfig.LINGER_MS_CONFIG, cl.getOptionValue(ProducerConfig.LINGER_MS_CONFIG.replace(".", "")));
        }
        if (cl.hasOption(ProducerConfig.BATCH_SIZE_CONFIG.replace(".", ""))) {
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, cl.getOptionValue(ProducerConfig.BATCH_SIZE_CONFIG.replace(".", "")));
        }
        if (cl.hasOption(ProducerConfig.BUFFER_MEMORY_CONFIG.replace(".", ""))) {
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, cl.getOptionValue(ProducerConfig.BUFFER_MEMORY_CONFIG.replace(".", "")));
        }
        if (cl.hasOption(ProducerConfig.MAX_REQUEST_SIZE_CONFIG.replace(".", ""))) {
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, cl.getOptionValue(ProducerConfig.MAX_REQUEST_SIZE_CONFIG.replace(".", "")));
        }
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        List<byte[]> bytesList = new ArrayList<>();
        Random random = new Random();
        for (int j = 0; j < 1024; j++) {
            int i1 = random.nextInt(10);
            if (i1 == 0) {
                i1 = 1;
            }
            byte[] bytes = new byte[1024 * i1];
            for (int i = 0; i < bytes.length - 1; i++) {
                bytes[i] = (byte) str.charAt(random.nextInt(62));
            }
            bytesList.add(bytes);
        }

        while (true) {
            long start = System.currentTimeMillis();
            producer.send(new ProducerRecord<>(cl.getOptionValue("topic"), bytesList.get(random.nextInt(1023))));
            long end = System.currentTimeMillis() - start;
            if (end > 100) {
                System.out.println("发送耗时:" + end);
            }
        }
    }

}
