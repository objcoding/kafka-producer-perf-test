package com.kafka.producer.perftest;

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

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 5242880);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 268435456);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 52428800);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 20);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        List<byte[]> bytesList = new ArrayList<byte[]>();
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
            producer.send(new ProducerRecord<String, byte[]>("test_topic_992340", bytesList.get(random.nextInt(1023))));
            long end = System.currentTimeMillis() - start;
            if (end > 100) {
                System.out.println("发送耗时:" + end);
            }
        }
    }

}
