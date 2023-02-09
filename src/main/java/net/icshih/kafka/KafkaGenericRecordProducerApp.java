package net.icshih.kafka;

import net.icshih.avro.GenericRecordStore;
import net.icshih.avro.GenericSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class KafkaGenericRecordProducerApp {

    private final GenericSerializer serializer;

    private final Producer<String, GenericRecord> producer;

    final String outTopic;

    public KafkaGenericRecordProducerApp(final GenericSerializer serializer, final Producer<String, GenericRecord> producer, final String outTopic) {
        this.serializer = serializer;
        this.producer = producer;
        this.outTopic = outTopic;
    }

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties envProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

    public Future<RecordMetadata> produce(final GenericRecord record) {
        final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(outTopic, record);
        return producer.send(producerRecord);
    }

    public String printMetadata(final Future<RecordMetadata> metadata) {
        String message = new String();
        try {
            final RecordMetadata recordMetadata = metadata.get();
            message = String.format("Record written to offset %s timestamp %s", recordMetadata.offset(), recordMetadata.timestamp());
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        return message;
    }

    public void shutdown() {
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException(
                    "This program takes two arguments: the path to an environment configuration file and" +
                            "the path to the file with records to send");
        }

        final Properties props = KafkaGenericRecordProducerApp.loadProperties(args[0]);
        final GenericSerializer serializer = new GenericSerializer(args[1]);
        final String topic = props.getProperty("output.topic.name");
        final Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        final KafkaGenericRecordProducerApp producerApp = new KafkaGenericRecordProducerApp(serializer, producer, topic);

        BufferedReader br = null;

        try {
            br = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                System.out.print("Enter Your last name, first name, and owner id : ");
                String input = br.readLine();
                final String[] userInfo = input.split(" ");
                final GenericRecord record = serializer.create(userInfo[0], userInfo[1], userInfo[2]);

                final Future<RecordMetadata> metadata = producerApp.produce(record);
                System.out.println(producerApp.printMetadata(metadata));
            }
        } catch (WakeupException e) {
            System.out.println(e);
        } finally {
            producerApp.shutdown();
        }
    }

}
