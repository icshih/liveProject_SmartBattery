package net.icshih.web;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import net.icshih.avro.GenericSerializer;
import net.icshih.kafka.KafkaGenericRecordProducerApp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class SimpleWebServer {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(SimpleWebServer.class);
        // Initial Kafka Producer App
        final Properties props = KafkaGenericRecordProducerApp.loadProperties("kafka.properties");
        final GenericSerializer serializer = new GenericSerializer("user.avsc");
        final String topic = props.getProperty("output.topic.name");
        final KafkaGenericRecordProducerApp producerApp = new KafkaGenericRecordProducerApp(
                serializer, new KafkaProducer<>(props), topic);

        // Initial Web server
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 0);
        server.createContext("/test", new MyHttpHandler(serializer, producerApp));
        server.setExecutor(null);
        server.start();
        logger.info(" Server started on port 8001");
    }
}
