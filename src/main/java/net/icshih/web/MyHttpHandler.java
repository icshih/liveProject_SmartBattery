package net.icshih.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import net.icshih.avro.GenericSerializer;
import net.icshih.kafka.KafkaGenericRecordProducerApp;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MyHttpHandler implements HttpHandler {

    private final GenericSerializer serializer;
    private final KafkaGenericRecordProducerApp producerApp;

    public MyHttpHandler(GenericSerializer serializer, KafkaGenericRecordProducerApp producerApp) {
        this.serializer = serializer;
        this.producerApp = producerApp;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        String method = httpExchange.getRequestMethod();
        String requestParamValue = "";
        if ("GET".equals(method)) {
            requestParamValue = handleGetRequest(httpExchange);
        }
        if ("POST".equals(method)) {
            requestParamValue = handlePostRequest(httpExchange);
        }
        handleResponse(httpExchange, requestParamValue);
    }

    private String handleGetRequest(HttpExchange httpExchange) {
        return httpExchange.getRequestURI().toString();
    }

    private String handlePostRequest(HttpExchange httpExchange) throws IOException {
        final GenericRecord record = serializer.createFrom(parse(httpExchange));
        final Future<RecordMetadata> meta = producerApp.produce(record);
        return producerApp.printMetadata(meta);
    }

    private void handleResponse(HttpExchange httpExchange, String requestParamValue) throws IOException {
        String response = requestParamValue;
        httpExchange.sendResponseHeaders(200, response.length());
        OutputStream outputStream = httpExchange.getResponseBody();
        outputStream.write(response.toString().getBytes());
        outputStream.flush();
        outputStream.close();
    }

    private HashMap<String, String> parse(HttpExchange httpExchange) throws UnsupportedEncodingException {
        List<String> headers = httpExchange.getRequestHeaders().get("Content-Type");
        String boundary = headers.stream()
                .filter(h -> h.contains("boundary"))
                .map(h -> h.split("=")[1])
                .findFirst()
                .get();
        final InputStreamReader requestBody = new InputStreamReader(httpExchange.getRequestBody(), "utf-8");
        BufferedReader reader = new BufferedReader(requestBody);
        final List<String> body = reader.lines().collect(Collectors.toList());
        List<Integer> bodyIdx = IntStream
                .range(0, body.size())
                .filter(i -> body.get(i).contains("--" + boundary))
                .boxed()
                .collect(Collectors.toList());
        List<String> fields = bodyIdx.subList(0, bodyIdx.size() - 1).stream()
                .map(j -> body.get(j+1))
                .map(f -> f.split(";")[1].split("=")[1].replace("\"", ""))
                .collect(Collectors.toList());
        List<String> values = bodyIdx.subList(0, bodyIdx.size() - 1).stream()
                .map(j -> body.get(j+3))
                .collect(Collectors.toList());

        return IntStream.range(0, fields.size())
                .collect(
                        HashMap::new,
                        (m, i) -> m.put(fields.get(i), values.get(i)),
                        Map::putAll
                );
    }
}
