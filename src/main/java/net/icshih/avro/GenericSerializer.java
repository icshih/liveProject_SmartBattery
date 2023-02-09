package net.icshih.avro;

import org.apache.avro.Schema;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;

public class GenericSerializer {

    private final Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private Queue<GenericRecord> genericRecordQueue = new ArrayDeque<>();

    public GenericSerializer(String schemaFile) throws IOException {
        schema = new Schema.Parser().parse(new File(schemaFile));
    }

    public Schema getSchema() {
        return schema;
    }

    public GenericRecord createFrom(HashMap<String, String> data) {
        GenericRecord record = new GenericData.Record(schema);
        data.keySet().forEach(key -> record.put(key, data.get(key)));
        return record;
    }

    public GenericRecord create(String lastName, String firstName, String ownerId) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("first_name", firstName);
        record.put("last_name", lastName);
        record.put("battery_id", ownerId);
        return record;
    }

    public void add(GenericRecord record) {
        genericRecordQueue.add(record);
    }

    public GenericRecord poll() {
        return genericRecordQueue.poll();
    }

    public void open(String avroFile) throws IOException {
        dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
        dataFileWriter.create(schema, new File(avroFile));
    }

    public void append(GenericRecord record) {
        try {
            dataFileWriter.append(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(List<GenericRecord> records) throws IOException {
        records.stream().forEach(r -> append(r));
    }

    public void writeAndClose() throws IOException {
        dataFileWriter.close();
    }

}
