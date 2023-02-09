package net.icshih.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class GenericSerializerTest {


    @Test
    public void shallResturnTheExpectedSchemaFields() throws IOException {
        List<String> expected = List.of("first_name", "last_name", "battery_id");
        GenericSerializer battery = new GenericSerializer("src/test/resources/test.avsc");
        final List<String> fields = battery.getSchema()
                .getFields()
                .stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        assertEquals(expected, fields);
    }

    @Test
    public void testAddWithHashMap() throws IOException {
        HashMap<String, String> test = new HashMap<>();
        test.put("first_name", "Weili");
        test.put("last_name", "Hsu");
        test.put("battery_id", "25");
        GenericSerializer battery = new GenericSerializer("src/test/resources/test.avsc");
        GenericRecord testRecord = battery.createFrom(test);
        assertEquals("Weili", testRecord.get("first_name"));
        assertEquals("Hsu", testRecord.get("last_name"));
        assertEquals("25", testRecord.get("battery_id"));
    }
}
