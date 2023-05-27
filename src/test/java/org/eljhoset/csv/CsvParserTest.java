package org.eljhoset.csv;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;

public class CsvParserTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void parse() throws IOException {
        String[] headers = new String[]{"id", "name"};
        Create.Values<CSVRecordMap> lines = Create.of(
                mockRecord(headers, "1", "model one"),
                mockRecord(headers, "2", "model two")
        );
        PCollection<CSVRecordMap> linesPColl = testPipeline.apply(lines);

        SerializableFunction<CSVRecordMap, Model> mapper = record ->
        {
            assert record != null;
            return new Model(record.get(0), record.get("name"));
        };
        CsvParser<Model> transformer = CsvParser.of(Model.class).using(mapper);

        PCollection<Model> asPojo = linesPColl.apply(transformer);

        PAssert.that(asPojo).containsInAnyOrder(
                new Model("1", "model one"),
                new Model("2", "model two")
        );
        testPipeline.run().waitUntilFinish();
    }

    static CSVRecordMap mockRecord(String[] headers, String... data) throws IOException {
        String separator = ";";
        String values = StringUtils.join(data, separator);
        try (CSVParser parser = CSVFormat.DEFAULT.builder().setHeader(headers).setDelimiter(separator).build().parse(new StringReader(values))) {
            CSVRecord record = parser.iterator().next();
            return CSVRecordMap.valueOf(record);
        }
    }
    record Model(String id, String name) implements Serializable {}

}