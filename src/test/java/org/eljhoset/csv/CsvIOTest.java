package org.eljhoset.csv;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class CsvIOTest {
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void read() {
        CsvIO.Read csvIO = CsvIO.read("src/test/resources/products.csv")
                .withCsvTransformer(CsvTransformer.of()
                        .withHeaders("id", "name", "department")
                        .withDelimiter(',')
                        .withNullString("null")
                        .build())
                .build();
        String format = "%s->%s,%s";

        PCollection<String> records = testPipeline.apply(csvIO)
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(record -> {
                                    assert record != null;
                                    return String.format(format, record.get("id"),
                                            record.get(1),
                                            record.get("department"));
                                }
                        ));
        PAssert.that(records).containsInAnyOrder(
                String.format(format, "1", "Kellogg Special K Cereal", "Kids"),
                String.format(format, "2", "Wine - Tribal Avignon", "Kids"),
                String.format(format, "8", "Crab - Blue, Frozen", "Electronics")
        );
        testPipeline.run().waitUntilFinish();
    }

}