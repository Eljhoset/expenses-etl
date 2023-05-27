package org.eljhoset.csv;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;


public class CsvParser<T> extends PTransform<@NonNull PCollection<CSVRecordMap>, @NonNull PCollection<T>> {


    public static <T> Builder<T> of(Class<T> tClass) {
        return new Builder<>(tClass);
    }

    private final SerializableFunction<CSVRecordMap, T> mapper;
    private final Class<T> clazz;

    private CsvParser(Class<T> clazz, SerializableFunction<CSVRecordMap, T> mapper) {
        this.mapper = mapper;
        this.clazz = clazz;
    }

    @Override
    public @NonNull PCollection<T> expand(PCollection<CSVRecordMap> stingLine) {
        return stingLine.apply(MapElements.into(TypeDescriptor.of(clazz)).via(mapper));
    }

    public record Builder<T>(Class<T> clazz) {
        public CsvParser<T> using(SerializableFunction<CSVRecordMap, T> mapper) {
            return new CsvParser<>(clazz, mapper);
        }
    }
}


