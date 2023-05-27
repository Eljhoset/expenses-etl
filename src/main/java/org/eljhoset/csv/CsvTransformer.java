package org.eljhoset.csv;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.util.Objects;
import java.util.Optional;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CsvTransformer extends PTransform<@NonNull PCollection<FileIO.ReadableFile>, @NonNull PCollection<CSVRecordMap>> {
    public static Builder of() {
        return new Builder();
    }

    private final CSVFormat csvFormat;

    @Override
    public @NonNull PCollection<CSVRecordMap> expand(PCollection<FileIO.ReadableFile> input) {
        return input.apply(ParDo.of(new DoFn<FileIO.ReadableFile, CSVRecordMap>() {
            @ProcessElement
            public void processElement(@Element FileIO.ReadableFile element,
                                       OutputReceiver<CSVRecordMap> outputReceiver) throws IOException {
                Reader reader = new InputStreamReader(Channels.newInputStream(element.open()));
                csvFormat.parse(reader).stream()
                        .map(CSVRecordMap::valueOf)
                        .forEach(outputReceiver::output);
            }
        }));
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String[] headers;
        private char delimiter;
        private String nullString;

        public Builder withHeaders(String... headers) {
            this.headers = headers;
            return this;
        }

        public Builder withNullString(String nullString) {
            this.nullString = nullString;
            return this;
        }

        public Builder withDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public CsvTransformer build() {
            CSVFormat csvFormat = CSVFormat.Builder.create()
                    .setHeader(Objects.requireNonNull(headers))
                    .setDelimiter(delimiter)
                    .setSkipHeaderRecord(true)
                    .setNullString(Optional.ofNullable(nullString).orElse("NULL"))
                    .build();
            return new CsvTransformer(csvFormat);
        }
    }
}
