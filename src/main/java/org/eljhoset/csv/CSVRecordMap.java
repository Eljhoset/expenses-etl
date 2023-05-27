package org.eljhoset.csv;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVRecord;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class CSVRecordMap implements Serializable {
    @EqualsAndHashCode.Include
    private final UUID id = UUID.randomUUID();
    private final Map<String, Integer> headerMap;
    private final CSVRecord record;

    public static CSVRecordMap valueOf(CSVRecord record) {
        return new CSVRecordMap(record.getParser().getHeaderMap(), record);
    }

    public String get(final String name) {
        return Optional.ofNullable(headerMap)
                .map(m -> m.get(name))
                .map(record::get)
                .orElse(null);
    }

    public String get(final int i) {
        return record.get(i);
    }

    public long recordNumber() {
        return record.getRecordNumber();
    }

    @Override
    public String toString() {
        return "CSVRecordMap{" +
                "id=" + id +
                ", headerMap=" + headerMap +
                ", record=" + record +
                '}';
    }
}
