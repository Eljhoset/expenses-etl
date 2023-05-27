package org.eljhoset.functions;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ForwardFill<T extends Serializable> extends PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> {
    private final SerializableUnaryOperator<T> mapFunction;
    private final WithKeys<? extends Serializable, T> groupKeyFn;
    private final SerializableComparator<T> sortFn;

    public ForwardFill(SerializableUnaryOperator<T> mapFunction, WithKeys<? extends Serializable, T> groupKeyFn, SerializableComparator<T> sortFn) {
        this.mapFunction = mapFunction;
        this.groupKeyFn = groupKeyFn;
        this.sortFn = sortFn;
    }

    @Override
    public @NonNull PCollection<T> expand(@NonNull PCollection<T> input) {
        return input.apply(groupKeyFn)
                .apply(GroupByKey.create())
                .apply(Values.create())
                .apply(FlatMapElements.via(fillElements()));
    }

    private @NonNull SimpleFunction<Iterable<T>, Iterable<T>> fillElements() {
        return new SimpleFunction<>() {
            @Override
            public Iterable<T> apply(Iterable<T> iterable) {
                List<T> list = StreamSupport.stream(iterable.spliterator(), false).sorted(sortFn).toList();
                return list.stream().findFirst()
                        .map(seed -> Stream.concat(Stream.of(seed), list.stream().skip(1)
                                .map(e -> mapFunction.apply(seed, e))).toList()
                        ).orElse(List.of());
            }
        };
    }
}
