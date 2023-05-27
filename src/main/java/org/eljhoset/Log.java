package org.eljhoset;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

public class Log {
    public static <T> PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> ofElements() {
        return new LoggingTransform<>();
    }

    public static class LoggingTransform<T> extends PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> {

        @Override
        public @NonNull PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new DoFn<T, T>() {

                @ProcessElement
                public void processElement(@Element T element, OutputReceiver<T> out, BoundedWindow window) {
                    String message = element.toString();
                    if (!(window instanceof GlobalWindow)) {
                        message = message + " Window:" + window.toString();
                    }
                    System.out.println(message);
                    out.output(element);
                }
            }));
        }
    }
}