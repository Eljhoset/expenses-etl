package org.eljhoset.functions;

import org.apache.beam.sdk.transforms.SerializableBiFunction;
@FunctionalInterface
public interface SerializableUnaryOperator<T> extends SerializableBiFunction<T,T,T> {
}
