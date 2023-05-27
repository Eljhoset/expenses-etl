package org.eljhoset.functions;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class ForwardFillTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    private final Collection<Transaction> data;
    private final Collection<Transaction> expected;

    public ForwardFillTest(Map.Entry<String, String> entry) {
        data = initTransactions(entry.getKey());
        expected = initTransactions(entry.getValue());
    }

    private static List<Transaction> initTransactions(String entry) {
        return Arrays.stream(entry.split("\n"))
                .map(s -> s.split(","))
                .map(s -> new Transaction(Integer.parseInt(s[0]), s.length > 1 ? s[0] : ""))
                .toList();
    }

    @Test
    public void apply() {
        SerializableFunction<Transaction, Integer> groupKeyFunction = transaction -> {
            assert transaction != null;
            return transaction.id;
        };
        SerializableUnaryOperator<Transaction> mapFunction = (parent, child) -> new Transaction(child.id, parent.type);
        WithKeys<Integer, Transaction> groupFn = WithKeys.of(groupKeyFunction).withKeyType(TypeDescriptors.integers());
        SerializableComparator<Transaction> sortFn = (o1, o2) -> o2.type.compareTo(o1.type);
        ForwardFill<Transaction> forwardFill = new ForwardFill<>(mapFunction, groupFn, sortFn);
        PCollection<Transaction> filled = pipeline
                .apply(Create.of(data))
                .apply(forwardFill);
        PAssert.that(filled).containsInAnyOrder(expected);
        pipeline.run().waitUntilFinish();
    }

    @Parameterized.Parameters
    public static Collection<Map.Entry<String, String>> data() {
        return List.of(
                Map.entry(
                        """
                                1,Electronics
                                1,
                                1,
                                """,
                        """
                                1,Electronics
                                1,Electronics
                                1,Electronics
                                """
                ), Map.entry(
                        """
                                1,Electronics
                                1,
                                1,
                                2,Toys
                                2,
                                2,Toys
                                """,
                        """
                                1,Electronics
                                1,Electronics
                                1,Electronics
                                2,Toys
                                2,Toys
                                2,Toys
                                """
                ), Map.entry(
                        """
                                1,Toys
                                2,Electronics
                                2,
                                """,
                        """
                                1,Toys
                                2,Electronics
                                2,Electronics
                                """
                ), Map.entry(
                        """
                                1,
                                1,Electronics
                                1,
                                """,
                        """
                                1,Electronics
                                1,Electronics
                                1,Electronics
                                """
                ), Map.entry(
                        """
                                1,
                                1,Toys
                                1,
                                2,Electronics
                                2,
                                """,
                        """
                                1,Toys
                                1,Toys
                                1,Toys
                                2,Electronics
                                2,Electronics
                                """
                ), Map.entry(
                        """
                                1,Electronics
                                2,Toys
                                3,Food
                                """,
                        """
                                1,Electronics
                                2,Toys
                                3,Food
                                """
                )
        );
    }


    private record Transaction(int id, String type) implements Serializable {}
}