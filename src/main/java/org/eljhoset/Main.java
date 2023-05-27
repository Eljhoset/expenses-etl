package org.eljhoset;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.eljhoset.paypal.PayPallExpensesCalculator;

public class Main {
    public static void main(String[] args) {
        ExpensesOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ExpensesOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        PayPallExpensesCalculator.apply(options,pipeline);
        pipeline.run();
    }
}