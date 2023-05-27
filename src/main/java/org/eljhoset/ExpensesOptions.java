package org.eljhoset;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExpensesOptions extends PipelineOptions {

    @Description("Source type")
    @Validation.Required
    Source getSource();
    void setSource(Source type);

    @Description("source csv file")
    @Validation.Required
    String getSourceFile();
    void setSourceFile(String value);
    enum Source {
        PAYPAL
    }
}
