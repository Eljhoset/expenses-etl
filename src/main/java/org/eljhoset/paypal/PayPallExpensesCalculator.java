package org.eljhoset.paypal;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.eljhoset.ExpensesOptions;
import org.eljhoset.Log;
import org.eljhoset.Result;
import org.eljhoset.csv.CSVRecordMap;
import org.eljhoset.csv.CsvIO;
import org.eljhoset.csv.CsvParser;
import org.eljhoset.csv.CsvTransformer;
import org.eljhoset.functions.ForwardFill;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

public interface PayPallExpensesCalculator {

    NumberFormat numberFormat = NumberFormat.getInstance(Locale.US);

    static PCollection<Result> apply(ExpensesOptions options, Pipeline pipeline) {
        CsvTransformer csvTransformer = CsvTransformer.of()
                .withDelimiter(',')
                .withHeaders("Fecha",
                        "Hora",
                        "Zona horaria",
                        "Nombre",
                        "Tipo",
                        "Estado",
                        "Divisa",
                        "Importe",
                        "Id.del formato de pago",
                        "Saldo")
                .build();
        pipeline.apply("Reading CSV", CsvIO.read(options.getSourceFile()).withCsvTransformer(csvTransformer).build())
                .apply(Filter.by(line -> {
                    String amount = Objects.requireNonNull(line).get("Importe");
                    return !amount.isBlank();
                }))
                .apply(transactions())
                //.apply(new ForwardFill<PaypalTransaction>())
                .apply(Log.ofElements());
        return null;
    }


    static CsvParser<PaypalTransaction> transactions() {
        return CsvParser.of(PaypalTransaction.class).using(e -> {
            CSVRecordMap input = Objects.requireNonNull(e);
            long recordNumber = input.recordNumber();
            String date = input.get("Fecha");
            String time = input.get("Hora");
            String dateTime = String.format("%s %s", date, time);
            String name = input.get("Nombre");
            String currency = input.get("Divisa");
            double amount;
            try {
                amount = numberFormat.parse(input.get("Importe")).doubleValue();
            } catch (ParseException ex) {
                throw new RuntimeException(ex);
            }
            return new PaypalTransaction(recordNumber, dateTime, name, currency, amount);
        });
    }
}
