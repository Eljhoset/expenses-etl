package org.eljhoset.paypal;

import java.io.Serializable;

public record PaypalTransaction(long transactionNumber, String time, String name, String currency, Double amount) implements Serializable {}