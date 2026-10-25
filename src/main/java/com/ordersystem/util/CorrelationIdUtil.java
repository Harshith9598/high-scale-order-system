package com.ordersystem.util;

import org.slf4j.MDC;

public class CorrelationIdUtil {
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private CorrelationIdUtil() {}

    public static void setCorrelationId(String correlationId) {
        MDC.put(CORRELATION_ID_MDC_KEY, correlationId);
    }

    public static String getCorrelationId() {
        return MDC.get(CORRELATION_ID_MDC_KEY);
    }

    public static void clear() {
        MDC.remove(CORRELATION_ID_MDC_KEY);
    }
}
