package io.rueishi.marketdata.crypto.core.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.agrona.concurrent.status.CountersReader;

/**
 * Converts Agrona counter labels and values into Prometheus text format.
 *
 * <p>{@code PrometheusCounterFormatter} is used only by {@link MetricsEndpoint}
 * on the metrics HTTP thread. It iterates a supplied {@link CountersReader}
 * generically at scrape time, parses the gateway counter label convention
 * written by {@link GatewayCounters}, sanitizes metric and label names, escapes
 * label values, and emits Prometheus-compatible text format version 0.0.4.</p>
 *
 * <p>The formatter has no registration list and does not know about individual
 * gateway or instrument counter accessors. This keeps P3-001 aligned with the
 * generic discovery contract: a counter allocated through the shared manager
 * becomes visible in the next formatted scrape as long as it has an allocated
 * Agrona record.</p>
 */
public final class PrometheusCounterFormatter {

    /**
     * Formats all allocated counters from the supplied reader.
     *
     * <p>Malformed labels are made safe rather than failing the scrape. A blank
     * or invalid metric name is replaced with a deterministic per-counter
     * fallback name, and label fragments without an equals sign are ignored.</p>
     *
     * @param countersReader generic Agrona reader over the shared counter store
     * @return Prometheus text body containing one sample line per allocated counter
     * @throws NullPointerException if {@code countersReader} is null
     */
    public String format(CountersReader countersReader) {
        Objects.requireNonNull(countersReader, "countersReader");
        StringBuilder builder = new StringBuilder();
        countersReader.forEach((counterId, label) -> appendCounter(builder, countersReader, counterId, label));
        return builder.toString();
    }

    private void appendCounter(
            StringBuilder builder,
            CountersReader countersReader,
            int counterId,
            String label) {
        ParsedLabel parsedLabel = parseLabel(counterId, label);
        builder.append(parsedLabel.metricName());
        if (!parsedLabel.labels().isEmpty()) {
            builder.append('{');
            for (int i = 0; i < parsedLabel.labels().size(); i++) {
                if (i > 0) {
                    builder.append(',');
                }
                LabelPair pair = parsedLabel.labels().get(i);
                builder.append(pair.name()).append("=\"").append(escapeLabelValue(pair.value())).append('"');
            }
            builder.append('}');
        }
        builder.append(' ').append(countersReader.getCounterValue(counterId)).append('\n');
    }

    private static ParsedLabel parseLabel(int counterId, String label) {
        String safeLabel = label == null ? "" : label;
        String[] parts = safeLabel.split(",", -1);
        String metricName = sanitizeMetricName(parts.length == 0 ? "" : parts[0], "gateway_counter_" + counterId);
        List<LabelPair> labels = new ArrayList<>();
        for (int i = 1; i < parts.length; i++) {
            int separator = parts[i].indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String labelName = sanitizeLabelName(parts[i].substring(0, separator));
            if (labelName.isEmpty()) {
                continue;
            }
            labels.add(new LabelPair(labelName, parts[i].substring(separator + 1)));
        }
        return new ParsedLabel(metricName, labels);
    }

    private static String sanitizeMetricName(String rawName, String fallback) {
        String sanitized = sanitizeName(rawName, true);
        if (sanitized.isEmpty()) {
            return fallback;
        }
        if (Character.isDigit(sanitized.charAt(0))) {
            return "_" + sanitized;
        }
        return sanitized;
    }

    private static String sanitizeLabelName(String rawName) {
        String normalized = normalizeKnownLabelName(rawName == null ? "" : rawName.trim());
        String sanitized = sanitizeName(normalized, false);
        if (!sanitized.isEmpty() && Character.isDigit(sanitized.charAt(0))) {
            return "_" + sanitized;
        }
        return sanitized;
    }

    private static String normalizeKnownLabelName(String rawName) {
        return switch (rawName) {
            case "instanceId" -> "instance_id";
            case "instrumentId" -> "instrument_id";
            default -> rawName;
        };
    }

    private static String sanitizeName(String rawName, boolean allowColon) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < rawName.length(); i++) {
            char ch = rawName.charAt(i);
            if (isNameCharacter(ch, allowColon)) {
                builder.append(ch);
            } else if (!builder.isEmpty() || ch != ' ') {
                builder.append('_');
            }
        }
        while (!builder.isEmpty() && builder.charAt(builder.length() - 1) == '_') {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    private static boolean isNameCharacter(char ch, boolean allowColon) {
        return (ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9')
                || ch == '_'
                || (allowColon && ch == ':');
    }

    private static String escapeLabelValue(String value) {
        StringBuilder builder = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '\\' || ch == '"') {
                builder.append('\\').append(ch);
            } else if (ch == '\n') {
                builder.append("\\n");
            } else {
                builder.append(ch);
            }
        }
        return builder.toString();
    }

    private record ParsedLabel(String metricName, List<LabelPair> labels) {
    }

    private record LabelPair(String name, String value) {
    }
}
