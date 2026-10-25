package com.ordersystem.perftest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple performance test runner.
 * Measures RPS, avg latency, p95, p99.
 *
 * Usage:
 *   javac PerformanceTest.java
 *   java PerformanceTest --url http://localhost:8080/api/products --users 50 --duration 60
 */
public class PerformanceTest {

    public static void main(String[] args) throws Exception {
        String url = "http://localhost:8080/api/products";
        int users = 20;
        int durationSeconds = 30;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url": url = args[++i]; break;
                case "--users": users = Integer.parseInt(args[++i]); break;
                case "--duration": durationSeconds = Integer.parseInt(args[++i]); break;
            }
        }

        System.out.printf("Performance Test: url=%s users=%d duration=%ds%n", url, users, durationSeconds);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(users);
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        long startTime = System.nanoTime();
        long endTime = startTime + TimeUnit.SECONDS.toNanos(durationSeconds);

        CountDownLatch latch = new CountDownLatch(users);

        for (int i = 0; i < users; i++) {
            executor.submit(() -> {
                try {
                    while (System.nanoTime() < endTime) {
                        long reqStart = System.nanoTime();
                        try {
                            HttpRequest request = HttpRequest.newBuilder()
                                    .uri(URI.create(url))
                                    .timeout(Duration.ofSeconds(10))
                                    .GET()
                                    .build();

                            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reqStart);
                            latencies.add(latencyMs);

                            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reqStart);
                            latencies.add(latencyMs);
                            failureCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(durationSeconds + 10, TimeUnit.SECONDS);
        executor.shutdownNow();

        long totalTimeSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
        if (totalTimeSeconds == 0) totalTimeSeconds = 1;

        Collections.sort(latencies);

        double avg = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = percentile(latencies, 50);
        long p95 = percentile(latencies, 95);
        long p99 = percentile(latencies, 99);
        long min = latencies.isEmpty() ? 0 : latencies.get(0);
        long max = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);
        int total = successCount.get() + failureCount.get();
        double rps = (double) total / totalTimeSeconds;

        System.out.println("\n=== Results ===");
        System.out.printf("Total Requests: %d%n", total);
        System.out.printf("Success: %d, Failures: %d, Error Rate: %.2f%%%n",
                successCount.get(), failureCount.get(),
                total == 0 ? 0 : (failureCount.get() * 100.0 / total));
        System.out.printf("Duration: %d seconds%n", totalTimeSeconds);
        System.out.printf("RPS: %.2f%n", rps);
        System.out.printf("Latency - Min: %d ms, Avg: %.2f ms, Max: %d ms%n", min, avg, max);
        System.out.printf("p50: %d ms, p95: %d ms, p99: %d ms%n", p50, p95, p99);

        System.out.println("\nNote: Run multiple times and take median. Document environment details.");
    }

    private static long percentile(List<Long> sorted, int percentile) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }
}
