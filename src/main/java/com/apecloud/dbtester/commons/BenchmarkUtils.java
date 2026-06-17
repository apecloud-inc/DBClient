package com.apecloud.dbtester.commons;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unified benchmark runner.
 *
 * Addresses typical copy-paste issues across engine bench implementations:
 * 1. Concurrency safety: provides a per-thread connection pool for engines whose
 *    clients are not thread-safe (JDBC, RabbitMQ, etc.), while reusing a single
 *    shared connection for thread-safe clients (MongoDB, Elasticsearch,
 *    RedisCluster, HttpClient, etc.).
 * 2. Metrics: consistently reports total iterations, concurrency, successful/failed
 *    counts, total time, QPS, average/min/max latency.
 * 3. Error handling: failures are counted and do not inflate the success count.
 * 4. Safety: guards against zero/negative iterations and concurrency, and shuts down
 *    the executor with a timeout.
 */
public class BenchmarkUtils {

    @FunctionalInterface
    public interface BenchTask {
        void run(DatabaseConnection connection) throws IOException;
    }

    /**
     * Runs a benchmark using a single shared connection.
     * Suitable for thread-safe clients such as MongoDB, Elasticsearch,
     * RedisCluster and HTTP clients.
     */
    public static String run(int iterations, int concurrency,
                             DatabaseConnection sharedConnection,
                             BenchTask task) {
        return runInternal(iterations, concurrency,
                () -> sharedConnection, task, false);
    }

    /**
     * Runs a benchmark where each worker thread owns an independent connection.
     * Suitable for clients that must be used within a single thread, such as
     * JDBC connections and RabbitMQ channels. Connections are created by the
     * provided factory and closed after the test.
     */
    public static String run(int iterations, int concurrency,
                             Callable<DatabaseConnection> connectionFactory,
                             BenchTask task) {
        return runInternal(iterations, concurrency,
                connectionFactory, task, true);
    }

    private static String runInternal(int iterations, int concurrency,
                                      Callable<DatabaseConnection> connectionFactory,
                                      BenchTask task,
                                      boolean connectionPerThread) {
        if (iterations <= 0) {
            return "Benchmark iterations must be > 0";
        }
        if (concurrency <= 0) {
            concurrency = 1;
        }

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch latch = new CountDownLatch(iterations);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);
        AtomicLong totalLatencyNs = new AtomicLong(0);
        AtomicLong minLatencyNs = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxLatencyNs = new AtomicLong(0);

        List<DatabaseConnection> threadConnections = new ArrayList<>();
        BlockingQueue<DatabaseConnection> availableConnections = new LinkedBlockingQueue<>();

        if (connectionPerThread) {
            try {
                for (int i = 0; i < concurrency; i++) {
                    DatabaseConnection conn = connectionFactory.call();
                    if (conn == null) {
                        throw new IOException("connectionFactory returned null");
                    }
                    threadConnections.add(conn);
                    availableConnections.add(conn);
                }
            } catch (Exception e) {
                executor.shutdownNow();
                for (DatabaseConnection conn : threadConnections) {
                    closeQuietly(conn);
                }
                return "Benchmark failed to create connections: " + e.getMessage();
            }
        }

        ThreadLocal<DatabaseConnection> threadConn = ThreadLocal.withInitial(() -> {
            DatabaseConnection conn = availableConnections.poll();
            if (conn == null) {
                throw new IllegalStateException("Not enough benchmark connections for worker threads");
            }
            return conn;
        });

        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            executor.execute(() -> {
                DatabaseConnection conn = null;
                long t0 = System.nanoTime();
                try {
                    if (connectionPerThread) {
                        conn = threadConn.get();
                    } else {
                        conn = connectionFactory.call();
                    }
                    task.run(conn);
                    success.incrementAndGet();
                } catch (Throwable e) {
                    failure.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    long latency = System.nanoTime() - t0;
                    totalLatencyNs.addAndGet(latency);
                    updateMin(minLatencyNs, latency);
                    updateMax(maxLatencyNs, latency);
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
        long totalNs = System.nanoTime() - startTime;

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }

        if (connectionPerThread) {
            for (DatabaseConnection conn : threadConnections) {
                closeQuietly(conn);
            }
        }

        double totalSeconds = totalNs / 1_000_000_000.0;
        int successful = success.get();
        int failed = failure.get();
        long avgLatencyMs = successful > 0 ? totalLatencyNs.get() / successful / 1_000_000 : 0;
        long minLatencyMs = successful > 0 ? minLatencyNs.get() / 1_000_000 : 0;
        long maxLatencyMs = successful > 0 ? maxLatencyNs.get() / 1_000_000 : 0;

        StringBuilder sb = new StringBuilder();
        sb.append("Benchmark results:\n")
          .append("Total iterations: ").append(iterations).append("\n")
          .append("Concurrency level: ").append(concurrency).append("\n")
          .append("Successful operations: ").append(successful).append("\n")
          .append("Failed operations: ").append(failed).append("\n")
          .append("Total time: ").append(String.format("%.3f", totalSeconds)).append(" seconds\n");
        if (totalSeconds > 0) {
            sb.append("Operations per second: ").append(String.format("%.2f", successful / totalSeconds)).append("\n");
        }
        if (successful > 0) {
            sb.append("Average latency: ").append(avgLatencyMs).append(" ms\n")
              .append("Min latency: ").append(minLatencyMs).append(" ms\n")
              .append("Max latency: ").append(maxLatencyMs).append(" ms\n");
        }
        return sb.toString();
    }

    private static void updateMin(AtomicLong current, long value) {
        long prev;
        do {
            prev = current.get();
            if (value >= prev) break;
        } while (!current.compareAndSet(prev, value));
    }

    private static void updateMax(AtomicLong current, long value) {
        long prev;
        do {
            prev = current.get();
            if (value <= prev) break;
        } while (!current.compareAndSet(prev, value));
    }

    private static void closeQuietly(DatabaseConnection conn) {
        if (conn == null) return;
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
