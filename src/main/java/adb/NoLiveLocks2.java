package adb;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 * 
 *         # Permission is hereby granted, free of charge, to any person
 *         obtaining a copy
 *         # of this software and associated documentation files (the
 *         "Software"), to deal
 *         # in the Software without restriction, including without limitation
 *         the rights
 *         # to use, copy, modify, merge, publish, distribute, sublicense,
 *         and/or sell
 *         # copies of the Software, and to permit persons to whom the Software
 *         is
 *         # furnished to do so, subject to the following conditions:
 *         #
 *         # The above copyright notice and this permission notice shall be
 *         included in
 *         # all copies or substantial portions of the Software.
 *         #
 *         # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *         EXPRESS OR
 *         # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *         MERCHANTABILITY,
 *         # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
 *         SHALL THE
 *         # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 *         OTHER
 *         # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 *         ARISING FROM,
 *         # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *         DEALINGS IN
 *         # THE SOFTWARE.
 */

public class NoLiveLocks2 extends Benchmarkable {
    private static final String DEADLOCK_CODE = "40P01";
    private static final int DEFAULT_BATCH_SIZE = 64;
    private static final int MAX_BACKOFF_MS = 100;

    private final int batchSize;
    final AtomicInteger tasks_restarted = new AtomicInteger();

    enum QueryType {
        MULTI_POINT_READ, INSERT, FULL_SCAN, UPDATE
    }

    // Data only: statements and connections belong to the execution stage.
    record TransactionTask(int taskId, QueryType type, int[] tupleIds,
                           double[] values, int insertId) {
    }

    public NoLiveLocks2() {
        this(DEFAULT_BATCH_SIZE);
    }

    public NoLiveLocks2(int batchSize) {
        super("NoLiveLocks2");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive.");
        }
        this.batchSize = batchSize;
    }

    @Override
    protected Runnable createTask(int taskId) {
        TransactionTask task = generateTask(taskId, current_num_tuple.get());
        return () -> executeTask(task);
    }

    @Override
    protected void resetRunState() {
        super.resetRunState();
        tasks_restarted.set(0);
    }

    @Override
    public double[] benchmark(int numThreads, long durationMillis) throws InterruptedException {
        if (numThreads <= 0 || durationMillis <= 0) {
            throw new IllegalArgumentException("Worker count and benchmark duration must be positive.");
        }
        if (num_queries_per_task <= 0 || num_tuple < num_queries_per_task) {
            throw new IllegalArgumentException("The table must contain enough tuples for distinct point reads.");
        }
        resetRunState();
        createDB();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
        long start = System.nanoTime();
        long durationNanos = TimeUnit.MILLISECONDS.toNanos(durationMillis);
        boolean completedNormally = false;
        try {
            while (run && System.nanoTime() - start < durationNanos) {
                List<TransactionTask> pending = new ArrayList<>(batchSize);
                // Only sample IDs committed by preceding batches. Inserts in this batch
                // reserve IDs immediately but become eligible for sampling next batch.
                int tupleLimit = current_num_tuple.get();
                for (int i = 0; i < batchSize && run
                        && System.nanoTime() - start < durationNanos; i++) {
                    pending.add(generateTask(next_task.getAndIncrement(), tupleLimit));
                }
                // Always drain the generated batch, including a final partial batch.
                while (!pending.isEmpty()) {
                    executeWave(selectWave(pending, numThreads));
                }
            }
            completedNormally = true;
        } finally {
            run = false;
            if (completedNormally) {
                executor.shutdown();
            } else {
                executor.shutdownNow();
            }
            try {
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                    throw new IllegalStateException("Benchmark workers did not terminate; results are invalid.");
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                throw e;
            }
        }
        double elapsedMillis = (System.nanoTime() - start) / 1_000_000.0;
        closeWorkerConnections();
        double throughput = tasks_finished.get() / (elapsedMillis / 1000.0);
        System.out.println("[DONE] " + identifier + ", batch size: " + batchSize
                + ", elapsed: " + elapsedMillis / 1000.0 + " s, throughput: " + throughput
                + " Tasks/s, finished: " + tasks_finished.get() + ", aborted: " + aborted_tasks.get()
                + ", deadlocks: " + num_deadlocks.get() + ", restarts: " + tasks_restarted.get());
        return new double[] { elapsedMillis, throughput };
    }

    // Greedy selection preserves each transaction's update order. The oldest
    // pending task is always selected, so deferred tasks cannot starve.
    static List<TransactionTask> selectWave(List<TransactionTask> pending, int numThreads) {
        List<TransactionTask> wave = new ArrayList<>();
        Set<Integer> writtenIds = new HashSet<>();
        Iterator<TransactionTask> iterator = pending.iterator();
        while (iterator.hasNext() && wave.size() < numThreads) {
            TransactionTask task = iterator.next();
            if (task.type() == QueryType.UPDATE) {
                boolean conflicts = false;
                for (int id : task.tupleIds()) {
                    if (writtenIds.contains(id)) {
                        conflicts = true;
                        break;
                    }
                }
                if (conflicts) {
                    continue;
                }
                for (int id : task.tupleIds()) {
                    writtenIds.add(id);
                }
            } else if (task.type() == QueryType.INSERT && !writtenIds.add(task.insertId())) {
                continue;
            }
            // Plain SELECTs at READ COMMITTED can run alongside these writes.
            wave.add(task);
            iterator.remove();
        }
        return wave;
    }

    private void executeWave(List<TransactionTask> wave) throws InterruptedException {
        List<Future<?>> futures = new ArrayList<>(wave.size());
        for (TransactionTask task : wave) {
            futures.add(executor.submit(() -> executeTask(task)));
        }
        RuntimeException failure = null;
        long start = System.nanoTime();
        for (Future<?> future : futures) {
            try {
                long remaining = TimeUnit.MINUTES.toNanos(1) - (System.nanoTime() - start);
                future.get(Math.max(0, remaining), TimeUnit.NANOSECONDS);
            } catch (ExecutionException e) {
                if (failure == null) {
                    failure = new IllegalStateException("A scheduled transaction failed; results are invalid.",
                            e.getCause());
                } else {
                    failure.addSuppressed(e.getCause());
                }
            } catch (TimeoutException e) {
                throw new IllegalStateException("An execution group did not finish within one minute.", e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private TransactionTask generateTask(int taskId, int tupleLimit) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        double probability = random.nextDouble();
        QueryType type = probability < 0.6 ? QueryType.MULTI_POINT_READ
                : probability < 0.7 ? QueryType.INSERT
                : probability < 0.8 ? QueryType.FULL_SCAN : QueryType.UPDATE;
        int[] tupleIds = new int[0];
        double[] values = new double[0];
        int insertId = -1;
        if (type == QueryType.MULTI_POINT_READ || type == QueryType.UPDATE) {
            tupleIds = new int[num_queries_per_task];
            Set<Integer> usedIds = new HashSet<>();
            for (int q = 0; q < tupleIds.length; q++) {
                int id;
                do {
                    id = random.nextInt(tupleLimit);
                } while (type == QueryType.MULTI_POINT_READ && !usedIds.add(id));
                tupleIds[q] = id;
            }
            if (type == QueryType.UPDATE) {
                values = new double[tupleIds.length];
                for (int q = 0; q < values.length; q++) {
                    values[q] = random.nextDouble() * max_value;
                }
            }
        } else if (type == QueryType.INSERT) {
            insertId = current_num_tuple.getAndIncrement();
            values = new double[] { random.nextDouble() * max_value };
        }
        return new TransactionTask(taskId, type, tupleIds, values, insertId);
    }

    protected Connection openConnection() throws SQLException {
        return workerConnection.get(); // one connection per worker thread, reused across its transactions
    }

    private void executeTask(TransactionTask task) {
        try {
            Connection connection = openConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new SQLException("Transaction execution interrupted.", "57014");
                }
                try {
                    String result = executeTransaction(connection, task);
                    connection.commit();
                    int finished = tasks_finished.incrementAndGet();
                    System.out.println("Done task " + task.taskId() + ", " + result
                            + ". I am finisher number " + finished);
                    return;
                } catch (SQLException e) {
                    try {
                        connection.rollback();
                    } catch (SQLException rollbackError) {
                        e.addSuppressed(rollbackError);
                        throw e;
                    }
                    boolean deadlock = false;
                    for (SQLException cause = e; cause != null; cause = cause.getNextException()) {
                        deadlock |= DEADLOCK_CODE.equals(cause.getSQLState());
                    }
                    if (!deadlock) {
                        throw e;
                    }
                    num_deadlocks.incrementAndGet();
                    tasks_restarted.incrementAndGet();
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(MAX_BACKOFF_MS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Deadlock retry interrupted.", "57014", interrupted);
                    }
                }
            }
        } catch (SQLException e) {
            aborted_tasks.incrementAndGet();
            synchronized (errors) {
                errors.append(e.getSQLState()).append(':').append(e.getMessage()).append('\n');
            }
            throw new IllegalStateException("Transaction " + task.taskId() + " failed", e);
        }
    }

    private String executeTransaction(Connection connection, TransactionTask task) throws SQLException {
        switch (task.type()) {
            case MULTI_POINT_READ:
                double sum = 0;
                try (PreparedStatement ps = connection.prepareStatement(
                        "Select data_value from blocking_data where data_id = ?")) {
                    for (int id : task.tupleIds()) {
                        ps.setInt(1, id);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                sum += rs.getDouble("data_value");
                            }
                        }
                    }
                }
                return "Read-only multi-point query. My sum is: " + sum;
            case INSERT:
                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO blocking_data VALUES (?, ?, ?)")) {
                    ps.setInt(1, task.insertId());
                    ps.setDouble(2, task.values()[0]);
                    ps.setInt(3, task.taskId());
                    ps.executeUpdate();
                }
                return "Insert tuple into DB with ID " + task.insertId();
            case FULL_SCAN:
                try (PreparedStatement ps = connection.prepareStatement("Select sum(data_value) from blocking_data");
                        ResultSet rs = ps.executeQuery()) {
                    return "Full table scan and sum. My sum is: " + (rs.next() ? rs.getDouble(1) : 0);
                }
            case UPDATE:
                try (PreparedStatement ps = connection.prepareStatement(
                        "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?")) {
                    for (int q = 0; q < task.tupleIds().length; q++) {
                        ps.setDouble(1, task.values()[q]);
                        ps.setInt(2, task.taskId());
                        ps.setInt(3, task.tupleIds()[q]);
                        ps.executeUpdate();
                    }
                }
                return "Write query with update";
            default:
                throw new IllegalArgumentException("Unknown query type: " + task.type());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        new NoLiveLocks2().benchmark(4, 30_000);
    }
}
