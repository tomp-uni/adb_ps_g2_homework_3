package adb;

import adb.util.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;

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

public class RestartBlocking extends Benchmarkable {
    private static final String DEADLOCK_CODE = "40P01";// PostgreSQL SQLSTATE for "deadlock detected"

    AtomicInteger tasks_restarted;

    public RestartBlocking() {
        super("RestartBlocking");
        tasks_restarted = new AtomicInteger(0);
    }

    @Override
    protected Runnable createTask(int taskId) {
        return new Blocker(taskId);
    }

    @Override
    protected void resetRunState() {
        super.resetRunState();
        tasks_restarted.set(0);
    }

    public static void main(String[] args) throws InterruptedException {
        // Create DB
        RestartBlocking rb = new RestartBlocking();
        rb.createDB();

        // Start Transaction generation and execution
        rb.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);// XXX - Do change the number of threads
                                                                           // used
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < num_starting_tasks; i++) {
                rb.generateAndRunNewTask();
            }

            Thread.sleep(30000);
        } finally {
            rb.run = false;
            rb.executor.shutdown();
        }
        if (!rb.executor.awaitTermination(1, TimeUnit.MINUTES)) {
            rb.executor.shutdownNow();
            throw new IllegalStateException(
                    "Benchmark workers did not terminate within 10 minutes; results are invalid.");
        }
        long stop = System.currentTimeMillis();

        System.out.println("[DONE] Task execution after roughly " + ((stop - start) / 1000.0) + " s. Task throughput: "
                + rb.tasks_finished.get() / ((stop - start) / 1000.0) +
                "Tasks/s. Tasks finished: " + rb.tasks_finished.get() + ", Tasks aborted: " + rb.aborted_tasks.get()
                + ", Deadlocks detected: " + rb.num_deadlocks.get() + ", Restarts: "
                + rb.tasks_restarted.get());

        // Print any existing errors
        if (rb.errors.isEmpty() == false) {
            System.out.println(rb.errors);
        }
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and
     * updating major parts of the database.
     *
     */
    public class Blocker implements Runnable {
        final int task;

        public Blocker(int id) {
            this.task = id;
        }

        @Override
        public void run() {
            double p = RandomGenerator.getDefault().nextDouble();
            int[] tupleIds = new int[0];
            double[] values = new double[0];
            int insertId = -1;

            // Generate the transaction once so every retry repeats the same operations.
            if (p < 0.6 || p >= 0.8) {
                tupleIds = new int[num_queries_per_task];
                HashSet<Integer> usedTuples = new HashSet<>();
                int tupleLimit = current_num_tuple.get();
                for (int q = 0; q < tupleIds.length; q++) {
                    int tupleId;
                    do {
                        tupleId = ThreadLocalRandom.current().nextInt(tupleLimit);
                    } while (p < 0.6 && !usedTuples.add(tupleId));
                    tupleIds[q] = tupleId;
                }
                if (p >= 0.8) {
                    values = new double[num_queries_per_task];
                    for (int q = 0; q < values.length; q++) {
                        values[q] = rand.nextDouble() * max_value;
                    }
                }
            } else if (p < 0.7) {
                insertId = current_num_tuple.getAndIncrement();
                values = new double[] { rand.nextDouble() * max_value };
            }

            try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                boolean committed = false;

                while (!committed) {
                    try {
                        String result = executeTransaction(con, p, tupleIds, values, insertId);
                        con.commit();
                        committed = true;
                        int numFinished = tasks_finished.incrementAndGet();
                        System.out.println("Done task " + task + ", " + result
                                + ". I am finisher number " + numFinished);
                    } catch (SQLException e) {
                        try {
                            con.rollback();
                        } catch (SQLException rollbackError) {
                            e.addSuppressed(rollbackError);
                            throw e;
                        }
                        if (!DEADLOCK_CODE.equals(e.getSQLState())) {
                            throw e;
                        }
                        num_deadlocks.incrementAndGet();
                        tasks_restarted.incrementAndGet();
                    }
                }
            } catch (SQLException e) {
                aborted_tasks.incrementAndGet();
                synchronized (errors) {
                    errors.append(e.getSQLState()).append('\n');
                    errors.append(e.getMessage()).append('\n');
                }
                throw new RuntimeException("Transaction " + task + " failed", e);
            }
            generateAndRunNewTask();
        }

        private String executeTransaction(Connection con, double p, int[] tupleIds,
                double[] values, int insertId) throws SQLException {
            if (p < 0.6) {
                double sum = 0;
                try (PreparedStatement ps = con.prepareStatement(
                        "Select data_value from blocking_data where data_id = ?")) {
                    for (int tupleId : tupleIds) {
                        ps.setInt(1, tupleId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                sum += rs.getDouble("data_value");
                            }
                        }
                    }
                }
                return "Read-only multi-point query. My sum is: " + sum;
            } else if (p < 0.7) {
                try (PreparedStatement ps = con.prepareStatement(
                        "INSERT INTO blocking_data VALUES (?, ?, ?)")) {
                    ps.setInt(1, insertId);
                    ps.setDouble(2, values[0]);
                    ps.setInt(3, task);
                    ps.executeUpdate();
                }
                return "Insert tuple into DB with ID " + insertId;
            } else if (p < 0.8) {
                double sum = 0;
                try (PreparedStatement ps = con.prepareStatement(
                        "Select sum(data_value) from blocking_data");
                        ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        sum = rs.getDouble("sum");
                    }
                }
                return "Full table scan and sum. My sum is: " + sum;
            } else {
                try (PreparedStatement ps = con.prepareStatement(
                        "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;")) {
                    for (int q = 0; q < tupleIds.length; q++) {
                        ps.setDouble(1, values[q]);
                        ps.setInt(2, task);
                        ps.setInt(3, tupleIds[q]);
                        ps.executeUpdate();
                    }
                }
                return "Write query with update";
            }
        }
    }
}
