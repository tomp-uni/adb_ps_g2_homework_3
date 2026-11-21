package adb;

import java.util.concurrent.*;
import adb.util.ConnectionFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Benchmarkable {
    public final String identifier;
    protected static int num_starting_tasks = 32; // Initial starting tasks
    protected static int num_queries_per_task = 3;
    protected Connection con;
    protected ThreadPoolExecutor executor;
    protected Random rand = new Random(123456);
    protected int num_tuple = 5000;
    protected double max_value = 50.0f;
    protected AtomicInteger tasks_finished;
    protected AtomicInteger current_num_tuple;
    protected AtomicInteger next_task;
    protected AtomicInteger aborted_tasks;
    protected AtomicInteger num_deadlocks;
    protected volatile boolean run; // volatile needed for threads
    protected StringBuilder errors;

    protected Benchmarkable(String id) {
        this.identifier = id;
        tasks_finished = new AtomicInteger(0);
        current_num_tuple = new AtomicInteger(num_tuple);
        next_task = new AtomicInteger(0);
        aborted_tasks = new AtomicInteger(0);
        num_deadlocks = new AtomicInteger(0);
        run = true;
        errors = new StringBuilder();

    }

    protected void executeSQL(String sql, Connection con) {
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing " + sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void createDB() {
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL(
                    "CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",
                    con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println(Arrays.toString(results));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void generateAndRunNewTask() {
        if (run == false) {
            return;
        } else {
            try {
                executor.execute(createTask(next_task.getAndIncrement()));
            } catch (RejectedExecutionException e) {
                // do nothing, only happens on shutdown if a thread still wants to start a new
                // execution
                // and then narrowly passes the run = false check.
            }
        }
    }

    protected abstract Runnable createTask(int taskId);

    protected void resetRunState() {
        run = true;
        tasks_finished.set(0);
        aborted_tasks.set(0);
        num_deadlocks.set(0);
        next_task.set(0);
        current_num_tuple.set(num_tuple);
        errors.setLength(0);
    }

    public double[] benchmark(int numThreads, long durationMillis) throws InterruptedException {
        resetRunState();
        if (durationMillis <= 0) {
            throw new IllegalArgumentException("Benchmark duration must be greater than zero milliseconds.");
        }
        double[] results = new double[2];
        // Create DB
        this.createDB();

        // Start Transaction generation and execution
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);// Do change the number of threads
                                                                                      // used
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < num_starting_tasks; i++) {
                this.generateAndRunNewTask();
            }

            Thread.sleep(durationMillis);
        } finally {
            this.run = false;
            this.executor.shutdown();
        }
        if (!this.executor.awaitTermination(1, TimeUnit.MINUTES)) {
            this.executor.shutdownNow();
            throw new IllegalStateException(
                    "Benchmark workers did not terminate within 1 minutes; results are invalid.");
        }
        long stop = System.currentTimeMillis();

        System.out.println("[DONE] Task execution after roughly " + ((stop - start) / 1000.0) + " s. Task throughput: "
                + this.tasks_finished.get() / ((stop - start) / 1000.0) +
                "Tasks/s. Tasks finished: " + this.tasks_finished.get() + ", Tasks aborted: " + this.aborted_tasks.get()
                + ", Deadlocks detected: " + this.num_deadlocks.get());

        // Print any existing errors
        if (errors.isEmpty() == false) {
            System.out.println(errors);
        }
        results[0] = stop - start;
        results[1] = this.tasks_finished.get() / ((stop - start) / 1000.0);
        return results;
    }
}
