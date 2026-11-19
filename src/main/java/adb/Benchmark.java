package adb;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Locale;
import java.util.Arrays;

public class Benchmark {
    private Blocking blocking;
    private RestartBlocking restartBlocking;
    private NoLiveLocks noLiveLocks;
    private File logFile;
    static StringBuilder errors;
    private int[] threadCounts;
    private int numRuns;
    private long durationMillis;

    public Benchmark() {
        blocking = new Blocking();
        restartBlocking = new RestartBlocking();
        noLiveLocks = new NoLiveLocks();

        logFile = new File("benchmark_results.csv");
        threadCounts = new int[] {1, 8, 12, 16};
        // threadCounts = new int[] { 1 }; // NOTE: temporary, revert to proper values
        // later
        numRuns = 3;
        durationMillis = 30_000; // 60 seconds per run

    }

    private void runBenchmarkBlocking(BufferedWriter logWriter) throws Exception {
        for (int numThreads : threadCounts) {
            double[][] results = new double[numRuns][2];
            for (int i = 0; i < numRuns; i++) {
                results[i] = blocking.benchmark(numThreads, durationMillis);
            }

            double[] averageResults = new double[2];
            for (double[] result : results) {
                averageResults[0] += result[0];
                averageResults[1] += result[1];
            }
            averageResults[0] /= numRuns;
            averageResults[1] /= numRuns;

            logWriter.write(String.format(Locale.ROOT, "Blocking,%d,%d,%d,%.4f,%.4f",
                    numThreads, numRuns, durationMillis, averageResults[0], averageResults[1]));
            logWriter.newLine();
            logWriter.flush();
            System.out.println("Average results for " + numThreads + " thread(s): "
                    + Arrays.toString(averageResults));
        }
    }

    private void runBenchmarkRestartBlocking(BufferedWriter logWriter) throws Exception {
        for (int numThreads : threadCounts) {
            double[][] results = new double[numRuns][2];
            for (int i = 0; i < numRuns; i++) {
                results[i] = restartBlocking.benchmark(numThreads, durationMillis);
            }

            double[] averageResults = new double[2];
            for (double[] result : results) {
                averageResults[0] += result[0];
                averageResults[1] += result[1];
            }
            averageResults[0] /= numRuns;
            averageResults[1] /= numRuns;

            logWriter.write(String.format(Locale.ROOT, "RestartBlocking,%d,%d,%d,%.4f,%.4f",
                    numThreads, numRuns, durationMillis, averageResults[0], averageResults[1]));
            logWriter.newLine();
            logWriter.flush();
            System.out.println("Average results for " + numThreads + " thread(s): "
                    + Arrays.toString(averageResults));
        }
    }

    private void runBenchmarkAll() throws Exception {
        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile))) {
            logWriter.write("implementation,threads,runs,duration_ms,avg_elapsed_ms,avg_transactions_per_second");
            logWriter.newLine();
            runBenchmarkBlocking(logWriter);
            runBenchmarkRestartBlocking(logWriter);

        }
    }

    public static void main(String[] args) {
        Benchmark b = new Benchmark();
        try {
            b.runBenchmarkAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
