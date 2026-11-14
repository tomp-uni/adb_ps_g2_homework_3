package adb;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.Locale;
import java.util.Arrays;

public class Benchmark {
    private Blocking blocking;
    private RestartBlocking restartBlocking;
    private NoLiveLocks noLiveLocks;
    private File logFile;

    public Benchmark() {
        blocking = new Blocking();
        restartBlocking = new RestartBlocking();
        noLiveLocks = new NoLiveLocks();

        logFile = new File("benchmark_results.csv");

    }

    private void run_benchmark_all() throws Exception {
        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile))) {
            logWriter.write("implementation,threads,runs,duration_ms,avg_elapsed_ms,avg_transactions_per_second");
            logWriter.newLine();
            // int[] threadCounts = { 1, 2, 4, 8 };
            int[] threadCounts = { 1 }; // NOTE: temüporary, revert to proper values later
            int numRuns = 5;
            long durationMillis = 60_000; // 60 seconds per run
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
            // TODO: add other classes to benchmark
        }
    }

    public static void main(String[] args) {
        Benchmark b = new Benchmark();
        try {
            b.run_benchmark_all();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
