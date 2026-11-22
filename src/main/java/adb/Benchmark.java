package adb;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.util.Locale;
import java.util.Arrays;

public class Benchmark {
    private final Benchmarkable[] implementations = {
            new Blocking(),
            new RestartBlocking(),
            new NoLiveLocks()
    };
    private File logFile;
    private int[] threadCounts;
    private int numRuns;
    private long durationMillis;

    public Benchmark() {
        logFile = new File("benchmark_results.csv");
        threadCounts = new int[] { 1, 4, 10, 16 };
        numRuns = 3;
        durationMillis = 30_000; // 30 seconds per run
    }

    private void runBenchmark(Benchmarkable implementation, BufferedWriter logWriter) throws Exception {
        for (int numThreads : threadCounts) {
            double[][] results = new double[numRuns][2];
            for (int i = 0; i < numRuns; i++) {
                results[i] = implementation.benchmark(numThreads, durationMillis);
            }

            double[] averageResults = new double[2];
            for (double[] result : results) {
                averageResults[0] += result[0];
                averageResults[1] += result[1];
            }
            averageResults[0] /= numRuns;
            averageResults[1] /= numRuns;

            logWriter.write(String.format(Locale.ROOT, "%s,%d,%d,%d,%.4f,%.4f",
                    implementation.identifier, numThreads, numRuns, durationMillis,
                    averageResults[0], averageResults[1]));
            logWriter.newLine();
            logWriter.flush();
            System.out.println("Average results for " + implementation.identifier + ", " + numThreads + " thread(s): "
                    + Arrays.toString(averageResults));
        }
    }

    private void runBenchmarkAll() throws Exception {
        try (BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile))) {
            logWriter.write("implementation,threads,runs,duration_ms,avg_elapsed_ms,avg_transactions_per_second");
            logWriter.newLine();
            for (Benchmarkable implementation : implementations) {
                runBenchmark(implementation, logWriter);
            }
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
