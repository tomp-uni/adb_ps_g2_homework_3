#!/usr/bin/env python3
"""Plot throughput and thread scaling from the benchmark's averaged CSV rows."""

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path


def read_results(path):
    results = defaultdict(list)
    required = {"implementation", "threads", "avg_transactions_per_second"}
    with path.open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("No results found; finish a benchmark run first")
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("CSV must contain: " + ", ".join(sorted(required)))
        for line, row in enumerate(reader, start=2):
            try:
                name = row["implementation"].strip()
                threads = int(row["threads"])
                throughput = float(row["avg_transactions_per_second"])
                if not name or threads <= 0 or throughput < 0 or not math.isfinite(throughput):
                    raise ValueError()
            except (ValueError, TypeError, AttributeError) as error:
                raise ValueError(f"Invalid benchmark result on CSV line {line}") from error
            if any(count == threads for count, _ in results[name]):
                raise ValueError(f"Duplicate result for {name} with {threads} workers")
            results[name].append((threads, throughput))
    if not results:
        raise ValueError("No results found; finish a benchmark run first")
    return results


def plot_results(results, output):
    try:
        import matplotlib
    except ImportError as error:
        raise RuntimeError("Install plotting dependencies: pip install -r requirements-plot.txt") from error

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figure, (throughput_plot, scaling_plot) = plt.subplots(
        1, 2, figsize=(11, 4), constrained_layout=True
    )
    has_baseline = False
    worker_counts = sorted({count for rows in results.values() for count, _ in rows})
    for name, rows in sorted(results.items()):
        rows = sorted(rows)
        workers, throughput = zip(*rows)
        line, = throughput_plot.plot(workers, throughput, marker="o", label=name)
        baseline = dict(rows).get(1, 0)
        if baseline > 0:
            has_baseline = True
            scaling_plot.plot(
                workers, [value / baseline for value in throughput],
                marker="o", color=line.get_color(), label=name,
            )

    throughput_plot.set_title("Average throughput")
    throughput_plot.set_ylabel("Committed transactions / second")
    throughput_plot.legend()
    scaling_plot.set_title("Thread scaling")
    scaling_plot.set_ylabel("Speed-up vs. 1 worker")
    if has_baseline:
        scaling_plot.plot(
            worker_counts, worker_counts, "--", color="gray", label="Ideal linear speed-up"
        )
        scaling_plot.legend()
    else:
        scaling_plot.text(
            0.5, 0.5, "A positive 1-worker result is needed for speed-up",
            ha="center", va="center", transform=scaling_plot.transAxes, wrap=True,
        )
    for axes in (throughput_plot, scaling_plot):
        axes.set_xlabel("Workers")
        axes.set_xticks(worker_counts)
        axes.set_ylim(bottom=0)
        axes.grid(True, alpha=0.3)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=180)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "csv", nargs="?", type=Path,
        default=Path(__file__).resolve().parent / "benchmark_results.csv",
        help="Benchmark CSV (default: benchmark_results.csv beside this script)",
    )
    parser.add_argument("-o", "--output", type=Path, help="Output image (default: CSV path with .png suffix)")
    args = parser.parse_args()
    output = args.output or args.csv.with_suffix(".png")
    if output.resolve() == args.csv.resolve():
        parser.error("The output image must have a different path from the input CSV")
    try:
        plot_results(read_results(args.csv), output)
    except (OSError, ValueError, RuntimeError) as error:
        parser.error(str(error))
    print(f"Saved {output}")


if __name__ == "__main__":
    main()
