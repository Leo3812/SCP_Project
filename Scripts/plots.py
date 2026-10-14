import os
import pandas as pd
import matplotlib.pyplot as plt


def plot_approach_comparison(results, out_dir):
    df = pd.DataFrame(results)

    fig = plt.figure()
    plt.bar(df["approach"].astype(str), df["elapsedTime"], yerr=df["elapsedTimeStd"], capsize=4)
    plt.xlabel("Approach")
    plt.ylabel("Elapsed time (s)")
    plt.title("Elapsed time per approach")
    plt.savefig(os.path.join(out_dir, "approach_comparison.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_strong_scaling(results, out_dir):
    df = pd.DataFrame(results).sort_values(["approach", "numWorkers"])
    min_workers = df["numWorkers"].min()
    worker_ticks = sorted(df["numWorkers"].unique())

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    for approach, g in df.groupby("approach"):
        g = g.copy()

        base_workers = g["numWorkers"].iloc[0]
        base_time = g["elapsedTime"].iloc[0]
        base_std = g["elapsedTimeStd"].iloc[0]

        g["speedup"] = base_time / g["elapsedTime"]
        g["efficiency"] = g["speedup"] / (g["numWorkers"] / base_workers)

        rel_err = ((g["elapsedTimeStd"] / g["elapsedTime"]) ** 2 + (base_std / base_time) ** 2) ** 0.5
        rel_err.iloc[0] = 0   # baseline is 1 by definition
        speedup_err = g["speedup"] * rel_err
        efficiency_err = g["efficiency"] * rel_err

        label = f"Approach {approach}"
        axes[0].errorbar(g["numWorkers"], g["speedup"], yerr=speedup_err, marker="o", capsize=4, label=label)
        axes[1].errorbar(g["numWorkers"], g["efficiency"], yerr=efficiency_err, marker="o", capsize=4, label=label)

    axes[0].plot(worker_ticks, [w / min_workers for w in worker_ticks], linestyle="--", color="k", label="ideal")
    axes[1].axhline(1.0, linestyle="--", color="k", label="ideal")

    axes[0].set_xlabel("Workers")
    axes[0].set_ylabel(f"Speedup (vs {min_workers} workers)")
    axes[1].set_xlabel("Workers")
    axes[1].set_ylabel(f"Efficiency (vs {min_workers} workers)")
    for ax in axes:
        ax.set_xticks(worker_ticks)
        ax.legend()

    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "strong_scaling.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_partition_sensitivity(results, out_dir):
    df = pd.DataFrame(results).sort_values(["approach", "numPartitions"])

    fig = plt.figure()
    for approach, g in df.groupby("approach"):
        plt.errorbar(g["numPartitions"], g["elapsedTime"], yerr=g["elapsedTimeStd"], marker="o", capsize=4, label=f"Approach {approach}")
    plt.xlabel("Number of partitions")
    plt.ylabel("Elapsed time (s)")
    plt.xscale("log", base=2)
    plt.xticks(sorted(df["numPartitions"].unique()), [str(p) for p in sorted(df["numPartitions"].unique())])
    plt.title("Partition sensitivity")
    plt.legend()
    plt.savefig(os.path.join(out_dir, "partition_sensitivity.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)