#!/usr/bin/env python3
import datetime
import json
import statistics
import subprocess
import sys

from config import *
from plots import *


class Config:
    def __init__(self, approach, num_partitions, num_workers=NUM_WORKERS):
        self.approach = approach
        self.num_partitions = num_partitions
        self.num_workers = num_workers
        self.label = f"a{approach}_p{num_partitions}_w{num_workers}"

# ---------------------------------------------------------------- gcloud helpers

def run(args):
    print("Running:", " ".join(args))
    r = subprocess.run(args, text=True, capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(f"Command failed ({r.returncode}): {' '.join(args)}\n{r.stderr}")
    return r.stdout


def cluster_exists() -> bool:
    r = subprocess.run(
        ["gcloud", "dataproc", "clusters", "describe", CLUSTER_NAME, f"--region={REGION}"],
        capture_output=True,
    )
    return r.returncode == 0


def delete_cluster():
    if cluster_exists():
        run(["gcloud", "dataproc", "clusters", "delete", CLUSTER_NAME,
             f"--region={REGION}", "--quiet"])


def create_cluster(num_workers: int):
    delete_cluster()  # leftover from an interrupted run
    run(["gcloud", "dataproc", "clusters", "create", CLUSTER_NAME,
         f"--region={REGION}",
         f"--master-machine-type={MACHINE_TYPE}",
         f"--worker-machine-type={MACHINE_TYPE}",
         f"--master-boot-disk-size={DISK_SIZE}",
         f"--worker-boot-disk-size={DISK_SIZE}",
         f"--num-workers={num_workers}"])

def safe_delete_cluster():
    try:
        delete_cluster()
    except Exception as e:
        print(f"WARNING: cluster cleanup failed, it may still be running: {e}")

def upload():
    run(["gcloud", "storage", "cp", LOCAL_FILE, INPUT_URI])
    run(["gcloud", "storage", "cp", LOCAL_JAR_PATH, JAR_URI])


def submit_job(cfg: Config, output_uri: str) -> dict:
    run(["gcloud", "dataproc", "jobs", "submit", "spark",
         f"--region={REGION}", f"--cluster={CLUSTER_NAME}",
         f"--class={MAIN_CLASS}", f"--jars={JAR_URI}",
         "--", INPUT_URI, output_uri, str(cfg.num_partitions), str(cfg.approach)])
    return json.loads(run(["gcloud", "storage", "cat", output_uri]))


# ---------------------------------------------------------------- experiments

def run_config(cfg: Config, run_id: str, num_trials: int, save_dir: str) -> dict:
    os.makedirs(save_dir, exist_ok=True)
    trials = []
    for t in range(1, num_trials + 1):
        out = f"gs://{DATA_BUCKET}/output/{run_id}/{cfg.label}_trial{t}.json"
        data = submit_job(cfg, out)
        trials.append(data)
        print(f"    {cfg.label} trial {t}/{num_trials}: {data['elapsedTime']:.2f}s")
        with open(os.path.join(save_dir, f"trial{t}.json"), "w") as f:
            json.dump(data, f, indent=2)

        try:
            txt = run(["gcloud", "storage", "cat", out.replace(".json", ".txt")])
            with open(os.path.join(save_dir, f"trial{t}.txt"), "w") as f:
                f.write(txt)
        except Exception as e:
            print(f"    WARNING: could not fetch txt for trial {t}: {e}")

    times = [d["elapsedTime"] for d in trials]
    result = {
        **trials[-1],
        "approach": cfg.approach,
        "numPartitions": cfg.num_partitions,
        "numWorkers": cfg.num_workers,
        "elapsedTime": statistics.median(times),
        "elapsedTimeStd": statistics.stdev(times) if len(times) > 1 else 0.0,
        "elapsedTimeTrials": times
    }
    with open(os.path.join(save_dir, "summary.json"), "w") as f:
        json.dump(result, f, indent=2)
    return result


def experiment(name: str, configs: list, num_trials: int):
    run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(OUTPUT_DIR, f"{name}_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    upload()
    results = []
    for workers in sorted({c.num_workers for c in configs}):
        group = [c for c in configs if c.num_workers == workers]
        try:
            create_cluster(workers)
            for cfg in group:
                r = run_config(cfg, run_id, num_trials, os.path.join(run_dir, cfg.label))
                results.append(r)
                print(f"{cfg.label}: median {r['elapsedTime']:.2f}s "
                      f"(std {r['elapsedTimeStd']:.2f}s, n={num_trials})")
        finally:
            safe_delete_cluster()

    with open(os.path.join(run_dir, "summary.json"), "w") as f:
        json.dump(results, f, indent=2)
    return results, run_dir


EXPERIMENTS = {
    "approach":       ([Config(a, 64) for a in (1, 2, 3)], plot_approach_comparison),
    "partition":      ([Config(a, p, 4)  for a in (1, 2, 3) for p in (16, 32, 64, 128, 256)], plot_partition_sensitivity),
    "strong-scaling": ([Config(3, 64, w) for w in (2, 3, 4)], plot_strong_scaling)
}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "approach"
    num_trials = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_NUM_TRIALS

    if mode not in EXPERIMENTS:
        sys.exit(f"Unknown mode '{mode}'. Use one of: {', '.join(EXPERIMENTS)}")

    configs, plot_fn = EXPERIMENTS[mode]
    results, run_dir = experiment(mode, configs, num_trials)

    if mode != "strong-scaling":
        for a in sorted({r["approach"] for r in results}):
            best = min((r for r in results if r["approach"] == a),
                       key=lambda r: r["elapsedTime"])
            print(f"Best for approach {a}: partitions={best['numPartitions']} "
                  f"({best['elapsedTime']:.2f}s)")
    plot_fn(results, run_dir)


if __name__ == "__main__":
    main()