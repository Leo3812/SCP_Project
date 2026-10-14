import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

PROJECT_ID = "project-0e93de98-3aae-4b4f-a2a"
REGION = "europe-west3"

NUM_WORKERS = 4
CLUSTER_NAME = f"spark-cluster-{NUM_WORKERS}w"
MACHINE_TYPE = "n2-standard-4"
DISK_SIZE = 240

DATA_BUCKET = "earthquake-bucket-2380"

DATASET_FILE_NAME = "dataset-earthquakes-full.csv"
LOCAL_FILE = os.path.join(PROJECT_ROOT, "Datasets", "dataset-earthquakes-full.csv")

JAR_FILE_NAME = "EarthquakeCo-assembly-0.1.0-SNAPSHOT.jar"
LOCAL_JAR_PATH = os.path.join(PROJECT_ROOT, "target", "scala-2.12", JAR_FILE_NAME)

INPUT_URI = f"gs://{DATA_BUCKET}/data/{DATASET_FILE_NAME}"
JAR_URI = f"gs://{DATA_BUCKET}/{JAR_FILE_NAME}"

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "Results")

DEFAULT_NUM_TRIALS = 3

MAIN_CLASS = "Main"
