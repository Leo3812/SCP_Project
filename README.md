# Progetto di Scalable and Cloud Programming A.A 2025-2026
### Analisi di co-occorrenze di terremoti con Scala e Spark

Dato un dataset di terremoti (longitudine, latitudine, data e ora), il programma trova la coppia di coordinate con il massimo 
numero di coocorrenze e le salva assieme alle date relative in ordine crescente.

## Approcci Implementati

Pre-processing: Le coordinate vengono arrotondate ad un decimale e le date troncate al giorno. Inoltre ogni approccio gestisce la rimozione dei duplicati. 

| # | Nome     | Idea                                                                                                                    |
|---|----------|-------------------------------------------------------------------------------------------------------------------------|
| 1 | Baseline | Raggruppa le coordinate per data, genera tutte le coppie e raggruppa le date per coppia                                             |
| 2 | TwoPass  | Deduplica le coordinate per data con un HashSet, aggrega i conteggi delle coppie e recupera le date della sola coppia vincente con una seconda scansione |
| 3 | LowerBoundPruning | Calcola un limite inferiore al punteggio ottimo, elimina le coordinate che non possono appartenere a una coppia ottima e applica la ricerca esatta al dataset filtrato                                                 |


## Struttura del repository

```
.
├── src/main/scala/        # Main, Types, Approach ...
├── scripts/               # script Python per gli esperimenti su Dataproc
│   ├── config.py          # parametri (bucket, regione, cluster, ...)
│   ├── experiments.py     # automazione: cluster, job, risultati
│   └── plots.py           # creazione grafici
├── Docs/                  # Report e consegna
├── Datasets/              # dataset 
├── Results/               # output e risultati degli esperimenti
├── build.sbt
└── README.md
```

## Requisiti

- JDK 11 o 17 (compatibile con la versione di Spark usata)
- Scala `2.12.18` e sbt
- Spark `3.5.3` (solo per l'esecuzione locale)
- [Google Cloud SDK](https://cloud.google.com/sdk) (`gcloud`), autenticato con `gcloud auth login`
- Python 3 con i pacchetti `pandas` e `matplotlib` (solo per gli script e i grafici)

## Compilazione

```bash
sbt assembly       
```

Il JAR risultante si trova in `target/scala-2.12/`


## Argomenti del programma

```
Main [input] [output] [partizioni] [approccio]
```

| Argomento | Descrizione | Default |
|-----------|-------------|---------|
| `input` | percorso del CSV (locale o `gs://`) | `Datasets/dataset-earthquakes-full.csv` |
| `output` | percorso del file JSON di output (locale o `gs://`) | `Results/output.json` |
| `partizioni` | numero di partizioni per gli shuffle | `defaultParallelism * 2` |
| `approccio` | `1`, `2` o `3` | `1` |

## Output

Il programma scrive due file nello stesso percorso:

- `output.json`: coppia, tempo di esecuzione in secondi e lista di date;
- `output.txt`: stesso risultato nel formato richiesto dalla traccia
  
Esempio di `output.txt`:

```
((38.8, -122.8), (38.8, -122.7))
1990-01-05
1990-01-06
...
```

## Esecuzione in locale

Sul dataset ridotto, per test:

```bash
spark-submit \
  --class Main \
  --master "local[*]" \
  target/scala-2.12/<nome-jar>.jar \
  Datasets/dataset-earthquakes-reduced.csv Results/output.json 16 2
```


## Esecuzione su Google Cloud Dataproc

### 1. Bucket e upload

```bash
gcloud storage buckets create gs://<bucket> --location=<regione>
```
```bash
gcloud storage cp Datasets/dataset-earthquakes-full.csv gs://<bucket>/data
```
```bash
gcloud storage cp target/scala-<versione>/<nome-jar>.jar gs://<bucket>/
```

### 2. Creazione del cluster

Macchine `n2-standard-4`, con dischi da 240 GB. `--num-workers` impostato a 2, 3 o 4.

```bash
gcloud dataproc clusters create <nome-cluster> \
  --region=<regione> \
  --num-workers=2 \
  --master-boot-disk-size=240 \
  --worker-boot-disk-size=240 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4
```

### 3. Invio del job

```bash
gcloud dataproc jobs submit spark \
  --cluster=<nome-cluster> \
  --region=<regione> \
  --class=Main \
  --jars=gs://<bucket>/<nome-jar>.jar \
  -- gs://<bucket>/data/dataset-earthquakes-full.csv \
     gs://<bucket>/output/result.json \
     64 \
     2
```

### 4. Lettura del risultato

```bash
gcloud storage cat gs://<bucket>/output/result.txt
```
```bash
gcloud storage cat gs://<bucket>/output/result.json
```

### 5. Eliminazione del cluster

Da fare per non consumare crediti:

```bash
gcloud dataproc clusters delete <nome-cluster> --region=<regione> --quiet
```

## Esperimenti automatizzati

È possibile svolgere le azioni sopra in modo automatizzato usando lo script `pipeline` in `Scripts/`.
Questo crea il cluster, esegue i job, scarica i risultati ed elimina il cluster (anche in caso di errore).

1. Imposta i parametri in `scripts/config.py`: nome del bucket, regione, nome del cluster, tipo di macchina, dimensione del disco, percorso del JAR locale, classe principale e numero di trial.
2. Compila il JAR (vedi sopra).
3. Lancia un esperimento:

```bash
python3 scripts/experiments.py approach          # confronto tra i tre approcci
```
```bash
python3 scripts/experiments.py partition         # sensibilità al numero di partizioni (4 worker)
```
```bash
python3 scripts/experiments.py strong-scaling    # 2, 3 e 4 worker
```

Un secondo argomento opzionale indica il numero di trial per configurazione (default in `config.py`):

```bash
python3 scripts/experiments.py partition 3
```

Ogni esecuzione crea una cartella in `Results/` con un sottodirectory per configurazione (`a<approccio>_p<partizioni>_w<worker>`) contenente i file `trialN.json` e `trialN.txt`, il `summary.json` con mediana e deviazione standard, e i grafici in PNG.

## Autore

`Leonardo Venturi` — `0001184072`