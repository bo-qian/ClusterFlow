# Readable case labels

For a mesh study:

```text
clusterflow prepare example-simulation --input inputs/grid1.toml --label MODEL_G1_240x160_dt0p01
clusterflow prepare example-simulation --input inputs/grid2.toml --label MODEL_G2_360x240_dt0p01
```

For a time-step study:

```text
clusterflow prepare example-simulation --input inputs/time1.toml --label MODEL_T1_240x160_dt0p1
```

These are fictional inputs under an already reviewed project binding. Preview reports the label, exact scheduler job name, full execution ID and input path. The scheduler name adds a short uniqueness suffix so separate executions of the same case can still be reconciled unambiguously.

Use the label for a human-readable description. Keep the numeric Slurm job ID, the full ClusterFlow execution ID and the application's run ID in separate fields. Follow the application's established output naming: if it defaults to `<job_id>/<job_id>_simdata.csv`, retain that default. Do not add an input `run_id` solely to copy the human label into the output path. Explicit requested IDs and compatible restart paths remain application-owned.

Do not rename existing running jobs or move their output directories to retrofit this convention. Existing records retain their original scheduler identity after updating the tool.
