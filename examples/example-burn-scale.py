from neptune_scale import Run


def main():
    run_n = 100
    metric_n = 200
    step_n = 10_000

    for run_i in range(run_n):
        print(f"Processing {run_i}")
        with Run(run_id=f"burn-{run_i}") as run:
            for step_i in range(step_n):
                metrics = {
                    f"burn/metrics/{metric_i}": run_i * 1000 + metric_i + step_i * 0.001 
                    for metric_i in range(metric_n)
                }
                run.log_metrics(step=step_i, data=metrics)

if __name__ == '__main__':
    main()

