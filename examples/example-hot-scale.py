from neptune_scale import Run


def main():
    for i in range(200):
        print(f"Processing {i}")
        with Run(run_id=f"hot-{i}") as run:
            configs_common = {
                f"hot/common/{j}": i + j * 0.001 for j in range(1000)
            }
            configs_unique = {
                f"hot/unique/{i}/{j}": i + j * 0.001 for j in range(1000)
            }
            run.log_configs(configs_common)
            run.log_configs(configs_unique)

            for step in range(100):
                metrics_common = {
                    f"hot/metrics/common/{j}": i * 1000 + j + step * 0.001 for j in range(100)
                }
                metrics_unique = {
                    f"hot/metrics/unique/{i}/{j}": i * 1000 + j + step * 0.001 for j in range(100)
                }
                run.log_metrics(step=step, data=metrics_common)
                run.log_metrics(step=step, data=metrics_unique)


if __name__ == '__main__':
    main()

