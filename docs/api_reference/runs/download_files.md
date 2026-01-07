# `download_files()`

Downloads files from the specified Neptune runs.

> **Related:**
>
> - [Download files from experiments][download-files]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `files` | `File` \| Iterable of `File` \| `pandas.Series` \| `pandas.DataFrame` | - | **Required.** Which files to download, specified using the file fetching methods:<br><br>- For files logged as a series, use `fetch_series()` to specify the content containing the files and pass the output to the `files` argument.<br>- For individually assigned files, use the output of `fetch_runs_table()`.<br><br>You can also pass a reference to a single File object or an Iterable of them. |
| `destination` | `str` | `None` | Path to where the files should be downloaded. Can be relative or absolute.<br><br>If `None`, the files are downloaded to the current working directory (CWD). |

> **Note:** The Neptune project isn't specified directly for the function call, because the project is encoded in the `files` argument.
>
> For how to set the project manually, see the [example](#example).

## Returns

`pandas.DataFrame` – A DataFrame mapping runs and attributes to the paths of downloaded files. The DataFrame has a MultiIndex with:

- **Index:** `["run", "step"]`
    - For individually assigned files, the step is `NaN`.
- **Columns:** Single-level index with attribute names.

## Raises

`NeptuneUserError` – If files don't have associated run IDs.

This indicates that the incorrect API was used. Make sure to import and use the methods from the correct module:

- When targeting experiments, use the `neptune_query` module to fetch experiments and download files.
- When targeting runs, use the `neptune_query.runs` module to fetch runs and download files.

## Constructing the destination path

Files are downloaded to the following directory:

```
<destination>/<run_id>/<attribute_path>/<file_name>
```

Note that:

- The directory specified with the `destination` parameter requires write permissions.
- If an attribute path includes slashes `/`, each element that follows the slash is treated as a subdirectory.
- The directory and subdirectories are automatically created if they don't already exist.

## Example

Specify files from a given step range of a series:

```py
import neptune_query.runs as nq_runs


interesting_files = nq_runs.fetch_series(
    project="team-alpha/project-x",
    runs=["kittiwake-i2ufv", "kittiwake-h7jk2"],
    attributes=r"^predictions/",
    step_range=(1.0, 3.0),
)

nq_runs.download_files(files=interesting_files)
```

Sample output:

```sh
attribute                                                                       predictions
run             step
kittiwake-i2ufv  1.0   /home/sigurd/project-x/kittiwake-i2ufv/predictions/step_1_000000.png
                 2.0   /home/sigurd/project-x/kittiwake-i2ufv/predictions/step_2_000000.png
                 3.0   /home/sigurd/project-x/kittiwake-i2ufv/predictions/step_3_000000.png
kittiwake-h7jk2  1.0   /home/sigurd/project-x/kittiwake-h7jk2/predictions/step_1_000000.png
                 2.0   /home/sigurd/project-x/kittiwake-h7jk2/predictions/step_2_000000.png
                 3.0   /home/sigurd/project-x/kittiwake-h7jk2/predictions/step_3_000000.png
```

Specify individually assigned files:

```py
import neptune_query.runs as nq_runs


interesting_files = nq_runs.fetch_runs_table(
    project="team-alpha/project-x",
    runs=["kittiwake-i2ufv", "kittiwake-h7jk2"],
    attributes=r"sample | labels",
)

nq_runs.download_files(files=interesting_files)
```

Sample output:

```sh
attribute                                                     data sample                                              labels
run              step
kittiwake-i2ufv   NaN   /home/sigurd/project-x/kittiwake-i2ufv/sample.csv  /home/sigurd/project-x/kittiwake-i2ufv/labels.json
kittiwake-h7jk2   NaN   /home/sigurd/project-x/kittiwake-h7jk2/sample.csv  /home/sigurd/project-x/kittiwake-h7jk2/labels.json
```

[download-files]: ../download_files.md

