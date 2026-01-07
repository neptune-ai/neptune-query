# `download_files()`

Downloads files from the specified Neptune experiments.

> **Related:**
>
> - [Download files from runs][download-files-runs]

## Parameters

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `files` | `File` \| Iterable of `File` \| `pandas.Series` \| `pandas.DataFrame` | - | **Required.** Which files to download, specified using the file fetching methods:<br><br>- For files logged as a series, use `fetch_series()` to specify the content containing the files and pass the output to the `files` argument.<br>- For individually assigned files, use the output of `fetch_experiments_table()`.<br><br>You can also pass a reference to a single File object or an Iterable of them. |
| `destination` | `str` | `None` | Path to where the files should be downloaded. Can be relative or absolute.<br><br>If `None`, the files are downloaded to the current working directory (CWD). |

> **Note:** The Neptune project isn't specified directly for the function call, because the project is encoded in the `files` argument.
>
> For how to set the project manually, see the [example](#example).

## Returns

`pandas.DataFrame` – A DataFrame mapping experiments and attributes to the paths of downloaded files. The DataFrame has a MultiIndex with:

- **Index:** `["experiment", "step"]`
    - For individually assigned files, the step is `NaN`.
- **Columns:** Single-level index with attribute names.

## Raises

`NeptuneUserError` – If files don't have associated experiment names.

This indicates that the incorrect API was used. Make sure to import and use the methods from the correct module:

- When targeting experiments, use the `neptune_query` module to fetch experiments and download files.
- When targeting runs, use the `neptune_query.runs` module to fetch runs and download files.

## Constructing the destination path

Files are downloaded to the following directory:

```
<destination>/<experiment_name>/<attribute_path>/<file_name>
```

Note that:

- The directory specified with the `destination` parameter requires write permissions.
- If the experiment name or an attribute path includes slashes `/`, each element that follows the slash is treated as a subdirectory.
- The directory and subdirectories are automatically created if they don't already exist.

## Example

Specify files from a given step range of a series:

```py
import neptune_query as nq


interesting_files = nq.fetch_series(
    project="team-alpha/project-x",
    experiments=["seabird-4", "seabird-5"],
    attributes=r"^predictions/",
    step_range=(1.0, 3.0),
)

nq.download_files(files=interesting_files)
```

Sample output:

```sh
attribute                                                           predictions
experiment step
seabird-4  1.0   /home/sigurd/project-x/seabird-4/predictions/step_1_000000.png
           2.0   /home/sigurd/project-x/seabird-4/predictions/step_2_000000.png
           3.0   /home/sigurd/project-x/seabird-4/predictions/step_3_000000.png
seabird-5  1.0   /home/sigurd/project-x/seabird-5/predictions/step_1_000000.png
           2.0   /home/sigurd/project-x/seabird-5/predictions/step_2_000000.png
           3.0   /home/sigurd/project-x/seabird-5/predictions/step_3_000000.png
```

Specify individually assigned files:

```py
interesting_files = nq.fetch_experiments_table(
    project="team-alpha/project-x",
    experiments=["seabird-4", "seabird-5"],
    attributes=r"sample | labels",
)

nq.download_files(files=interesting_files)
```

Sample output:

```sh
attribute                                         data sample                                        labels
experiment step
seabird-4   NaN   /home/sigurd/project-x/seabird-4/sample.csv  /home/sigurd/project-x/seabird-4/labels.json
seabird-5   NaN   /home/sigurd/project-x/seabird-5/sample.csv  /home/sigurd/project-x/seabird-5/labels.json
```

[download-files-runs]: runs/download_files.md
