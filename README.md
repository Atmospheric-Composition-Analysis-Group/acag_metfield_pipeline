## Example usage

Start a container:
```shell
$ docker run -it -v path/to/grid_files:/grid_data ghcr.io/atmospheric-composition-analysis-group/acag_metfield_pipeline:main
```

To process all metfields since 2022-05-01T0000:
```shell
$ luigi --module acag_metfield_pipeline.geos_fp_tasks AllGEOSFPTasks --start 2022-05-01T0000 --local-scheduler --workers 6
```

You can also specify a stop time.
