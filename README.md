# target-parquet

A [Singer](https://singer.io) target that writes data to parquet files. This target is based on [`target-csv`] [Targetcsv] and the code was adapted to generate parquet files instead of csv files.

## How to use it

`target-parquet` works with a [Singer Tap] in order to move data ingested by the tap into parquet files.
Note that the parquet file will be written once all the data is imported from the tap.

### Install

We will use [`tap-exchangeratesapi`][exchangeratesapi] to pull currency exchange rate data from a public data set as an example.

First, make sure Python 3 is installed on your system or follow these installation instructions for [Linux] or [Mac].

It is recommended to install each Tap and Target in a separate Python virtual environment to avoid conflicting dependencies between any Taps and Targets.

```bash
 # Install tap-exchangeratesapi in its own virtualenv
python3 -m venv ~/.virtualenvs/tap-exchangeratesapi
source ~/.virtualenvs/tap-exchangeratesapi/bin/activate
pip3 install tap-exchangeratesapi
deactivate

# Install target-parquet in its own virtualenv
python3 -m venv ~/.virtualenvs/target-parquet
source ~/.virtualenvs/target-parquet/bin/activate
pip3 install target-parquet
deactivate
```

### Run

We can now run `tap-exchangeratesapi` and pipe the output to `target-parquet`.

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-parquet/bin/target-parquet
```

By default, the data will be written into a file called `exchange_rate-{timestamp}.parquet` in your working directory.

### Optional Configuration

If you want to save the file in a specific location and not the working directory, then, you need to create a configuration file, in which you specify the path to the directory you are interested in and pass the `-c` argument to the target.
Also, you can compress the parquet file by passing the `compression_method` argument in the configuration file. Note that, these compression methods have to be supported by `Pyarrow`, and at the moment (October, 2020), the only compression modes available are: snappy (recommended), zstd, brotli and gzip. The library will check these, and default to `None` if something else is provided.
For an example of the configuration file, see [config.sample.json](config.sample.json).
There is also an `streams_in_separate_folder` option to create each stream in a different folder, as these are expected to come in different schema.
To run `target-parquet` with the configuration file, use this command:

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-parquet/bin/target-parquet -c config.json
```

---

Copyright &copy; 2017 Stitch

[singer tap]: https://singer.io
[targetcsv]: https://github.com/singer-io/target-csv
[exchangeratesapi]: https://github.com/singer-io/tap-exchangeratesapi
[mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[linux]: https://docs.python-guide.org/starting/install3/linux/
