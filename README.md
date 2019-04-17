# target-parquet

A [Singer](https://singer.io) target that writes data to parquet files. This target is based on [`target-csv`] [Targetcsv] and the code was adapted to generate parquet files instead of csv files.

## How to use it

`target-parquet` works with a [Singer Tap] in order to move data ingested by the tap into parquet files. 
Note that the parquet file will be written once all the data is imported from the tap.

### Install 

We will use [`tap-exchangeratesapi`][Exchangeratesapi] to pull currency exchange rate data from a public data set as an example.

First, make sure Python 3 is installed on your system or follow these installation instructions for [Mac] or [Ubuntu].

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

```bash
› cat exchange_rate-{timestamp}.parquet
AUD,BGN,BRL,CAD,CHF,CNY,CZK,DKK,GBP,HKD,HRK,HUF,IDR,ILS,INR,JPY,KRW,MXN,MYR,NOK,NZD,PHP,PLN,RON,RUB,SEK,SGD,THB,TRY,ZAR,EUR,USD,date
1.3023,1.8435,3.0889,1.3109,1.0038,6.869,25.47,7.0076,0.79652,7.7614,7.0011,290.88,13317.0,3.6988,66.608,112.21,1129.4,19.694,4.4405,8.3292,1.3867,50.198,4.0632,4.2577,58.105,8.9724,1.4037,34.882,3.581,12.915,0.9426,1.0,2017-02-24T00:00:00Z
```

### Optional Configuration

If you want to save the file in a specific location and not the working directory, then, you need to create a configuration file, in which you specify the path to the directory you are interested in and pass the `-c` argument to the target. 
Also, you can compress the parquet file by passing the `compression_method` argument in the configuration file. Note that, at the moment (April, 2019), the only compression mode available in `pyarrow` is `gzip`.  Thus, this is the only value that will ensure that the parquet file will be compressed. Anything else (bz2, zip, xz), will generate an exception in the `pyarrow` module.
For an example of the configuration file, see [config.sample.json](config.sample.json).

To run `target-parquet` with the configuration file, use this command:

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-parquet/bin/target-parquet -c config.json
```

---

Copyright &copy; 2017 Stitch

[Singer Tap]: https://singer.io
[Targetcsv]: https://github.com/singer-io/target-csv
[Exchangeratesapi]: https://github.com/singer-io/tap-exchangeratesapi
[Mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[Ubuntu]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04