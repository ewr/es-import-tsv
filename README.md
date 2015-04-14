# es-import-tsv

Simple example of creating a pipeline that reads in a TSV file, does some
basic cleaning, and indexes the results into Elasticsearch.

Installation:

    npm i -g ewr/es-import-tsv

Usage:

    es-import-tsv --index my_data --type expenses < expenses.tsv

See `es-import-tsv --help` for the full option list.

## Cleaning

The specific cleaning here was an example using data from the [calaccess-raw-data project](https://github.com/california-civic-data-coalition/django-calaccess-raw-data).

* Parse timestamps into something ES will understand for any field named `*_DATE`
* Treat fields as numbers if they are named `AMOUNT` or `*_YTD`
* For field prefixes passed in via `--names`, concatenate `*_NAMF` and `*_NAML` to `*_NAME`
