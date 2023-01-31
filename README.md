# MIMIC-IV-eventlog-merger

A microservice designed to merge the admissions and transfers eventlogs of the MIMIC-IV dataset.

## Setup

The following changes have to be performed manually to the datasets:

Transfers Eventlog:

-   switch column names of `concept:name` and `eventtype`
-   disch zu discharge umbennen

Admissions Eventlog:

-   rename `disch` to `discharge`

-   append one of the eventlogs onto the other

## Changes

Your eventlog's `subject_id` will be enriched in the following way:

-   admissions contain `eventtype` (if available) or `admission_location` (if available)
-   transfers contain `eventtype` (if available)
-   edreg/edout is deleted as the information is stored in the `ED` event
-   duplicate events will be removed within the specified timeframe

## Input

The script takes three inputs, namely:

-   `input_file`, your merged eventlogs
-   `time_frame`, the time frame in which you want duplicates removed
-   `output_file`, your output file, defaults to `merged_output_file.csv`

Example:

```
node eventlog-merger.js --input_file "./admission_merged.csv" --time_frame 3600000 --output_file "./merged_output_file.csv"
```
