# AirDNA Data Engineering Project

You have been tasked with creating a prototype data ingest pipeline of property prices data.
Two files will be delivered daily `data/property.csv.gz` and `data/property_day.json.gz` (descriptions below).  
Note that these are just samples and the actual delivery will be on the order of millions of records.
Your task will be to prepare this data for analysis.  You are expected to write a spark job that processes
the property and property day (calendar like data) into two parquet outputs.


## Our Objectives

- Can you translate requirements into functional code?
- Do you produce readable, well organized and extensible code?
- Are you familiar with Spark and data processing?
- Can your code be easily tested?
- Is your code performant?
- Can the service scale well?
- Do you handle errors gracefully?

## If You Have Questions

This is meant to simulate a real project that you would be working on at AirDNA. If you have any questions, please do not hesitate to ask them. Furthermore, if you feel something is missing from the requirements, go ahead and add it in and document what you did (code comments are fine).

## Language Choice

A basic project outline is provided for both python and scala.  Depending on your language choice, please select either the `data_engineer_python` or `data_engineer_scala` project to complete.

## How To Run

This project contains a docker file that is built via a Makefile.  Please use the following commands to build and run the project.

```
# Run Tests
make test

# Run Application
make run
```

** Feel free to adjust the project as needed, but these commands should still work.

## Delivery Of Project

Finished code should be zipped up and emailed or shared as a private github repository.  The project should contain a description of how to build and run the spark jobs locally.

## Outputs

### 1) Property

Input File: `property.csv.gz`

Description: A list of properties (ex: AirBNB) and their attributes.

Steps
- Create a new column "valid" and mark properties as valid or not (true/false) based on the following
  -  a property must have a non null value for bedrooms
  -  a property must have a non null value for bathrooms
  -  a property must have a non null value for rating overall
- Create a new column "size" using the following calculations
  - ```
    valid == false are UNKNOWN
    bedrooms >= 4 and bathrooms >= 3 are LARGE
    bedrooms >= 2 or < 4 and bathrooms >= 1 are MEDIUM
    All others are SMALL
    ```
- Write the resulting data set to `parquet` with the following target schema to the local directory `output/property`

Target Schema

| column            | type    |
|-------------------|---------|
| property_id       | string  |
| city_name         | string  |
| state_name        | string  |
| country_name      | string  |
| bedrooms          | integer |
| bathrooms         | float   |
| first_observed_dt | date    |
| listing_type      | string  |
| latitude          | float   |
| longitude         | float   |
| rating_overall    | integer |
| valid             | boolean |
| size              | string  |

### 2) Property Day

Input File: `property_day.json.gz`

Description: Daily level calendar data for a property.

Steps
- Ensure that every record that has a reservation_id has `status == reserved`
  - fail the job gracefully if it does not
- Write the resulting data set to `parquet` with the following target schema to the local directory `output/property_day`

Target Schema

| column         | type    |
|----------------|---------|
| property_id    | string  |
| calendar_dt    | date    |
| status         | string  |
| price          | integer |
| reservation_id | string  |
| booking_dt     | date    |
| start_dt       | date    |
