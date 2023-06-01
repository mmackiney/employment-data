#!/bin/sh

python fivetran/create_connectors.py
python fivetran/create_destination.py

cd dbt/employee_data
dbt run --profiles-dir .
