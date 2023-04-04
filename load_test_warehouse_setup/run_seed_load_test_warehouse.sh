#!/bin/bash

for file in "./load_test_warehouse_setup/test_warehouse_setup"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt -p 5432 -h localhost
  
done

