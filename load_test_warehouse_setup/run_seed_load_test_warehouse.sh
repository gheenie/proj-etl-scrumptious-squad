#!/bin/bash

for file in "./load_test_warehouse_setup/test_warehouse_setup"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
  
done

