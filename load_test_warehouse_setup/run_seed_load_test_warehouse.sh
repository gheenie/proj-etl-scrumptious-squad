#!/bin/bash

for file in "./test_warehouse_setup"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
  
done

