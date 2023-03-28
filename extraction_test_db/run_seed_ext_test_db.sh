#!/bin/bash

for file in "./test_db_setup"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
  
done

