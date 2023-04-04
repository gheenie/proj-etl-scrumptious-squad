#!/bin/bash

for file in "./extraction_test_db/test_db_setup"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
  
done

