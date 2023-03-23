#!/bin/bash

for file in "./test-db-setup"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
  
done

