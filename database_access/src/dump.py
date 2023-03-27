new_rows = []
for row in [dict(zip(keys, row)) for row in rows]:                                                  
    if row['created_at'] > most_recent_readings['created_at'] or row['last_updated'] > most_recent_readings['last_updated']:
        new_rows.append(row)    