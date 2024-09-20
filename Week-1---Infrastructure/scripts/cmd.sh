python3 load_data_to_postgres.py \
  --user=admin \
  --password=admin123 \
  --host=localhost \
  --port=5432 \
  --db=nyc_taxi \
  --table_name=yellow_taxi_trips \
  --csv_file=${URL}