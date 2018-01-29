# Module for taking raw bulk json from stat

## CSV outputs are currently only working

### I think the main issue was that these days wasn't typical and had strange characters that caused things to failed. 

### Meaning, the json has to be read in to escape characters e.g. strict=False'
```python
data2 = json.loads(data.decode('utf-8'), strict=False) 
```

## All dependencies are located in the venv
### To Run Script to take raw json to csv and upload to: /PROD/API-STAT/Bulk/csv/{YYYYMMDD}/data{YYYYMMDD}.csv :
```bash
~$ source venv/bin activate
(venv) ~$ python auto_loop.py
```

### To Run Script to take converted csv files (in DataLake) to parquet and upload to: /PROD/API-STAT/Bulk/parquet/{YYYYMMDD}/data{YYYYMMDD}.parquet :
```bash
~$ source venv/bin activate
(venv) ~$ python read_csv_adl_to_parquet.py
```

## Future possible work:

### Combine 'auto_loop.py' and 'read_csv_adl_to_parquet.py' to run in one job

### Run a job in lambda? either aws or azure