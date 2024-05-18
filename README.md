# LinkedIn csv file reader

This program reads an exported LinkedIn CSV file and saves the data to a Postgres database.

It uses:
- Pandas
- SQL Alchemy
- psycopg2 (Postgres)
- lang detect (to detect if content is in English)

## How to run

Create python env: 

`python -m venv venv`

Activate env:

`./venv/Scripts/activate`

Install requirements:

`pip install -r requirements`

Run it:

`python import-linkedin-files.py`

Optional argument to not detect if content is in English:

`--skip-english-detection` or `-sed`
