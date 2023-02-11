# KPI BigData Repository

Everything should be running with kpi as a root dir.
# Environment Creation
1. conda create -n kpi python==3.10
2. pip install -r requirements.txt
3. docker compose up
4. Go to: http://localhost:5050/browser/ (EMAIL & PASSWORD are from pgadmin service.)
5. Add New Server
    - Name: kpi
    - Host: local_pgdb
    - Username & Pass from db service



# Test running Creation
``` python -m pytest ``` -- For all tests 
``` python -m pytest tests/unit/test_job_1.py ``` -- Single test