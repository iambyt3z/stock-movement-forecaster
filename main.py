from datetime import date
from fastapi import FastAPI
from pyhive import hive

app = FastAPI()
conn = hive.Connection(host='localhost', port=10000, username='siddhant', database='default')
cursor = conn.cursor()

conpany_database_name = {
    "AMERICAN EXPRESS COMPANY": "amex_findata",
    "BANK OF AMERICA, NATIONAL ASSOCIATION": "boa_findata",
    "CAPITAL ONE FINANCIAL CORPORATION": "capone_findata",
    "CITIBANK, N.A.": "citibank_findata",
    "DISCOVER BANK": "discover_findata",
    "JPMORGAN CHASE & CO.": "jpmorgan_findata",
    "Navient Solutions, LLC.": "navient_findata",
    "SYNCHRONY FINANCIAL": "synchrony_findata",
    "U.S. BANCORP": "usbancorp_findata",
    "WELLS FARGO & COMPANY": "wellsfargo_findata"
}

@app.get("/company")
async def get_company_data(
    company_name: str,
    start_date: date,
    end_date: date,
    complaints_count: int
):
    query = f"SELECT date_received, company, complaint_id FROM complaints WHERE company = '{company_name}' AND date_received BETWEEN '{start_date}' AND '{end_date}' LIMIT 10"
    
    cursor.execute(query)
    response = cursor.fetchall()
    
    return response
