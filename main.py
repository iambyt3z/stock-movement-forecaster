from datetime import date
from fastapi import FastAPI
from pyhive import hive
from typing import List, Dict

app = FastAPI()

conn = hive.Connection(host='localhost', port=10000, username='siddhant', database='default')
cursor = conn.cursor()

company_database_name = {
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

@app.get("/company", response_model=List[Dict[str, str]])
async def get_company_data(
    company_name: str,
    start_date: date,
    end_date: date,
    complaints_count: int
):
    """
    Get complaint data for a specific company between a given date range.

    - **company_name**: The name of the company to filter the complaints data.
    - **start_date**: The start date to filter complaints based on when they were received.
    - **end_date**: The end date to filter complaints based on when they were received.
    - **complaints_count**: The maximum number of complaints to return.

    Returns a list of complaints filtered by the given parameters.
    """
    if company_name not in company_database_name:
        return {"error": "Company not found"}

    query = f"""
    SELECT date_received, company, complaint_id 
    FROM {company_database_name[company_name]} 
    WHERE company = '{company_name}' 
    AND date_received BETWEEN '{start_date}' AND '{end_date}' 
    LIMIT {complaints_count}
    """

    cursor.execute(query)
    response = cursor.fetchall()
    
    return [{"date_received": row[0], "company": row[1], "complaint_id": row[2]} for row in response]
