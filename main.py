from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from cachetools import TTLCache, cached
from datetime import date
from pyhive import hive
from typing import List, Dict

app = FastAPI()

origins = [
    "http://localhost:5173",  # Allow your frontend's origin
]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,  # List of origins that are allowed to make requests
    allow_credentials = True,
    allow_methods = ["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers = ["*"],  # Allow all headers
)

conn = hive.Connection(host='localhost', port=10000, username='siddhant', database='default')
cursor = conn.cursor()

cache = TTLCache(maxsize=1000, ttl=300)

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

@app.get("/company", response_model = List[Dict[str, str]])
async def get_company_data(
    company_name: str,
    start_date: date,
    end_date: date,
    complaints_count: int
):    
    if company_name not in company_database_name:
        return {"error": "Company not found"}
    
    cache_key = f"{company_name}_{start_date}_{end_date}"
    if cache_key in cache:
        return cache[cache_key]
    
    query = f"""
        SELECT 
            c.date_received AS `date`,
            c.company,
            COUNT(c.complaint_id) AS complaint_count,
            f.open_price AS open_price,
            f.high_price AS high_price,
            f.low_price AS low_price,
            f.close_price AS close_price
        FROM 
            complaints c
        JOIN 
            {company_database_name[company_name]} f
        ON 
            f.trade_date = DATE_SUB(c.date_received, 1)
        WHERE
            c.company = '{company_name}'
            AND c.date_received BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 
            c.date_received, c.company, f.open_price, f.high_price, f.low_price, f.close_price
        ORDER BY 
            c.date_received, c.company
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    response = []
    for row in rows:
        formatted_row = {
            "date": str(row[0]),
            "company": str(row[1]),
            "complaint_count": str(row[2]),
            "open_price": str(row[3]),
            "high_price": str(row[4]),
            "low_price": str(row[5]),
            "close_price": str(row[6])
        }

        response.append(formatted_row)

    cache[cache_key] = response

    return response

