from __future__ import annotations

import os
from datetime import date, timedelta


EU_TICKERS = [
    "KRZ.IR",      # Kerry Group
    "A5G.IR",      # AIB Group
    "BIRG.IR",     # Bank of Ireland
    "ASML.AS",     # ASML
    "SAP.DE",      # SAP
    "MC.PA",       # LVMH
    "NOVO-B.CO",   # Novo Nordisk
    "SIE.DE",      # Siemens
    "OR.PA",       # L'Oreal
    "NESN.SW",     # Nestle
]

# Match US scripts/config.py rolling window behavior.
START_DATE = os.getenv("START_DATE", "2022-01-03")
END_DATE_INCLUSIVE = date.today().isoformat()
_end_date_obj = date.fromisoformat(END_DATE_INCLUSIVE)
END_DATE_EXCLUSIVE = (_end_date_obj + timedelta(days=1)).isoformat()

ROIC_SOURCE_NAME_DEFAULT = os.getenv("ROIC_SOURCE_NAME_DEFAULT", "roic.ai")
