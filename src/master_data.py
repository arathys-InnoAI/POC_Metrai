import pandas as pd
from dataclasses import dataclass
from typing import List, Dict
import datetime

@dataclass
class MasterData:
    stores: pd.DataFrame
    dcs: pd.DataFrame
    suppliers: pd.DataFrame
    items: pd.DataFrame
    calendar: pd.DataFrame
    currency: pd.DataFrame

def build_master_data(config) -> MasterData:
    stores_df = pd.DataFrame(config.stores)
    dcs_df = pd.DataFrame(config.dcs)
    suppliers_df = pd.DataFrame(config.suppliers)
    items_df = pd.DataFrame(config.items)
    
    # Pre-calculate calendar
    start = pd.to_datetime(config.start_date)
    # The spec asks for forward horizon of 26 weeks (~182 days) for POs
    dates = [start + pd.Timedelta(days=i) for i in range(config.days + 182)]
    
    calendar_records = []
    for d in dates:
        week_start = d - pd.Timedelta(days=d.dayofweek)
        calendar_records.append({
            'CalendarDate': d.strftime('%Y-%m-%d'),
            'WeekId': week_start.strftime('%Y%m%d'),  # Simpler week ID format
            'WeekStartDate': week_start.strftime('%Y-%m-%d'),
            'MonthId': d.strftime('%Y%m'),
            'QuarterId': f"{d.year}Q{(d.month-1)//3 + 1}",
            'YearId': str(d.year)
        })
    calendar_df = pd.DataFrame(calendar_records)
    
    currency_df = pd.DataFrame([{
        'CurrencyCode': config.currency,
        'CurrencyName': config.currency
    }])
    
    return MasterData(
        stores=stores_df,
        dcs=dcs_df,
        suppliers=suppliers_df,
        items=items_df,
        calendar=calendar_df,
        currency=currency_df
    )
