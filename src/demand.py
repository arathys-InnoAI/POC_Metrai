import pandas as pd
from typing import Dict, Any

def build_demand_matrix(config) -> Dict[str, Dict[str, Dict[str, int]]]:
    """
    Returns nested dict: requested_qty[store_code][item_code][date_str] = int
    Precomputes the latent demanded quantity incorporating:
    - base demand
    - promo_events (multipliers)
    - demand_override_events (explicit overrides)
    """
    demand = {}
    start = pd.to_datetime(config.start_date)
    sim_dates = [(start + pd.Timedelta(days=i)).strftime('%Y-%m-%d') for i in range(config.days)]

    for store in config.stores:
        s_code = store['code']
        demand[s_code] = {}
        for item in config.items:
            i_code = item['code']
            base = config.base_demand.get(i_code, 0)

            demand[s_code][i_code] = {}
            for d in sim_dates:
                demand[s_code][i_code][d] = base

    # Apply promo events as multipliers
    for promo in config.promo_events:
        item = promo['item']
        store = promo['store']
        mult = promo.get('multiplier', 1.0)
        start_day = promo['start_day']
        end_day = promo['end_day']

        for i in range(start_day - 1, end_day):
            if i < len(sim_dates):
                date_str = sim_dates[i]
                if store in demand and item in demand[store]:
                    demand[store][item][date_str] = int(demand[store][item][date_str] * mult)

    # Apply demand_override_events (explicit quantity overrides — highest priority)
    for ov in config.demand_override_events:
        item = ov['item']
        store = ov['store']
        day = ov['day']    # 1-indexed
        quantity = ov['quantity']
        i = day - 1
        if i < len(sim_dates):
            date_str = sim_dates[i]
            if store in demand and item in demand[store]:
                demand[store][item][date_str] = quantity

    return demand
