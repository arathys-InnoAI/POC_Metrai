import pandas as pd
import random
from typing import Dict, Any

def build_demand_matrix(config) -> Dict[str, Dict[str, Dict[str, int]]]:
    """
    Returns nested dict: requested_qty[store_code][item_code][date_str] = int
    Formula: Demand = Base * Popularity * Seasonality * Promotion * (1 + Noise)
    """
    random.seed(config.seed)
    demand = {}
    start = pd.to_datetime(config.start_date)
    sim_dates = [(start + pd.Timedelta(days=i)) for i in range(config.days)]
    date_strs = [d.strftime('%Y-%m-%d') for d in sim_dates]

    for store in config.stores:
        s_code = store['code']
        popularity = store.get('popularity', 1.0)
        demand[s_code] = {}

        for item in config.items:
            i_code = item['code']
            base = config.base_demand.get(i_code, 0)
            demand[s_code][i_code] = {}

            for idx, d in enumerate(sim_dates):
                date_str = date_strs[idx]
                
                # 1. Base * Popularity
                val = base * popularity
                
                # 2. Seasonality (Weekend lift)
                if d.dayofweek >= 5: # Saturday=5, Sunday=6
                    val *= config.seasonality.get('weekend', 1.0)
                
                # 3. Noise (Gaussian)
                if config.demand_noise > 0:
                    # Noise is a percentage of the current value
                    val = random.gauss(val, val * config.demand_noise)
                
                demand[s_code][i_code][date_str] = max(0, int(round(val)))

    # 4. Apply promo events as multipliers (Compound)
    for promo in config.promo_events:
        item = promo['item']
        store = promo['store']
        mult = promo.get('multiplier', 1.0)
        start_day = promo['start_day']
        end_day = promo['end_day']

        for i in range(start_day - 1, end_day):
            if i < len(date_strs):
                d_str = date_strs[i]
                if store in demand and item in demand[store]:
                    demand[store][item][d_str] = int(round(demand[store][item][d_str] * mult))

    # 5. Apply demand_override_events (Explicit overrides — highest priority)
    for ov in config.demand_override_events:
        item = ov['item']
        store = ov['store']
        day = ov['day']
        quantity = ov['quantity']
        i = day - 1
        if i < len(date_strs):
            d_str = date_strs[i]
            if store in demand and item in demand[store]:
                demand[store][item][d_str] = quantity

    return demand
