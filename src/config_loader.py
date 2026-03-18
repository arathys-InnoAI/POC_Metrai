import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

@dataclass
class Config:
    seed: int
    run_id: str
    scenario: str
    start_date: str
    days: int
    currency: str
    
    stores: List[Dict[str, Any]]
    dcs: List[Dict[str, Any]]
    suppliers: List[Dict[str, Any]]
    items: List[Dict[str, Any]]
    
    initial_inventory: Dict[str, Dict[str, int]]
    dc_to_store_lead_time_days: int
    base_demand: Dict[str, int]
    
    replenishment: Dict[str, Any]
    receipt_randomness: Dict[str, Any]
    replenishment_policy: str = 'coverage_based'  # 'demand_driven' | 'coverage_based'

    promo_events: List[Dict[str, Any]] = field(default_factory=list)
    shortage_events: List[Dict[str, Any]] = field(default_factory=list)
    demand_override_events: List[Dict[str, Any]] = field(default_factory=list)
    dc_ship_override_events: List[Dict[str, Any]] = field(default_factory=list)

def load_config(path: str | Path) -> Config:
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    
    return Config(
        seed=data['seed'],
        run_id=data['run_id'],
        scenario=data['scenario'],
        start_date=data['start_date'],
        days=data['days'],
        currency=data['currency'],
        stores=data.get('stores', []),
        dcs=data.get('dcs', []),
        suppliers=data.get('suppliers', []),
        items=data.get('items', []),
        initial_inventory=data.get('initial_inventory', {}),
        dc_to_store_lead_time_days=data.get('dc_to_store_lead_time_days', 0),
        base_demand=data.get('base_demand', {}),
        replenishment=data.get('replenishment', {}),
        receipt_randomness=data.get('receipt_randomness', {}),
        replenishment_policy=data.get('replenishment_policy', 'coverage_based'),
        promo_events=data.get('promo_events', []),
        shortage_events=data.get('shortage_events', []),
        demand_override_events=data.get('demand_override_events', []),
        dc_ship_override_events=data.get('dc_ship_override_events', []),
    )
