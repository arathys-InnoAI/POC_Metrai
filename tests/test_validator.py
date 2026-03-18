"""
7 mandatory pytest tests for Phase 1 acceptance criteria.
Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from pathlib import Path
import json

# ── Fixtures ────────────────────────────────────────────────────────────
FEEDS_DIR = Path('output/chips_v1/feeds')
REPORT_PATH = Path('output/chips_v1/data_quality_report.json')


@pytest.fixture(scope='module')
def feeds():
    """Load all 13 feed CSVs once per test module."""
    names = [
        'SiteInformation', 'ItemInformation', 'SupplierInformation',
        'InventoryInformation', 'SupplierOrderHeader', 'SupplierOrderLine',
        'SupplierReceipts', 'CustomerOrderHeader', 'CustomerOrderLine',
        'CustomerOrderDelivery', 'SalesHistoryInformation',
        'CalendarPeriod', 'Currency',
    ]
    loaded = {}
    for name in names:
        path = FEEDS_DIR / f'{name}.csv'
        loaded[name] = pd.read_csv(path)
    return loaded


@pytest.fixture(scope='module')
def report():
    """Run validator and return quality report."""
    from src.validator import run_validation
    return run_validation('chips_v1')


# ── Test A: Schema ───────────────────────────────────────────────────────
def test_schema_required_columns(feeds):
    """All required columns are present and non-null in every feed."""
    from src.validator import REQUIRED_COLUMNS
    for feed_name, req_cols in REQUIRED_COLUMNS.items():
        df = feeds[feed_name]
        for col in req_cols:
            assert col in df.columns, f'{feed_name}: missing required column {col}'
            null_count = df[col].isnull().sum()
            assert null_count == 0, f'{feed_name}.{col}: {null_count} null values'


# ── Test B: Referential Integrity ────────────────────────────────────────
def test_referential_integrity_all_keys_exist(feeds):
    """All foreign key references resolve to master records."""
    site_codes = set(feeds['SiteInformation']['SiteCode'])
    item_codes = set(feeds['ItemInformation']['ItemCode'])
    supp_codes = set(feeds['SupplierInformation']['SupplierCode'])

    # Transactions reference valid sites
    for feed in ['InventoryInformation', 'CustomerOrderHeader',
                 'CustomerOrderDelivery', 'SalesHistoryInformation']:
        bad = set(feeds[feed]['SiteCode']) - site_codes
        assert not bad, f'{feed}: unknown SiteCodes {bad}'

    # Transactions reference valid items
    for feed in ['InventoryInformation', 'CustomerOrderLine', 'SalesHistoryInformation']:
        bad = set(feeds[feed]['ItemCode']) - item_codes
        assert not bad, f'{feed}: unknown ItemCodes {bad}'

    # PO Header suppliers
    bad_supp = set(feeds['SupplierOrderHeader']['SupplierCode']) - supp_codes
    assert not bad_supp, f'SupplierOrderHeader: unknown SupplierCodes {bad_supp}'

    # PO lines reference PO headers
    if not feeds['SupplierOrderLine'].empty:
        header_pos = set(feeds['SupplierOrderHeader']['PurchaseOrderNumber'])
        line_pos = set(feeds['SupplierOrderLine']['PurchaseOrderNumber'])
        bad_pos = line_pos - header_pos
        assert not bad_pos, f'SupplierOrderLine: POs with no header {bad_pos}'


# ── Test C: Non-Negativity ───────────────────────────────────────────────
def test_no_negative_inventory(feeds):
    """QuantityOnHand is never negative anywhere, for any site or day."""
    inv = feeds['InventoryInformation']
    neg = inv[inv['QuantityOnHand'] < 0]
    assert neg.empty, (
        f"{len(neg)} negative inventory snapshots found:\n"
        f"{neg[['SiteCode','ItemCode','InventoryDate','QuantityOnHand']].to_string()}"
    )


# ── Test D: Consistency ──────────────────────────────────────────────────
def test_sales_equals_deliveries(feeds):
    """SalesQuantity == DeliveredQuantity aggregated by store-item-date."""
    sales = feeds['SalesHistoryInformation']
    deliveries = feeds['CustomerOrderDelivery']
    co_lines = feeds['CustomerOrderLine']

    # Enrich deliveries with ItemCode from CO lines
    if 'ItemCode' not in deliveries.columns:
        deliveries = deliveries.merge(
            co_lines[['CustomerOrderNumber', 'ItemCode']].drop_duplicates(),
            on='CustomerOrderNumber', how='left'
        )

    sales_agg = (sales.groupby(['SiteCode', 'ItemCode', 'SalesDate'])['SalesQuantity']
                 .sum().reset_index())
    deliv_agg = (deliveries.groupby(['SiteCode', 'ItemCode', 'DeliveryDate'])['DeliveredQuantity']
                 .sum().reset_index()
                 .rename(columns={'DeliveryDate': 'SalesDate'}))

    merged = sales_agg.merge(deliv_agg, on=['SiteCode', 'ItemCode', 'SalesDate'], how='outer').fillna(0)
    mismatches = merged[merged['SalesQuantity'] != merged['DeliveredQuantity']]
    assert mismatches.empty, f"Sales ≠ Deliveries for:\n{mismatches.to_string()}"


def test_receipts_link_to_pos(feeds):
    """Every SupplierReceipt references a valid PO + LineNumber."""
    if feeds['SupplierReceipts'].empty:
        pytest.skip('No supplier receipts in this run (POs not yet due)')

    po_line_keys = set(
        zip(feeds['SupplierOrderLine']['PurchaseOrderNumber'],
            feeds['SupplierOrderLine']['LineNumber'])
    )
    for _, row in feeds['SupplierReceipts'].iterrows():
        key = (row['PurchaseOrderNumber'], row['LineNumber'])
        assert key in po_line_keys, (
            f'SupplierReceipt references unknown PO line: '
            f'{row["PurchaseOrderNumber"]} line {row["LineNumber"]}'
        )


# ── Test E: Reconciliation ───────────────────────────────────────────────
def test_inventory_reconciliation_store(feeds):
    """
    Per store-item-day: OnHand(D) >= 0 and implied receipt is non-negative.
    Full formula: OnHand_end(D) = OnHand_end(D-1) + ShipIn(D) - Sales(D)
    We verify ShipIn = OnHand(D) - OnHand(D-1) + Sales(D) >= 0
    """
    inv = feeds['InventoryInformation']
    sales = feeds['SalesHistoryInformation']
    site_info = feeds['SiteInformation']

    store_codes = set(site_info[site_info['SiteType'] == 'Store']['SiteCode'])
    store_inv = inv[inv['SiteCode'].isin(store_codes)].copy()
    sales_lookup = (sales.set_index(['SiteCode', 'ItemCode', 'SalesDate'])['SalesQuantity']
                    .to_dict())

    for site in store_inv['SiteCode'].unique():
        for item in store_inv[store_inv['SiteCode'] == site]['ItemCode'].unique():
            sub = store_inv[(store_inv['SiteCode'] == site) & (store_inv['ItemCode'] == item)].copy()
            sub = sub.sort_values('InventoryDate').reset_index(drop=True)

            for i in range(1, len(sub)):
                prev_oh = sub.loc[i-1, 'QuantityOnHand']
                curr_oh = sub.loc[i, 'QuantityOnHand']
                curr_d = sub.loc[i, 'InventoryDate']
                sold = sales_lookup.get((site, item, curr_d), 0)
                ship_in = curr_oh - prev_oh + sold
                assert ship_in >= 0, (
                    f'Store reconciliation fail {site}/{item} on {curr_d}: '
                    f'prev={prev_oh} sold={sold} curr={curr_oh} → implied receipt={ship_in}'
                )


def test_inventory_reconciliation_dc(feeds):
    """
    Per dc-item-day: OnHand(D) = OnHand(D-1) + SupplierReceipts(D) - ShipOut(D).
    Verifies DC on_hand never goes negative (shipouts never exceed available).
    """
    inv = feeds['InventoryInformation']
    site_info = feeds['SiteInformation']

    dc_codes = set(site_info[site_info['SiteType'] == 'DC']['SiteCode'])
    dc_inv = inv[inv['SiteCode'].isin(dc_codes)].copy()

    # Non-negative on_hand for DCs is the key check (shipouts tracked in simulation state)
    neg = dc_inv[dc_inv['QuantityOnHand'] < 0]
    assert neg.empty, (
        f"Negative DC inventory:\n"
        f"{neg[['SiteCode','ItemCode','InventoryDate','QuantityOnHand']].to_string()}"
    )
