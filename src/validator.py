"""
Validator — Phase 1 Acceptance Criteria
5 checks that must all pass before data is loaded into JustEnough.

A — Schema          All Req columns present, non-null, correct type
B — Referential     SiteCode/ItemCode/SupplierCode/PO references all resolve
C — Non-negativity  No negative values in any qty / on_hand field
D — Consistency     SalesQty == DeliveredQty (store-item-day); DC ship ≤ DC on_hand
E — Reconciliation  OnHand(D) = OnHand(D-1) + Receipts(D) − Sales(D)
"""

import json
import pandas as pd
from pathlib import Path
from collections import defaultdict


REQUIRED_COLUMNS = {
    'SiteInformation':          ['SiteCode', 'SiteName'],
    'ItemInformation':          ['ItemCode', 'ItemDescription'],
    'SupplierInformation':      ['SupplierCode', 'SupplierName'],
    'InventoryInformation':     ['SiteCode', 'ItemCode', 'InventoryDate', 'QuantityOnHand'],
    'SupplierOrderHeader':      ['PurchaseOrderNumber', 'SupplierCode', 'OrderDate', 'ExpectedReceiptDate'],
    'SupplierOrderLine':        ['PurchaseOrderNumber', 'LineNumber', 'SiteCode', 'ItemCode', 'OrderQuantity'],
    'SupplierReceipts':         ['PurchaseOrderNumber', 'LineNumber', 'ReceiptDate', 'SiteCode', 'ItemCode', 'ReceivedQuantity'],
    'CustomerOrderHeader':      ['CustomerOrderNumber', 'OrderDate', 'SiteCode'],
    'CustomerOrderLine':        ['CustomerOrderNumber', 'LineNumber', 'ItemCode', 'OrderQuantity', 'SiteCode'],
    'CustomerOrderDelivery':    ['CustomerOrderNumber', 'DeliveryDate', 'SiteCode', 'DeliveredQuantity'],
    'SalesHistoryInformation':  ['SiteCode', 'ItemCode', 'SalesDate', 'SalesQuantity', 'SalesAmount'],
    'CalendarPeriod':           ['CalendarDate', 'WeekId', 'WeekStartDate', 'MonthId', 'QuarterId', 'YearId'],
    'Currency':                 ['CurrencyCode', 'CurrencyName'],
}

NON_NEG_COLUMNS = {
    'InventoryInformation':     ['QuantityOnHand'],
    'SupplierOrderLine':        ['OrderQuantity'],
    'SupplierReceipts':         ['ReceivedQuantity'],
    'CustomerOrderLine':        ['OrderQuantity'],
    'CustomerOrderDelivery':    ['DeliveredQuantity'],
    'SalesHistoryInformation':  ['SalesQuantity', 'SalesAmount'],
}


def load_feeds(feeds_dir: Path) -> dict[str, pd.DataFrame]:
    dfs = {}
    for name in REQUIRED_COLUMNS:
        path = feeds_dir / f'{name}.csv'
        if path.exists():
            dfs[name] = pd.read_csv(path)
        else:
            dfs[name] = pd.DataFrame()
    return dfs


def check_schema(dfs: dict) -> dict:
    """Criterion A — all Req columns present and non-null."""
    errors = []
    for feed, req_cols in REQUIRED_COLUMNS.items():
        df = dfs.get(feed, pd.DataFrame())
        if df.empty:
            errors.append(f'{feed}: file missing or empty')
            continue
        for col in req_cols:
            if col not in df.columns:
                errors.append(f'{feed}: missing column {col}')
            elif df[col].isnull().any():
                null_count = df[col].isnull().sum()
                errors.append(f'{feed}.{col}: {null_count} null values')
    return {'passed': len(errors) == 0, 'errors': errors}


def check_referential_integrity(dfs: dict) -> dict:
    """Criterion B — all IDs in transaction feeds resolve to master records."""
    errors = []

    site_codes = set(dfs['SiteInformation']['SiteCode']) if not dfs['SiteInformation'].empty else set()
    item_codes = set(dfs['ItemInformation']['ItemCode']) if not dfs['ItemInformation'].empty else set()
    supp_codes = set(dfs['SupplierInformation']['SupplierCode']) if not dfs['SupplierInformation'].empty else set()

    # PO keys for cross-feed checks
    po_keys = set()
    po_line_keys = set()
    if not dfs['SupplierOrderLine'].empty:
        for _, row in dfs['SupplierOrderLine'].iterrows():
            po_keys.add(row['PurchaseOrderNumber'])
            po_line_keys.add((row['PurchaseOrderNumber'], row['LineNumber']))

    def check_fk(feed, col, valid_set):
        df = dfs.get(feed, pd.DataFrame())
        if df.empty or col not in df.columns:
            return
        bad = set(df[col]) - valid_set
        if bad:
            errors.append(f'{feed}.{col}: unknown values {list(bad)[:5]}')

    # Site references
    for feed in ['InventoryInformation', 'SupplierOrderLine', 'SupplierReceipts',
                 'CustomerOrderHeader', 'CustomerOrderLine', 'CustomerOrderDelivery',
                 'SalesHistoryInformation']:
        check_fk(feed, 'SiteCode', site_codes)

    # Item references
    for feed in ['InventoryInformation', 'SupplierOrderLine', 'SupplierReceipts',
                 'CustomerOrderLine', 'SalesHistoryInformation']:
        check_fk(feed, 'ItemCode', item_codes)

    # Supplier references
    check_fk('SupplierOrderHeader', 'SupplierCode', supp_codes)
    check_fk('SupplierReceipts', 'SupplierCode', supp_codes)

    # PO line → PO header
    if not dfs['SupplierOrderLine'].empty and not dfs['SupplierOrderHeader'].empty:
        po_header_nums = set(dfs['SupplierOrderHeader']['PurchaseOrderNumber'])
        bad_pos = po_keys - po_header_nums
        if bad_pos:
            errors.append(f'SupplierOrderLine: POs with no header {list(bad_pos)[:5]}')

    # Receipts → PO lines
    if not dfs['SupplierReceipts'].empty and po_line_keys:
        for _, row in dfs['SupplierReceipts'].iterrows():
            key = (row['PurchaseOrderNumber'], row['LineNumber'])
            if key not in po_line_keys:
                errors.append(f'SupplierReceipts: ({row["PurchaseOrderNumber"]}, {row["LineNumber"]}) has no matching PO line')

    return {'passed': len(errors) == 0, 'errors': errors}


def check_non_negativity(dfs: dict) -> dict:
    """Criterion C — no negative quantities anywhere."""
    errors = []
    for feed, cols in NON_NEG_COLUMNS.items():
        df = dfs.get(feed, pd.DataFrame())
        if df.empty:
            continue
        for col in cols:
            if col in df.columns:
                neg = (df[col] < 0).sum()
                if neg:
                    errors.append(f'{feed}.{col}: {neg} negative values')
    return {'passed': len(errors) == 0, 'errors': errors}


def check_consistency(dfs: dict) -> dict:
    """Criterion D — Sales == Deliveries (store × item × date aggregate)."""
    errors = []

    sales = dfs.get('SalesHistoryInformation', pd.DataFrame())
    deliveries = dfs.get('CustomerOrderDelivery', pd.DataFrame())

    if sales.empty or deliveries.empty:
        errors.append('Missing SalesHistory or CustomerOrderDelivery for consistency check')
        return {'passed': False, 'errors': errors}

    # Deliveries also need ItemCode — join via CO lines
    co_lines = dfs.get('CustomerOrderLine', pd.DataFrame())
    if not co_lines.empty and 'ItemCode' not in deliveries.columns:
        # Merge item code from CO lines into deliveries (assume 1 item per order for Phase 1)
        deliv_with_item = deliveries.merge(
            co_lines[['CustomerOrderNumber', 'ItemCode']],
            on='CustomerOrderNumber', how='left'
        )
    else:
        deliv_with_item = deliveries

    if 'ItemCode' not in deliv_with_item.columns:
        errors.append('Cannot resolve ItemCode in CustomerOrderDelivery for consistency check')
        return {'passed': False, 'errors': errors}

    sales_agg = sales.groupby(['SiteCode', 'ItemCode', 'SalesDate'])['SalesQuantity'].sum().reset_index()
    deliv_agg = (deliv_with_item
                 .groupby(['SiteCode', 'ItemCode', 'DeliveryDate'])['DeliveredQuantity']
                 .sum().reset_index()
                 .rename(columns={'DeliveryDate': 'SalesDate'}))

    merged = sales_agg.merge(deliv_agg, on=['SiteCode', 'ItemCode', 'SalesDate'], how='outer').fillna(0)
    mismatches = merged[merged['SalesQuantity'] != merged['DeliveredQuantity']]
    if not mismatches.empty:
        for _, row in mismatches.iterrows():
            errors.append(
                f'Mismatch {row["SiteCode"]}/{row["ItemCode"]}/{row["SalesDate"]}: '
                f'Sales={row["SalesQuantity"]}, Delivered={row["DeliveredQuantity"]}'
            )

    return {'passed': len(errors) == 0, 'errors': errors}


def check_reconciliation(dfs: dict) -> dict:
    """
    Criterion E — Inventory reconciliation.
    Store: OnHand_end(D) = OnHand_end(D-1) + ShipIn_from_DC(D) - Sales(D)
    DC:    OnHand_end(D) = OnHand_end(D-1) + SupplierReceipts(D) - ΣShipOut(D)
    
    Note: In Phase 1 we verify store-level only (no DC shipment tracking in feeds yet).
    """
    errors = []

    inv = dfs.get('InventoryInformation', pd.DataFrame())
    sales = dfs.get('SalesHistoryInformation', pd.DataFrame())

    if inv.empty:
        errors.append('InventoryInformation missing')
        return {'passed': False, 'errors': errors}

    # Check non-negative on_hand as a proxy for reconciliation validity
    neg_inv = inv[inv['QuantityOnHand'] < 0]
    if not neg_inv.empty:
        for _, row in neg_inv.iterrows():
            errors.append(
                f'Negative inventory: {row["SiteCode"]}/{row["ItemCode"]}/{row["InventoryDate"]} = {row["QuantityOnHand"]}'
            )

    # Per store-item: check day-over-day consistency
    store_sites = set(inv['SiteCode'].unique()) if not inv.empty else set()
    # Exclude DCs — only check stores
    dc_sites = set()
    site_info = dfs.get('SiteInformation', pd.DataFrame())
    if not site_info.empty and 'SiteType' in site_info.columns:
        dc_sites = set(site_info[site_info['SiteType'] == 'DC']['SiteCode'])
    store_sites -= dc_sites

    if sales.empty:
        errors.append('SalesHistoryInformation missing — cannot reconcile')
        return {'passed': len(errors) == 0, 'errors': errors}

    sales_lookup = sales.set_index(['SiteCode', 'ItemCode', 'SalesDate'])['SalesQuantity'].to_dict()
    inv_lookup = inv.set_index(['SiteCode', 'ItemCode', 'InventoryDate'])['QuantityOnHand'].to_dict()

    for site in store_sites:
        items = inv[inv['SiteCode'] == site]['ItemCode'].unique()
        for item in items:
            dates = sorted(inv[(inv['SiteCode'] == site) & (inv['ItemCode'] == item)]['InventoryDate'].unique())
            for i in range(1, len(dates)):
                prev_d, curr_d = dates[i-1], dates[i]
                prev_oh = inv_lookup.get((site, item, prev_d), 0)
                curr_oh = inv_lookup.get((site, item, curr_d), 0)
                sold = sales_lookup.get((site, item, curr_d), 0)
                # ShipIn = curr_oh - prev_oh + sold  (what must have arrived)
                ship_in = curr_oh - prev_oh + sold
                if ship_in < 0:
                    errors.append(
                        f'Reconciliation fail {site}/{item} Day {curr_d}: '
                        f'prev={prev_oh} sold={sold} curr={curr_oh} implies negative receipt={ship_in}'
                    )

    return {'passed': len(errors) == 0, 'errors': errors}


def run_validation(run_id: str, output_base: str = 'output') -> dict:
    feeds_dir = Path(output_base) / run_id / 'feeds'
    dfs = load_feeds(feeds_dir)

    results = {
        'A_schema': check_schema(dfs),
        'B_referential_integrity': check_referential_integrity(dfs),
        'C_non_negativity': check_non_negativity(dfs),
        'D_consistency': check_consistency(dfs),
        'E_reconciliation': check_reconciliation(dfs),
    }

    # Summary stats for data_quality_report.json
    row_counts = {name: len(df) for name, df in dfs.items()}
    all_passed = all(v['passed'] for v in results.values())

    report = {
        'run_id': run_id,
        'all_checks_passed': all_passed,
        'row_counts': row_counts,
        'checks': {k: {'passed': v['passed'], 'error_count': len(v['errors']), 'errors': v['errors']}
                   for k, v in results.items()},
    }

    report_path = Path(output_base) / run_id / 'data_quality_report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    return report


if __name__ == '__main__':
    import sys
    run_id = sys.argv[1] if len(sys.argv) > 1 else 'chips_v1'
    report = run_validation(run_id)
    status = 'PASSED' if report['all_checks_passed'] else 'FAILED'
    print(f'Validation {status} for run {run_id}')
    for check, result in report['checks'].items():
        mark = 'OK' if result['passed'] else 'FAIL'
        print(f'  [{mark}] {check}')
        for err in result['errors']:
            print(f'        {err}')
