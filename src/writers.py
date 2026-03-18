import os
import json
import pandas as pd
from pathlib import Path

def write_feeds(config, master_data, simulation):
    out_dir = Path(f"output/{config.run_id}")
    feeds_dir = out_dir / "feeds"
    feeds_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Site Information
    master_data.stores['SiteType'] = 'Store'
    master_data.dcs['SiteType'] = 'DC'
    sites_col = ['code', 'name', 'SiteType']
    sites = pd.concat([master_data.stores[sites_col], master_data.dcs[sites_col]])
    sites = sites.rename(columns={'code': 'SiteCode', 'name': 'SiteName'})
    sites.to_csv(feeds_dir / 'SiteInformation.csv', index=False)
    
    # 2. Item Information
    items = master_data.items[['code', 'name', 'uom', 'supplier', 'velocity']].copy()
    items = items.rename(columns={'code': 'ItemCode', 'name': 'ItemDescription', 'uom': 'UOM'})
    items.to_csv(feeds_dir / 'ItemInformation.csv', index=False)
    
    # 3. Supplier Information
    suppliers = master_data.suppliers[['code', 'name']].copy()
    suppliers = suppliers.rename(columns={'code': 'SupplierCode', 'name': 'SupplierName'})
    suppliers.to_csv(feeds_dir / 'SupplierInformation.csv', index=False)
    
    # 4. InventoryInformation
    pd.DataFrame(simulation.inventory_history).to_csv(feeds_dir / 'InventoryInformation.csv', index=False)
    
    # 5 & 6. Supplier Orders
    pd.DataFrame(simulation.po_header_history).to_csv(feeds_dir / 'SupplierOrderHeader.csv', index=False)
    pd.DataFrame(simulation.po_line_history).to_csv(feeds_dir / 'SupplierOrderLine.csv', index=False)
    
    # 7. Supplier Receipts
    if simulation.supplier_receipt_history:
        pd.DataFrame(simulation.supplier_receipt_history).to_csv(feeds_dir / 'SupplierReceipts.csv', index=False)
    else:
        pd.DataFrame(columns=['PurchaseOrderNumber', 'LineNumber', 'ReceiptDate', 'SiteCode', 'ItemCode', 'ReceivedQuantity']).to_csv(feeds_dir / 'SupplierReceipts.csv', index=False)
        
    # 8, 9 & 10. Customer Orders
    pd.DataFrame(simulation.co_header_history).to_csv(feeds_dir / 'CustomerOrderHeader.csv', index=False)
    pd.DataFrame(simulation.co_line_history).to_csv(feeds_dir / 'CustomerOrderLine.csv', index=False)
    pd.DataFrame(simulation.co_delivery_history).to_csv(feeds_dir / 'CustomerOrderDelivery.csv', index=False)
    
    # 11. Sales History
    pd.DataFrame(simulation.sales_history).to_csv(feeds_dir / 'SalesHistoryInformation.csv', index=False)
    
    # 12 & 13. Calendar & Currency
    master_data.calendar.to_csv(feeds_dir / 'CalendarPeriod.csv', index=False)
    master_data.currency.to_csv(feeds_dir / 'Currency.csv', index=False)
    
    # Write Manifest
    manifest = {
        'run_id': config.run_id,
        'scenario': config.scenario,
        'timestamp': pd.Timestamp.now().isoformat(),
        'records_generated': {
            'SiteInformation': len(sites),
            'ItemInformation': len(items),
            'SalesHistoryInformation': len(simulation.sales_history),
            'InventoryInformation': len(simulation.inventory_history)
        }
    }
    with open(out_dir / 'run_manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

