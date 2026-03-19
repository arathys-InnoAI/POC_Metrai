import pandas as pd
import typer
from pathlib import Path

app = typer.Typer()

@app.command()
def main(run_id: str = "chips_v1"):
    feeds_dir = Path(f"output/{run_id}/feeds")
    if not feeds_dir.exists():
        print(f"Error: Run directory {feeds_dir} not found.")
        return

    # Load required feeds
    try:
        inv = pd.read_csv(feeds_dir / "InventoryInformation.csv")
        sales_hist = pd.read_csv(feeds_dir / "SalesHistoryInformation.csv")
        store_receipts = pd.read_csv(feeds_dir / "StoreReceipts.csv")
        supp_receipts = pd.read_csv(feeds_dir / "SupplierReceipts.csv")
        supp_orders = pd.read_csv(feeds_dir / "SupplierOrderHeader.csv")
        co_headers = pd.read_csv(feeds_dir / "CustomerOrderHeader.csv")
        co_lines = pd.read_csv(feeds_dir / "CustomerOrderLine.csv")
        sites = pd.read_csv(feeds_dir / "SiteInformation.csv")
    except FileNotFoundError as e:
        print(f"Error: Missing feed file. {e}")
        return

    # Metadata mapping
    site_meta = sites.set_index("SiteCode")[["SiteName", "SiteType"]].to_dict(orient="index")
    dc_codes = sites[sites["SiteType"] == "DC"]["SiteCode"].tolist()
    store_codes = sites[sites["SiteType"] == "Store"]["SiteCode"].tolist()
    
    # Map PO to OrderDate and SupplierCode
    po_info = supp_orders.set_index("PurchaseOrderNumber")[["OrderDate", "SupplierCode"]].to_dict(orient="index")
    
    # Pre-aggregate demand by site and date
    demand_df = co_headers.merge(co_lines, on="CustomerOrderNumber")
    demand_agg = demand_df.groupby(["SiteCode_x", "OrderDate"])["OrderQuantity"].sum().reset_index()
    demand_agg = demand_agg.rename(columns={"SiteCode_x": "SiteCode", "OrderDate": "Date"})
    demand_lookup = demand_agg.set_index(["SiteCode", "Date"])["OrderQuantity"].to_dict()

    print(f"\n{'='*150}")
    print(f"  RETAIL DATA FACTORY: PERFORMANCE DASHBOARD ({run_id})")
    print(f"{'='*150}")
    
    print("COLUMN DEFINITIONS:")
    print("  Date         : Simulation day")
    print("  Site         : Node (DC or Store)")
    print("  Available    : Inventory available BEFORE sales on this day")
    print("  OH           : End-of-day On-Hand Inventory (Available - Satisfied)")
    print("  Demand       : Total units requested (Latent)")
    print("  Satisfied    : Total units fulfilled (Sales)")
    print("  Ship Out     : Units leaving this site for stores (DC only)")
    print("  Receipt In   : Units arriving (qty | from origin date | by sender)")
    print("  Status       : Operational Alerts (STOCKOUT, LOW_SUPPLY, DC_SHORTAGE)")
    print(f"{'='*150}")

    header = f"{'Date':<12} | {'Site':<15} | {'Available':>9} | {'OH':>5} | {'Demand':>6} | {'Satisfied':>9} | {'ShipOut':>7} | {'Receipt In (qty | from | by)':<30} | {'Status'}"
    print(header)
    print(f"{'-'*150}")

    for date in sorted(inv["InventoryDate"].unique()):
        for site in dc_codes + store_codes:
            # 1. Inventory OH
            oh_row = inv[(inv["InventoryDate"] == date) & (inv["SiteCode"] == site)]
            oh = int(oh_row["QuantityOnHand"].values[0]) if not oh_row.empty else 0
            
            # 2. Demand & Satisfied (Sales)
            demand = int(demand_lookup.get((site, date), 0))
            s_row = sales_hist[(sales_hist["SalesDate"] == date) & (sales_hist["SiteCode"] == site)]
            satisfied = int(s_row["SalesQuantity"].sum()) if not s_row.empty else 0
            
            # 3. Available (Inventory BEFORE sales)
            available = oh + satisfied
            
            # 4. Ship Out (from DC to Stores)
            ship_qty = int(store_receipts[store_receipts["ShipDate"] == date]["Quantity"].sum()) if site in dc_codes else 0
            
            # 5. Receipt In
            receipt_str = ""
            if site in dc_codes:
                recs = supp_receipts[supp_receipts["ReceiptDate"] == date]
                if not recs.empty:
                    qty = int(recs["ReceivedQuantity"].sum())
                    po_num = recs["PurchaseOrderNumber"].iloc[0]
                    info = po_info.get(po_num, {"OrderDate": "unk", "SupplierCode": "SUP"})
                    receipt_str = f"{qty:>3} | {info['OrderDate']} | {info['SupplierCode']}"
            else:
                recs = store_receipts[store_receipts["ArrivalDate"] == date]
                if not recs.empty:
                    qty = int(recs["Quantity"].sum())
                    receipt_str = f"{qty:>3} | {recs['ShipDate'].iloc[0]} | {dc_codes[0] if dc_codes else 'DC'}"
            
            # 6. Status
            status = ""
            if site in store_codes:
                if oh == 0:
                    status = "[STOCKOUT]"
                elif oh < 20:
                    status = "[LOW_SUPPLY]"
            elif site in dc_codes:
                if oh < 50:
                    status = "[DC_SHORTAGE]"

            name = site_meta.get(site, {"SiteName": site})["SiteName"][:15]
            row = f"{date:<12} | {name:<15} | {available:>9} | {oh:>5} | {demand:>6} | {satisfied:>9} | {ship_qty:>7} | {receipt_str:<30} | {status}"
            print(row)
        print(f"{'-'*150}")

if __name__ == "__main__":
    app()
