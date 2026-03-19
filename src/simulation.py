"""
Phase 1 Simulation Engine — Option B1
Supplier → DC → Stores → Customers

8-Step Daily Loop (Spec-correct):
  Step 1  → Post supplier receipts into DC        (dc_receipt_events[D])
  Step 2  → Post DC→store receipts into stores    (store_receipt_events[D])
  Step 3  → Create customer orders                (demand matrix)
  Step 4  → Compute store replenishment need      (demand-driven or coverage-based)
  Step 5  → Allocate DC → store shipments         (constrained by DC on_hand + shortage caps)
            → schedule store_receipt_events[D + dc_to_store_lead_time_days]
  Step 6  → Fulfill deliveries at stores          (delivered = min(req, on_hand[store]))
  Step 7  → Write sales history                   (sales_qty = delivered_qty)
  Step 8a → Create supplier POs                   (Mondays only, weekly review)
            → schedule dc_receipt_events[D + supplier_lead_time_days]
  Step 8b → Write end-of-day inventory snapshot   (all sites, all items, every day)
"""

import pandas as pd
import math
import random
from collections import defaultdict


class Simulation:
    def __init__(self, config, master_data, demand):
        self.config = config
        self.master_data = master_data
        self.demand = demand  # requested_qty[store][item][date_str]

        # ── In-memory state ──────────────────────────────────────────────
        self.on_hand = defaultdict(lambda: defaultdict(int))  # on_hand[node][item]
        self.on_order_dc = defaultdict(int)                   # on_order_dc[item]

        # Event Queues: date_str → list of events
        self.dc_receipt_events = defaultdict(list)            # supplier → DC
        self.store_receipt_events = defaultdict(list)         # DC → store

        # Sequence counters
        self.po_seq = 1
        self.co_seq = 1

        # ── Output accumulators ──────────────────────────────────────────
        self.supplier_receipt_history = []
        self.co_header_history = []
        self.co_line_history = []
        self.co_delivery_history = []
        self.sales_history = []
        self.po_header_history = []
        self.po_line_history = []
        self.inventory_history = []
        self.store_receipt_history = []

        # Rolling demand history for moving average (coverage_based mode)
        self.requested_history = defaultdict(lambda: defaultdict(list))  # [store][item] → list

        # Apply initial inventory
        for site, inv in config.initial_inventory.items():
            for item, qty in inv.items():
                self.on_hand[site][item] = qty

        # ── Pre-index config events for O(1) lookups ─────────────────────
        # shortage: (dc, item) → {start_day, end_day, max_ship_per_day}
        self._shortages = {}
        for sh in config.shortage_events:
            self._shortages[(sh['dc'], sh['item'])] = sh

        # dc_ship_overrides: (dc, item, day_idx) → ship_qty
        self._ship_overrides = {}
        for ov in getattr(config, 'dc_ship_override_events', []):
            day_idx = ov['day'] - 1  # convert to 0-indexed
            self._ship_overrides[(ov['dc'], ov['item'], day_idx)] = ov['ship_qty']

    # ─────────────────────────────────────────────────────────────────────
    def run(self):
        start = pd.to_datetime(self.config.start_date)
        sim_dates = [(start + pd.Timedelta(days=i)).strftime('%Y-%m-%d')
                     for i in range(self.config.days)]

        for d_idx, d_str in enumerate(sim_dates):
            current_date = pd.to_datetime(d_str)
            lead = self.config.dc_to_store_lead_time_days

            # ── Step 1: Supplier receipts arrive at DC ─────────────────
            receipts_today = self.dc_receipt_events[d_str]
            self.dc_receipt_events[d_str] = [] # Clear current queue before processing

            for event in receipts_today:
                # Randomness parameters from config
                p_late = self.config.receipt_randomness.get('p_late', 0.0)
                p_partial = self.config.receipt_randomness.get('p_partial', 0.0)

                # 1.1 Late check
                if random.random() < p_late:
                    k = random.randint(self.config.receipt_randomness.get('late_days_min', 1),
                                       self.config.receipt_randomness.get('late_days_max', 5))
                    new_date = (current_date + pd.Timedelta(days=k)).strftime('%Y-%m-%d')
                    self.dc_receipt_events[new_date].append(event)
                    continue

                # 1.2 Partial check
                received_qty = event['qty']
                if random.random() < p_partial:
                    f = random.uniform(self.config.receipt_randomness.get('partial_frac_min', 0.5),
                                       self.config.receipt_randomness.get('partial_frac_max', 0.9))
                    received_qty = int(event['qty'] * f)
                    remainder = event['qty'] - received_qty
                    if remainder > 0:
                        gap = random.randint(self.config.receipt_randomness.get('remainder_delay_min', 2),
                                             self.config.receipt_randomness.get('remainder_delay_max', 7))
                        new_date = (current_date + pd.Timedelta(days=gap)).strftime('%Y-%m-%d')
                        remainder_event = event.copy()
                        remainder_event['qty'] = remainder
                        self.dc_receipt_events[new_date].append(remainder_event)

                # Post valid receipt to DC inventory
                if received_qty > 0:
                    item = event['item']
                    dc = event['dc']
                    self.on_hand[dc][item] += received_qty
                    self.on_order_dc[item] -= received_qty

                    self.supplier_receipt_history.append({
                        'PurchaseOrderNumber': event['po'],
                        'LineNumber': event['po_line'],
                        'ReceiptDate': d_str,
                        'SiteCode': dc,
                        'ItemCode': item,
                        'ReceivedQuantity': received_qty,
                        'SupplierCode': event['supplier'],
                    })

            # ── Step 2: DC→Store receipts arrive at stores ─────────────
            for event in self.store_receipt_events[d_str]:
                self.on_hand[event['store']][event['item']] += event['qty']

            # ── Step 3: Create customer orders ─────────────────────────
            for store_info in self.config.stores:
                store = store_info['code']
                header_id = f"CO_{self.config.run_id}_{d_str.replace('-','')}_{store}_{self.co_seq:04d}"
                line_seq = 1
                has_demand = False

                for item_info in self.config.items:
                    item = item_info['code']
                    req = self.demand[store][item].get(d_str, 0)
                    self.requested_history[store][item].append(req)

                    if req > 0:
                        has_demand = True
                        self.co_line_history.append({
                            'CustomerOrderNumber': header_id,
                            'LineNumber': line_seq,
                            'ItemCode': item,
                            'OrderQuantity': req,
                            'SiteCode': store,
                            'UOM': item_info.get('uom', 'EA'),
                        })
                        line_seq += 1

                if has_demand:
                    self.co_header_history.append({
                        'CustomerOrderNumber': header_id,
                        'OrderDate': d_str,
                        'SiteCode': store,
                        'CustomerId': 'SYNTH',
                        'Channel': 'Store',
                    })
                    self.co_seq += 1

            # ── Step 4: Compute store replenishment need ───────────────
            store_needs = defaultdict(lambda: defaultdict(int))
            policy = getattr(self.config, 'replenishment_policy', 'coverage_based')
            window = self.config.replenishment.get('smoothing_window_days', 7)

            for store_info in self.config.stores:
                store = store_info['code']
                for item_info in self.config.items:
                    item = item_info['code']

                    if policy == 'demand_driven':
                        # 1-for-1 replenishment: ship what was sold YESTERDAY
                        if d_idx == 0:
                            need = self.config.base_demand.get(item, 0)
                        else:
                            prev_date = (pd.to_datetime(d_str) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                            need = self.demand[store][item].get(prev_date, 0)
                    else:
                        # Coverage-based: target = moving_avg × coverage_days
                        velocity = item_info.get('velocity', 'fast')
                        cov_days = self.config.replenishment.get(
                            'store_coverage_days', {}).get(velocity, 7)
                        hist = self.requested_history[store][item][-window:]
                        avg_daily = sum(hist) / len(hist) if hist else 0
                        target = avg_daily * cov_days
                        need = max(0, round(target - self.on_hand[store][item]))

                    store_needs[item][store] = need

            # ── Step 5: Allocate DC → store shipments ──────────────────
            arrival_date = (current_date + pd.Timedelta(days=lead)).strftime('%Y-%m-%d')

            for dc_info in self.config.dcs:
                dc = dc_info['code']
                for item_info in self.config.items:
                    item = item_info['code']

                    # Check for explicit ship override (e.g. Day 7 recovery)
                    override_key = (dc, item, d_idx)
                    if override_key in self._ship_overrides:
                        total_ship = min(self._ship_overrides[override_key],
                                        self.on_hand[dc][item])
                        if total_ship > 0:
                            self.on_hand[dc][item] -= total_ship
                            self.store_receipt_events[arrival_date].append({
                                'item': item, 'store': self.config.stores[0]['code'],
                                'qty': total_ship
                            })
                            # Record for status/feeds CLI
                            self.store_receipt_history.append({
                                'ItemCode': item, 'SiteCode': self.config.stores[0]['code'],
                                'ShipDate': d_str, 'ArrivalDate': arrival_date,
                                'Quantity': total_ship
                            })
                        continue

                    # Normal path: apply shortage cap
                    avail = self.on_hand[dc][item]
                    sh = self._shortages.get((dc, item))
                    if sh and sh['start_day'] <= (d_idx + 1) <= sh['end_day']:
                        avail = min(avail, sh['max_ship_per_day'])

                    need_dict = store_needs[item]
                    need_sum = sum(need_dict.values())
                    if need_sum == 0 or avail == 0:
                        continue

                    ships = {}
                    if avail >= need_sum:
                        ships = {s: n for s, n in need_dict.items()}
                    else:
                        # Proportional allocation with remainder distribution
                        sum_ships = 0
                        remainders = []
                        for store, need in need_dict.items():
                            f_ship = avail * need / need_sum
                            i_ship = int(f_ship)
                            ships[store] = i_ship
                            sum_ships += i_ship
                            remainders.append((f_ship - i_ship, store))

                        # Distribute remainder units to stores with highest fractional part
                        leftover = avail - sum_ships
                        remainders.sort(reverse=True, key=lambda x: x[0])
                        for i in range(leftover):
                            store = remainders[i][1]
                            ships[store] += 1

                    for store, ship_qty in ships.items():
                        if ship_qty > 0:
                            self.on_hand[dc][item] -= ship_qty
                            self.store_receipt_events[arrival_date].append({
                                'item': item, 'store': store, 'qty': ship_qty
                            })
                            # Record for status/feeds CLI
                            self.store_receipt_history.append({
                                'ItemCode': item, 'SiteCode': store,
                                'ShipDate': d_str, 'ArrivalDate': arrival_date,
                                'Quantity': ship_qty
                            })

            # ── Step 6 & 7: Fulfill deliveries + Write Sales ──────────
            for store_info in self.config.stores:
                store = store_info['code']
                co_nums = [co['CustomerOrderNumber']
                           for co in self.co_header_history
                           if co['SiteCode'] == store and co['OrderDate'] == d_str]
                co_header = co_nums[0] if co_nums else None

                for item_info in self.config.items:
                    item = item_info['code']
                    req = self.demand[store][item].get(d_str, 0)
                    if req == 0:
                        continue

                    avail = self.on_hand[store][item]
                    deliv = min(req, avail)
                    self.on_hand[store][item] -= deliv

                    self.co_delivery_history.append({
                        'CustomerOrderNumber': co_header,
                        'DeliveryDate': d_str,
                        'SiteCode': store,
                        'ItemCode': item,
                        'DeliveredQuantity': deliv,
                        'DeliveryStatus': 'FULL' if deliv == req else 'PARTIAL',
                    })

                    if deliv > 0:
                        self.sales_history.append({
                            'SiteCode': store,
                            'ItemCode': item,
                            'SalesDate': d_str,
                            'SalesQuantity': deliv,
                            'SalesAmount': round(deliv * item_info.get('unit_price', 0), 2),
                            'CurrencyCode': self.config.currency,
                        })

            # ── Step 8a: Create Supplier POs (Mondays only) ────────────
            if current_date.dayofweek == 0:  # Monday = 0
                dc_cov_days = self.config.replenishment.get('dc_coverage_days', 28)
                for dc_info in self.config.dcs:
                    dc = dc_info['code']
                    po_id = f"PO_{self.config.run_id}_{d_str.replace('-','')}_{dc}_{self.po_seq:04d}"
                    line_seq = 1
                    po_written = False
                    last_eta = d_str

                    for item_info in self.config.items:
                        item = item_info['code']
                        supplier_code = item_info.get('supplier')
                        supp_info = next(
                            (s for s in self.config.suppliers if s['code'] == supplier_code), None)
                        supp_lt = supp_info['lead_time_days'] if supp_info else 3

                        # Aggregate avg daily demand across all stores
                        total_avg = sum(
                            (sum(self.requested_history[s['code']][item][-window:]) /
                             max(len(self.requested_history[s['code']][item][-window:]), 1))
                            for s in self.config.stores
                        )
                        target_dc = total_avg * dc_cov_days
                        inv_pos = self.on_hand[dc][item] + self.on_order_dc[item]
                        order_qty = max(0, round(target_dc - inv_pos))

                        if order_qty > 0:
                            last_eta = (current_date + pd.Timedelta(days=supp_lt)).strftime('%Y-%m-%d')
                            self.po_line_history.append({
                                'PurchaseOrderNumber': po_id,
                                'LineNumber': line_seq,
                                'SiteCode': dc,
                                'ItemCode': item,
                                'OrderQuantity': order_qty,
                                'UOM': item_info.get('uom', 'EA'),
                                'UnitCost': item_info.get('cost_price', 0),
                            })
                            self.dc_receipt_events[last_eta].append({
                                'po': po_id, 'po_line': line_seq,
                                'dc': dc, 'supplier': supplier_code,
                                'item': item, 'qty': order_qty,
                            })
                            self.on_order_dc[item] += order_qty
                            line_seq += 1
                            po_written = True

                    if po_written:
                        self.po_header_history.append({
                            'PurchaseOrderNumber': po_id,
                            'SupplierCode': self.config.items[0].get('supplier'),
                            'OrderDate': d_str,
                            'ExpectedReceiptDate': last_eta,
                            'OrderStatus': 'OPEN',
                        })
                        self.po_seq += 1

            # ── Step 8b: End-of-day inventory snapshot ─────────────────
            all_sites = ([s['code'] for s in self.config.stores] +
                         [d['code'] for d in self.config.dcs])
            for site in all_sites:
                for item_info in self.config.items:
                    item = item_info['code']
                    self.inventory_history.append({
                        'SiteCode': site,
                        'ItemCode': item,
                        'InventoryDate': d_str,
                        'QuantityOnHand': self.on_hand[site][item],
                    })
