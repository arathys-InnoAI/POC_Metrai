# Retail Data Factory — Phase 1

This tool is a synthetic data generator designed to create high-fidelity, consistent retail datasets for demand planning software. It simulates a realistic "world" of stores, DCs, and suppliers.

## 🚀 Quick Start

### 1. Install Dependencies
Ensure you have Python 3.8+ installed, then run:
```bash
pip install -r requirements.txt
```

### 2. Run the Potato Chips Scenario
This scenario demonstrates stable demand, a promo spike, a supply shortage stockout, and partial recovery.
```bash
python run.py --config config/chips_baseline.yaml
```

### 3. Verify the Output
The tool automatically runs validation checks. You can also view a **Performance Dashboard** for a day-by-day business view:
```bash
python src/view_status.py --run-id chips_v1
```

### 4. Run Automated Tests
```bash
python -m pytest tests/ -v
```

---

## 📂 Project Structure

- `config/`: YAML files defining scenarios, networks, and event schedules.
- `src/`: Core engine logic.
  - `simulation.py`: The 8-step daily loop (receipts -> orders -> shipments -> sales).
  - `demand.py`: Latent demand pre-calculation (multiplicative formula & overrides).
  - `view_status.py`: Professional CLI Performance Dashboard.
  - `validator.py`: Implementation of 5 Phase 1 acceptance criteria.
  - `writers.py`: Generator for the 14 Common Core CSV feeds.
- `output/`: Generated CSV feeds, manifests, and quality reports.
- `tests/`: Pytest suite for automated compliance checks.

---

## 🛠 Features

### Multiplicative Demand (Phase 2 Realism)
The demand model uses a professional multiplicative formula for realistic compound effects:
**`Demand = Base * StorePopularity * Seasonality * Promotion * (1 + Noise)`**
- **StorePopularity**: Multiplier per site (e.g. 1.2 for flagships).
- **Seasonality**: Weekend lifts (e.g. +15% on Sat/Sun).
- **Promotion**: Compound multipliers (e.g. 2.0x promo).
- **Stochastic Noise**: Gaussian "fuzziness" seeded for repeatability.
- **Overrides**: Absolute priority overrides for precise scenario matching.

### Dual Replenishment Policies
- `demand_driven`: Ship today exactly what was sold yesterday. Perfect for exact demo scenarios.
- `coverage_based`: Ship based on `moving_avg * coverage_days`. Used for large-scale realistic simulations.

### Determinism
Seeded runs (`seed: 42`) ensure that the same configuration always produces the identical CSV output, making it perfect for demo repeatability.

---

## 📊 Output Feeds (14 Feeds)

The tool outputs 14 CSV files into `output/<run_id>/feeds/`:
- **Master Data**: Site, Item, Supplier, Calendar, Currency.
- **Transactions**: Supplier Order/Line/Receipt, Customer Order/Line/Delivery, Sales History.
- **Store Logistics**: **StoreReceipts.csv** (DC → Store shipments & arrival dates).
- **State**: Inventory Information (daily snapshots).

---

## ⚙️ Customization

To create a new scenario:
1. Copy `config/chips_baseline.yaml` to a new file.
2. Update the `run_id` and scenario parameters.
3. Define your promo, shortage, or demand override events.
4. Run: `python run.py --config config/your_scenario.yaml`

---

## OUTPUT EXAMPLE:

```bash
======================================================================================================================================================
  RETAIL DATA FACTORY: PERFORMANCE DASHBOARD (chips_v1)
======================================================================================================================================================
COLUMN DEFINITIONS:
  Date         : Simulation day
  Site         : Node (DC or Store)
  Available    : Inventory available BEFORE sales on this day
  OH           : End-of-day On-Hand Inventory (Available - Satisfied)
  Demand       : Total units requested (Latent)
  Satisfied    : Total units fulfilled (Sales)
  Ship Out     : Units leaving this site for stores (DC only)
  Receipt In   : Units arriving (qty | from origin date | by sender)
  Status       : Operational Alerts (STOCKOUT, LOW_SUPPLY, DC_SHORTAGE)
======================================================================================================================================================
Date         | Site            | Available |    OH | Demand | Satisfied | ShipOut | Receipt In (qty | from | by)   | Status
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-01   | DC East         |       470 |   470 |      0 |         0 |      30 |                                |
2024-01-01   | Store 101       |       100 |    70 |     30 |        30 |       0 |                                |
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-02   | DC East         |       440 |   440 |      0 |         0 |      30 |                                |
2024-01-02   | Store 101       |       100 |    70 |     30 |        30 |       0 |  30 | 2024-01-01 | DC_EAST     |
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-03   | DC East         |       410 |   410 |      0 |         0 |      30 |                                |
2024-01-03   | Store 101       |       100 |    40 |     60 |        60 |       0 |  30 | 2024-01-02 | DC_EAST     |
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-04   | DC East         |       760 |   760 |      0 |         0 |      20 | 370 | 2024-01-01 | SUP_001     |
2024-01-04   | Store 101       |        70 |    10 |     60 |        60 |       0 |  30 | 2024-01-03 | DC_EAST     | [LOW_SUPPLY]        
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-05   | DC East         |       740 |   740 |      0 |         0 |      20 |                                |
2024-01-05   | Store 101       |        30 |     0 |     30 |        30 |       0 |  20 | 2024-01-04 | DC_EAST     | [STOCKOUT]
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-06   | DC East         |       690 |   690 |      0 |         0 |      50 |                                |
2024-01-06   | Store 101       |        20 |     0 |     30 |        20 |       0 |  20 | 2024-01-05 | DC_EAST     | [STOCKOUT]
------------------------------------------------------------------------------------------------------------------------------------------------------
2024-01-07   | DC East         |       660 |   660 |      0 |         0 |      30 |                                |
2024-01-07   | Store 101       |        50 |     5 |     45 |        45 |       0 |  50 | 2024-01-06 | DC_EAST     | [LOW_SUPPLY]        
------------------------------------------------------------------------------------------------------------------------------------------------------
```