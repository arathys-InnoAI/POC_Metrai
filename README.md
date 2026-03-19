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
The tool automatically runs validation checks. You can also run the full test suite manually:
```bash
python -m pytest tests/ -v
```

---

## 📂 Project Structure

- `config/`: YAML files defining scenarios, networks, and event schedules.
- `src/`: Core engine logic.
  - `simulation.py`: The 8-step daily loop (receipts -> orders -> shipments -> sales).
  - `demand.py`: Latent demand pre-calculation (promo multipliers & overrides).
  - `validator.py`: Implementation of 5 Phase 1 acceptance criteria.
  - `writers.py`: Generator for the 13 Common Core CSV feeds.
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

## 📊 Output Feeds (13 Feeds)

The tool outputs 13 CSV files into `output/<run_id>/feeds/`:
- **Master Data**: Site, Item, Supplier, Calendar, Currency.
- **Transactions**: Supplier Order/Line/Receipt, Customer Order/Line/Delivery, Sales History.
- **State**: Inventory Information (daily snapshots).

---

## ⚙️ Customization

To create a new scenario:
1. Copy `config/chips_baseline.yaml` to a new file.
2. Update the `run_id` and scenario parameters.
3. Define your promo, shortage, or demand override events.
4. Run: `python run.py --config config/your_scenario.yaml`
