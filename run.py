import typer
import random
import numpy as np
from src.config_loader import load_config
from src.master_data import build_master_data
from src.demand import build_demand_matrix
from src.simulation import Simulation
from src.writers import write_feeds
from src.validator import run_validation

app = typer.Typer()

@app.command()
def main(config_path: str = "config/chips_baseline.yaml"):
    print(f"Loading configuration from {config_path}...")
    config = load_config(config_path)

    # Set seed for determinism
    random.seed(config.seed)
    np.random.seed(config.seed)
    
    print(f"Source of truth: {config.scenario} (Seed: {config.seed})")
    print("Building master data...")
    master_data = build_master_data(config)
    
    print("Pre-calculating latent demand...")
    demand = build_demand_matrix(config)
    
    print("Running simulation engine...")
    sim = Simulation(config, master_data, demand)
    sim.run()
    
    print(f"Simulation completed. 8-step daily loops executed for {config.days} days.")
    print("Writing output feeds...")
    write_feeds(config, master_data, sim)
    
    print("Running validation checks...")
    report = run_validation(config.run_id)
    status = "PASSED" if report["all_checks_passed"] else "FAILED"
    print(f"Validation {status} — see output/{config.run_id}/data_quality_report.json")

    print(f"Output written to output/{config.run_id}/feeds/")
    print("Data Factory execution successful!")
    
if __name__ == "__main__":
    app()
