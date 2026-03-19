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
def main(config: str = typer.Option("config/chips_baseline.yaml", "--config", help="Path to scenario YAML")):
    print(f"Loading configuration from {config}...")
    config_obj = load_config(config)

    # Set seed for determinism
    random.seed(config_obj.seed)
    np.random.seed(config_obj.seed)
    
    print(f"Source of truth: {config_obj.scenario} (Seed: {config_obj.seed})")
    print("Building master data...")
    master_data = build_master_data(config_obj)
    
    print("Pre-calculating latent demand...")
    demand = build_demand_matrix(config_obj)
    
    print("Running simulation engine...")
    sim = Simulation(config_obj, master_data, demand)
    sim.run()
    
    print(f"Simulation completed. 8-step daily loops executed for {config_obj.days} days.")
    print("Writing output feeds...")
    write_feeds(config_obj, master_data, sim)
    
    print("Running validation checks...")
    report = run_validation(config_obj.run_id)
    status = "PASSED" if report["all_checks_passed"] else "FAILED"
    print(f"Validation {status} — see output/{config_obj.run_id}/data_quality_report.json")

    print(f"Output written to output/{config_obj.run_id}/feeds/")
    print("Data Factory execution successful!")
    
if __name__ == "__main__":
    app()
