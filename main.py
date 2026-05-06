import papermill as pm
import logging
import os
import yaml
from datetime import datetime

# ------------------ Load YAML Config ------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

log_file_path = config["log_file"]
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# Truncate log file on each run
with open(log_file_path, "w"):
    pass

# ------------------ Setup Logging ------------------
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(levelname)s - %(message)s"
)

# ------------------ Run Notebook Function ------------------
def run_notebook(name, notebook_info):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = notebook_info["input"]
    output_path = notebook_info["output_template"].format(timestamp=timestamp)

    try:
        logging.info(f"Running notebook: {name}")
        logging.info(f"Input Notebook:  {input_path}")
        logging.info(f"Output Notebook: {output_path}")

        pm.execute_notebook(
            input_path=input_path,
            output_path=output_path,
            parameters={}
        )

        logging.info(f"Successfully executed: {input_path}")

        # Delete the executed output notebook
        if os.path.exists(output_path):
            os.remove(output_path)
            logging.info(f"Deleted output notebook: {output_path}")

    except Exception as e:
        logging.error(f"Error executing {name}: {str(e)}")
        raise

# ------------------ Entry Point ------------------
if __name__ == "__main__":
    for step_name, notebook in config["notebooks"].items():
        run_notebook(step_name, notebook)
