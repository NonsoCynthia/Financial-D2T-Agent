import subprocess
import sys


def run_script(script_name):
    """
    Executes a Python script in sequence.
    Stops execution if any script fails.
    """

    print(f"\nRunning {script_name}")
    result = subprocess.run([sys.executable, script_name])

    if result.returncode != 0:
        print(f"Error occurred in {script_name}")
        sys.exit(1)


if __name__ == "__main__":

    scripts = [
        "01_download_prices_eu.py",
        "02_compute_returns_eu.py",
        "03_download_fundamentals_eu.py",
        "04_make_monthly_panel_eu.py"
    ]

    for script in scripts:
        run_script(script)

    print("\nEU pipeline completed successfully.")