from dotenv import load_dotenv
from experiments import ExperimentMetadata, Model, Intensity
from experiments.final_report2025.agent import run as run_agent
from experiments.final_report2025.workflow import run as run_workflow
from financial_agents.us_indicator_schema import IndicatorOutput
import time
import os
from pathlib import Path

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
WRITE_FOLDER = str(ROOT / "results" / "final_report2025_us")
os.makedirs(WRITE_FOLDER, exist_ok=True)

RUN_AGENT_TOO = os.getenv("RUN_AGENT_TOO", "0") == "1"


def run_one(model: Model, reflection: bool) -> None:
    experiment = ExperimentMetadata(
        model=model,
        write_folder=WRITE_FOLDER,
        max_turns=15,
        structured_output=IndicatorOutput.model_json_schema(),
        reasoning=Intensity.MEDIUM,   # medium reasoning for indicator computation
        verbosity=Intensity.MEDIUM,
        reflection=reflection,
    )

    print(f"Workflow: {model} reflection={reflection}")
    run_workflow(experiment_metadata=experiment)

    if RUN_AGENT_TOO:
        print(f"Agent: {model} reflection={reflection}")
        run_agent(experiment_metadata=experiment)


if __name__ == "__main__":
    while True:
        is_error = False
        try:
            # Preferred production setting
            run_one(Model.GPT_5_MINI, reflection=False)
            run_one(Model.GPT_5_MINI, reflection=True)

        except Exception as e:
            print(f"Error: {e}. Retrying in 1 minute.")
            time.sleep(60)
            is_error = True

        if not is_error:
            break

# export RUN_AGENT_TOO=0
# python main.py
