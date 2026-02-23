from enum import StrEnum
from pydantic import BaseModel, Field


class Model(StrEnum):
    GPT_4_1_MINI = "gpt-4.1-mini"
    GPT_4_1_NANO = "gpt-4.1-nano"
    GPT_5_NANO = "gpt-5-nano"

    # Preferred model for this project. Change the string if your account uses a different name.
    GPT_5_MINI = "gpt-5-mini"


class Intensity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ExperimentMetadata(BaseModel):
    model: Model = Field(description="Model to be used")
    max_turns: int = Field(default=30, description="Maximum number of turns")
    structured_output: dict = Field(description="Structured output")
    reflection: bool = Field(description="Use reflection")
    write_folder: str = Field(description="Folder to write results")
    reasoning: Intensity | None = Field(default=None, description="Reasoning intensity")
    verbosity: Intensity | None = Field(default=None, description="Verbosity intensity")


class StockInput(BaseModel):
    # Keeping Thiago's field names for compatibility with his code structure:
    # - `cnpj` will store CIK10 (or "N/A") in the US port.
    name: str
    cnpj: str
    stock_id: str
   
