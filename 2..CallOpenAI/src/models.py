"""
Pydantic models for type safety and validation.

This module defines all data models used throughout the application,
ensuring type safety and automatic validation.
"""

from typing import Optional, List, Union, Dict, Any

from pydantic import BaseModel, Field, field_validator


class DiamondGradingReport(BaseModel):
    """Model representing a diamond grading report with all extracted fields."""
    
    report_number: Optional[str] = Field(None, description="Certificate/report number")
    key_to_symbols: Optional[List[str]] = Field(None, description="Clarity characteristics from symbols key")
    carat: Optional[str] = Field(None, description="Carat weight")
    clarity: Optional[str] = Field(None, description="Clarity grade (FL, IF, VVS1, VVS2, VS1, VS2, SI1, SI2, I1, I2, I3)")
    color_grade: Optional[str] = Field(None, description="Color grade letter(s)")
    crown_angle: Optional[str] = Field(None, description="Crown angle in degrees")
    crown_height: Optional[str] = Field(None, description="Crown height percentage")
    culet: Optional[str] = Field(None, description="Culet description")
    cut: Optional[str] = Field(None, description="Cut grade")
    depth: Optional[str] = Field(None, description="Total depth percentage")
    fluorescence: Optional[str] = Field(None, description="Fluorescence description")
    girdle: Optional[str] = Field(None, description="Girdle description")
    lower_half_length: Optional[str] = Field(None, description="Lower girdle facet length percentage")
    measurement: Optional[str] = Field(None, description="Dimensions in mm")
    name: Optional[str] = Field(None, description="Grading laboratory name")
    pavilion_angle: Optional[str] = Field(None, description="Pavilion angle in degrees")
    pavilion_height: Optional[str] = Field(None, description="Pavilion height percentage")
    polish: Optional[str] = Field(None, description="Polish grade")
    shape: Optional[str] = Field(None, description="Diamond shape")
    star_length: Optional[str] = Field(None, description="Star facet length percentage")
    symmetry: Optional[str] = Field(None, description="Symmetry grade")
    table_size: Optional[str] = Field(None, description="Table percentage")

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "report_number": "1234567890",
                "shape": "Round",
                "carat": "1.50",
                "color_grade": "G",
                "clarity": "VS1",
                "cut": "Excellent"
            }
        }


class FewShotExample(BaseModel):
    """Model representing a few-shot example for the AI."""
    
    role: str = Field(..., description="Role in the conversation (user/assistant)")
    content: str = Field(..., description="Content of the message")
    
    @field_validator('role')
    @classmethod
    def validate_role(cls, v: str) -> str:
        """Validate that role is either 'user' or 'assistant'."""
        if v not in ['user', 'assistant', 'system']:
            raise ValueError("Role must be 'user', 'assistant', or 'system'")
        return v


class OpenAIMessage(BaseModel):
    """Model representing a message in OpenAI API format."""

    role: str = Field(..., description="Role of the message sender")
    content: Union[List[Dict[str, Any]], str] = Field(..., description="Content of the message")


class OpenAIRequest(BaseModel):
    """Model representing an OpenAI API request."""
    
    model: str = Field(default="gpt-4o", description="Model to use for completion")
    temperature: float = Field(default=0, ge=0, le=2, description="Sampling temperature")
    messages: List[OpenAIMessage] = Field(..., description="List of messages")
    max_tokens: int = Field(default=2000, gt=0, description="Maximum tokens to generate")
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "model": "gpt-4o",
                "temperature": 0,
                "messages": [
                    {
                        "role": "user",
                        "content": "Extract diamond information"
                    }
                ],
                "max_tokens": 2000
            }
        }


class OpenAIResponse(BaseModel):
    """Model representing an OpenAI API response."""
    
    content: str = Field(..., description="Response content from the API")
    model: Optional[str] = Field(None, description="Model used for the response")
    usage: Optional[dict] = Field(None, description="Token usage information")


class Config(BaseModel):
    """Application configuration model."""
    
    openai_api_key: str = Field(..., description="OpenAI API key")
    model_name: str = Field(default="gpt-4o", description="OpenAI model to use")
    temperature: float = Field(default=0, ge=0, le=2, description="Model temperature")
    max_tokens: int = Field(default=2000, gt=0, description="Maximum tokens")
    max_retries: int = Field(default=4, gt=0, description="Maximum retry attempts")
    prompt_file: str = Field(
        default="prompts/ai-congfigration/certificate_config.py",
        description="Path to the prompt configuration file"
    )
    few_shot_file: str = Field(
        default="prompts/shots/examples.yaml",
        description="Path to few-shot examples file"
    )
    output_dir: str = Field(default="output", description="Directory for output files")
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "openai_api_key": "sk-...",
                "model_name": "gpt-4o",
                "temperature": 0,
                "max_tokens": 2000,
                "max_retries": 4
            }
        }


class ProcessingResult(BaseModel):
    """Model representing the result of processing an image."""
    
    success: bool = Field(..., description="Whether processing was successful")
    data: Optional[DiamondGradingReport] = Field(None, description="Extracted diamond data")
    error: Optional[str] = Field(None, description="Error message if processing failed")
    retries: int = Field(default=0, description="Number of retries attempted")
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "success": True,
                "data": {
                    "report_number": "1234567890",
                    "shape": "Round",
                    "carat": "1.50"
                },
                "error": None,
                "retries": 0
            }
        }

