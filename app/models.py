"""Pydantic response models for all API endpoints."""

from pydantic import BaseModel
from typing import Optional


class HealthResponse(BaseModel):
    status: str
    timestamp: str


class CategoryResponse(BaseModel):
    id: str
    name: str
    stock_count: int


class StockResponse(BaseModel):
    ticker: str
    cmp: Optional[float] = None
    diff_200dma: Optional[float] = None
    trend: Optional[str] = None
    car_status: Optional[str] = None
    changed: bool = False


class AlertResponse(BaseModel):
    ticker: str
    previous_trend: Optional[str] = None
    current_trend: Optional[str] = None
    previous_car: Optional[str] = None
    current_car: Optional[str] = None
    timestamp: str


class MarketStatusResponse(BaseModel):
    is_open: bool
    next_open: str
    last_refreshed: str
