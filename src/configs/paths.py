from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Paths:
   
    raw_events: str = os.getenv("RAW_EVENTS_PATH", "data/raw/events")
    bronze_events: str = os.getenv("BRONZE_EVENTS_PATH", "data/out/bronze/events")
    silver_events: str = os.getenv("SILVER_EVENTS_PATH", "data/out/silver/events")
    gold_daily_metrics: str = os.getenv("GOLD_DAILY_METRICS_PATH", "data/out/gold/daily_metrics")
 
