"""
Agent that answers:
"What’s the average temperature in Paris over the last 3 days, and convert it to Fahrenheit?"

Dependencies:
    pip install requests python-dateutil

Run:
    python agent_weather.py
"""

import requests
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from datetime import datetime, timezone, timedelta
import statistics
import time
import os
from langchain_openai import AzureChatOpenAI

class SimpleMemory:
    """Very small memory store for past actions/results."""
    def __init__(self):
        self.records = []

    def add(self, kind, content):
        timestamp = datetime.now(timezone.utc).isoformat()
        self.records.append({"time": timestamp, "kind": kind, "content": content})

    def recent(self, n=5):
        return list(reversed(self.records))[:n]


class GeocodeTool:
    """Geocode place names to latitude/longitude using Nominatim (OpenStreetMap)."""
    USER_AGENT = "applied-agent-demo/1.0 (contact: example@example.com)"

    @staticmethod
    def geocode(place_name, max_retries=2, pause=1.0):
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": place_name, "format": "json", "limit": 1}
        headers = {"User-Agent": GeocodeTool.USER_AGENT}
        for attempt in range(max_retries + 1):
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                if data:
                    entry = data[0]
                    lat = float(entry["lat"])
                    lon = float(entry["lon"])
                    return {"lat": lat, "lon": lon, "display_name": entry.get("display_name")}
                else:
                    return None
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(pause)
                    continue
                else:
                    raise


class WeatherTool:
    """
    Fetch hourly temperatures for a given lat/lon and date range using Open-Meteo (no API key).
    Docs: https://open-meteo.com/en/docs
    We'll request hourly temperature_2m for the date range inclusive.
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    @staticmethod
    def fetch_hourly_temps(lat, lon, start_date, end_date, timezone_str="UTC", max_retries=2):
        """
        start_date/end_date: str 'YYYY-MM-DD' (ISO date)
        Returns dict: { "hours": ["2025-10-12T00:00"], "temps": [12.3, ...] }
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone_str
        }
        for attempt in range(max_retries + 1):
            try:
                r = requests.get(WeatherTool.BASE_URL, params=params, timeout=15)
                r.raise_for_status()
                j = r.json()
                hourly = j.get("hourly", {})
                times = hourly.get("time", [])
                temps = hourly.get("temperature_2m", [])
                if not times or not temps:
                    # treat as incomplete
                    return None
                return {"times": times, "temps": temps}
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(1.0)
                    continue
                else:
                    raise


class CalculatorTool:
    """Small utility functions: average, convert C->F."""
    @staticmethod
    def average(values):
        if not values:
            return None
        return statistics.mean(values)

    @staticmethod
    def c_to_f(c):
        # Fahrenheit = Celsius × 9/5 + 32
        return (c * 9.0 / 5.0) + 32.0

from langchain_openai import AzureChatOpenAI
import os

class LangChainPlanner:
    def __init__(self):
        self.llm = AzureChatOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
            temperature=0.3
        )

    def parse_prompt(self, prompt: str):
        system_prompt = (
            "You are an intelligent planner agent. "
            "Given a natural language query about weather or temperature, "
            "extract structured info as JSON with keys: 'location', 'days', and 'action'. "
            "Example: {'location': 'Paris', 'days': 3, 'action': 'avg_temp_and_convert'}"
        )

        from langchain_core.messages import SystemMessage, HumanMessage
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=prompt),
        ]

        response = self.llm.invoke(messages)
        try:
            import json
            return json.loads(response.content)
        except Exception:
            return {"location": "Paris", "days": 3, "action": "avg_temp_and_convert"}




class Agent:
    def __init__(self):
        self.memory = SimpleMemory()
        self.planner = LangChainPlanner()
        self.tools = {
            "geocode": GeocodeTool,
            "weather": WeatherTool,
            "calc": CalculatorTool
        }

    def run(self, prompt):
        plan = self.planner.parse_prompt(prompt)
        self.memory.add("plan", plan)

        location = plan["location"]
        days = plan["days"]

        # Step 1: Geocode
        try:
            geocode_res = self.tools["geocode"].geocode(location)
        except Exception as e:
            self.memory.add("error", {"stage": "geocode", "error": str(e)})
            return f"Sorry — I couldn't geocode '{location}': {e}"

        if not geocode_res:
            self.memory.add("error", {"stage": "geocode", "error": "no result"})
            return f"Sorry — no geocoding result for '{location}'."

        self.memory.add("geocode", geocode_res)

        lat = geocode_res["lat"]
        lon = geocode_res["lon"]

       
        today_utc = datetime.now(timezone.utc).date()
      
        start_date = (today_utc - relativedelta(days=days)).isoformat()
        end_date = (today_utc - relativedelta(days=1)).isoformat()

        # Step 3: Fetch weather
        try:
            weather_res = self.tools["weather"].fetch_hourly_temps(
                lat, lon, start_date, end_date, timezone_str="UTC"
            )
        except Exception as e:
            self.memory.add("error", {"stage": "weather_fetch", "error": str(e)})
            return f"Sorry — weather fetch failed: {e}"

        if not weather_res:
            self.memory.add("note", {"stage": "weather_incomplete", "message": "No hourly data returned, retrying with expanded window."})
            alt_start = (today_utc - relativedelta(days=days+1)).isoformat()
            alt_end = end_date
            try:
                weather_res = self.tools["weather"].fetch_hourly_temps(lat, lon, alt_start, alt_end, timezone_str="UTC")
            except Exception as e:
                self.memory.add("error", {"stage": "weather_fetch_retry", "error": str(e)})
                return f"Sorry — weather fetch failed on retry: {e}"

            if not weather_res:
                self.memory.add("error", {"stage": "weather_fetch", "error": "incomplete data after retry"})
                return "Sorry — couldn't retrieve weather data for that range."

        self.memory.add("weather_raw", {"start": start_date, "end": end_date, "counts": len(weather_res.get("temps", []))})

        temps_c = weather_res["temps"]
        if not temps_c:
            return "Sorry — weather API returned no temperature values."

        # Step 4: Compute average in Celsius
        avg_c = self.tools["calc"].average(temps_c)
        if avg_c is None:
            return "Sorry — couldn't compute average temperature."

        # Step 5: Convert to Fahrenheit
        avg_f = self.tools["calc"].c_to_f(avg_c)

        avg_c_rounded = round(avg_c, 1)
        avg_f_rounded = round(avg_f, 1)

        result = {
            "location": location,
            "lat": lat,
            "lon": lon,
            "start_date": start_date,
            "end_date": end_date,
            "avg_c": avg_c_rounded,
            "avg_f": avg_f_rounded,
            "hours_counted": len(temps_c)
        }
        self.memory.add("result", result)

        answer = (
            f"Average temperature in {location} from {start_date} to {end_date} (UTC) "
            f"was {avg_c_rounded}°C, which is {avg_f_rounded}°F. "
            f"(Based on {result['hours_counted']} hourly readings.)"
        )
        return answer


if __name__ == "__main__":
    agent = Agent()
    prompt = "What's the average temperature in Paris over the last 4 days, and convert it to Fahrenheit?"
    prompt = "What's the average temperature in Mumbai over the last 3 days, and convert it to Fahrenheit?"
    # prompt = "What's the average temperature in Tokyo over the last 2 days, and convert it to Fahrenheit?"

    print("Prompt:", prompt)
    try:
        out = agent.run(prompt)
        print("\nAgent answer:\n", out)
        
        print("\nRecent agent memory records (most recent first):")
        for rec in agent.memory.recent(6):
            print("-", rec["kind"], rec["content"])
    except Exception as ex:
        print("Agent encountered an error:", ex)
