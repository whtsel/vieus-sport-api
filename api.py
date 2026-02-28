import os
import json
import logging
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import vet # The Scraper

app = Flask(__name__)
# Enable CORS for production - allows UI to connect from any domain
CORS(app, resources={r"/*": {"origins": "*"}})

DATA_CACHE_FILE = 'Day1.json'
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scheduled_scrape():
    """Triggered by the background scheduler every 30 mins."""
    logger.info(f"--- BEGINNING DATA REFRESH: {datetime.now()} ---")
    try:
        # This calls the run function in your vet.py
        vet.run_scraper_and_get_data()
        logger.info("--- REFRESH SUCCESSFUL ---")
    except Exception as e:
        logger.error(f"SCRAPE ERROR: {e}")

@app.route('/', methods=['GET'])
def get_full_fixtures():
    """
    Transforms the Object Map from Day1.json into a sorted Array for the UI.
    Includes lineups, standings, and all stream links.
    """
    if not os.path.exists(DATA_CACHE_FILE):
        return jsonify({"error": "Initializing", "message": "Scraper is fetching initial data..."}), 503

    try:
        with open(DATA_CACHE_FILE, 'r', encoding='utf-8') as f:
            data_map = json.load(f)
        
        output_list = []
        for event_id, item in data_map.items():
            # EXTRACTING DEEP DATA POINTS
            output_list.append({
                "event_id": event_id,
                "competition": item.get("competition", "Other"),
                "matchup": item.get("matchup", "Unknown Match"),
                "date_time": item.get("date_time", ""),
                "parsed_datetime": item.get("parsed_datetime", "9999-12-31"), # Fallback for sort
                "is_live": item.get("is_live", False),
                "team_logos": item.get("team_logos", []),
                "streams": item.get("streams", []),
                "starting_lineups": item.get("starting_lineups", {"home_team": [], "away_team": []}),
                "league_table": item.get("league_table", []),
                "event_url": item.get("event_url", "#"),
                "last_updated": item.get("last_updated", "")
            })
        
        # PRODUCTION SORTING: 
        # 1. Live matches at the top.
        # 2. Upcoming matches sorted by time.
        output_list.sort(key=lambda x: (not x['is_live'], x['parsed_datetime']))
        
        return jsonify(output_list)

    except Exception as e:
        logger.error(f"API RUNTIME ERROR: {e}")
        return jsonify({"error": "Failed to parse data", "details": str(e)}), 500

# --- Production Scheduler Logic ---
scheduler = BackgroundScheduler()

# Prevents the scheduler from running twice in Flask's 'Reload' mode
if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
    # Populate data immediately on boot if missing
    if not os.path.exists(DATA_CACHE_FILE):
        scheduled_scrape()
    
    scheduler.add_job(scheduled_scrape, 'interval', minutes=30, id='refresh_job')
    scheduler.start()
    logger.info("✅ Scheduler Active: Refreshing every 30 minutes.")

if __name__ == '__main__':
    # host='0.0.0.0' is required for Docker/GitHub/Cloud deployments
    app.run(host='0.0.0.0', port=5000)
