import os
import json
from flask import Flask, jsonify
from flask_cors import CORS 
import glob
import logging
from datetime import datetime
import uuid 
import tempfile

# --- CRITICAL IMPORT: Import your actual scraper function ---
from vrt import run_scraper_and_get_data
# -----------------------------------------------------------

# --- APScheduler Imports ---
from apscheduler.schedulers.background import BackgroundScheduler
# ---------------------------

# --- Configuration ---
app = Flask(__name__) 
# Allow CORS for all routes (fixes the issue where the root endpoint was not covered)
CORS(app, resources={r"/*": {"origins": "*"}}) 
DATA_DIR = os.getcwd() 
DATA_CACHE_FILE = 'latest_data.json' # File to cache the raw scraped data

# Set up logging for the Flask app
app.logger.setLevel(logging.INFO)

# --- KJ List (Priority/Grouping Order) ---
KJ_ORDER = [
    "Premier League", "LaLiga", "Bundesliga", "Serie A", "Eredivisie", "Ligue 1", 
    "Champions League", "Europa League", "World Cup", "Afcon", "World Cup U17"
]
KJ_PRIORITY = {league.lower(): i for i, league in enumerate(KJ_ORDER)}
DEFAULT_PRIORITY = len(KJ_ORDER)

# --- Helper Functions ---

def extract_teams_from_matchup(matchup):
    """Safely extracts home and away teams from a matchup string."""
    separators = [' – ', ' - ', ' vs ', ' VS ', ' v ', ' V ']
    for sep in separators:
        if sep in matchup:
            home, away = matchup.split(sep, 1)
            return home.strip(), away.strip()
    return matchup.strip(), matchup.strip()

# --- Scheduling Function (The Producer) ---

def scheduled_scrape_and_save():
    """
    Function executed by the scheduler every 30 minutes.
    It scrapes data using `vrt.run_scraper_and_get_data()` and writes the cache file atomically.
    """
    start_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{start_ts}] --- STARTING SCHEDULED SCRAPE (30 min interval) ---")
    
    try:
        # 1. Run the core scraping function from vrt.py
        fixtures_data = run_scraper_and_get_data() or []
        
        # 2. Package data with metadata
        data_to_save = {
            "timestamp": datetime.now().isoformat(),
            "fixtures": fixtures_data, # Cache the RAW fixtures data
            "status": "Success"
        }

        # 3. Save the result to the static JSON cache file **atomically** (safest method)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix=DATA_CACHE_FILE + '.', dir='.')
        try:
            with os.fdopen(tmp_fd, 'w', encoding='utf-8') as tmpf:
                json.dump(data_to_save, tmpf, indent=2, ensure_ascii=False, default=str)
                tmpf.flush()
                os.fsync(tmpf.fileno())
            os.replace(tmp_path, DATA_CACHE_FILE)
        finally:
            # Cleanup if something went wrong and temp file still exists
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        count = len(fixtures_data) if isinstance(fixtures_data, (list, tuple)) else 0
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scrape SUCCESS. Saved {count} RAW fixtures to {DATA_CACHE_FILE}\n")
        
    except Exception as e:
        app.logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] CRITICAL SCRAPING JOB FAILURE: {e}\n")

# --- Data Loading Function (The Consumer) ---

def load_raw_fixtures_from_cache():
    """Loads raw fixtures data from the cache file created by the scheduler."""
    if not os.path.exists(DATA_CACHE_FILE):
        app.logger.warning(f"Cache file {DATA_CACHE_FILE} not found.")
        return []
    
    try:
        with open(DATA_CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('fixtures', [])
    except Exception as e:
        app.logger.error(f"FATAL API ERROR during cache file read: {e}")
        return []

# --- ROOT ENDPOINT (The required output structure) ---

@app.route('/', methods=['GET'])
def get_all_fixtures_for_frontend():
    """
    Serves the root endpoint (/) by loading RAW data from the cache and 
    transforming it into the exact simple array structure requested by the frontend.
    """
    raw_fixtures = load_raw_fixtures_from_cache()
    
    if not raw_fixtures:
        return jsonify({
            "error": "Data file not yet created or empty.",  
            "message": "The initial scrape job is running or has not finished yet. Please try again in a few minutes."
        }), 503 
    
    processed_fixtures = []
    
    for fixture in raw_fixtures:
        
        # --- Transformation Logic ---
        matchup_str = fixture.get("matchup", "Home – Away")
        home_team, away_team = extract_teams_from_matchup(matchup_str)
        
        # Find logos in the nested 'team_logos' array
        home_logo_url = next((tl.get('logo_url') for tl in fixture.get('team_logos', []) 
                              if home_team.lower() in tl.get('team_name', '').lower()), "")
        away_logo_url = next((tl.get('logo_url') for tl in fixture.get('team_logos', []) 
                              if away_team.lower() in tl.get('team_name', '').lower()), "")

        processed_fixture = {
            # Robust fallback for event_id
            "event_id": fixture.get("event_id") or str(uuid.uuid4()), 
            "competition": fixture.get("competition", ""),
            "matchup": matchup_str,
            "date_time": fixture.get("date_time", ""),
            "parsed_datetime": fixture.get("parsed_datetime", ""), 
            "is_live": fixture.get("is_live") is True, 
            "team_logos": [
                {"logo_url": home_logo_url},
                {"logo_url": away_logo_url}
            ],
            "streams": fixture.get("streams", []),
            "event_url": fixture.get("event_url", "#")
        }
        processed_fixtures.append(processed_fixture)
    
    app.logger.info(f"Root endpoint served {len(processed_fixtures)} fixtures in simple array format.")
    
    # Return direct array, no wrapper
    return jsonify(processed_fixtures)

# --- Health endpoint ---

@app.route('/health', methods=['GET'])
def health():
    """Provides status and cache information for monitoring."""
    exists = os.path.exists(DATA_CACHE_FILE)
    info = {
        "cache_exists": exists,
        "cache_path": os.path.abspath(DATA_CACHE_FILE) if exists else None,
        "timestamp": datetime.now().isoformat()
    }
    return jsonify(info), 200

# --- Scheduler Startup (Gunicorn/Production Logic) ---

# Initialize the scheduler once
scheduler = BackgroundScheduler()

# Control whether to enable the in-process scheduler via env var.
ENABLE_SCHEDULER = os.environ.get("ENABLE_SCHEDULER", "true").lower() in ("1", "true", "yes")
# If running under Gunicorn, only allow worker '0' to start the scheduler.
GUNICORN_WORKER_ID = os.environ.get("GUNICORN_WORKER_ID")

# This block is executed when the app is loaded by Gunicorn workers.
if __name__ != '__main__': 
    if ENABLE_SCHEDULER and (GUNICORN_WORKER_ID in (None, "", "0")):
        # Run the initial scrape to populate the cache
        scheduled_scrape_and_save()
        
        # Add job to run every 30 minutes
        scheduler.add_job(
            scheduled_scrape_and_save, 
            'interval', 
            minutes=30, 
            id='scheduled_scrape'
        )
        
        # Start the scheduler thread
        scheduler.start()
        app.logger.info("\n✅ APScheduler started. Scrape scheduled every 30 minutes.\n")
    else:
        app.logger.info(f"Scheduler disabled or not started in this process (ENABLE_SCHEDULER={ENABLE_SCHEDULER}, GUNICORN_WORKER_ID={GUNICORN_WORKER_ID}).\n")
