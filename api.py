import os
import json
import time
import threading
import random
from flask import Flask, jsonify
from flask_cors import CORS  # Keep this import
import logging
from datetime import datetime
import urllib3
import requests
from bs4 import BeautifulSoup
import re

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
app = Flask(__name__)

# CORRECTED CORS: Allow all origins (*) for all API endpoints (/api/*)
# NOTE: Removed the static file routes from CORS definition as they are now gone.
CORS(app, resources={r"/api/*": {"origins": "*"}}) 

# Set up logging for the Flask app
app.logger.setLevel(logging.INFO)

# ... (Global data storage and KJ_ORDER are unchanged) ...

# --- Global data storage ---
scraped_data = {
    'live_fixtures': [],
    'upcoming_fixtures': [],
    'last_scrape_time': None,
    'is_scraping': False,
    'last_successful_scrape': None,
    'using_fallback_data': False
}

# --- KJ List (User's requested order) ---
KJ_ORDER = [
    "Premier League", "Ligue 1", "Bundesliga", "Serie A", "Eredivisie", 
    "LaLiga", "Euro", "Champions League", "Europa League", "Conference League", 
    "UEFA Nations League", "Copa Libertadores", "World Cup", "World Cup U17", 
    "World Cup Women U17"
]

# Top European leagues
TOP_EUROPEAN_LEAGUES = [
    "Premier League", "LaLiga", "Bundesliga", "Serie A", "Ligue 1"
]

# Top world leagues  
TOP_WORLD_LEAGUES = [
    "Champions League", "Europa League", "Conference League", "Euro", "World Cup"
]

# Create a mapping for quick lookup of priority
KJ_PRIORITY = {league.lower(): i for i, league in enumerate(KJ_ORDER)}

# --- Broadcast Scraper Class (from dut.py) ---
class BroadcastScraper:
    def __init__(self, base_url="https://m.livetv.sx"):
        self.base_url = base_url
        self.session = requests.Session()
        
        # SSL Bypass: Disable verification (Using stored instruction)
        self.session.verify = False
        
        # User Agent: Set to Mac (Using stored instruction)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def scrape_broadcasts(self, page_url):
        """Scrape broadcast information from the schedule page."""
        try:
            # Random delay to avoid bot detection (1-3 seconds)
            time.sleep(random.uniform(1, 3))
            
            response = self.session.get(page_url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            broadcasts = []
            
            # Use the most robust selector to capture ALL list items in all relevant containers
            broadcast_items = soup.select('ul.broadcasts li')
            
            for item in broadcast_items:
                broadcast_data = self._parse_broadcast_item(item)
                if broadcast_data:
                    broadcasts.append(broadcast_data)
            
            return broadcasts
            
        except requests.exceptions.RequestException as e:
            app.logger.error(f"Error fetching page: {e}")
            return []
        except Exception as e:
            app.logger.error(f"Error parsing page: {e}")
            return []
    
    def _parse_broadcast_item(self, item):
        """Parse individual broadcast item"""
        broadcast_data = {}
        
        try:
            # Extract logo URL
            logo_div = item.find('div', class_='logo')
            if logo_div:
                logo_img = logo_div.find('img')
                if logo_img and logo_img.get('src'):
                    logo_src = logo_img['src']
                    if logo_src.startswith('//'):
                        logo_src = 'https:' + logo_src
                    elif logo_src.startswith('/'):
                        logo_src = self.base_url + logo_src
                    broadcast_data['logo_url'] = logo_src
            
            info_div = item.find('div', class_='info')
            if info_div:
                title_div = info_div.find('div', class_='title')
                if title_div:
                    # Fallback logic to grab matchup text
                    title_link = title_div.find('a')
                    matchup_text = title_link.get_text(strip=True) if title_link else title_div.get_text(strip=True)

                    if matchup_text:
                        broadcast_data['fixture'] = matchup_text
                        broadcast_data['matchup'] = matchup_text
                        
                        # Parse team names
                        separators = ['–', '-', 'vs', 'VS']
                        
                        # Only attempt split if a separator is present
                        if any(sep in matchup_text for sep in separators):
                            for separator in separators:
                                if separator in broadcast_data['fixture']:
                                    teams = broadcast_data['fixture'].split(separator)
                                    if len(teams) == 2:
                                        broadcast_data['team_home'] = teams[0].strip()
                                        broadcast_data['team_away'] = teams[1].strip()
                                        break
                
                # Extract time and sport from the note
                note_div = info_div.find('div', class_='note')
                if note_div:
                    time_text = note_div.get_text(strip=True)
                    live_span = note_div.find('span', class_='Live')
                    
                    if live_span:
                        time_text = time_text.replace(live_span.get_text(strip=True), '').strip()
                    
                    broadcast_data['datetime'] = time_text
                    broadcast_data['is_live'] = live_span is not None
                    
                    # Extract the League/Sport name from the parentheses
                    league_match = re.search(r'\((.*?)\)', time_text)
                    if league_match:
                        sport_name = league_match.group(1).strip()
                        broadcast_data['sport'] = sport_name
                        broadcast_data['league'] = sport_name
                    
                    # Extract time only (remove date and league info)
                    time_match = re.search(r'at (\d{2}:\d{2})', time_text)
                    if time_match:
                        broadcast_data['time'] = time_match.group(1)
                    else:
                        broadcast_data['time'] = time_text.split('(')[0].strip()

            # Extract stream link
            if info_div:
                stream_link = info_div.find('a', target='_blank')
                if stream_link and stream_link.get('href'):
                    stream_href = stream_link['href']
                    if stream_href.startswith('//'):
                        stream_href = 'https:' + stream_href
                    elif stream_href.startswith('/'):
                        stream_href = self.base_url + stream_href
                    broadcast_data['stream_url'] = stream_href
            
            # Only return if we have meaningful data
            if broadcast_data.get('fixture') or broadcast_data.get('datetime'):
                return broadcast_data
            else:
                return None
            
        except Exception as e:
            app.logger.debug(f"Error parsing item: {e}")
            return None

    def categorize_fixtures(self, broadcasts):
        """Categorize broadcasts into live (with stream links) and upcoming (without stream links)"""
        live_fixtures = []
        upcoming_fixtures = []

        for fixture in broadcasts:
            # Check if fixture has a valid stream URL
            has_stream_url = fixture.get('stream_url') and fixture['stream_url'] not in ['', '#', 'N/A']
            
            if has_stream_url:
                live_fixtures.append(fixture)
            else:
                upcoming_fixtures.append(fixture)

        return live_fixtures, upcoming_fixtures

    def enhance_fixture_data(self, fixture):
        """Enhance fixture with additional fields for better frontend compatibility"""
        enhanced = fixture.copy()
        
        # Ensure required fields exist
        if 'fixture' not in enhanced:
            enhanced['fixture'] = enhanced.get('matchup', 'Unknown Match')
        
        if 'league' not in enhanced:
            enhanced['league'] = enhanced.get('sport', 'Unknown League')
        
        if 'time' not in enhanced:
            enhanced['time'] = enhanced.get('datetime', 'TBD')
        
        # Set default values for frontend
        enhanced['venue'] = enhanced.get('venue', 'TBD')
        enhanced['status'] = 'Live' if enhanced.get('is_live') else 'Scheduled'
        enhanced['broadcast'] = 'LiveSports808' if enhanced.get('stream_url') else 'No Stream Available'
        
        return enhanced

# Initialize scraper
scraper = BroadcastScraper()

# --- Fallback Mock Data ---
# (The get_fallback_data function remains unchanged)
def get_fallback_data():
    """Generate realistic fallback data when scraping fails"""
    current_time = datetime.now()
    
    # Sample live matches
    live_matches = [
        {
            'fixture': 'Manchester United vs Liverpool',
            'team_home': 'Manchester United',
            'team_away': 'Liverpool',
            'league': 'Premier League',
            'time': '15:00',
            'stream_url': 'https://m.livetv.sx/enx/event/123456/',
            'status': 'Live',
            'broadcast': 'LiveSports808',
            'venue': 'Old Trafford',
            'logo_url': 'https://via.placeholder.com/60/0f3460/ffffff?text=PREM'
        },
        {
            'fixture': 'Barcelona vs Real Madrid',
            'team_home': 'Barcelona',  
            'team_away': 'Real Madrid',
            'league': 'LaLiga',
            'time': '20:00',
            'stream_url': 'https://m.livetv.sx/enx/event/123457/',
            'status': 'Live',
            'broadcast': 'LiveSports808',
            'venue': 'Camp Nou',
            'logo_url': 'https://via.placeholder.com/60/0f3460/ffffff?text=LL'
        }
    ]
    
    # Sample upcoming matches
    upcoming_matches = [
        {
            'fixture': 'Bayern Munich vs Borussia Dortmund',
            'team_home': 'Bayern Munich',
            'team_away': 'Borussia Dortmund',
            'league': 'Bundesliga',
            'time': '17:30',
            'stream_url': None,
            'status': 'Scheduled',
            'broadcast': 'No Stream Available',
            'venue': 'Allianz Arena',
            'logo_url': 'https://via.placeholder.com/60/0f3460/ffffff?text=BUN'
        },
        {
            'fixture': 'PSG vs Marseille',
            'team_home': 'PSG',
            'team_away': 'Marseille',
            'league': 'Ligue 1',
            'time': '19:45',
            'stream_url': None,
            'status': 'Scheduled',
            'broadcast': 'No Stream Available',
            'venue': 'Parc des Princes',
            'logo_url': 'https://via.placeholder.com/60/0f3460/ffffff?text=L1'
        },
        {
            'fixture': 'Juventus vs AC Milan',
            'team_home': 'Juventus',
            'team_away': 'AC Milan',
            'league': 'Serie A',
            'time': '20:45',
            'stream_url': None,
            'status': 'Scheduled',
            'broadcast': 'No Stream Available',
            'venue': 'Allianz Stadium',
            'logo_url': 'https://via.placeholder.com/60/0f3460/ffffff?text=SA'
        }
    ]
    
    return live_matches, upcoming_matches

# --- Scraping/Data Functions (perform_scraping, use_fallback_data, save_to_json, load_from_json, background_scraper) ---
# These functions remain unchanged as they were correct.

def perform_scraping():
    """Perform the actual scraping and update global data"""
    global scraped_data
    
    if scraped_data['is_scraping']:
        app.logger.info("Scraping already in progress, skipping...")
        return
    
    scraped_data['is_scraping'] = True
    try:
        app.logger.info("Starting scheduled scraping...")
        url = "https://m.livetv.sx/en/allupcoming/"
        
        broadcasts = scraper.scrape_broadcasts(url)
        
        if broadcasts:
            # Categorize the data into live and upcoming
            live_fixtures, upcoming_fixtures = scraper.categorize_fixtures(broadcasts)
            
            # Enhance the data
            enhanced_live = [scraper.enhance_fixture_data(f) for f in live_fixtures]
            enhanced_upcoming = [scraper.enhance_fixture_data(f) for f in upcoming_fixtures]
            
            # Update global data
            scraped_data['live_fixtures'] = enhanced_live
            scraped_data['upcoming_fixtures'] = enhanced_upcoming
            scraped_data['last_scrape_time'] = datetime.now().isoformat()
            scraped_data['last_successful_scrape'] = datetime.now().isoformat()
            scraped_data['using_fallback_data'] = False
            
            app.logger.info(f"Scraping completed: {len(enhanced_live)} live, {len(enhanced_upcoming)} upcoming fixtures")
            
            # Save to files
            save_to_json(enhanced_live, 'live.json')
            save_to_json(enhanced_upcoming, 'upcoming.json')
            
        else:
            app.logger.warning("No broadcasts found during scraping, using fallback data")
            use_fallback_data()
            
    except Exception as e:
        app.logger.error(f"Error during scraping: {e}")
        use_fallback_data()
    finally:
        scraped_data['is_scraping'] = False

def use_fallback_data():
    """Use fallback data when scraping fails"""
    global scraped_data
    live_fixtures, upcoming_fixtures = get_fallback_data()
    
    scraped_data['live_fixtures'] = live_fixtures
    scraped_data['upcoming_fixtures'] = upcoming_fixtures
    scraped_data['last_scrape_time'] = datetime.now().isoformat()
    scraped_data['using_fallback_data'] = True
    
    # Save fallback data to files
    save_to_json(live_fixtures, 'live.json')
    save_to_json(upcoming_fixtures, 'upcoming.json')

def save_to_json(data, filename):
    """Save data to JSON file"""
    try:
        filepath = os.path.join(os.getcwd(), filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        app.logger.info(f"Data saved to {filename}")
    except Exception as e:
        app.logger.error(f"Error saving {filename}: {e}")

def load_from_json(filename):
    """Load data from JSON file"""
    try:
        filepath = os.path.join(os.getcwd(), filename)
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        app.logger.error(f"Error loading {filename}: {e}")
    return []

def background_scraper():
    """Background thread that runs scraping every 5-7 minutes randomly"""
    while True:
        try:
            # Random interval between 5-7 minutes (300-420 seconds)
            interval = random.randint(300, 420)
            app.logger.info(f"Next scrape in {interval} seconds")
            time.sleep(interval)
            
            perform_scraping()
            
        except Exception as e:
            app.logger.error(f"Error in background scraper: {e}")
            time.sleep(60)  # Wait 1 minute before retrying

# --- Helper Functions ---
# (The sort_fixtures_by_kj and filter_fixtures_by_league functions remain unchanged)

def sort_fixtures_by_kj(fixtures):
    """Sorts fixtures based on the KJ_ORDER list"""
    def get_sort_key(fixture):
        league_name = fixture.get('league', '').lower()
        # Use a large number for leagues not in the priority list
        return KJ_PRIORITY.get(league_name, len(KJ_ORDER))
    
    return sorted(fixtures, key=get_sort_key)

def filter_fixtures_by_league(fixtures, target_leagues):
    """Filter fixtures by league names"""
    filtered = []
    for fixture in fixtures:
        league = fixture.get('league', '')
        if any(target_league.lower() in league.lower() for target_league in target_leagues):
            filtered.append(fixture)
    return filtered

# --- API Endpoints (Routes) ---

@app.route('/', methods=['GET'])
def serve_index():
    """Returns API status on the root path."""
    return jsonify({
        "status": "API is Live",
        "message": "Access data via /api/live_fixtures or /api/upcoming_fixtures",
        "last_update": scraped_data['last_scrape_time']
    })

# --- API Endpoints for Frontend (Unchanged) ---

@app.route('/api/live_fixtures', methods=['GET'])
def get_live_fixtures():
    """Returns fixtures with active streams"""
    fixtures = scraped_data['live_fixtures']
    sorted_fixtures = sort_fixtures_by_kj(fixtures)
    
    return jsonify({
        "sport": "Live",
        "type": "live",
        "count": len(sorted_fixtures),
        "last_update": scraped_data['last_scrape_time'],
        "data": sorted_fixtures
    })

@app.route('/api/upcoming_fixtures', methods=['GET'])
def get_upcoming_fixtures():
    """Returns fixtures without active streams"""
    fixtures = scraped_data['upcoming_fixtures']
    sorted_fixtures = sort_fixtures_by_kj(fixtures)
    
    return jsonify({
        "sport": "Upcoming", 
        "type": "upcoming",
        "count": len(sorted_fixtures),
        "last_update": scraped_data['last_scrape_time'],
        "data": sorted_fixtures
    })

@app.route('/api/european_leagues', methods=['GET'])
def get_european_leagues():
    """Returns fixtures from top European leagues"""
    fixtures = scraped_data['upcoming_fixtures']
    european_fixtures = filter_fixtures_by_league(fixtures, TOP_EUROPEAN_LEAGUES)
    sorted_fixtures = sort_fixtures_by_kj(european_fixtures)
    
    return jsonify({
        "sport": "Football",
        "type": "european_leagues", 
        "count": len(sorted_fixtures),
        "last_update": scraped_data['last_scrape_time'],
        "data": sorted_fixtures
    })

@app.route('/api/world_leagues', methods=['GET'])
def get_world_leagues():
    """Returns fixtures from top world leagues"""
    fixtures = scraped_data['upcoming_fixtures']
    world_fixtures = filter_fixtures_by_league(fixtures, TOP_WORLD_LEAGUES)
    sorted_fixtures = sort_fixtures_by_kj(world_fixtures)
    
    return jsonify({
        "sport": "Football",
        "type": "world_leagues",
        "count": len(sorted_fixtures),
        "last_update": scraped_data['last_scrape_time'],
        "data": sorted_fixtures
    })

@app.route('/api/hero-fixtures/<sport>', methods=['GET'])
def get_hero_fixtures(sport):
    """Returns fixtures for hero carousel"""
    if sport.lower() == 'live':
        # For live sport, return all fixtures (live + upcoming)
        all_fixtures = scraped_data['live_fixtures'] + scraped_data['upcoming_fixtures']
        fixtures = all_fixtures
    else:
        # For other sports, return upcoming fixtures
        fixtures = scraped_data['upcoming_fixtures']
    
    sorted_fixtures = sort_fixtures_by_kj(fixtures)
    
    return jsonify({
        "sport": sport.capitalize(),
        "count": len(sorted_fixtures),
        "last_update": scraped_data['last_scrape_time'],
        "data": sorted_fixtures
    })

@app.route('/api/all_fixtures', methods=['GET'])
def get_all_fixtures():
    """Returns all fixtures combined"""
    all_fixtures = scraped_data['live_fixtures'] + scraped_data['upcoming_fixtures']
    sorted_fixtures = sort_fixtures_by_kj(all_fixtures)
    
    return jsonify({
        "sport": "All",
        "count": len(sorted_fixtures),
        "last_update": scraped_data['last_scrape_time'],
        "data": sorted_fixtures
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "last_scrape_time": scraped_data['last_scrape_time'],
        "is_scraping": scraped_data['is_scraping'],
        "using_fallback_data": scraped_data['using_fallback_data'],
        "data_counts": {
            "live_fixtures": len(scraped_data['live_fixtures']),
            "upcoming_fixtures": len(scraped_data['upcoming_fixtures'])
        }
    })

@app.route('/api/force-scrape', methods=['POST'])
def force_scrape():
    """Force an immediate scrape (for testing)"""
    perform_scraping()
    return jsonify({
        "status": "scraping_completed",
        "last_scrape_time": scraped_data['last_scrape_time']
    })

@app.route('/api/scraper-status', methods=['GET'])
def scraper_status():
    """Get current scraper status"""
    return jsonify({
        "is_scraping": scraped_data['is_scraping'],
        "last_scrape_time": scraped_data['last_scrape_time'],
        "last_successful_scrape": scraped_data['last_successful_scrape'],
        "using_fallback_data": scraped_data['using_fallback_data'],
        "fixture_counts": {
            "live": len(scraped_data['live_fixtures']),
            "upcoming": len(scraped_data['upcoming_fixtures'])
        }
    })

# --- Initialize and Start ---
def initialize_app():
    """Initialize the application with initial data"""
    app.logger.info("Initializing application...")
    
    # Try to load existing data first
    try:
        existing_live = load_from_json('live.json')
        existing_upcoming = load_from_json('upcoming.json')
        
        if existing_live and existing_upcoming:
            scraped_data['live_fixtures'] = existing_live
            scraped_data['upcoming_fixtures'] = existing_upcoming
            scraped_data['last_scrape_time'] = datetime.now().isoformat()
            app.logger.info("Loaded existing data from JSON files")
        else:
            # Perform initial scrape
            perform_scraping()
    except Exception as e:
        app.logger.warning(f"Could not load existing data: {e}")
        perform_scraping()
    
    # Start background scraper thread
    scraper_thread = threading.Thread(target=background_scraper, daemon=True)
    scraper_thread.start()
    
    app.logger.info("Background scraper thread started")
    
initialize_app()
