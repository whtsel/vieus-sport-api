import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import urllib3
import re
from urllib.parse import urljoin
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import signal

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- BroadcastScraper Class (Keep this section exactly as you defined it) ---

class BroadcastScraper:
    def __init__(self, base_url="https://livetv.sx", max_workers=10):
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()
        # NOTE: Using the remembered instruction to ensure SSL Bypass is applied.
        self.session.verify = False 
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # Thread-safe counters
        self.stats_lock = threading.Lock()
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Progress tracking (We'll comment out the CLI progress bar logic)
        self.progress_lock = threading.Lock()
        self.completed_tasks = 0
        self.total_tasks = 0
        
    def _parse_broadcast_item(self, table):
        # ... (Keep this method as is) ...
        fixture_data = {}
        
        try:
            link_tag = table.find('a', class_='live')
            if not link_tag:
                link_tag = table.find('a', class_='bottomgray')
            
            if not link_tag:
                return None
            
            # 1. Extract Matchup and Stream Link
            matchup_text = link_tag.get_text(strip=True)
            fixture_data['matchup'] = matchup_text
            
            stream_href = link_tag.get('href')
            if stream_href:
                if stream_href.startswith('/'):
                    fixture_data['event_url'] = self.base_url + stream_href
                    match = re.search(r'/eventinfo/(\d+)', stream_href)
                    if match:
                        fixture_data['event_id'] = match.group(1)
                else:
                    fixture_data['event_url'] = stream_href
            
            # 2. Extract Date, Time, and Competition
            evdesc_span = table.find('span', class_='evdesc')
            if evdesc_span:
                desc_text = evdesc_span.get_text(separator=' ', strip=True)
                
                if '\n' in evdesc_span.text:
                    desc_parts = [p.strip() for p in evdesc_span.get_text('\n').split('\n') if p.strip()]
                else:
                    desc_parts = [desc_text]
                
                if desc_parts:
                    date_time_text = desc_parts[0]
                    fixture_data['date_time'] = date_time_text
                    
                    if len(desc_parts) >= 2:
                        competition_text = desc_parts[1].strip('()')
                        fixture_data['competition'] = competition_text
                    
                    # 3. Parse Date/Time
                    try:
                        month_pattern = r'(\d+)\s+([A-Za-z]+)\s+at\s+(\d+:\d+)'
                        match = re.search(month_pattern, date_time_text)
                        
                        if match:
                            date_part = f"{match.group(1)} {match.group(2)} at {match.group(3)}"
                            current_year = datetime.now().year
                            parsed_date = datetime.strptime(date_part, '%d %B at %H:%M')
                            parsed_date = parsed_date.replace(year=current_year)
                            fixture_data['parsed_datetime'] = parsed_date.isoformat()
                            fixture_data['datetime_obj'] = parsed_date
                    except Exception as e:
                        fixture_data['parsed_datetime'] = None
                        fixture_data['datetime_obj'] = None
            
            # 4. Extract Logo/Country Info
            img_tag = table.find('img', alt=True)
            if img_tag:
                fixture_data['logo_alt'] = img_tag['alt']
            
            # 5. Check if it's a live match
            live_img = table.find('img', src=lambda x: x and 'live.gif' in x)
            fixture_data['is_live'] = live_img is not None
            
            return fixture_data
                
        except Exception:
            return None

    def get_fixtures_for_sport(self, sport_url):
        # ... (Keep this method as is) ...
        today_fixtures = []
        
        try:
            response = self.session.get(sport_url, timeout=15)
            response.raise_for_status()
            
            with self.stats_lock:
                self.successful_requests += 1
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            today = datetime.now().date()
            today_day = datetime.now().day
            
            fixture_tables = soup.find_all('table', {'cellpadding': '1', 'cellspacing': '2'})
            
            # print(f"📋 Found {len(fixture_tables)} potential fixture tables") # REMOVE CLI OUTPUT
            
            for table in fixture_tables:
                fixture = self._parse_broadcast_item(table)
                if fixture:
                    is_today = False
                    
                    if fixture.get('datetime_obj'):
                        fixture_date = fixture['datetime_obj'].date()
                        if fixture_date == today:
                            is_today = True
                    elif fixture.get('date_time'):
                        date_str = fixture['date_time']
                        if re.search(rf'^{today_day}\s+[A-Za-z]+', date_str) or \
                           re.search(rf'\b{today_day}\s+[A-Za-z]+', date_str):
                            is_today = True
                    
                    if is_today:
                        is_duplicate = any(
                            f.get('event_id') == fixture.get('event_id') and 
                            f.get('event_id') is not None
                            for f in today_fixtures
                        )
                        
                        if not is_duplicate:
                            today_fixtures.append(fixture)
            
            # print(f"✅ Parsed {len(today_fixtures)} fixtures for today") # REMOVE CLI OUTPUT
            return [], today_fixtures
            
        except requests.exceptions.RequestException as e:
            with self.stats_lock:
                self.failed_requests += 1
            # print(f"❌ Request failed: {e}") # REMOVE CLI OUTPUT
            return [], []
        except Exception as e:
            # print(f"❌ Error parsing fixtures: {e}") # REMOVE CLI OUTPUT
            return [], []

    def scrape_team_logos(self, event_url):
        # ... (Keep this method as is) ...
        try:
            response = self.session.get(event_url, timeout=10)
            response.raise_for_status()
            
            with self.stats_lock:
                self.successful_requests += 1
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            team_logos = []
            logo_images = soup.find_all('img', itemprop='image', alt=True)
            
            for img in logo_images:
                logo_url = img.get('src', '')
                if logo_url:
                    if logo_url.startswith('//'):
                        logo_url = 'https:' + logo_url
                    elif logo_url.startswith('/'):
                        logo_url = urljoin(self.base_url, logo_url)
                    
                    team_logos.append({
                        'team_name': img.get('alt', '').strip(),
                        'logo_url': logo_url,
                        'style': img.get('style', '')
                    })
            
            return team_logos
            
        except Exception as e:
            with self.stats_lock:
                self.failed_requests += 1
            return []

    def _parse_stream_table(self, table):
        # ... (Keep this method as is) ...
        try:
            stream_data = {}
            cells = table.find_all('td')
            if len(cells) < 7:
                return None
            
            # 1. Language/flag info
            flag_img = cells[0].find('img')
            if flag_img:
                stream_data['language'] = flag_img.get('title', '')
                stream_data['flag_src'] = flag_img.get('src', '')
                if stream_data['flag_src'].startswith('//'):
                    stream_data['flag_src'] = 'https:' + stream_data['flag_src']
            
            # 2. Bitrate
            bitrate_cell = cells[1]
            stream_data['bitrate'] = bitrate_cell.get('title', '')
            
            # 3. Rating information
            rating_div = table.find('div', id=lambda x: x and x.startswith('rali'))
            if rating_div:
                stream_data['rating'] = rating_div.get_text(strip=True)
                stream_data['rating_color'] = rating_div.get('style', '')
            
            # 4. Stream link
            play_link = cells[5].find('a') if len(cells) > 5 else None
            if play_link:
                stream_url = play_link.get('href', '')
                if stream_url:
                    if stream_url.startswith('//'):
                        stream_url = 'https:' + stream_url
                    elif stream_url.startswith('/'):
                        stream_url = urljoin(self.base_url, stream_url)
                    
                    stream_data['stream_url'] = stream_url
                    stream_data['stream_title'] = play_link.get('title', '')
            
            # 5. Stream type/description
            if len(cells) > 6:
                type_cell = cells[6]
                type_span = type_cell.find('span')
                if type_span:
                    stream_data['stream_type'] = type_span.get_text(strip=True)
                else:
                    stream_data['stream_type'] = type_cell.get_text(strip=True)
            
            return stream_data
            
        except Exception:
            return None

    def get_event_details_concurrent(self, event_url):
        # ... (Keep this method as is) ...
        try:
            response = self.session.get(event_url, timeout=15)
            response.raise_for_status()
            
            with self.stats_lock:
                self.successful_requests += 1
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            event_data = {
                'event_url': event_url,
                'team_logos': [],
                'streams': [],
                'starting_lineups': {},
                'match_info': {}
            }
            
            # 1. Extract team logos
            logo_images = soup.find_all('img', itemprop='image', alt=True)
            for img in logo_images:
                logo_url = img.get('src', '')
                if logo_url:
                    if logo_url.startswith('//'):
                        logo_url = 'https:' + logo_url
                    elif logo_url.startswith('/'):
                        logo_url = urljoin(self.base_url, logo_url)
                    
                    event_data['team_logos'].append({
                        'team_name': img.get('alt', '').strip(),
                        'logo_url': logo_url,
                        'style': img.get('style', '')
                    })
            
            # 2. Extract stream links from the links_block
            links_block = soup.find('div', id='links_block')
            if links_block:
                stream_tables = links_block.find_all('table', class_='lnktbj')
                
                if stream_tables:
                    with ThreadPoolExecutor(max_workers=min(self.max_workers, len(stream_tables))) as executor:
                        futures = [executor.submit(self._parse_stream_table, table) for table in stream_tables]
                        
                        for future in as_completed(futures):
                            try:
                                stream_data = future.result(timeout=5)
                                if stream_data:
                                    event_data['streams'].append(stream_data)
                            except Exception:
                                continue
            
            return event_data
            
        except Exception as e:
            with self.stats_lock:
                self.failed_requests += 1
            return None

    def process_fixture_concurrent(self, fixture):
        # ... (Keep this method as is, removing CLI print/progress) ...
        try:
            event_url = fixture.get('event_url')
            
            if event_url:
                detailed_info = self.get_event_details_concurrent(event_url)
                
                if detailed_info:
                    if detailed_info.get('team_logos'):
                        fixture['team_logos'] = detailed_info['team_logos']
                    
                    if detailed_info.get('streams'):
                        fixture['streams'] = detailed_info['streams']
                
                # Update progress
                with self.progress_lock:
                    self.completed_tasks += 1
                    # self._show_progress() # REMOVE CLI PROGRESS BAR
                
            return fixture
            
        except Exception as e:
            # print(f"\n⚠️ Error processing fixture: {e}") # REMOVE CLI OUTPUT
            with self.progress_lock:
                self.completed_tasks += 1
                # self._show_progress() # REMOVE CLI PROGRESS BAR
            return fixture

    def _show_progress(self):
        # ... (Remove or comment out this method entirely, as the API doesn't use stdout) ...
        pass
    
    def process_all_fixtures_concurrent(self, fixtures):
        """
        Process all fixtures concurrently using ThreadPoolExecutor.
        """
        if not fixtures:
            return fixtures
        
        # Initialize progress tracking
        with self.progress_lock:
            self.completed_tasks = 0
            self.total_tasks = len(fixtures)
        
        # print(f"\n🚀 Starting concurrent processing of {len(fixtures)} fixtures with {self.max_workers} workers...") # REMOVE CLI OUTPUT
        
        processed_fixtures = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {executor.submit(self.process_fixture_concurrent, fixture): i 
                       for i, fixture in enumerate(fixtures)}
            
            # Process results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    processed_fixtures.append(result)
                except Exception as e:
                    # idx = futures[future] # KEEP THIS FOR DEBUG IF NEEDED
                    # print(f"\n❌ Task {idx} failed: {e}") # REMOVE CLI OUTPUT
                    # Append original fixture on failure (optional)
                    # if idx < len(fixtures):
                    #     processed_fixtures.append(fixtures[idx])
                    pass
        
        # sys.stdout.write("\r" + " " * 80 + "\r") # REMOVE CLI OUTPUT
        
        return processed_fixtures

# --- END OF BroadcastScraper Class ---


# 💡 EXPORTABLE FUNCTION FOR API
def run_scraper_and_get_data():
    """
    Runs the full scraping process and returns the final list of fixtures.
    This function replaces the old main().
    """
    # Create scraper with 10 workers
    scraper = BroadcastScraper(max_workers=10) 
    sport_url = "https://livetv.sx/enx/allupcomingsports/1/"
    
    # 1. Get TODAY'S fixtures only
    _, today_fixtures = scraper.get_fixtures_for_sport(sport_url)
    
    if not today_fixtures:
        return []
        
    # 2. Process all fixtures concurrently (team logos + event details)
    fixtures_with_details = scraper.process_all_fixtures_concurrent(today_fixtures)
    
    # 3. Return the processed list
    return fixtures_with_details

# ❌ REMOVE THE ORIGINAL main() and if __name__ == "__main__": BLOCKS
