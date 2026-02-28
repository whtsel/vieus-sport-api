import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import urllib3
import re
from urllib.parse import urljoin
import threading
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress SSL warnings for production stability
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class BroadcastScraper:
    def __init__(self, base_url="https://livetv.sx", max_workers=10):
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.verify = False 
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        })
        self.stats_lock = threading.Lock()
        self.successful_requests = 0
        self.failed_requests = 0

    def _parse_broadcast_item(self, table):
        fixture_data = {}
        try:
            link_tag = table.find('a', class_='live') or table.find('a', class_='bottomgray')
            if not link_tag: return None
            
            fixture_data['matchup'] = link_tag.get_text(strip=True)
            stream_href = link_tag.get('href', '')
            if stream_href:
                fixture_data['event_url'] = self.base_url + stream_href if stream_href.startswith('/') else stream_href
                match = re.search(r'/eventinfo/(\d+)', stream_href)
                if match: fixture_data['event_id'] = match.group(1)
            
            evdesc_span = table.find('span', class_='evdesc')
            if evdesc_span:
                desc_parts = [p.strip() for p in evdesc_span.get_text('\n').split('\n') if p.strip()]
                if desc_parts:
                    fixture_data['date_time'] = desc_parts[0]
                    if len(desc_parts) >= 2: fixture_data['competition'] = desc_parts[1].strip('()')
                    try:
                        month_pattern = r'(\d+)\s+([A-Za-z]+)\s+at\s+(\d+:\d+)'
                        m = re.search(month_pattern, desc_parts[0])
                        if m:
                            date_part = f"{m.group(1)} {m.group(2)} at {m.group(3)}"
                            parsed_date = datetime.strptime(date_part, '%d %B at %H:%M').replace(year=datetime.now().year)
                            fixture_data['parsed_datetime'] = parsed_date.isoformat()
                            fixture_data['datetime_obj'] = parsed_date
                    except: pass
            
            img_tag = table.find('img', alt=True)
            if img_tag: fixture_data['logo_alt'] = img_tag['alt']
            live_img = table.find('img', src=lambda x: x and 'live.gif' in x)
            fixture_data['is_live'] = live_img is not None
            return fixture_data
        except: return None

    def get_fixtures_for_sport(self, sport_url):
        today_fixtures = []
        try:
            response = self.session.get(sport_url, timeout=15)
            response.raise_for_status()
            with self.stats_lock: self.successful_requests += 1
            soup = BeautifulSoup(response.content, 'html.parser')
            today_day = datetime.now().day
            fixture_tables = soup.find_all('table', {'cellpadding': '1', 'cellspacing': '2'})
            
            for table in fixture_tables:
                fixture = self._parse_broadcast_item(table)
                if fixture:
                    is_today = False
                    if fixture.get('datetime_obj'):
                        if fixture['datetime_obj'].day == today_day: is_today = True
                    elif fixture.get('date_time'):
                        if str(today_day) in fixture['date_time']: is_today = True
                    
                    if is_today and not any(f.get('event_id') == fixture.get('event_id') for f in today_fixtures if f.get('event_id')):
                        today_fixtures.append(fixture)
            return today_fixtures
        except:
            with self.stats_lock: self.failed_requests += 1
            return []

    def _extract_lineups(self, soup):
        lineups = {"home_team": [], "away_team": []}
        try:
            lineup_header = soup.find('span', string=re.compile(r'Starting Lineup', re.I))
            if lineup_header:
                row = lineup_header.find_parent('tr').find_next_sibling('tr')
                cells = row.find_all('td', class_='small', limit=2)
                keys = ["home_team", "away_team"]
                for idx, key in enumerate(keys):
                    if idx < len(cells):
                        players = cells[idx].get_text('\n', strip=True).split('\n')
                        lineups[key] = [p.strip() for p in players if p.strip()]
        except: pass
        return lineups

    def _extract_league_table(self, soup):
        standings = []
        try:
            header = soup.find('b', string='Pl')
            if not header: return standings
            table = header.find_parent('table')
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 6:
                    pos = cols[0].find('span', class_='date')
                    team = cols[1].find('a', class_='ps')
                    if pos and team:
                        standings.append({
                            'pos': pos.get_text(strip=True),
                            'team': team.get_text(strip=True),
                            'played': cols[2].get_text(strip=True),
                            'pts': cols[7].get_text(strip=True) if len(cols) > 7 else ""
                        })
        except: pass
        return standings

    def _parse_stream_table(self, table):
        try:
            cells = table.find_all('td')
            if len(cells) < 7: return None
            stream_data = {}
            flag_img = cells[0].find('img')
            if flag_img:
                src = flag_img.get('src', '')
                stream_data['language'] = flag_img.get('title', '')
                stream_data['flag_src'] = 'https:' + src if src.startswith('//') else src
            
            stream_data['bitrate'] = cells[1].get('title', '')
            play_link = cells[5].find('a')
            if play_link:
                href = play_link.get('href', '')
                stream_data['stream_url'] = urljoin(self.base_url, href) if href.startswith('/') else href
                stream_data['stream_title'] = play_link.get('title', '')
            
            type_cell = cells[6]
            stream_data['stream_type'] = type_cell.get_text(strip=True)
            return stream_data
        except: return None

    def get_event_details_concurrent(self, event_url):
        try:
            response = self.session.get(event_url, timeout=15)
            response.raise_for_status()
            with self.stats_lock: self.successful_requests += 1
            soup = BeautifulSoup(response.content, 'html.parser')
            
            event_data = {
                'team_logos': [],
                'streams': [],
                'starting_lineups': self._extract_lineups(soup),
                'league_table': self._extract_league_table(soup)
            }
            
            logos = soup.find_all('img', itemprop='image', alt=True)
            for img in logos:
                src = img.get('src', '')
                event_data['team_logos'].append({
                    'team_name': img.get('alt', '').strip(),
                    'logo_url': urljoin(self.base_url, src) if src.startswith('/') else src
                })
            
            links_block = soup.find('div', id='links_block')
            if links_block:
                tables = links_block.find_all('table', class_='lnktbj')
                for t in tables:
                    s = self._parse_stream_table(t)
                    if s: event_data['streams'].append(s)
            
            return event_data
        except:
            with self.stats_lock: self.failed_requests += 1
            return None

    def process_fixture_concurrent(self, fixture):
        url = fixture.get('event_url')
        if url:
            details = self.get_event_details_concurrent(url)
            if details:
                fixture.update(details)
        return fixture

def run_scraper_and_get_data():
    scraper = BroadcastScraper(max_workers=10) 
    sport_url = "https://livetv.sx/enx/allupcomingsports/1/"
    
    today_fixtures = scraper.get_fixtures_for_sport(sport_url)
    if not today_fixtures:
        print("No fixtures found for today.")
        return {}
    
    final_data_map = {}
    with ThreadPoolExecutor(max_workers=scraper.max_workers) as executor:
        futures = [executor.submit(scraper.process_fixture_concurrent, f) for f in today_fixtures]
        for future in as_completed(futures):
            try:
                item = future.result(timeout=30)
                event_id = item.get('event_id')
                if not event_id: continue
                
                # Cleanup internal objects before JSON serialization
                if 'datetime_obj' in item: del item['datetime_obj']
                
                final_data_map[event_id] = {
                    "matchup": item.get("matchup", "Unknown Match"),
                    "event_url": item.get("event_url", ""),
                    "competition": item.get("competition", "General"),
                    "date_time": item.get("date_time", ""),
                    "parsed_datetime": item.get("parsed_datetime", ""),
                    "is_live": item.get("is_live", False),
                    "team_logos": item.get("team_logos", []),
                    "streams": item.get("streams", []),
                    "starting_lineups": item.get("starting_lineups", {"home_team": [], "away_team": []}),
                    "league_table": item.get("league_table", []),
                    "last_updated": datetime.now().isoformat()
                }
            except Exception as e:
                print(f"Error processing item: {e}")

    # Atomic Save to Day1.json
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(dir=".", prefix="Day1.json.tmp")
        with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
            json.dump(final_data_map, f, indent=4, ensure_ascii=False)
        os.replace(tmp_path, "Day1.json")
        print(f"✅ Day1.json updated: {len(final_data_map)} items at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"❌ File Save Error: {e}")
        
    return final_data_map

if __name__ == "__main__":
    run_scraper_and_get_data()
