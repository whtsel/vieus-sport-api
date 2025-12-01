import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import urllib3
import re

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class BroadcastScraper:
    def __init__(self, base_url="https://m.livetv.sx"):
        self.base_url = base_url
        self.session = requests.Session()
        
        # SSL Bypass: Disable verification
        self.session.verify = False
        
        # User Agent: Set to Mac
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
    
    def scrape_broadcasts(self, page_url):
        """Scrape broadcast information from the schedule page."""
        try:
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
            print(f"Error fetching page: {e}")
            return []
        except Exception as e:
            print(f"Error parsing page: {e}")
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
            print(f"Error parsing item: {e}") 
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

    def save_to_json(self, data, filename):
        """Save scraped data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to {filename}")


def main():
    scraper = BroadcastScraper()
    url = "https://m.livetv.sx/en/allupcoming/"
    
    print(f"Scraping broadcasts from: {url}")
    
    broadcasts = scraper.scrape_broadcasts(url)
    
    if broadcasts:
        # Categorize the data into live and upcoming
        live_fixtures, upcoming_fixtures = scraper.categorize_fixtures(broadcasts)
        
        # Enhance the data for frontend compatibility
        enhanced_live = [scraper.enhance_fixture_data(f) for f in live_fixtures]
        enhanced_upcoming = [scraper.enhance_fixture_data(f) for f in upcoming_fixtures]
        
        # Save the two categories to separate files
        scraper.save_to_json(enhanced_live, 'live.json')
        scraper.save_to_json(enhanced_upcoming, 'upcoming.json')
        
        print("-" * 80)
        print(f"Total broadcasts scraped: {len(broadcasts)}")
        print(f"Live fixtures (with stream links): {len(enhanced_live)}")
        print(f"Upcoming fixtures (without stream links): {len(enhanced_upcoming)}")
        print("-" * 80)
        
        # Print sample data for verification
        if enhanced_live:
            print("\nSample Live Fixture:")
            print(f"  Fixture: {enhanced_live[0].get('fixture')}")
            print(f"  League: {enhanced_live[0].get('league')}")
            print(f"  Time: {enhanced_live[0].get('time')}")
            print(f"  Stream URL: {enhanced_live[0].get('stream_url', 'No stream')}")
        
        if enhanced_upcoming:
            print("\nSample Upcoming Fixture:")
            print(f"  Fixture: {enhanced_upcoming[0].get('fixture')}")
            print(f"  League: {enhanced_upcoming[0].get('league')}")
            print(f"  Time: {enhanced_upcoming[0].get('time')}")
            print(f"  Stream URL: {enhanced_upcoming[0].get('stream_url', 'No stream')}")
            
    else:
        print("\nNo broadcasts found. Please check the URL and try again.")


if __name__ == "__main__":
    main()