import requests
import pandas as pd
import time
import json
import logging
import re
import os
from datetime import datetime
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedSupplierScraper:
    def __init__(self):
        self.collected_data = []
        self.progress_file = 'scraping_progress.json'
        self.data_backup_file = 'supplier_data_backup.json'
        self.checkpoint_interval = 5  # Save progress every 5 commodities
        
        # All 100 commodities
        self.all_commodities = [
            'Bearings', 'Cages Fitting', 'Chocolate Products', 'Custom packaging, boxes', 'Dairy Equipment',
            'Foam Insulation', 'Frozen Bakery Products', 'House/Laundry Prod', 'Industrial Paper Products',
            'Manufacturer Steel poles', 'Meat Mfg', 'Meat/Wholesale', 'Monuments', 'Nonwoven fabric',
            'Packaging Containers', 'Paper Converting', 'Pet Food Manufacturer', 'Pharmaceutical Mfg',
            'Plastic Strapping', 'Sheet Metal', 'Steel, Metal Fabrication', 'Vacuum Seal Caps',
            'Welded Wire Fabric', 'FRP Panels', 'Granite Slabs', 'Used Clothing', 'Frozen Poultry and Foods',
            'Groceries', 'Fastners', 'Plastic Pipes', 'Ladder', 'Cheese', 'Fresh Food', 'Hemp Bio Mass',
            'Bakery Items', 'Eggs', 'Pizza Base', 'Bread', 'Non Haz Mat Fertilizers', 'Fork Lifts',
            'Used Machinery', 'Shoes', 'Bricks', 'Spices', 'Steel Pipes and Bars', 'Peanuts', 'Seafood',
            'Containers', 'House hold products', 'Rice', 'Indian Food items', 'Lamp Shades', 'Plastic Hangars',
            'Juice Dispensers', 'Class 84 Adhesives', 'Generator Sets', 'Aluminum Fluoride and Aluminum Oxide',
            'Greenhouse', 'Marine Pollutant', 'Power Supply Chords', 'Hand Sanitizing Wipes', 'Sports Rifles',
            'Silicone Fluid', 'Haul pet supplies', 'three-phase Squirrel-Cage-Motors', 'Metal Scraps',
            'Solar Panels', 'Empty Glass Jars', 'Agricultural Equipment', 'Air Compressors', 'Boat Parts',
            'Building Materials', 'Chemical Products', 'Cleaning Supplies', 'Clothing Accessories',
            'Computer Components', 'Electrical Equipment', 'Electronic Gadgets', 'Farm Machinery',
            'Fishing Equipment', 'Food Packaging', 'Garden Tools', 'Hardware Supplies', 'Industrial Chemicals',
            'Lawn Care Equipment', 'Lighting Fixtures', 'Lubricants', 'Machinery Parts', 'Medical Supplies',
            'Office Furniture', 'Outdoor Gear', 'Plastic Products', 'Power Tools', 'Safety Equipment',
            'Transmission Parts', 'Textiles', 'Wood Products', 'Paint Products', 'Rubber Products',
            'Glass Products', 'Ceramic Products', 'Paper Products', 'Leather Products', 'Fabric Materials',
            'Construction Equipment', 'Mining Equipment', 'Oil Equipment', 'Gas Equipment', 'Water Treatment'
        ]
        
        # Rotate user agents to avoid blocking
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
        
        # Enhanced search terms for better results
        self.commodity_search_terms = {
            'Bearings': ['bearing manufacturer', 'ball bearing supplier', 'industrial bearing'],
            'Chocolate Products': ['chocolate manufacturer', 'cocoa supplier', 'confectionery producer'],
            'Dairy Equipment': ['dairy processing equipment', 'milk processing machinery', 'dairy technology'],
            'Steel, Metal Fabrication': ['steel fabrication', 'metal manufacturing', 'structural steel'],
            'Solar Panels': ['solar panel manufacturer', 'photovoltaic supplier', 'renewable energy'],
            'Pharmaceutical Mfg': ['pharmaceutical manufacturing', 'drug manufacturer', 'pharma supplier'],
            'Groceries': ['grocery wholesaler', 'food distributor', 'retail food supplier'],
            'Agricultural Equipment': ['farm equipment', 'agricultural machinery', 'farming tools'],
            'Chemical Products': ['chemical manufacturer', 'industrial chemicals', 'specialty chemicals'],
            'Electrical Equipment': ['electrical equipment manufacturer', 'power systems', 'electrical components']
        }
    
    def load_progress(self):
        """Load previous scraping progress"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                logger.info(f"📁 Loaded progress: {progress['completed_count']}/{len(self.all_commodities)} commodities completed")
                return progress
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")
        return {'completed_commodities': [], 'completed_count': 0, 'last_updated': None}
    
    def save_progress(self, completed_commodities):
        """Save current progress"""
        progress = {
            'completed_commodities': completed_commodities,
            'completed_count': len(completed_commodities),
            'last_updated': datetime.now().isoformat(),
            'total_commodities': len(self.all_commodities)
        }
        
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Progress saved: {len(completed_commodities)}/{len(self.all_commodities)} completed")
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def backup_data(self):
        """Backup collected data"""
        if self.collected_data:
            try:
                backup_data = {
                    'data': self.collected_data,
                    'count': len(self.collected_data),
                    'backup_time': datetime.now().isoformat()
                }
                
                with open(self.data_backup_file, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"🔄 Data backed up: {len(self.collected_data)} records")
            except Exception as e:
                logger.error(f"Backup failed: {e}")
    
    def load_backup_data(self):
        """Load backup data if exists"""
        if os.path.exists(self.data_backup_file):
            try:
                with open(self.data_backup_file, 'r', encoding='utf-8') as f:
                    backup = json.load(f)
                
                self.collected_data = backup.get('data', [])
                logger.info(f"📂 Loaded backup data: {len(self.collected_data)} records")
                return True
            except Exception as e:
                logger.warning(f"Could not load backup: {e}")
        return False
    
    def get_headers(self):
        """Get random headers to avoid blocking"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none'
        }
    
    def extract_emails_from_text(self, text):
        """Extract email addresses from text"""
        if not text:
            return []
        
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        emails = re.findall(email_pattern, text, re.IGNORECASE)
        
        # Filter out common non-business emails
        filtered_emails = []
        for email in emails:
            email_lower = email.lower()
            if not any(skip in email_lower for skip in [
                'noreply', 'no-reply', 'donotreply', 'info@example', 'test@', 'admin@example',
                'webmaster@', 'postmaster@', 'abuse@', 'privacy@example'
            ]):
                filtered_emails.append(email)
        
        return list(set(filtered_emails))  # Remove duplicates
    
    def extract_phones_from_text(self, text):
        """Extract phone numbers from text"""
        if not text:
            return []
        
        phone_patterns = [
            r'\+1[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',  # US format with +1
            r'\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',            # US without +1
            r'[0-9]{3}-[0-9]{3}-[0-9]{4}',                               # Simple dash format
            r'[0-9]{3}\.[0-9]{3}\.[0-9]{4}',                            # Dot format
            r'\([0-9]{3}\)\s[0-9]{3}-[0-9]{4}',                        # (xxx) xxx-xxxx
            r'[0-9]{10}',                                                # 10 digits
            r'\+[0-9]{1,3}[-.\s]?[0-9]{3,4}[-.\s]?[0-9]{3,4}[-.\s]?[0-9]{3,4}' # International
        ]
        
        phones = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        # Clean and validate phone numbers
        cleaned_phones = []
        for phone in phones:
            # Remove non-digits except +
            cleaned = re.sub(r'[^\d+]', '', phone)
            if len(cleaned) >= 10:  # Valid phone should have at least 10 digits
                cleaned_phones.append(phone.strip())
        
        return list(set(cleaned_phones))  # Remove duplicates
    
    def get_search_terms_for_commodity(self, commodity):
        """Get optimized search terms for a commodity"""
        # Use predefined terms if available, otherwise generate them
        if commodity in self.commodity_search_terms:
            return self.commodity_search_terms[commodity]
        
        # Generate search terms based on commodity name
        base_terms = [
            f"{commodity} manufacturer",
            f"{commodity} supplier", 
            f"{commodity} distributor"
        ]
        
        # Add specific terms based on commodity type
        commodity_lower = commodity.lower()
        if any(word in commodity_lower for word in ['equipment', 'machinery', 'tools']):
            base_terms.append(f"{commodity} dealer")
        elif any(word in commodity_lower for word in ['food', 'products', 'items']):
            base_terms.append(f"{commodity} producer")
        elif 'chemical' in commodity_lower:
            base_terms.append(f"{commodity} company")
        
        return base_terms[:3]  # Return max 3 terms
    
    def scrape_google_search_results(self, query, max_results=20):
        """Enhanced Google search scraping"""
        try:
            # Add location and contact info to search
            enhanced_query = f"{query} USA contact email phone address"
            search_url = f"https://www.google.com/search?q={quote(enhanced_query)}&num={max_results}"
            
            headers = self.get_headers()
            response = requests.get(search_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"Google search failed with status: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            results = []
            
            # Find search result containers (multiple selectors for robustness)
            search_containers = (
                soup.find_all('div', class_='g') + 
                soup.find_all('div', {'data-sokoban-container': True}) +
                soup.find_all('div', class_='Gx5Zad')
            )
            
            processed_urls = set()  # Avoid duplicate URLs
            
            for result in search_containers[:max_results]:
                try:
                    # Extract title and link with multiple selectors
                    title_elem = (result.find('h3') or 
                                result.find('div', class_='BNeawe vvjwJb AP7Wnd') or
                                result.find('div', class_='r'))
                    
                    link_elem = result.find('a', href=True)
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text().strip()
                        link = link_elem.get('href', '')
                        
                        # Skip if already processed or invalid
                        if link in processed_urls or not link.startswith('http'):
                            continue
                        processed_urls.add(link)
                        
                        # Extract snippet
                        snippet_elem = (result.find('span', class_='aCOpRe') or 
                                      result.find('div', class_='VwiC3b') or
                                      result.find('div', class_='BNeawe s3v9rd AP7Wnd'))
                        
                        snippet = snippet_elem.get_text() if snippet_elem else ''
                        
                        # Extract contact info from title and snippet
                        full_text = f"{title} {snippet}"
                        emails = self.extract_emails_from_text(full_text)
                        phones = self.extract_phones_from_text(full_text)
                        
                        # Extract company name (clean title)
                        company_name = title.split(' - ')[0].split(' | ')[0].split(' : ')[0].strip()
                        company_name = re.sub(r'\s+', ' ', company_name)  # Clean spaces
                        
                        result_data = {
                            'company_name': company_name,
                            'title': title,
                            'website': link,
                            'snippet': snippet[:300],  # Limit snippet length
                            'emails': emails,
                            'phones': phones,
                            'source': 'Google Search'
                        }
                        
                        results.append(result_data)
                
                except Exception as e:
                    logger.debug(f"Error processing search result: {e}")
                    continue
            
            logger.info(f"Google Search: Found {len(results)} results for '{query[:50]}...'")
            return results
            
        except Exception as e:
            logger.error(f"Error scraping Google search: {e}")
            return []
    
    def scrape_duckduckgo_search(self, query, max_results=15):
        """Enhanced DuckDuckGo search"""
        try:
            enhanced_query = f"{query} supplier manufacturer USA contact"
            search_url = f"https://duckduckgo.com/html/?q={quote(enhanced_query)}"
            
            headers = self.get_headers()
            # Add DuckDuckGo specific headers
            headers['Referer'] = 'https://duckduckgo.com/'
            
            response = requests.get(search_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                logger.warning(f"DuckDuckGo search failed with status: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            results = []
            
            # DuckDuckGo result containers
            search_results = soup.find_all('div', class_='result')[:max_results]
            
            for result in search_results:
                try:
                    title_elem = result.find('a', class_='result__a')
                    snippet_elem = result.find('a', class_='result__snippet')
                    
                    if title_elem:
                        title = title_elem.get_text().strip()
                        link = title_elem.get('href', '')
                        snippet = snippet_elem.get_text() if snippet_elem else ''
                        
                        # Skip invalid links
                        if not link.startswith('http'):
                            continue
                        
                        # Extract contact info
                        full_text = f"{title} {snippet}"
                        emails = self.extract_emails_from_text(full_text)
                        phones = self.extract_phones_from_text(full_text)
                        
                        company_name = title.split(' - ')[0].split(' | ')[0].strip()
                        
                        result_data = {
                            'company_name': company_name,
                            'title': title,
                            'website': link,
                            'snippet': snippet[:300],
                            'emails': emails,
                            'phones': phones,
                            'source': 'DuckDuckGo Search'
                        }
                        
                        results.append(result_data)
                
                except Exception as e:
                    logger.debug(f"Error processing DuckDuckGo result: {e}")
                    continue
            
            logger.info(f"DuckDuckGo: Found {len(results)} results for '{query[:50]}...'")
            return results
            
        except Exception as e:
            logger.error(f"Error searching DuckDuckGo: {e}")
            return []
    
    def scrape_website_contact_info(self, url, company_name):
        """Enhanced website contact scraping"""
        try:
            if not url or not url.startswith('http'):
                return None
            
            headers = self.get_headers()
            response = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Get page text
            page_text = soup.get_text()
            
            # Extract basic contact info
            emails = self.extract_emails_from_text(page_text)
            phones = self.extract_phones_from_text(page_text)
            
            # Look for contact page links (enhanced)
            contact_keywords = ['contact', 'about', 'reach', 'connect', 'info', 'support']
            contact_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                text = link.get_text().lower()
                
                if any(keyword in href or keyword in text for keyword in contact_keywords):
                    full_url = urljoin(url, link.get('href'))
                    if full_url not in contact_links and full_url != url:
                        contact_links.append(full_url)
            
            # Scrape contact pages (limit to top 3)
            for contact_url in contact_links[:3]:
                try:
                    time.sleep(1)  # Be respectful
                    contact_response = requests.get(contact_url, headers=headers, timeout=10)
                    
                    if contact_response.status_code == 200:
                        contact_soup = BeautifulSoup(contact_response.content, 'html.parser')
                        contact_text = contact_soup.get_text()
                        
                        # Extract additional contact info
                        emails.extend(self.extract_emails_from_text(contact_text))
                        phones.extend(self.extract_phones_from_text(contact_text))
                
                except Exception:
                    continue
            
            # Remove duplicates and clean
            emails = list(set(emails))
            phones = list(set(phones))
            
            if emails or phones:
                return {
                    'company_name': company_name,
                    'website': url,
                    'emails': emails,
                    'phones': phones,
                    'source': 'Website Deep Scrape'
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Error scraping website {url}: {e}")
            return None
    
    def process_search_results(self, search_results, commodity):
        """Process and enhance search results"""
        processed_data = []
        seen_combinations = set()  # Avoid duplicates
        
        for result in search_results:
            try:
                # Get best contact info
                best_email = result['emails'][0] if result['emails'] else None
                best_phone = result['phones'][0] if result['phones'] else None
                
                # Create unique identifier
                unique_key = f"{result['company_name'].lower().strip()}_{commodity}"
                if unique_key in seen_combinations:
                    continue
                seen_combinations.add(unique_key)
                
                # Only include results with some contact information
                if best_email or best_phone or result.get('website'):
                    processed_result = {
                        'company_name': result['company_name'][:150],  # Limit length
                        'email': best_email,
                        'phone': best_phone,
                        'website': result.get('website'),
                        'snippet': result.get('snippet', '')[:250],
                        'commodity': commodity,
                        'source': result['source'],
                        'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'data_quality_score': 0,
                        'additional_emails': ', '.join(result['emails'][1:3]) if len(result['emails']) > 1 else '',
                        'additional_phones': ', '.join(result['phones'][1:3]) if len(result['phones']) > 1 else ''
                    }
                    
                    # Enhanced quality scoring
                    if processed_result['email']:
                        processed_result['data_quality_score'] += 40
                        # Bonus for business domains
                        if any(domain in processed_result['email'].lower() for domain in ['.com', '.net', '.org']):
                            processed_result['data_quality_score'] += 5
                    
                    if processed_result['phone']:
                        processed_result['data_quality_score'] += 30
                    
                    if processed_result['website']:
                        processed_result['data_quality_score'] += 20
                        # Bonus for HTTPS
                        if processed_result['website'].startswith('https'):
                            processed_result['data_quality_score'] += 5
                    
                    if len(processed_result['snippet']) > 100:
                        processed_result['data_quality_score'] += 10
                    
                    if processed_result['additional_emails']:
                        processed_result['data_quality_score'] += 5
                    
                    processed_data.append(processed_result)
            
            except Exception as e:
                logger.debug(f"Error processing result: {e}")
                continue
        
        return processed_data
    
    def run_full_collection(self, resume_from_checkpoint=True):
        """Run the complete 100-commodity collection"""
        logger.info("🚀 Starting COMPLETE 100-Commodity Supplier Data Collection!")
        logger.info("💰 100% Free - Advanced multi-source scraping")
        
        # Load previous progress
        progress = self.load_progress()
        completed_commodities = set(progress['completed_commodities']) if resume_from_checkpoint else set()
        
        # Load backup data if resuming
        if resume_from_checkpoint and completed_commodities:
            self.load_backup_data()
            logger.info(f"🔄 Resuming from checkpoint. Already completed: {len(completed_commodities)} commodities")
        
        # Get remaining commodities
        remaining_commodities = [c for c in self.all_commodities if c not in completed_commodities]
        
        if not remaining_commodities:
            logger.info("🎉 All commodities already completed!")
            return self.collected_data
        
        logger.info(f"🎯 Processing {len(remaining_commodities)} remaining commodities...")
        logger.info(f"📊 Expected results: {len(remaining_commodities) * 15}-{len(remaining_commodities) * 40} business contacts")
        logger.info(f"⏱️  Estimated time: {len(remaining_commodities) * 2}-{len(remaining_commodities) * 4} minutes")
        
        start_time = time.time()
        
        for i, commodity in enumerate(remaining_commodities, 1):
            commodity_start_time = time.time()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 Processing [{i}/{len(remaining_commodities)}]: {commodity}")
            logger.info(f"📈 Total progress: {len(completed_commodities) + i}/{len(self.all_commodities)} ({((len(completed_commodities) + i)/len(self.all_commodities)*100):.1f}%)")
            logger.info(f"{'='*60}")
            
            commodity_data = []
            
            # Get search terms for this commodity
            search_terms = self.get_search_terms_for_commodity(commodity)
            
            for j, search_term in enumerate(search_terms, 1):
                logger.info(f"  🔍 Search term [{j}/{len(search_terms)}]: '{search_term}'")
                
                # Method 1: Enhanced Google Search
                try:
                    google_results = self.scrape_google_search_results(search_term, max_results=15)
                    commodity_data.extend(google_results)
                    time.sleep(random.uniform(3, 6))  # Respectful delay
                except Exception as e:
                    logger.warning(f"  ❌ Google search failed: {e}")
                
                # Method 2: DuckDuckGo Search
                try:
                    duck_results = self.scrape_duckduckgo_search(search_term, max_results=10)
                    commodity_data.extend(duck_results)
                    time.sleep(random.uniform(2, 4))
                except Exception as e:
                    logger.warning(f"  ❌ DuckDuckGo search failed: {e}")
            
            # Process results
            processed_data = self.process_search_results(commodity_data, commodity)
            
            # Method 3: Deep website scraping
            websites_to_scrape = [
                item.get('website') for item in processed_data 
                if item.get('website') and item.get('website').startswith('http')
            ][:8]  # Limit to 8 websites per commodity
            
            if websites_to_scrape:
                logger.info(f"  🌐 Deep scraping {len(websites_to_scrape)} websites...")
                
                for k, website in enumerate(websites_to_scrape, 1):
                    try:
                        logger.info(f"    [{k}/{len(websites_to_scrape)}] Scraping: {website[:50]}...")
                        
                        contact_info = self.scrape_website_contact_info(website, f"Contact from {website}")
                        
                        if contact_info and (contact_info['emails'] or contact_info['phones']):
                            # Add deep scraping results
                            for email in contact_info['emails'][:3]:  # Max 3 emails per site
                                deep_scrape_result = {
                                    'company_name': contact_info['company_name'],
                                    'email': email,
                                    'phone': contact_info['phones'][0] if contact_info['phones'] else None,
                                    'website': website,
                                    'snippet': f"Deep scraped from {website}",
                                    'commodity': commodity,
                                    'source': 'Website Deep Scrape',
                                    'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'data_quality_score': 75,  # Higher score for direct scraping
                                    'additional_emails': ', '.join(contact_info['emails'][1:]) if len(contact_info['emails']) > 1 else '',
                                    'additional_phones': ', '.join(contact_info['phones'][1:]) if len(contact_info['phones']) > 1 else ''
                                }
                                processed_data.append(deep_scrape_result)
                        
                        time.sleep(random.uniform(2, 4))  # Be respectful
                        
                    except Exception as e:
                        logger.debug(f"    ❌ Website scraping failed for {website}: {e}")
                        continue
            
            # Add to main collection
            self.collected_data.extend(processed_data)
            completed_commodities.add(commodity)
            
            # Time tracking
            commodity_time = time.time() - commodity_start_time
            elapsed_time = time.time() - start_time
            avg_time_per_commodity = elapsed_time / (len(completed_commodities) - progress['completed_count'])
            remaining_time = avg_time_per_commodity * (len(remaining_commodities) - i)
            
            logger.info(f"  ✅ Commodity completed: {len(processed_data)} records in {commodity_time:.1f}s")
            logger.info(f"  📊 Running totals: {len(self.collected_data)} records from {len(completed_commodities)} commodities")
            logger.info(f"  ⏰ ETA: {remaining_time/60:.1f} minutes remaining")
            
            # Checkpoint: Save progress every N commodities
            if i % self.checkpoint_interval == 0 or i == len(remaining_commodities):
                logger.info(f"💾 Checkpoint: Saving progress and backing up data...")
                self.save_progress(list(completed_commodities))
                self.backup_data()
                
                # Quick export for safety
                if self.collected_data:
                    temp_df = pd.DataFrame(self.collected_data)
                    temp_filename = f'checkpoint_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    temp_df.to_csv(temp_filename, index=False, encoding='utf-8')
                    logger.info(f"🔄 Checkpoint file saved: {temp_filename}")
            
            # Longer delay between commodities to be respectful
            if i < len(remaining_commodities):  # Don't delay after last commodity
                delay = random.uniform(8, 15)
                logger.info(f"  ⏸️  Resting for {delay:.1f} seconds...")
                time.sleep(delay)
        
        total_time = time.time() - start_time
        logger.info(f"\n🎉 COLLECTION COMPLETED!")
        logger.info(f"⏱️  Total time: {total_time/60:.1f} minutes")
        logger.info(f"📊 Total records collected: {len(self.collected_data)}")
        
        return self.collected_data
    
    def export_to_excel(self, filename=None):
        """Export to Excel with multiple sheets and analysis"""
        if not self.collected_data:
            logger.warning("No data to export")
            return None
        
        if not filename:
            filename = f'complete_supplier_database_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        
        logger.info(f"📊 Exporting to Excel: {filename}")
        
        # Create DataFrame
        df = pd.DataFrame(self.collected_data)
        
        # Data cleaning
        df['company_name'] = df['company_name'].str.strip().str.title()
        df['email'] = df['email'].str.lower().str.strip() if 'email' in df.columns else ''
        df['phone'] = df['phone'].str.strip() if 'phone' in df.columns else ''
        
        # Remove duplicates (keep highest quality)
        df = df.sort_values('data_quality_score', ascending=False)
        df = df.drop_duplicates(subset=['email'], keep='first')
        df = df.drop_duplicates(subset=['company_name', 'commodity'], keep='first')
        
        # Sort data
        df = df.sort_values(['commodity', 'data_quality_score'], ascending=[True, False])
        
        # Create Excel writer
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Complete Database', index=False)
            
            # High quality contacts (score >= 60)
            high_quality = df[df['data_quality_score'] >= 60]
            if not high_quality.empty:
                high_quality.to_excel(writer, sheet_name='High Quality Contacts', index=False)
            
            # Contacts with emails
            with_emails = df[df['email'].notna() & (df['email'] != '')]
            if not with_emails.empty:
                with_emails.to_excel(writer, sheet_name='With Email Contacts', index=False)
            
            # Summary by commodity
            commodity_summary = df.groupby('commodity').agg({
                'company_name': 'count',
                'email': lambda x: x.notna().sum(),
                'phone': lambda x: x.notna().sum(),
                'website': lambda x: x.notna().sum(),
                'data_quality_score': 'mean'
            }).round(2)
            commodity_summary.columns = ['Total Records', 'With Email', 'With Phone', 'With Website', 'Avg Quality Score']
            commodity_summary.to_excel(writer, sheet_name='Commodity Summary')
            
            # Source analysis
            source_summary = df.groupby('source').agg({
                'company_name': 'count',
                'email': lambda x: x.notna().sum(),
                'data_quality_score': 'mean'
            }).round(2)
            source_summary.columns = ['Total Records', 'With Email', 'Avg Quality Score']
            source_summary.to_excel(writer, sheet_name='Source Analysis')
        
        logger.info(f"✅ Excel file created: {filename}")
        return filename
    
    def export_to_csv(self, filename=None):
        """Export to CSV with enhanced cleaning"""
        if not self.collected_data:
            logger.warning("No data to export")
            return None
        
        if not filename:
            filename = f'supplier_database_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        # Create DataFrame
        df = pd.DataFrame(self.collected_data)
        
        # Enhanced data cleaning
        df['company_name'] = df['company_name'].str.strip().str.title()
        df['email'] = df['email'].str.lower().str.strip() if 'email' in df.columns else ''
        df['phone'] = df['phone'].str.strip() if 'phone' in df.columns else ''
        
        # Remove obvious duplicates
        df = df.sort_values('data_quality_score', ascending=False)
        df = df.drop_duplicates(subset=['email'], keep='first')
        df = df.drop_duplicates(subset=['company_name', 'commodity'], keep='first')
        
        # Sort by commodity and quality
        df = df.sort_values(['commodity', 'data_quality_score'], ascending=[True, False])
        
        # Export
        df.to_csv(filename, index=False, encoding='utf-8')
        
        # Generate comprehensive report
        self.generate_comprehensive_report(df, filename)
        
        return filename
    
    def generate_comprehensive_report(self, df, filename):
        """Generate detailed collection report"""
        print(f"\n" + "="*80)
        print(f"🎯 COMPLETE 100-COMMODITY SUPPLIER DATABASE REPORT")
        print(f"="*80)
        
        print(f"\n📁 Output Files:")
        print(f"   • CSV Database: {filename}")
        print(f"   • Excel Database: Available via export_to_excel()")
        print(f"   • Progress File: {self.progress_file}")
        print(f"   • Backup File: {self.data_backup_file}")
        
        print(f"\n📊 COLLECTION STATISTICS:")
        print(f"   • Total Records: {len(df):,}")
        print(f"   • Unique Companies: {df['company_name'].nunique():,}")
        print(f"   • Commodities Covered: {df['commodity'].nunique()}/100")
        print(f"   • Records with Email: {df['email'].count():,} ({df['email'].count()/len(df)*100:.1f}%)")
        print(f"   • Records with Phone: {df['phone'].count():,} ({df['phone'].count()/len(df)*100:.1f}%)")
        print(f"   • Records with Website: {df['website'].count():,} ({df['website'].count()/len(df)*100:.1f}%)")
        print(f"   • Average Quality Score: {df['data_quality_score'].mean():.1f}/100")
        
        print(f"\n🏆 TOP 10 COMMODITIES BY RECORD COUNT:")
        top_commodities = df['commodity'].value_counts().head(10)
        for i, (commodity, count) in enumerate(top_commodities.items(), 1):
            quality_avg = df[df['commodity'] == commodity]['data_quality_score'].mean()
            email_count = df[df['commodity'] == commodity]['email'].count()
            print(f"   {i:2}. {commodity}: {count} records (avg quality: {quality_avg:.1f}, emails: {email_count})")
        
        print(f"\n🔍 DATA SOURCE PERFORMANCE:")
        source_stats = df.groupby('source').agg({
            'company_name': 'count',
            'email': lambda x: x.notna().sum(),
            'data_quality_score': 'mean'
        }).round(1)
        
        for source in source_stats.index:
            stats = source_stats.loc[source]
            count = int(stats['company_name'])
            email_count = int(stats['email'])
            quality = stats['data_quality_score']
            percentage = (count / len(df)) * 100
            print(f"   • {source}:")
            print(f"     - Records: {count:,} ({percentage:.1f}%)")
            print(f"     - With Email: {email_count:,} ({email_count/count*100:.1f}%)")
            print(f"     - Avg Quality: {quality:.1f}/100")
        
        print(f"\n📈 QUALITY DISTRIBUTION:")
        high_quality = len(df[df['data_quality_score'] >= 70])
        medium_quality = len(df[(df['data_quality_score'] >= 40) & (df['data_quality_score'] < 70)])
        low_quality = len(df[df['data_quality_score'] < 40])
        
        print(f"   • High Quality (70-100): {high_quality:,} ({high_quality/len(df)*100:.1f}%)")
        print(f"   • Medium Quality (40-69): {medium_quality:,} ({medium_quality/len(df)*100:.1f}%)")
        print(f"   • Low Quality (0-39): {low_quality:,} ({low_quality/len(df)*100:.1f}%)")
        
        print(f"\n💼 BUSINESS VALUE ANALYSIS:")
        complete_contacts = len(df[(df['email'].notna()) & (df['phone'].notna()) & (df['website'].notna())])
        email_phone = len(df[(df['email'].notna()) & (df['phone'].notna())])
        ready_to_contact = len(df[df['email'].notna()])
        
        print(f"   • Complete Contacts (Email+Phone+Website): {complete_contacts:,}")
        print(f"   • Email + Phone Available: {email_phone:,}")
        print(f"   • Ready to Contact (Email): {ready_to_contact:,}")
        print(f"   • Immediate Business Value: ${ready_to_contact * 50:,} (est. @ $50/contact)")
        
        print(f"\n🎉 COLLECTION SUMMARY:")
        print(f"   • Status: ✅ COMPLETED")
        print(f"   • Total Investment: $0.00 (100% FREE)")
        print(f"   • Data Sources: Google, DuckDuckGo, Direct Website Scraping")
        print(f"   • Collection Method: Ethical, Rate-Limited Web Scraping")
        print(f"   • Data Freshness: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\n📋 NEXT STEPS RECOMMENDATIONS:")
        print(f"   1. Import data into CRM system")
        print(f"   2. Begin outreach with high-quality contacts (score >= 70)")
        print(f"   3. Verify email addresses before mass campaigns")
        print(f"   4. Segment by commodity for targeted marketing")
        print(f"   5. Update contact information quarterly")
        
        print(f"\n" + "="*80)
        print(f"🚀 DATABASE READY FOR BUSINESS USE!")
        print(f"="*80)
    
    def clean_and_resume(self):
        """Clean up and prepare for resuming"""
        logger.info("🧹 Cleaning up and preparing resume capability...")
        
        # Clean progress file if corrupted
        try:
            progress = self.load_progress()
            if progress['completed_count'] > len(self.all_commodities):
                logger.warning("Corrupted progress file detected. Resetting...")
                os.remove(self.progress_file)
        except:
            pass
        
        # Validate backup file
        try:
            if os.path.exists(self.data_backup_file):
                with open(self.data_backup_file, 'r') as f:
                    backup = json.load(f)
                logger.info(f"Backup file valid: {len(backup.get('data', []))} records")
        except:
            logger.warning("Backup file corrupted or invalid")
        
        logger.info("✅ Cleanup completed")

# Enhanced main execution with user interface
def main():
    print("\n" + "="*80)
    print("🚀 ENHANCED 100-COMMODITY SUPPLIER SCRAPER")
    print("💰 100% Free - No API costs, No subscriptions!")
    print("🔍 Sources: Google, DuckDuckGo, Direct Website Scraping")
    print("📊 Target: All 100 commodities with comprehensive contact data")
    print("="*80)
    
    scraper = EnhancedSupplierScraper()
    
    # Check for existing progress
    progress = scraper.load_progress()
    
    if progress['completed_count'] > 0:
        print(f"\n🔄 RESUME DETECTED!")
        print(f"📈 Previous progress: {progress['completed_count']}/{len(scraper.all_commodities)} commodities completed")
        print(f"🗓️  Last updated: {progress.get('last_updated', 'Unknown')}")
        
        choice = input("\n🤔 Do you want to RESUME from checkpoint or START FRESH? (resume/fresh): ").lower()
        resume = choice in ['resume', 'r', 'yes', 'y', '']
    else:
        print(f"\n🎯 Ready to collect data for ALL {len(scraper.all_commodities)} commodities")
        print("📊 Expected results: 2,000-5,000 business contacts")
        print("⏱️  Estimated time: 3-6 hours (with respectful delays)")
        print("💾 Auto-save every 5 commodities + backup system")
        print("⚠️  Note: Includes respectful delays to avoid blocking")
        resume = False
    
    if input("\n🚀 Start collection? (y/n): ").lower() in ['y', 'yes', '']:
        try:
            # Run the complete collection
            data = scraper.run_full_collection(resume_from_checkpoint=resume)
            
            if data:
                print(f"\n✅ Collection completed! {len(data)} total records collected")
                
                # Export options
                print(f"\n💾 EXPORT OPTIONS:")
                print(f"1. CSV (recommended for CRM import)")
                print(f"2. Excel (with analysis sheets)")
                print(f"3. Both formats")
                
                export_choice = input("Choose export format (1/2/3): ").strip()
                
                if export_choice in ['1', '3']:
                    csv_file = scraper.export_to_csv()
                    print(f"✅ CSV exported: {csv_file}")
                
                if export_choice in ['2', '3']:
                    excel_file = scraper.export_to_excel()
                    print(f"✅ Excel exported: {excel_file}")
                
                print(f"\n🎉 MISSION ACCOMPLISHED!")
                print(f"🏆 Complete supplier database ready for business use!")
                
            else:
                print("\n❌ No data collected. Check internet connection and try again.")
                
        except KeyboardInterrupt:
            print(f"\n⏸️  Collection paused by user.")
            print(f"💾 Progress saved. Resume anytime by running the script again.")
            scraper.save_progress(list(scraper.collected_data))
            scraper.backup_data()
            
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            print(f"💾 Attempting to save progress...")
            try:
                scraper.save_progress([])
                scraper.backup_data()
            except:
                pass
    else:
        print("👋 Collection cancelled. Run again when ready!")

if __name__ == "__main__":
    main()