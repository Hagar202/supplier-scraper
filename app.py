from flask import Flask, render_template_string, request, jsonify, send_file
import os
import json
import threading
import time
import pandas as pd
from datetime import datetime
import requests
import re
import random
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin
import logging
from io import BytesIO
import urllib3
import sqlite3
from werkzeug.serving import WSGIRequestHandler

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disable Flask request logging in production
class QuietWSGIRequestHandler(WSGIRequestHandler):
    def log_request(self, code='-', size='-'):
        pass

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'supplier-intelligence-pro-2024')

class ProfessionalSupplierScraper:
    def __init__(self):
        self.collected_data = []
        self.is_running = False
        self.progress = 0
        self.current_commodity = ""
        self.total_commodities = 0
        self.status_message = "Ready to start..."
        self.thread = None
        self.init_db()
        
        # Complete commodities list from your original script
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

        # Enhanced user agents rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]

        # Enhanced search terms for better results
        self.commodity_search_terms = {
            'Bearings': ['bearing manufacturer USA', 'ball bearing supplier', 'industrial bearing companies'],
            'Chocolate Products': ['chocolate manufacturer USA', 'cocoa supplier America', 'confectionery producer'],
            'Dairy Equipment': ['dairy processing equipment USA', 'milk processing machinery', 'dairy technology companies'],
            'Steel, Metal Fabrication': ['steel fabrication USA', 'metal manufacturing company', 'structural steel suppliers'],
            'Groceries': ['grocery wholesaler USA', 'food distributor America', 'retail food supplier'],
            'Chemical Products': ['chemical manufacturer USA', 'industrial chemicals supplier', 'specialty chemicals company'],
            'Cleaning Supplies': ['cleaning supplies manufacturer', 'janitorial products supplier', 'cleaning chemicals distributor']
        }

    def init_db(self):
        """Initialize SQLite database with proper schema"""
        try:
            conn = sqlite3.connect('suppliers.db', check_same_thread=False)
            c = conn.cursor()
            
            # Create suppliers table
            c.execute('''CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                website TEXT,
                commodity TEXT NOT NULL,
                source TEXT NOT NULL,
                snippet TEXT,
                quality_score INTEGER DEFAULT 50,
                collection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_name, commodity)
            )''')
            
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")

    def save_to_db(self, supplier_data):
        """Save supplier data to database"""
        try:
            conn = sqlite3.connect('suppliers.db', check_same_thread=False)
            c = conn.cursor()
            
            c.execute('''INSERT OR REPLACE INTO suppliers 
                         (company_name, email, phone, website, commodity, source, snippet, quality_score) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                     (supplier_data['company_name'], 
                      supplier_data.get('email'),
                      supplier_data.get('phone'), 
                      supplier_data.get('website'),
                      supplier_data['commodity'], 
                      supplier_data['source'],
                      supplier_data.get('snippet', ''),
                      supplier_data.get('quality_score', 50)))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Database save error: {e}")

    def get_headers(self):
        """Get randomized headers to avoid blocking"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'no-cache'
        }

    def extract_emails_from_text(self, text):
        """Extract email addresses from text"""
        if not text:
            return []
        
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        emails = re.findall(email_pattern, text, re.IGNORECASE)
        
        # Filter out fake emails
        filtered_emails = []
        for email in emails:
            email_lower = email.lower()
            if not any(skip in email_lower for skip in [
                'noreply', 'no-reply', 'donotreply', 'info@example', 'test@', 'admin@example',
                'webmaster@', 'postmaster@', 'abuse@', 'privacy@example', 'support@example'
            ]):
                filtered_emails.append(email)
        
        return list(set(filtered_emails))

    def extract_phones_from_text(self, text):
        """Extract phone numbers from text"""
        if not text:
            return []
        
        phone_patterns = [
            r'\+1[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',
            r'\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',
            r'[0-9]{3}-[0-9]{3}-[0-9]{4}',
            r'[0-9]{3}\.[0-9]{3}\.[0-9]{4}',
            r'\([0-9]{3}\)\s[0-9]{3}-[0-9]{4}',
            r'[0-9]{10}'
        ]
        
        phones = []
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        # Clean and validate
        cleaned_phones = []
        for phone in phones:
            cleaned = re.sub(r'[^\d+]', '', phone)
            if len(cleaned) >= 10:
                cleaned_phones.append(phone.strip())
        
        return list(set(cleaned_phones))

    def get_search_terms_for_commodity(self, commodity):
        """Get optimized search terms for a commodity"""
        if commodity in self.commodity_search_terms:
            return self.commodity_search_terms[commodity]
        
        # Generate search terms
        base_terms = [
            f"{commodity} manufacturer USA",
            f"{commodity} supplier America", 
            f"{commodity} distributor United States"
        ]
        
        # Add specific terms based on commodity type
        commodity_lower = commodity.lower()
        if any(word in commodity_lower for word in ['equipment', 'machinery', 'tools']):
            base_terms.append(f"{commodity} dealer USA")
        elif any(word in commodity_lower for word in ['food', 'products', 'items']):
            base_terms.append(f"{commodity} producer America")
        elif 'chemical' in commodity_lower:
            base_terms.append(f"{commodity} company USA")
        
        return base_terms[:3]

    def scrape_duckduckgo_search(self, query, max_results=15):
        """Enhanced DuckDuckGo search scraping"""
        try:
            enhanced_query = f"{query} USA contact email phone"
            search_url = f"https://duckduckgo.com/html/?q={quote(enhanced_query)}"
            
            headers = self.get_headers()
            headers['Referer'] = 'https://duckduckgo.com/'
            
            response = requests.get(search_url, headers=headers, timeout=15, verify=False)
            
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

    def scrape_thomasnet_directory(self, commodity):
        """Enhanced ThomasNet directory scraping"""
        try:
            # Multiple ThomasNet search approaches
            search_queries = [
                f"{commodity} suppliers",
                f"{commodity} manufacturers",
                f"{commodity} companies"
            ]
            
            all_results = []
            
            for query in search_queries:
                search_url = f"https://www.thomasnet.com/search.html?cov=NA&what={quote(query)}"
                
                headers = self.get_headers()
                response = requests.get(search_url, headers=headers, timeout=15, verify=False)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for supplier listings
                    suppliers = soup.find_all(['div', 'li'], class_=re.compile(r'supplier|company|listing'))
                    
                    for supplier in suppliers[:5]:  # Limit per query
                        try:
                            # Extract company name
                            name_elem = supplier.find(['h3', 'h4', 'span', 'a'], class_=re.compile(r'name|title|company'))
                            if not name_elem:
                                name_elem = supplier.find('a')
                            
                            if name_elem:
                                company_name = name_elem.get_text().strip()
                                
                                # Extract contact info from surrounding text
                                supplier_text = supplier.get_text()
                                emails = self.extract_emails_from_text(supplier_text)
                                phones = self.extract_phones_from_text(supplier_text)
                                
                                # Try to find website link
                                website_link = supplier.find('a', href=True)
                                website = website_link['href'] if website_link else None
                                if website and not website.startswith('http'):
                                    website = urljoin('https://www.thomasnet.com', website)
                                
                                result_data = {
                                    'company_name': company_name,
                                    'website': website,
                                    'snippet': f'ThomasNet supplier for {commodity}',
                                    'emails': emails,
                                    'phones': phones,
                                    'source': 'ThomasNet Directory'
                                }
                                
                                all_results.append(result_data)
                        except:
                            continue
                
                time.sleep(random.uniform(2, 4))  # Respectful delay
            
            logger.info(f"ThomasNet: Found {len(all_results)} results for '{commodity}'")
            return all_results
            
        except Exception as e:
            logger.error(f"Error scraping ThomasNet: {e}")
            return []

    def scrape_website_contact_info(self, url, company_name):
        """Enhanced website contact scraping"""
        try:
            if not url or not url.startswith('http'):
                return None
            
            headers = self.get_headers()
            response = requests.get(url, headers=headers, timeout=12, allow_redirects=True, verify=False)
            
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            page_text = soup.get_text()
            
            # Extract contact info
            emails = self.extract_emails_from_text(page_text)
            phones = self.extract_phones_from_text(page_text)
            
            # Look for contact page links
            contact_keywords = ['contact', 'about', 'reach', 'connect', 'info', 'support']
            contact_links = []
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                text = link.get_text().lower()
                
                if any(keyword in href or keyword in text for keyword in contact_keywords):
                    full_url = urljoin(url, link.get('href'))
                    if full_url not in contact_links and full_url != url:
                        contact_links.append(full_url)
            
            # Scrape contact pages
            for contact_url in contact_links[:2]:  # Limit to top 2
                try:
                    time.sleep(1)
                    contact_response = requests.get(contact_url, headers=headers, timeout=10, verify=False)
                    
                    if contact_response.status_code == 200:
                        contact_soup = BeautifulSoup(contact_response.content, 'html.parser')
                        contact_text = contact_soup.get_text()
                        
                        emails.extend(self.extract_emails_from_text(contact_text))
                        phones.extend(self.extract_phones_from_text(contact_text))
                
                except Exception:
                    continue
            
            # Remove duplicates
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
        seen_combinations = set()
        
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
                
                # Only include results with contact information
                if best_email or best_phone or result.get('website'):
                    processed_result = {
                        'company_name': result['company_name'][:150],
                        'email': best_email,
                        'phone': best_phone,
                        'website': result.get('website'),
                        'snippet': result.get('snippet', '')[:250],
                        'commodity': commodity,
                        'source': result['source'],
                        'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'quality_score': 0,
                        'additional_emails': ', '.join(result['emails'][1:3]) if len(result['emails']) > 1 else '',
                        'additional_phones': ', '.join(result['phones'][1:3]) if len(result['phones']) > 1 else ''
                    }
                    
                    # Calculate quality score
                    if processed_result['email']:
                        processed_result['quality_score'] += 40
                        if any(domain in processed_result['email'].lower() for domain in ['.com', '.net', '.org']):
                            processed_result['quality_score'] += 5
                    
                    if processed_result['phone']:
                        processed_result['quality_score'] += 30
                    
                    if processed_result['website']:
                        processed_result['quality_score'] += 20
                        if processed_result['website'].startswith('https'):
                            processed_result['quality_score'] += 5
                    
                    if len(processed_result['snippet']) > 100:
                        processed_result['quality_score'] += 10
                    
                    processed_data.append(processed_result)
            
            except Exception as e:
                logger.debug(f"Error processing result: {e}")
                continue
        
        return processed_data

    def process_commodity(self, commodity):
        """Process a single commodity with all sources"""
        self.current_commodity = commodity
        logger.info(f"Processing {commodity}")
        
        all_data = []
        
        # Get search terms
        search_terms = self.get_search_terms_for_commodity(commodity)
        
        # Method 1: DuckDuckGo Search
        for search_term in search_terms:
            try:
                duckduckgo_results = self.scrape_duckduckgo_search(search_term, max_results=10)
                all_data.extend(duckduckgo_results)
                time.sleep(random.uniform(3, 6))
            except Exception as e:
                logger.warning(f"DuckDuckGo search failed for {search_term}: {e}")
        
        # Method 2: ThomasNet Directory
        try:
            thomasnet_results = self.scrape_thomasnet_directory(commodity)
            all_data.extend(thomasnet_results)
            time.sleep(random.uniform(4, 7))
        except Exception as e:
            logger.warning(f"ThomasNet search failed: {e}")
        
        # Process results
        processed_data = self.process_search_results(all_data, commodity)
        
        # Method 3: Deep website scraping
        websites_to_scrape = [
            item.get('website') for item in processed_data 
            if item.get('website') and item.get('website').startswith('http')
        ][:6]  # Limit to 6 websites per commodity
        
        if websites_to_scrape:
            logger.info(f"Deep scraping {len(websites_to_scrape)} websites...")
            
            for website in websites_to_scrape:
                try:
                    contact_info = self.scrape_website_contact_info(website, f"Contact from {website}")
                    
                    if contact_info and (contact_info['emails'] or contact_info['phones']):
                        # Add deep scraping results
                        for email in contact_info['emails'][:2]:
                            deep_result = {
                                'company_name': contact_info['company_name'],
                                'email': email,
                                'phone': contact_info['phones'][0] if contact_info['phones'] else None,
                                'website': website,
                                'snippet': f"Deep scraped contact from {website}",
                                'commodity': commodity,
                                'source': 'Website Deep Scrape',
                                'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'quality_score': 75
                            }
                            processed_data.append(deep_result)
                    
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    logger.debug(f"Website scraping failed for {website}: {e}")
                    continue
        
        # Save to database
        for record in processed_data:
            self.save_to_db(record)
        
        logger.info(f"Completed {commodity}: {len(processed_data)} records")
        return processed_data

    def run_scraping(self, selected_commodities):
        """Run the complete scraping process"""
        self.is_running = True
        self.collected_data = []
        self.total_commodities = len(selected_commodities)
        self.progress = 0
        
        logger.info(f"Starting scraping for {len(selected_commodities)} commodities")
        logger.info("Sources: DuckDuckGo + ThomasNet + Deep Website Scraping")
        
        start_time = time.time()
        
        for i, commodity in enumerate(selected_commodities, 1):
            if not self.is_running:
                break
                
            try:
                self.status_message = f"Processing {commodity}... ({i}/{self.total_commodities})"
                
                commodity_results = self.process_commodity(commodity)
                self.collected_data.extend(commodity_results)
                
                # Update progress
                self.progress = (i / self.total_commodities) * 100
                
                # Time estimation
                elapsed_time = time.time() - start_time
                avg_time_per_commodity = elapsed_time / i
                remaining_time = avg_time_per_commodity * (self.total_commodities - i)
                
                logger.info(f"Progress: {i}/{self.total_commodities} ({self.progress:.1f}%) - ETA: {remaining_time/60:.1f} min")
                
                # Rest between commodities
                if i < len(selected_commodities):
                    delay = random.uniform(8, 15)
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error processing {commodity}: {e}")
                continue
        
        total_time = time.time() - start_time
        self.is_running = False
        self.status_message = f"Completed! Collected {len(self.collected_data)} records in {total_time/60:.1f} minutes"
        logger.info(f"Scraping completed. Total records: {len(self.collected_data)}")

    def get_progress_data(self):
        """Get current progress data"""
        avg_quality = 0
        if self.collected_data:
            quality_scores = [item.get('quality_score', 0) for item in self.collected_data]
            avg_quality = sum(quality_scores) / len(quality_scores)
        
        return {
            'is_running': self.is_running,
            'progress': round(self.progress, 1),
            'total_records': len(self.collected_data),
            'current_commodity': self.current_commodity,
            'status_message': self.status_message,
            'avg_quality': round(avg_quality, 1)
        }

    def stop_scraping(self):
        """Stop the scraping process"""
        self.is_running = False
        self.status_message = "Scraping stopped by user"

    def clear_data(self):
        """Clear all collected data"""
        self.collected_data = []
        self.progress = 0
        self.current_commodity = ""
        self.status_message = "Data cleared - ready to start"
        
        # Clear database
        try:
            conn = sqlite3.connect('suppliers.db', check_same_thread=False)
            c = conn.cursor()
            c.execute('DELETE FROM suppliers')
            conn.commit()
            conn.close()
            logger.info("Database cleared")
        except Exception as e:
            logger.error(f"Error clearing database: {e}")

    def export_to_excel(self):
        """Export collected data to Excel"""
        if not self.collected_data:
            return None
        
        df = pd.DataFrame(self.collected_data)
        
        # Clean and sort data
        df = df.drop_duplicates(subset=['email'], keep='first')
        df = df.drop_duplicates(subset=['company_name', 'commodity'], keep='first')
        df = df.sort_values(['quality_score', 'commodity'], ascending=[False, True])
        
        # Create Excel in memory
        excel_buffer = BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Complete Database', index=False)
            
            # High quality contacts
            high_quality = df[df['quality_score'] >= 60]
            if not high_quality.empty:
                high_quality.to_excel(writer, sheet_name='High Quality Contacts', index=False)
            
            # Contacts with emails
            with_emails = df[df['email'].notna() & (df['email'] != '')]
            if not with_emails.empty:
                with_emails.to_excel(writer, sheet_name='Email Contacts', index=False)
            
            # Summary by commodity
            commodity_summary = df.groupby('commodity').agg({
                'company_name': 'count',
                'email': lambda x: x.notna().sum(),
                'phone': lambda x: x.notna().sum(),
                'website': lambda x: x.notna().sum(),
                'quality_score': 'mean'
            }).round(2)
            commodity_summary.columns = ['Total Records', 'With Email', 'With Phone', 'With Website', 'Avg Quality Score']
            commodity_summary.to_excel(writer, sheet_name='Commodity Summary')
            
            # Source analysis
            source_summary = df.groupby('source').agg({
                'company_name': 'count',
                'email': lambda x: x.notna().sum(),
                'quality_score': 'mean'
            }).round(2)
            source_summary.columns = ['Total Records', 'With Email', 'Avg Quality Score']
            source_summary.to_excel(writer, sheet_name='Source Analysis')
        
        excel_buffer.seek(0)
        return excel_buffer

# Initialize scraper
scraper = ProfessionalSupplierScraper()

# Professional HTML Template
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Supplier Intelligence System - Professional</title>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body { 
            font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh; 
            color: white; 
            line-height: 1.6;
        }
        
        .container { 
            max-width: 1400px; 
            margin: 0 auto; 
            padding: 20px;
        }
        
        .header { 
            text-align: center; 
            margin-bottom: 40px; 
            padding: 30px 0;
            background: rgba(255,255,255,0.1);
            border-radius: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.2);
        }
        
        .title { 
            font-size: 3rem; 
            margin-bottom: 15px; 
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            font-weight: 700;
        }
        
        .subtitle { 
            opacity: 0.9; 
            font-size: 1.3rem;
            margin-bottom: 10px;
        }
        
        .tech-info {
            font-size: 0.9rem;
            opacity: 0.8;
            background: rgba(0,0,0,0.2);
            padding: 10px 20px;
            border-radius: 25px;
            display: inline-block;
            margin-top: 10px;
        }
        
        .card { 
            background: rgba(255,255,255,0.1); 
            backdrop-filter: blur(15px);
            border-radius: 20px; 
            padding: 30px; 
            margin-bottom: 25px;
            border: 1px solid rgba(255,255,255,0.2);
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .card:hover {
            transform: translateY(-2px);
            box-shadow: 0 12px 40px rgba(0,0,0,0.15);
        }
        
        .card h3 {
            margin-bottom: 20px;
            font-size: 1.4rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .commodity-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 15px; 
            max-height: 450px; 
            overflow-y: auto; 
            margin-bottom: 25px;
            padding: 15px;
            background: rgba(0,0,0,0.1);
            border-radius: 15px;
        }
        
        .commodity-item { 
            display: flex; 
            align-items: center; 
            padding: 15px;
            background: rgba(255,255,255,0.1); 
            border-radius: 12px;
            cursor: pointer; 
            transition: all 0.3s ease;
            border: 2px solid transparent;
        }
        
        .commodity-item:hover { 
            background: rgba(255,255,255,0.2); 
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .commodity-item.selected { 
            background: rgba(46, 213, 115, 0.3);
            border-color: rgba(46, 213, 115, 0.5);
            box-shadow: 0 0 20px rgba(46, 213, 115, 0.3);
        }
        
        .commodity-item input { 
            margin-right: 12px; 
            transform: scale(1.2);
            accent-color: #2ed573;
        }
        
        .commodity-item label {
            font-weight: 500;
            cursor: pointer;
            flex: 1;
        }
        
        .selection-controls {
            display: flex;
            gap: 15px;
            margin-bottom: 20px;
            flex-wrap: wrap;
            align-items: center;
        }
        
        .selected-count {
            margin-left: auto;
            font-weight: 600;
            font-size: 1.1rem;
            color: #4ecdc4;
        }
        
        .controls { 
            display: flex; 
            gap: 20px; 
            justify-content: center; 
            flex-wrap: wrap; 
            margin-bottom: 30px;
        }
        
        .btn { 
            padding: 15px 30px; 
            border: none; 
            border-radius: 50px;
            font-weight: 600; 
            font-size: 1rem;
            cursor: pointer; 
            transition: all 0.3s ease;
            text-decoration: none; 
            display: inline-flex;
            align-items: center;
            gap: 10px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .btn-primary { 
            background: linear-gradient(45deg, #ff6b6b, #ee5a24); 
            color: white; 
        }
        
        .btn-secondary { 
            background: linear-gradient(45deg, #4ecdc4, #44a08d); 
            color: white; 
        }
        
        .btn-danger { 
            background: linear-gradient(45deg, #ff4757, #c44569); 
            color: white; 
        }
        
        .btn-success { 
            background: linear-gradient(45deg, #2ed573, #1e3799); 
            color: white; 
        }
        
        .btn:hover { 
            transform: translateY(-3px); 
            box-shadow: 0 8px 25px rgba(0,0,0,0.3); 
        }
        
        .btn:disabled { 
            opacity: 0.6; 
            cursor: not-allowed; 
            transform: none; 
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .btn:disabled:hover {
            transform: none;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .progress-container { 
            margin: 25px 0; 
            display: none; 
        }
        
        .progress-bar { 
            width: 100%; 
            height: 15px; 
            background: rgba(255,255,255,0.2);
            border-radius: 10px; 
            overflow: hidden; 
            margin-bottom: 15px;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .progress-fill { 
            height: 100%; 
            background: linear-gradient(90deg, #4ecdc4, #44a08d, #2ed573);
            transition: width 0.5s ease; 
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(78, 205, 196, 0.4);
            position: relative;
        }
        
        .progress-fill::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(45deg, transparent 49%, rgba(255,255,255,0.3) 50%, transparent 51%);
            animation: progress-stripes 1s linear infinite;
        }
        
        @keyframes progress-stripes {
            0% { background-position: 0 0; }
            100% { background-position: 30px 0; }
        }
        
        .progress-text { 
            text-align: center; 
            font-size: 1rem;
            font-weight: 500;
        }
        
        .status-card { 
            display: none; 
        }
        
        .status-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px; 
            text-align: center;
            margin-bottom: 25px;
        }
        
        .status-item { 
            padding: 25px; 
            background: rgba(255,255,255,0.1); 
            border-radius: 15px;
            border: 1px solid rgba(255,255,255,0.2);
            transition: all 0.3s ease;
        }
        
        .status-item:hover {
            background: rgba(255,255,255,0.15);
            transform: translateY(-2px);
        }
        
        .status-number { 
            font-size: 2.5rem; 
            font-weight: 700; 
            color: #4ecdc4; 
            margin-bottom: 8px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
        }
        
        .status-label {
            font-size: 0.9rem;
            opacity: 0.9;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-message-container {
            background: rgba(0,0,0,0.2);
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            font-size: 1.1rem;
            font-weight: 500;
        }
        
        .alert, .success { 
            border-radius: 12px; 
            padding: 18px 25px; 
            margin: 15px 0; 
            text-align: center;
            display: none;
            font-weight: 500;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }
        
        .alert { 
            background: rgba(255, 107, 107, 0.2); 
            border: 1px solid rgba(255, 107, 107, 0.4);
            color: #ff6b6b;
        }
        
        .success { 
            background: rgba(46, 213, 115, 0.2); 
            border: 1px solid rgba(46, 213, 115, 0.4);
            color: #2ed573;
        }
        
        .data-table { 
            width: 100%; 
            border-collapse: collapse; 
            margin-top: 25px;
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }
        
        .data-table th, .data-table td { 
            padding: 15px 12px; 
            text-align: left; 
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        
        .data-table th { 
            background: rgba(255,255,255,0.1); 
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-size: 0.9rem;
        }
        
        .data-table tbody tr:hover {
            background: rgba(255,255,255,0.05);
        }
        
        .loading { 
            display: inline-block; 
            width: 20px; 
            height: 20px;
            border: 3px solid rgba(255,255,255,0.3);
            border-radius: 50%; 
            border-top-color: #fff;
            animation: spin 1s ease-in-out infinite;
            margin-right: 8px;
        }
        
        @keyframes spin { 
            to { transform: rotate(360deg); } 
        }
        
        .source-info {
            background: rgba(255,255,255,0.08);
            padding: 20px; 
            border-radius: 12px; 
            margin-bottom: 25px;
            font-size: 0.95rem; 
            line-height: 1.6;
            border-left: 4px solid #4ecdc4;
        }
        
        .source-info strong { 
            color: #4ecdc4; 
            font-weight: 600;
        }
        
        .features-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 25px 0;
        }
        
        .feature-item {
            background: rgba(255,255,255,0.08);
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
        }
        
        .feature-icon {
            font-size: 2.5rem;
            margin-bottom: 15px;
            color: #4ecdc4;
        }
        
        .scrollbar-custom::-webkit-scrollbar {
            width: 8px;
        }
        
        .scrollbar-custom::-webkit-scrollbar-track {
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
        }
        
        .scrollbar-custom::-webkit-scrollbar-thumb {
            background: rgba(255,255,255,0.3);
            border-radius: 4px;
        }
        
        .scrollbar-custom::-webkit-scrollbar-thumb:hover {
            background: rgba(255,255,255,0.5);
        }
        
        @media (max-width: 768px) {
            .container { padding: 15px; }
            .title { font-size: 2rem; }
            .subtitle { font-size: 1.1rem; }
            .commodity-grid { grid-template-columns: 1fr; max-height: 300px; }
            .controls { flex-direction: column; align-items: center; }
            .btn { width: 100%; max-width: 300px; justify-content: center; }
            .status-grid { grid-template-columns: repeat(2, 1fr); }
            .selection-controls { flex-direction: column; align-items: stretch; }
            .selected-count { margin-left: 0; text-align: center; margin-top: 10px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1 class="title"><i class="fas fa-rocket"></i> Supplier Intelligence System</h1>
            <p class="subtitle">Professional Multi-Source Data Collection Platform</p>
            <div class="tech-info">
                <i class="fas fa-cogs"></i> DuckDuckGo + ThomasNet + Deep Web Scraping
            </div>
        </div>

        <div class="source-info">
            <strong><i class="fas fa-info-circle"></i> Data Sources:</strong> 
            Advanced multi-source scraping using DuckDuckGo Search Engine, ThomasNet B2B Directory, and Direct Website Contact Extraction<br>
            <strong><i class="fas fa-shield-alt"></i> Technology:</strong> 
            Professional-grade scraping with anti-blocking measures, rotating user agents, and respectful rate limiting
        </div>

        <div class="features-grid">
            <div class="feature-item">
                <div class="feature-icon"><i class="fas fa-search"></i></div>
                <h4>Multi-Source Search</h4>
                <p>Comprehensive data collection from multiple reliable sources</p>
            </div>
            <div class="feature-item">
                <div class="feature-icon"><i class="fas fa-database"></i></div>
                <h4>Real-time Database</h4>
                <p>SQLite database with live progress tracking and data persistence</p>
            </div>
            <div class="feature-item">
                <div class="feature-icon"><i class="fas fa-file-excel"></i></div>
                <h4>Professional Export</h4>
                <p>Multi-sheet Excel export with quality analysis and filtering</p>
            </div>
            <div class="feature-item">
                <div class="feature-icon"><i class="fas fa-chart-line"></i></div>
                <h4>Quality Scoring</h4>
                <p>Intelligent quality assessment based on contact completeness</p>
            </div>
        </div>

        <div id="alert" class="alert">
            <strong><i class="fas fa-exclamation-triangle"></i> Notice:</strong> <span id="alertText"></span>
        </div>

        <div id="success" class="success">
            <strong><i class="fas fa-check-circle"></i> Success:</strong> <span id="successText"></span>
        </div>

        <div class="card">
            <h3><i class="fas fa-list-check"></i> Select Commodities</h3>
            <div class="selection-controls">
                <button class="btn btn-secondary" onclick="selectAll()">
                    <i class="fas fa-check-double"></i> Select All
                </button>
                <button class="btn btn-secondary" onclick="selectNone()">
                    <i class="fas fa-times"></i> Clear All
                </button>
                <button class="btn btn-secondary" onclick="selectRandom()">
                    <i class="fas fa-random"></i> Random 10
                </button>
                <div class="selected-count">
                    Selected: <strong id="selectedCount">0</strong> / {{ commodities|length }}
                </div>
            </div>
            <div class="commodity-grid scrollbar-custom" id="commodityGrid">
                {% for commodity in commodities %}
                <div class="commodity-item" onclick="toggleCommodity(this)">
                    <input type="checkbox" value="{{ commodity }}">
                    <label>{{ commodity }}</label>
                </div>
                {% endfor %}
            </div>
        </div>

        <div class="controls">
            <button class="btn btn-primary" id="startBtn" onclick="startScraping()">
                <span class="loading" style="display: none;"></span>
                <i class="fas fa-play" id="startIcon"></i>
                Start Collection
            </button>
            <button class="btn btn-danger" id="stopBtn" onclick="stopScraping()" style="display: none;">
                <i class="fas fa-stop"></i> Stop Process
            </button>
            <button class="btn btn-secondary" onclick="clearData()">
                <i class="fas fa-trash"></i> Clear Data
            </button>
            <button class="btn btn-success" id="exportBtn" onclick="exportData()" disabled>
                <i class="fas fa-download"></i> Export Excel
            </button>
        </div>

        <div class="progress-container" id="progressContainer">
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill"></div>
            </div>
            <div class="progress-text" id="progressText">Initializing...</div>
        </div>

        <div class="card status-card" id="statusCard">
            <h3><i class="fas fa-chart-bar"></i> Live Collection Status</h3>
            <div class="status-grid">
                <div class="status-item">
                    <div class="status-number" id="totalRecords">0</div>
                    <div class="status-label">Total Records</div>
                </div>
                <div class="status-item">
                    <div class="status-number" id="progressPercent">0%</div>
                    <div class="status-label">Progress</div>
                </div>
                <div class="status-item">
                    <div class="status-number" id="avgQuality">0</div>
                    <div class="status-label">Avg Quality</div>
                </div>
                <div class="status-item">
                    <div class="status-number" id="currentCommodity">-</div>
                    <div class="status-label">Current Item</div>
                </div>
            </div>
            <div class="status-message-container">
                <i class="fas fa-info-circle"></i> 
                <strong>Status:</strong> <span id="statusMessage">Ready</span>
            </div>
        </div>

        <div class="card" id="resultsCard" style="display: none;">
            <h3><i class="fas fa-chart-line"></i> Collection Results</h3>
            <div id="resultsContainer">
                <!-- Results will be displayed here -->
            </div>
        </div>
    </div>

    <script>
        let isRunning = false;
        let statusInterval = null;

        function updateSelectedCount() {
            const checkboxes = document.querySelectorAll('#commodityGrid input[type="checkbox"]');
            const selected = Array.from(checkboxes).filter(cb => cb.checked).length;
            const total = checkboxes.length;
            document.getElementById('selectedCount').textContent = selected;
        }

        function toggleCommodity(item) {
            const checkbox = item.querySelector('input[type="checkbox"]');
            checkbox.checked = !checkbox.checked;
            item.classList.toggle('selected', checkbox.checked);
            updateSelectedCount();
        }

        function selectAll() {
            const items = document.querySelectorAll('.commodity-item');
            items.forEach(item => {
                const checkbox = item.querySelector('input[type="checkbox"]');
                checkbox.checked = true;
                item.classList.add('selected');
            });
            updateSelectedCount();
        }

        function selectNone() {
            const items = document.querySelectorAll('.commodity-item');
            items.forEach(item => {
                const checkbox = item.querySelector('input[type="checkbox"]');
                checkbox.checked = false;
                item.classList.remove('selected');
            });
            updateSelectedCount();
        }

        function selectRandom() {
            selectNone();
            const items = Array.from(document.querySelectorAll('.commodity-item'));
            const randomItems = items.sort(() => 0.5 - Math.random()).slice(0, 10);
            
            randomItems.forEach(item => {
                const checkbox = item.querySelector('input[type="checkbox"]');
                checkbox.checked = true;
                item.classList.add('selected');
            });
            updateSelectedCount();
        }

        function showAlert(message) {
            const alert = document.getElementById('alert');
            document.getElementById('alertText').textContent = message;
            alert.style.display = 'block';
            setTimeout(() => alert.style.display = 'none', 6000);
        }

        function showSuccess(message) {
            const success = document.getElementById('success');
            document.getElementById('successText').textContent = message;
            success.style.display = 'block';
            setTimeout(() => success.style.display = 'none', 6000);
        }

        function getSelectedCommodities() {
            const checkboxes = document.querySelectorAll('#commodityGrid input[type="checkbox"]:checked');
            return Array.from(checkboxes).map(cb => cb.value);
        }

        function startScraping() {
            const selected = getSelectedCommodities();
            
            if (selected.length === 0) {
                showAlert('Please select at least one commodity to start the collection process.');
                return;
            }

            const startBtn = document.getElementById('startBtn');
            const stopBtn = document.getElementById('stopBtn');
            const loading = startBtn.querySelector('.loading');
            const startIcon = document.getElementById('startIcon');
            
            startBtn.disabled = true;
            loading.style.display = 'inline-block';
            startIcon.style.display = 'none';
            stopBtn.style.display = 'inline-flex';
            
            document.getElementById('progressContainer').style.display = 'block';
            document.getElementById('statusCard').style.display = 'block';
            
            isRunning = true;
            
            fetch('/start_scraping', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ commodities: selected })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showSuccess(`Data collection started successfully for ${selected.length} commodities!`);
                    startStatusUpdates();
                } else {
                    showAlert(data.error || 'Failed to start collection process');
                    resetButtons();
                }
            })
            .catch(error => {
                showAlert('Error starting collection: ' + error.message);
                resetButtons();
            });
        }

        function stopScraping() {
            fetch('/stop_scraping', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                showSuccess('Collection process stopped successfully!');
                isRunning = false;
                clearInterval(statusInterval);
                resetButtons();
            })
            .catch(error => {
                showAlert('Error stopping collection: ' + error.message);
            });
        }

        function clearData() {
            if (confirm('Are you sure you want to clear all collected data? This action cannot be undone.')) {
                fetch('/clear_data', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    showSuccess('All data cleared successfully!');
                    document.getElementById('resultsCard').style.display = 'none';
                    document.getElementById('exportBtn').disabled = true;
                    document.getElementById('progressContainer').style.display = 'none';
                    document.getElementById('statusCard').style.display = 'none';
                    updateStatus({
                        total_records: 0,
                        progress: 0,
                        avg_quality: 0,
                        current_commodity: '-',
                        status_message: 'Ready to start'
                    });
                });
            }
        }

        function exportData() {
            showSuccess('Generating Excel export... Download will start shortly.');
            window.open('/export_excel', '_blank');
        }

        function startStatusUpdates() {
            statusInterval = setInterval(() => {
                fetch('/get_progress')
                .then(response => response.json())
                .then(data => {
                    updateStatus(data);
                    
                    if (!data.is_running && isRunning) {
                        isRunning = false;
                        clearInterval(statusInterval);
                        resetButtons();
                        showSuccess(`Collection completed successfully! Collected ${data.total_records} records.`);
                        if (data.total_records > 0) {
                            document.getElementById('exportBtn').disabled = false;
                            loadResults();
                        }
                    }
                })
                .catch(error => {
                    console.error('Status update error:', error);
                });
            }, 3000);
        }

        function updateStatus(data) {
            document.getElementById('totalRecords').textContent = (data.total_records || 0).toLocaleString();
            document.getElementById('progressPercent').textContent = Math.round(data.progress || 0) + '%';
            document.getElementById('avgQuality').textContent = Math.round(data.avg_quality || 0);
            
            const currentCommodity = data.current_commodity || '-';
            document.getElementById('currentCommodity').textContent = currentCommodity.length > 15 ? 
                currentCommodity.substring(0, 15) + '...' : currentCommodity;
            
            document.getElementById('statusMessage').textContent = data.status_message || 'Ready';
            
            const progressFill = document.getElementById('progressFill');
            const progressText = document.getElementById('progressText');
            
            progressFill.style.width = (data.progress || 0) + '%';
            progressText.textContent = data.status_message || 'Ready';
        }

        function resetButtons() {
            const startBtn = document.getElementById('startBtn');
            const stopBtn = document.getElementById('stopBtn');
            const loading = startBtn.querySelector('.loading');
            const startIcon = document.getElementById('startIcon');
            
            startBtn.disabled = false;
            loading.style.display = 'none';
            startIcon.style.display = 'inline';
            stopBtn.style.display = 'none';
        }

        function loadResults() {
            fetch('/get_results')
            .then(response => response.json())
            .then(data => {
                if (data.success && data.results.length > 0) {
                    displayResults(data.results);
                    document.getElementById('resultsCard').style.display = 'block';
                }
            })
            .catch(error => {
                console.error('Results loading error:', error);
            });
        }

        function displayResults(results) {
            const container = document.getElementById('resultsContainer');
            
            // Calculate statistics
            const totalRecords = results.length;
            const withEmail = results.filter(r => r.email).length;
            const withPhone = results.filter(r => r.phone).length;
            const highQuality = results.filter(r => r.quality_score >= 70).length;
            
            let html = `
                <div class="features-grid" style="margin-bottom: 25px;">
                    <div class="feature-item">
                        <div class="feature-icon">${totalRecords}</div>
                        <h4>Total Records</h4>
                        <p>Complete supplier database</p>
                    </div>
                    <div class="feature-item">
                        <div class="feature-icon">${withEmail}</div>
                        <h4>Email Contacts</h4>
                        <p>${((withEmail/totalRecords)*100).toFixed(1)}% with emails</p>
                    </div>
                    <div class="feature-item">
                        <div class="feature-icon">${withPhone}</div>
                        <h4>Phone Contacts</h4>
                        <p>${((withPhone/totalRecords)*100).toFixed(1)}% with phones</p>
                    </div>
                    <div class="feature-item">
                        <div class="feature-icon">${highQuality}</div>
                        <h4>High Quality</h4>
                        <p>${((highQuality/totalRecords)*100).toFixed(1)}% premium leads</p>
                    </div>
                </div>
                
                <table class="data-table">
                    <thead>
                        <tr>
                            <th><i class="fas fa-building"></i> Company</th>
                            <th><i class="fas fa-envelope"></i> Email</th>
                            <th><i class="fas fa-phone"></i> Phone</th>
                            <th><i class="fas fa-globe"></i> Website</th>
                            <th><i class="fas fa-tags"></i> Commodity</th>
                            <th><i class="fas fa-star"></i> Quality</th>
                            <th><i class="fas fa-source"></i> Source</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            
            // Show first 15 results
            results.slice(0, 15).forEach(result => {
                const qualityColor = result.quality_score >= 70 ? '#2ed573' : 
                                   result.quality_score >= 50 ? '#ffa502' : '#ff4757';
                
                html += `
                    <tr>
                        <td><strong>${result.company_name || '-'}</strong></td>
                        <td>${result.email ? `<a href="mailto:${result.email}" style="color: #4ecdc4;">${result.email}</a>` : '-'}</td>
                        <td>${result.phone ? `<a href="tel:${result.phone}" style="color: #4ecdc4;">${result.phone}</a>` : '-'}</td>
                        <td>${result.website ? `<a href="${result.website}" target="_blank" style="color: #4ecdc4;"><i class="fas fa-external-link-alt"></i></a>` : '-'}</td>
                        <td><span style="background: rgba(78,205,196,0.2); padding: 2px 8px; border-radius: 12px; font-size: 0.8rem;">${result.commodity || '-'}</span></td>
                        <td><span style="color: ${qualityColor}; font-weight: bold;">${result.quality_score || 0}</span></td>
                        <td><span style="font-size: 0.8rem; opacity: 0.8;">${result.source || '-'}</span></td>
                    </tr>
                `;
            });
            
            html += '</tbody></table>';
            
            if (results.length > 15) {
                html += `
                    <div style="margin-top: 20px; text-align: center; padding: 15px; background: rgba(78,205,196,0.1); border-radius: 12px;">
                        <i class="fas fa-info-circle"></i> 
                        <strong>Showing first 15 of ${results.length.toLocaleString()} records.</strong><br>
                        <small>Export to Excel for complete data with advanced filtering and analysis.</small>
                    </div>
                `;
            }
            
            container.innerHTML = html;
        }

        // Initialize page
        updateSelectedCount();
        
        // Add keyboard shortcuts
        document.addEventListener('keydown', function(e) {
            if (e.ctrlKey || e.metaKey) {
                switch(e.key) {
                    case 'a':
                        e.preventDefault();
                        selectAll();
                        break;
                    case 'd':
                        e.preventDefault();
                        selectNone();
                        break;
                    case 'Enter':
                        e.preventDefault();
                        if (!isRunning) startScraping();
                        break;
                }
            }
        });
        
        // Auto-save selected commodities
        function saveSelection() {
            const selected = getSelectedCommodities();
            localStorage.setItem('selectedCommodities', JSON.stringify(selected));
        }
        
        function loadSelection() {
            try {
                const saved = localStorage.getItem('selectedCommodities');
                if (saved) {
                    const selected = JSON.parse(saved);
                    selected.forEach(commodity => {
                        const checkbox = document.querySelector(`input[value="${commodity}"]`);
                        if (checkbox) {
                            checkbox.checked = true;
                            checkbox.closest('.commodity-item').classList.add('selected');
                        }
                    });
                    updateSelectedCount();
                }
            } catch (e) {
                console.error('Error loading selection:', e);
            }
        }
        
        // Save selection when changed
        document.getElementById('commodityGrid').addEventListener('change', saveSelection);
        
        // Load saved selection on page load
        loadSelection();
    </script>
</body>
</html>'''

# Flask Routes
@app.route('/')
def index():
    """Main dashboard page"""
    return render_template_string(HTML_TEMPLATE, commodities=scraper.all_commodities)

@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    """Start the scraping process"""
    try:
        data = request.get_json()
        selected_commodities = data.get('commodities', [])
        
        if not selected_commodities:
            return jsonify({'success': False, 'error': 'No commodities selected'})
        
        if scraper.is_running:
            return jsonify({'success': False, 'error': 'Collection process is already running'})
        
        # Start scraping in background thread
        def run_scraping():
            try:
                scraper.run_scraping(selected_commodities)
            except Exception as e:
                logger.error(f"Scraping thread error: {e}")
                scraper.is_running = False
        
        scraper.thread = threading.Thread(target=run_scraping, daemon=True)
        scraper.thread.start()
        
        return jsonify({'success': True, 'message': 'Data collection started successfully'})
        
    except Exception as e:
        logger.error(f"Error starting scraping: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/stop_scraping', methods=['POST'])
def stop_scraping():
    """Stop the scraping process"""
    try:
        scraper.stop_scraping()
        return jsonify({'success': True, 'message': 'Collection process stopped'})
    except Exception as e:
        logger.error(f"Error stopping scraping: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/clear_data', methods=['POST'])
def clear_data():
    """Clear all collected data"""
    try:
        scraper.clear_data()
        return jsonify({'success': True, 'message': 'All data cleared successfully'})
    except Exception as e:
        logger.error(f"Error clearing data: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/get_progress')
def get_progress():
    """Get current progress status"""
    try:
        return jsonify(scraper.get_progress_data())
    except Exception as e:
        logger.error(f"Error getting progress: {e}")
        return jsonify({'error': str(e), 'is_running': False, 'progress': 0, 'total_records': 0})

@app.route('/get_results')
def get_results():
    """Get collected results"""
    try:
        return jsonify({
            'success': True,
            'results': scraper.collected_data,
            'total': len(scraper.collected_data)
        })
    except Exception as e:
        logger.error(f"Error getting results: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/export_excel')
def export_excel():
    """Export data to Excel"""
    try:
        excel_buffer = scraper.export_to_excel()
        
        if excel_buffer is None:
            return "No data available for export. Please collect some data first.", 404
        
        return send_file(
            excel_buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'supplier_intelligence_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        return f"Export failed: {str(e)}", 500

@app.route('/health')
def health_check():
    """Health check endpoint for deployment"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0',
        'database': 'connected' if os.path.exists('suppliers.db') else 'not_found'
    })

@app.route('/api/stats')
def get_stats():
    """Get database statistics API"""
    try:
        conn = sqlite3.connect('suppliers.db', check_same_thread=False)
        c = conn.cursor()
        
        # Get basic stats
        c.execute('SELECT COUNT(*) FROM suppliers')
        total_records = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM suppliers WHERE email IS NOT NULL AND email != ""')
        with_email = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM suppliers WHERE phone IS NOT NULL AND phone != ""')
        with_phone = c.fetchone()[0]
        
        c.execute('SELECT AVG(quality_score) FROM suppliers')
        avg_quality = c.fetchone()[0] or 0
        
        # Top commodities
        c.execute('SELECT commodity, COUNT(*) as count FROM suppliers GROUP BY commodity ORDER BY count DESC LIMIT 10')
        top_commodities = c.fetchall()
        
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'with_email': with_email,
            'with_phone': with_phone,
            'avg_quality': round(avg_quality, 2),
            'top_commodities': [{'commodity': row[0], 'count': row[1]} for row in top_commodities]
        })
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return jsonify({'error': str(e)}), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("=" * 80)
    print(" PROFESSIONAL SUPPLIER INTELLIGENCE SYSTEM")
    print("=" * 80)
    print(" Multi-Source Data Collection Platform")
    print(" Sources: DuckDuckGo + ThomasNet + Deep Web Scraping")
    print("  Database: SQLite with real-time persistence")
    print(" Features: Live progress tracking, quality scoring, Excel export")
    print(" Server: http://localhost:5000")
    print(" Health Check: http://localhost:5000/health")
    print(" Stats API: http://localhost:5000/api/stats")
    print("=" * 80)
    
    # Initialize database and scraper
    try:
        scraper.init_db()
        logger.info("Application initialized successfully")
        
        # Run Flask app
        app.run(
            debug=False,  # Set to False for production
            host='0.0.0.0', 
            port=int(os.environ.get('PORT', 5000)),
            threaded=True,
            request_handler=QuietWSGIRequestHandler
        )
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        print(f" Startup Error: {e}")