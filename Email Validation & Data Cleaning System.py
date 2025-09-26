import pandas as pd
import numpy as np
import re
import requests
import json
import os
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
import socket
from urllib.parse import urlparse
import openpyxl
from openpyxl.styles import Font, PatternFill, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdvancedEmailValidatorAndCleaner:
    def __init__(self):
        self.validated_emails = []
        self.validation_results = {}
        self.cleaned_data = []
        self.report_data = {}
        
        # Email validation patterns
        self.email_patterns = {
            'basic': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'strict': r'^[a-zA-Z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&\'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$'
        }
        
        # Common disposable email domains to filter out
        self.disposable_domains = [
            '10minutemail.com', 'tempmail.org', 'guerrillamail.com', 'mailinator.com',
            'throwaway.email', 'temp-mail.org', 'getairmail.com', 'yopmail.com',
            'maildrop.cc', 'sharklasers.com', 'grr.la', 'guerrillamailblock.com'
        ]
        
        # Business email domains (higher priority)
        self.business_domains = [
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
            'icloud.com', 'protonmail.com', 'zoho.com'
        ]
        
        # Phone number patterns
        self.phone_patterns = [
            r'\+1[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',
            r'\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',
            r'[0-9]{3}-[0-9]{3}-[0-9]{4}',
            r'[0-9]{10}',
            r'\+[0-9]{1,3}[-.\s]?[0-9]{3,4}[-.\s]?[0-9]{3,4}[-.\s]?[0-9]{3,4}'
        ]
    
    def load_scraped_data(self, file_path):
        """Load the scraped data from CSV or Excel"""
        logger.info(f"📂 Loading scraped data from: {file_path}")
        
        try:
            if file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8')
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel.")
            
            logger.info(f"✅ Loaded {len(df)} records from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return None
    
    def validate_email_syntax(self, email):
        """Validate email syntax using regex"""
        if not email or pd.isna(email):
            return {'valid': False, 'reason': 'Empty email'}
        
        email = str(email).strip().lower()
        
        # Check basic format
        if not re.match(self.email_patterns['basic'], email):
            return {'valid': False, 'reason': 'Invalid format'}
        
        # Check for disposable domains
        domain = email.split('@')[1] if '@' in email else ''
        if domain in self.disposable_domains:
            return {'valid': False, 'reason': 'Disposable email'}
        
        # Check for obvious fake patterns
        fake_patterns = ['test@', 'example@', 'dummy@', 'fake@', 'sample@']
        if any(pattern in email for pattern in fake_patterns):
            return {'valid': False, 'reason': 'Fake email pattern'}
        
        return {'valid': True, 'reason': 'Valid syntax'}
    
    def validate_email_domain(self, email):
        """Validate email domain using DNS lookup"""
        try:
            if not email or pd.isna(email):
                return {'valid': False, 'reason': 'Empty email'}
            
            domain = str(email).split('@')[1] if '@' in str(email) else ''
            if not domain:
                return {'valid': False, 'reason': 'No domain'}
            
            # Simple HTTP request to check if domain exists
            try:
                response = requests.head(f"http://{domain}", timeout=5)
                return {'valid': True, 'reason': 'Domain accessible'}
            except:
                try:
                    response = requests.head(f"https://{domain}", timeout=5)
                    return {'valid': True, 'reason': 'Domain accessible (HTTPS)'}
                except:
                    # Fallback to basic domain format check
                    if '.' in domain and len(domain.split('.')) >= 2:
                        tld = domain.split('.')[-1]
                        if 2 <= len(tld) <= 6:
                            return {'valid': True, 'reason': 'Basic domain check passed'}
            
            return {'valid': False, 'reason': 'Domain not accessible'}
            
        except Exception as e:
            return {'valid': False, 'reason': f'Domain error: {str(e)[:50]}'}
    
    def simple_domain_check(self, email):
        """Simple domain validation without external dependencies"""
        try:
            if not email or pd.isna(email):
                return {'valid': False, 'reason': 'Empty email'}
            
            domain = str(email).split('@')[1] if '@' in str(email) else ''
            if not domain:
                return {'valid': False, 'reason': 'No domain'}
            
            # Check if domain has valid format
            if '.' not in domain or len(domain.split('.')) < 2:
                return {'valid': False, 'reason': 'Invalid domain format'}
            
            # Check TLD length
            tld = domain.split('.')[-1]
            if len(tld) < 2 or len(tld) > 6:
                return {'valid': False, 'reason': 'Invalid TLD'}
            
            # Check for common business domains
            if domain in self.business_domains:
                return {'valid': True, 'reason': 'Recognized business domain'}
            
            return {'valid': True, 'reason': 'Basic domain check passed'}
            
        except Exception as e:
            return {'valid': False, 'reason': f'Domain error: {str(e)[:30]}'}
    
    def validate_phone_number(self, phone):
        """Validate and clean phone numbers"""
        if not phone or pd.isna(phone):
            return {'valid': False, 'cleaned': '', 'reason': 'Empty phone'}
        
        phone_str = str(phone).strip()
        
        # Remove common formatting
        cleaned_phone = re.sub(r'[^\d+]', '', phone_str)
        
        # Check patterns
        for pattern in self.phone_patterns:
            if re.match(pattern, phone_str):
                return {
                    'valid': True, 
                    'cleaned': cleaned_phone, 
                    'original': phone_str,
                    'reason': 'Valid format'
                }
        
        # Check if it's at least 10 digits
        digits_only = re.sub(r'\D', '', phone_str)
        if len(digits_only) >= 10:
            return {
                'valid': True, 
                'cleaned': digits_only, 
                'original': phone_str,
                'reason': 'Minimum digits met'
            }
        
        return {'valid': False, 'cleaned': '', 'reason': 'Invalid format'}
    
    def validate_website_url(self, url):
        """Validate and clean website URLs"""
        if not url or pd.isna(url):
            return {'valid': False, 'cleaned': '', 'reason': 'Empty URL'}
        
        url_str = str(url).strip()
        
        # Add protocol if missing
        if not url_str.startswith(('http://', 'https://')):
            url_str = 'https://' + url_str
        
        try:
            parsed = urlparse(url_str)
            if parsed.scheme and parsed.netloc:
                return {
                    'valid': True, 
                    'cleaned': url_str, 
                    'domain': parsed.netloc,
                    'reason': 'Valid URL structure'
                }
        except:
            pass
        
        return {'valid': False, 'cleaned': '', 'reason': 'Invalid URL format'}
    
    def clean_company_name(self, company_name):
        """Clean and standardize company names"""
        if not company_name or pd.isna(company_name):
            return ''
        
        name = str(company_name).strip()
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name)
        
        # Standardize common suffixes
        suffixes = {
            ' inc.': ' Inc.',
            ' llc.': ' LLC.',
            ' corp.': ' Corp.',
            ' ltd.': ' Ltd.',
            ' co.': ' Co.',
            ' company': ' Company'
        }
        
        name_lower = name.lower()
        for old, new in suffixes.items():
            if name_lower.endswith(old):
                name = name[:-len(old)] + new
                break
        
        # Capitalize properly
        name = name.title()
        
        return name
    
    def calculate_data_quality_score(self, row):
        """Calculate enhanced data quality score"""
        score = 0
        max_score = 100
        
        # Email validation (40 points max)
        if row.get('email_valid', False):
            score += 30
            if 'business domain' in str(row.get('email_validation_reason', '')):
                score += 10  # Bonus for business domains
            elif len(str(row.get('email', ''))) > 0:
                score += 5   # Basic email bonus
        
        # Phone validation (25 points max)
        if row.get('phone_valid', False):
            score += 20
            if len(str(row.get('phone_cleaned', ''))) > 10:
                score += 5   # Bonus for longer numbers
        
        # Website validation (20 points max)
        if row.get('website_valid', False):
            score += 15
            if str(row.get('website', '')).startswith('https'):
                score += 5   # Bonus for HTTPS
        
        # Company name quality (10 points max)
        company = str(row.get('company_name', ''))
        if len(company) > 3:
            score += 5
        if any(suffix in company.lower() for suffix in ['inc', 'llc', 'corp', 'ltd', 'company']):
            score += 5   # Bonus for proper business suffixes
        
        # Snippet quality (5 points max)
        snippet = str(row.get('snippet', ''))
        if len(snippet) > 50:
            score += 5
        
        return min(score, max_score)
    
    def process_validation_batch(self, df_batch, use_dns=False):
        """Process a batch of records for validation"""
        results = []
        
        for idx, row in df_batch.iterrows():
            try:
                # Email validation
                email = row.get('email', '')
                if email and not pd.isna(email):
                    syntax_check = self.validate_email_syntax(email)
                    if syntax_check['valid'] and use_dns:
                        domain_check = self.validate_email_domain(email)
                        email_valid = domain_check['valid']
                        email_reason = domain_check['reason']
                    else:
                        email_valid = syntax_check['valid']
                        email_reason = syntax_check['reason']
                        if email_valid and not use_dns:
                            # Use simple domain check as fallback
                            simple_check = self.simple_domain_check(email)
                            email_valid = simple_check['valid']
                            email_reason = simple_check['reason']
                else:
                    email_valid = False
                    email_reason = 'No email provided'
                
                # Phone validation
                phone_result = self.validate_phone_number(row.get('phone', ''))
                
                # Website validation
                website_result = self.validate_website_url(row.get('website', ''))
                
                # Company name cleaning
                cleaned_company = self.clean_company_name(row.get('company_name', ''))
                
                # Create cleaned record
                cleaned_record = {
                    'company_name': cleaned_company,
                    'email': str(email).lower().strip() if email and not pd.isna(email) else '',
                    'email_valid': email_valid,
                    'email_validation_reason': email_reason,
                    'phone': row.get('phone', ''),
                    'phone_cleaned': phone_result.get('cleaned', ''),
                    'phone_valid': phone_result.get('valid', False),
                    'phone_validation_reason': phone_result.get('reason', ''),
                    'website': row.get('website', ''),
                    'website_cleaned': website_result.get('cleaned', ''),
                    'website_valid': website_result.get('valid', False),
                    'website_validation_reason': website_result.get('reason', ''),
                    'commodity': row.get('commodity', ''),
                    'snippet': row.get('snippet', ''),
                    'source': row.get('source', ''),
                    'collection_date': row.get('collection_date', ''),
                    'original_quality_score': row.get('data_quality_score', 0)
                }
                
                # Calculate new quality score
                cleaned_record['final_quality_score'] = self.calculate_data_quality_score(cleaned_record)
                
                results.append(cleaned_record)
                
            except Exception as e:
                logger.error(f"Error processing record {idx}: {e}")
                continue
        
        return results
    
    def validate_and_clean_data(self, df, use_dns_validation=False, batch_size=100):
        """Main validation and cleaning function"""
        logger.info(f"🔧 Starting data validation and cleaning...")
        logger.info(f"📊 Processing {len(df)} records in batches of {batch_size}")
        
        if use_dns_validation:
            logger.info("🌐 DNS validation enabled (slower but more accurate)")
        else:
            logger.info("⚡ Using fast syntax validation")
        
        cleaned_results = []
        total_batches = len(df) // batch_size + (1 if len(df) % batch_size else 0)
        
        start_time = time.time()
        
        for i in range(0, len(df), batch_size):
            batch_num = (i // batch_size) + 1
            logger.info(f"🔄 Processing batch {batch_num}/{total_batches}...")
            
            batch_df = df.iloc[i:i+batch_size]
            batch_results = self.process_validation_batch(batch_df, use_dns_validation)
            cleaned_results.extend(batch_results)
            
            # Progress update
            if batch_num % 5 == 0 or batch_num == total_batches:
                elapsed_time = time.time() - start_time
                progress = (batch_num / total_batches) * 100
                logger.info(f"📈 Progress: {progress:.1f}% ({len(cleaned_results)} records processed)")
        
        self.cleaned_data = cleaned_results
        total_time = time.time() - start_time
        
        logger.info(f"✅ Validation completed in {total_time:.1f} seconds")
        logger.info(f"📊 Processed {len(cleaned_results)} records")
        
        return cleaned_results
    
    def generate_validation_report(self, cleaned_data):
        """Generate comprehensive validation report"""
        if not cleaned_data:
            return {}
        
        df = pd.DataFrame(cleaned_data)
        
        # Basic statistics
        total_records = len(df)
        valid_emails = len(df[df['email_valid'] == True])
        valid_phones = len(df[df['phone_valid'] == True])
        valid_websites = len(df[df['website_valid'] == True])
        
        # Quality distribution
        high_quality = len(df[df['final_quality_score'] >= 70])
        medium_quality = len(df[(df['final_quality_score'] >= 40) & (df['final_quality_score'] < 70)])
        low_quality = len(df[df['final_quality_score'] < 40])
        
        # Email validation reasons
        email_reasons = df['email_validation_reason'].value_counts().to_dict()
        
        # Top commodities
        top_commodities = df['commodity'].value_counts().head(10).to_dict()
        
        # Source performance
        source_stats = df.groupby('source').agg({
            'email_valid': 'sum',
            'phone_valid': 'sum',
            'final_quality_score': 'mean'
        }).to_dict()
        
        report = {
            'summary': {
                'total_records': total_records,
                'valid_emails': valid_emails,
                'valid_phones': valid_phones,
                'valid_websites': valid_websites,
                'email_validation_rate': (valid_emails / total_records * 100) if total_records > 0 else 0,
                'phone_validation_rate': (valid_phones / total_records * 100) if total_records > 0 else 0,
                'website_validation_rate': (valid_websites / total_records * 100) if total_records > 0 else 0
            },
            'quality_distribution': {
                'high_quality': high_quality,
                'medium_quality': medium_quality,
                'low_quality': low_quality,
                'average_quality_score': df['final_quality_score'].mean()
            },
            'validation_details': {
                'email_validation_reasons': email_reasons,
                'top_commodities': top_commodities,
                'source_performance': source_stats
            }
        }
        
        self.report_data = report
        return report
    
    def export_cleaned_data(self, cleaned_data, output_prefix="cleaned_supplier_data"):
        """Export cleaned data to multiple formats"""
        if not cleaned_data:
            logger.warning("No cleaned data to export")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create DataFrame
        df = pd.DataFrame(cleaned_data)
        
        # Sort by quality score (highest first)
        df = df.sort_values(['final_quality_score', 'commodity'], ascending=[False, True])
        
        # Remove duplicates (keep highest quality)
        df = df.drop_duplicates(subset=['email'], keep='first')
        df = df.drop_duplicates(subset=['company_name', 'commodity'], keep='first')
        
        files_created = []
        
        # 1. CSV Export (Main cleaned database)
        csv_filename = f"{output_prefix}_{timestamp}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        files_created.append(csv_filename)
        logger.info(f"✅ CSV exported: {csv_filename}")
        
        # 2. Excel Export with multiple sheets
        excel_filename = f"{output_prefix}_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            # Main sheet
            df.to_excel(writer, sheet_name='Cleaned Database', index=False)
            
            # High quality contacts only
            high_quality_df = df[df['final_quality_score'] >= 70]
            if not high_quality_df.empty:
                high_quality_df.to_excel(writer, sheet_name='High Quality Contacts', index=False)
            
            # Valid emails only
            valid_emails_df = df[df['email_valid'] == True]
            if not valid_emails_df.empty:
                valid_emails_df.to_excel(writer, sheet_name='Valid Emails', index=False)
            
            # Business ready contacts (email + phone + website)
            business_ready = df[
                (df['email_valid'] == True) & 
                (df['phone_valid'] == True) & 
                (df['website_valid'] == True)
            ]
            if not business_ready.empty:
                business_ready.to_excel(writer, sheet_name='Business Ready', index=False)
            
            # Summary by commodity
            commodity_summary = df.groupby('commodity').agg({
                'company_name': 'count',
                'email_valid': 'sum',
                'phone_valid': 'sum',
                'website_valid': 'sum',
                'final_quality_score': 'mean'
            }).round(2)
            commodity_summary.columns = ['Total Records', 'Valid Emails', 'Valid Phones', 'Valid Websites', 'Avg Quality']
            commodity_summary.to_excel(writer, sheet_name='Commodity Summary')
            
            # Validation report
            if hasattr(self, 'report_data') and self.report_data:
                report_df = pd.DataFrame([self.report_data['summary']])
                report_df.to_excel(writer, sheet_name='Validation Report', index=False)
        
        files_created.append(excel_filename)
        logger.info(f"✅ Excel exported: {excel_filename}")
        
        # 3. High-priority contacts CSV (for immediate use)
        priority_contacts = df[
            (df['email_valid'] == True) & 
            (df['final_quality_score'] >= 60)
        ][['company_name', 'email', 'phone_cleaned', 'website_cleaned', 'commodity', 'final_quality_score']]
        
        if not priority_contacts.empty:
            priority_filename = f"priority_contacts_{timestamp}.csv"
            priority_contacts.to_csv(priority_filename, index=False, encoding='utf-8')
            files_created.append(priority_filename)
            logger.info(f"✅ Priority contacts exported: {priority_filename}")
        
        return files_created
    
    def print_validation_summary(self, report_data):
        """Print comprehensive validation summary"""
        print(f"\n" + "="*60)
        print(f"📊 DATA VALIDATION & CLEANING SUMMARY")
        print(f"="*60)
        
        print(f"\n🔍 VALIDATION RESULTS:")
        print(f"   • Total Records Processed: {report_data['summary']['total_records']:,}")
        print(f"   • Valid Email Addresses: {report_data['summary']['valid_emails']:,} ({report_data['summary']['email_validation_rate']:.1f}%)")
        print(f"   • Valid Phone Numbers: {report_data['summary']['valid_phones']:,} ({report_data['summary']['phone_validation_rate']:.1f}%)")
        print(f"   • Valid Website URLs: {report_data['summary']['valid_websites']:,} ({report_data['summary']['website_validation_rate']:.1f}%)")
        
        print(f"\n📈 QUALITY DISTRIBUTION:")
        print(f"   • High Quality (70-100): {report_data['quality_distribution']['high_quality']:,}")
        print(f"   • Medium Quality (40-69): {report_data['quality_distribution']['medium_quality']:,}")
        print(f"   • Low Quality (0-39): {report_data['quality_distribution']['low_quality']:,}")
        print(f"   • Average Quality Score: {report_data['quality_distribution']['average_quality_score']:.1f}/100")
        
        print(f"\n💼 BUSINESS READY CONTACTS:")
        total_records = report_data['summary']['total_records']
        valid_emails = report_data['summary']['valid_emails']
        high_quality = report_data['quality_distribution']['high_quality']
        
        print(f"   • Ready for Email Marketing: {valid_emails:,} contacts")
        print(f"   • Premium Quality Leads: {high_quality:,} contacts")
        print(f"   • Estimated Business Value: ${valid_emails * 50:,} @ $50/contact")
        
        print(f"\n✅ DATA CLEANING COMPLETED")
        print(f"="*60)


def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("🔧 ADVANCED EMAIL VALIDATOR & DATA CLEANER")
    print("📊 Milestone 3: Final Data Validation & Cleaning")
    print("="*80)
    
    cleaner = AdvancedEmailValidatorAndCleaner()
    
    # Get input file
    print("\n📁 Please provide the path to your scraped data file:")
    print("   Example: supplier_database_20241220_143022.csv")
    print("   Example: complete_supplier_database_20241220_143022.xlsx")
    
    file_path = input("\n📂 Enter file path: ").strip().strip('"')
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    # Load data
    df = cleaner.load_scraped_data(file_path)
    if df is None:
        print("❌ Failed to load data. Please check file path and format.")
        return
    
    print(f"\n📊 Data loaded successfully: {len(df)} records")
    print(f"📋 Columns found: {list(df.columns)}")
    
    # Validation options
    print(f"\n🔧 VALIDATION OPTIONS:")
    print(f"1. Fast validation (syntax + basic domain check)")
    print(f"2. Deep validation (includes DNS checking - slower)")
    
    choice = input("\nChoose validation method (1/2): ").strip()
    use_dns = choice == '2'
    
    if use_dns:
        print("⚠️  Deep validation selected. This will be slower but more accurate.")
    
    # Run validation and cleaning
    try:
        print(f"\n🚀 Starting validation process...")
        cleaned_data = cleaner.validate_and_clean_data(df, use_dns_validation=use_dns)
        
        if not cleaned_data:
            print("❌ No data was cleaned. Please check your input file.")
            return
        
        # Generate report
        report = cleaner.generate_validation_report(cleaned_data)
        
        # Print summary
        cleaner.print_validation_summary(report)
        
        # Export options
        print(f"\n💾 EXPORT OPTIONS:")
        print(f"1. Export cleaned database (CSV + Excel)")
        print(f"2. Export high-priority contacts only")
        print(f"3. Export both")
        
        export_choice = input("Choose export option (1/2/3): ").strip()
        
        if export_choice in ['1', '3']:
            files_created = cleaner.export_cleaned_data(cleaned_data)
            print(f"\n✅ Files exported:")
            for file in files_created:
                print(f"   📄 {file}")
        
        if export_choice in ['2', '3']:
            # Export priority contacts
            df_cleaned = pd.DataFrame(cleaned_data)
            priority_df = df_cleaned[
                (df_cleaned['email_valid'] == True) & 
                (df_cleaned['final_quality_score'] >= 70)
            ]
            
            if not priority_df.empty:
                priority_file = f"priority_contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                priority_df.to_csv(priority_file, index=False, encoding='utf-8')
                print(f"   🎯 {priority_file} ({len(priority_df)} high-priority contacts)")
        
        print(f"\n🎉 VALIDATION & CLEANING COMPLETED SUCCESSFULLY!")
        print(f"📊 {len(cleaned_data)} records processed and ready for business use")
        
    except Exception as e:
        print(f"\n❌ Error during processing: {e}")
        print("Please check your input file format and try again.")

if __name__ == "__main__":
    main()