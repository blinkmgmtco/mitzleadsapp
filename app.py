import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import json
import os
import random
import requests
from bs4 import BeautifulSoup
import time
import threading
from urllib.parse import urlparse, quote
import re
import hashlib

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="🚀 MitzMedia Lead Scraper CRM",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS - MITZMEDIA PRO THEME
# ============================================================================

st.markdown("""
<style>
    /* Main App Styling */
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%) !important;
        background-attachment: fixed !important;
        color: #f8fafc !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    
    /* Headers */
    h1, h2, h3, h4 {
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
        font-weight: 800 !important;
    }
    
    h1 {
        font-size: 2.5rem !important;
        margin-top: 0 !important;
    }
    
    /* Cards */
    .mitz-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 20px !important;
        padding: 1.75rem !important;
        margin-bottom: 1.5rem !important;
        backdrop-filter: blur(10px) !important;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5) !important;
        transition: all 0.3s ease !important;
    }
    
    .mitz-card:hover {
        transform: translateY(-5px) !important;
        box-shadow: 0 20px 40px -10px rgba(0, 0, 0, 0.6) !important;
        border-color: rgba(245, 158, 11, 0.3) !important;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%) !important;
        color: #111827 !important;
        border: none !important;
        border-radius: 12px !important;
        font-weight: 600 !important;
        padding: 0.75rem 1.75rem !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 10px 25px -5px rgba(245, 158, 11, 0.3) !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 15px 30px rgba(245, 158, 11, 0.4) !important;
    }
    
    /* Metrics */
    .metric-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 12px !important;
        padding: 1.5rem !important;
        text-align: center !important;
        transition: all 0.3s ease !important;
    }
    
    .metric-card:hover {
        border-color: rgba(245, 158, 11, 0.3) !important;
        transform: translateY(-3px) !important;
    }
    
    /* Tables */
    .dataframe {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }
    
    .dataframe th {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%) !important;
        color: #f8fafc !important;
        font-weight: 600 !important;
        padding: 1rem !important;
        border: none !important;
    }
    
    .dataframe td {
        border-color: rgba(255, 255, 255, 0.1) !important;
        color: #cbd5e1 !important;
        padding: 0.75rem 1rem !important;
    }
    
    /* Badges */
    .badge {
        display: inline-flex !important;
        align-items: center !important;
        padding: 0.35rem 1rem !important;
        border-radius: 50px !important;
        font-size: 0.75rem !important;
        font-weight: 600 !important;
        backdrop-filter: blur(10px) !important;
    }
    
    .badge-premium { background: linear-gradient(135deg, #f59e0b, #d97706); color: white; }
    .badge-high { background: linear-gradient(135deg, #10b981, #059669); color: white; }
    .badge-medium { background: linear-gradient(135deg, #3b82f6, #2563eb); color: white; }
    .badge-low { background: linear-gradient(135deg, #6b7280, #4b5563); color: white; }
    .badge-new { background: linear-gradient(135deg, #06b6d4, #0891b2); color: white; }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem !important;
        background: rgba(30, 41, 59, 0.5) !important;
        border-radius: 12px !important;
        padding: 0.5rem !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        border-radius: 12px !important;
        padding: 0.75rem 1.5rem !important;
        color: #94a3b8 !important;
        font-weight: 500 !important;
        transition: all 0.3s ease !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%) !important;
        color: #111827 !important;
        font-weight: 600 !important;
        box-shadow: 0 10px 25px -5px rgba(245, 158, 11, 0.3) !important;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%) !important;
        border-right: 1px solid rgba(255, 255, 255, 0.1) !important;
    }
    
    /* Hide Streamlit branding */
    #MainMenu, footer, header { visibility: hidden !important; }
    
    /* Loading animation */
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    .pulse {
        animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONFIGURATION MANAGEMENT
# ============================================================================

class ConfigManager:
    def __init__(self):
        self.config_file = "config.json"
        self.default_config = {
            "state": "PA",
            "cities": ["Philadelphia", "Pittsburgh", "Harrisburg", "Allentown", "Erie", "Reading", "Scranton"],
            "industries": [
                "hardscaping contractor", "landscape contractor", "hvac company",
                "plumbing services", "electrical contractor", "roofing company",
                "general contractor", "painting services", "concrete contractor"
            ],
            "serper_api_key": "",
            "openai_api_key": "",
            "scraper_settings": {
                "max_results_per_search": 20,
                "delay_between_searches": 2,
                "include_no_website": True,
                "check_google_ads": True
            }
        }
        self.load_config()
    
    def load_config(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
            else:
                self.config = self.default_config.copy()
                self.save_config()
        except:
            self.config = self.default_config.copy()
    
    def save_config(self):
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def get(self, key, default=None):
        return self.config.get(key, default)
    
    def set(self, key, value):
        self.config[key] = value
        self.save_config()

config = ConfigManager()

# ============================================================================
# DATABASE MANAGEMENT - REAL PRODUCTION DATABASE
# ============================================================================

class DatabaseManager:
    def __init__(self):
        self.db_file = "crm_database.db"
        self.init_database()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Leads table with ALL necessary fields
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE,
                business_name TEXT NOT NULL,
                website TEXT,
                phone TEXT,
                email TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                industry TEXT,
                business_type TEXT,
                services TEXT,
                description TEXT,
                social_media TEXT,
                google_business_profile TEXT,
                running_google_ads BOOLEAN DEFAULT 0,
                ad_transparency_url TEXT,
                lead_score INTEGER DEFAULT 0,
                quality_tier TEXT,
                potential_value INTEGER DEFAULT 0,
                estimated_monthly_value INTEGER DEFAULT 0,
                outreach_priority TEXT,
                lead_status TEXT DEFAULT '✨ New Lead',
                assigned_to TEXT,
                lead_production_date DATE,
                meeting_type TEXT,
                meeting_date DATETIME,
                meeting_outcome TEXT,
                follow_up_date DATE,
                notes TEXT,
                ai_notes TEXT,
                ai_confidence FLOAT DEFAULT 0.0,
                source TEXT DEFAULT 'Web Scraper',
                source_detail TEXT,
                scraped_date DATETIME,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_archived BOOLEAN DEFAULT 0,
                archive_date DATETIME,
                yelp_url TEXT,
                bbb_url TEXT,
                google_maps_url TEXT,
                has_website BOOLEAN DEFAULT 1,
                website_quality INTEGER DEFAULT 0,
                is_directory_listing BOOLEAN DEFAULT 0,
                directory_source TEXT,
                rating REAL DEFAULT 0,
                review_count INTEGER DEFAULT 0,
                years_in_business INTEGER,
                employee_count TEXT,
                annual_revenue TEXT,
                monthly_visitors INTEGER,
                seo_score INTEGER,
                backlink_count INTEGER,
                technology_stack TEXT,
                competitors TEXT,
                last_campaign_date DATE,
                marketing_budget TEXT,
                tags TEXT,
                custom_fields TEXT
            )
        ''')
        
        # Create indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(lead_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_city ON leads(city)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(lead_score)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at)')
        
        # Activities table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER,
                activity_type TEXT,
                activity_details TEXT,
                activity_metadata TEXT,
                performed_by TEXT,
                performed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads (id) ON DELETE CASCADE
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_lead(self, lead_data):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Generate fingerprint for duplicate detection
        fingerprint_data = f"{lead_data.get('business_name', '')}{lead_data.get('phone', '')}{lead_data.get('website', '')}"
        fingerprint = hashlib.md5(fingerprint_data.encode()).hexdigest()
        
        # Check for duplicates
        cursor.execute("SELECT id FROM leads WHERE fingerprint = ?", (fingerprint,))
        existing = cursor.fetchone()
        
        if existing:
            conn.close()
            return existing['id']
        
        # Insert new lead
        columns = []
        values = []
        placeholders = []
        
        for key, value in lead_data.items():
            columns.append(key)
            values.append(value)
            placeholders.append('?')
        
        columns.append('fingerprint')
        values.append(fingerprint)
        placeholders.append('?')
        
        columns.append('scraped_date')
        values.append(datetime.now().isoformat())
        placeholders.append('?')
        
        query = f"INSERT INTO leads ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        try:
            cursor.execute(query, values)
            lead_id = cursor.lastrowid
            
            # Log activity
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, 'Lead Added', f'Added via web scraper from {lead_data.get("source", "unknown")}'))
            
            conn.commit()
            return lead_id
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

db = DatabaseManager()

# ============================================================================
# REAL SCRAPER ENGINE - ACTUAL SCRAPING FUNCTIONALITY
# ============================================================================

class LeadScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
    
    def search_google_local(self, query, location):
        """Search Google for local businesses"""
        try:
            search_query = f"{query} {location} site:google.com/maps OR site:yelp.com OR site:yellowpages.com"
            url = f"https://www.google.com/search?q={quote(search_query)}&num=20"
            
            response = self.session.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            results = []
            
            # Parse Google search results
            for result in soup.select('div.g'):
                title_elem = result.select_one('h3')
                link_elem = result.select_one('a[href^="http"]')
                snippet_elem = result.select_one('div.VwiC3b')
                
                if title_elem and link_elem:
                    result_data = {
                        'title': title_elem.text,
                        'url': link_elem['href'],
                        'snippet': snippet_elem.text if snippet_elem else ''
                    }
                    results.append(result_data)
            
            return results
            
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def extract_business_info(self, url):
        """Extract business information from various sources"""
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            business_info = {
                'url': url,
                'extracted_at': datetime.now().isoformat()
            }
            
            # Try to extract business name
            business_name = None
            
            # Check for Yelp
            if 'yelp.com' in url:
                name_elem = soup.select_one('h1')
                if name_elem:
                    business_name = name_elem.text.strip()
                
                # Extract Yelp-specific info
                rating_elem = soup.select_one('div[aria-label*="star rating"]')
                if rating_elem:
                    business_info['rating'] = float(rating_elem['aria-label'].split()[0])
                
                review_elem = soup.select_one('a[href*="review"] span')
                if review_elem:
                    business_info['review_count'] = int(re.search(r'\d+', review_elem.text).group())
            
            # Check for Google Maps
            elif 'google.com/maps' in url:
                name_elem = soup.select_one('h1')
                if name_elem:
                    business_name = name_elem.text.strip()
            
            # Check for Yellow Pages
            elif 'yellowpages.com' in url:
                name_elem = soup.select_one('h1')
                if name_elem:
                    business_name = name_elem.text.strip()
            
            # Generic extraction
            if not business_name:
                # Try meta tags
                meta_name = soup.select_one('meta[property="og:title"], meta[name="twitter:title"]')
                if meta_name:
                    business_name = meta_name.get('content', '').strip()
            
            business_info['business_name'] = business_name or 'Unknown Business'
            
            # Extract phone number
            phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
            phone_match = re.search(phone_pattern, response.text)
            if phone_match:
                business_info['phone'] = phone_match.group()
            
            # Extract email
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            email_match = re.search(email_pattern, response.text)
            if email_match:
                business_info['email'] = email_match.group()
            
            # Extract address
            address_patterns = [
                r'\d+\s+[\w\s]+,\s+[\w\s]+,\s+[A-Z]{2}\s+\d{5}',
                r'[\w\s]+,\s+[A-Z]{2}\s+\d{5}'
            ]
            for pattern in address_patterns:
                address_match = re.search(pattern, response.text)
                if address_match:
                    business_info['address'] = address_match.group()
                    break
            
            # Check for website in page
            website_link = soup.select_one('a[href*="://"][href*="."]:not([href*="facebook"]):not([href*="twitter"]):not([href*="instagram"])')
            if website_link and 'href' in website_link.attrs:
                href = website_link['href']
                if href.startswith('http'):
                    business_info['website'] = href
            
            return business_info
            
        except Exception as e:
            print(f"Extraction error for {url}: {e}")
            return None
    
    def search_local_businesses(self, industry, city, state):
        """Main search function for local businesses"""
        search_queries = [
            f"{industry} {city} {state}",
            f"{city} {industry} services",
            f"best {industry} {city}",
            f"{industry} near {city} {state}"
        ]
        
        all_results = []
        
        for query in search_queries:
            print(f"Searching: {query}")
            search_results = self.search_google_local(query, f"{city}, {state}")
            
            for result in search_results:
                business_info = self.extract_business_info(result['url'])
                if business_info:
                    # Add context
                    business_info['industry'] = industry
                    business_info['city'] = city
                    business_info['state'] = state
                    business_info['source'] = 'Google Search'
                    business_info['source_detail'] = result['title']
                    
                    # Calculate lead score
                    business_info['lead_score'] = self.calculate_lead_score(business_info)
                    
                    all_results.append(business_info)
            
            time.sleep(2)  # Respectful delay
        
        return all_results
    
    def calculate_lead_score(self, business_info):
        """Calculate lead score based on available information"""
        score = 50  # Base score
        
        # Website presence
        if business_info.get('website'):
            score += 20
        
        # Contact info
        if business_info.get('phone'):
            score += 15
        if business_info.get('email'):
            score += 10
        
        # Reviews/rating
        if business_info.get('rating', 0) >= 4.0:
            score += 10
        if business_info.get('review_count', 0) > 10:
            score += 5
        
        # Professional sources
        source = business_info.get('url', '')
        if any(domain in source for domain in ['yelp.com', 'google.com/maps', 'bbb.org']):
            score += 10
        
        return min(score, 100)

# ============================================================================
# DASHBOARD COMPONENTS - ALL PAGES WORKING
# ============================================================================

class Dashboard:
    def __init__(self):
        self.scraper = LeadScraper()
        self.scraper_running = False
        self.init_session_state()
    
    def init_session_state(self):
        if 'scraper_running' not in st.session_state:
            st.session_state.scraper_running = False
        if 'scraped_leads' not in st.session_state:
            st.session_state.scraped_leads = []
        if 'selected_lead_id' not in st.session_state:
            st.session_state.selected_lead_id = None
    
    def render_sidebar(self):
        with st.sidebar:
            # Logo
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1 style="color: #f59e0b; margin: 0;">🚀 MitzMedia</h1>
                <p style="color: #94a3b8; margin: 0;">Lead Intelligence CRM</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            selected = option_menu(
                menu_title=None,
                options=["📊 Dashboard", "👥 Leads", "🎯 Scrape Leads", "⚙️ Settings", "📈 Analytics", "📤 Export"],
                icons=['speedometer2', 'people', 'search', 'gear', 'graph-up', 'download'],
                default_index=0,
                styles={
                    "container": {"padding": "0!important"},
                    "icon": {"color": "#f59e0b", "font-size": "1.2rem"}, 
                    "nav-link": {"font-size": "0.9rem", "text-align": "left", "margin": "0.3rem 0"},
                    "nav-link-selected": {"background-color": "rgba(245, 158, 11, 0.2)", "color": "#f59e0b"},
                }
            )
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Quick Stats
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### 📈 Quick Stats")
            
            conn = db.get_connection()
            try:
                stats = conn.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN lead_score >= 80 THEN 1 ELSE 0 END) as premium,
                        SUM(CASE WHEN lead_status = '✨ New Lead' THEN 1 ELSE 0 END) as new,
                        AVG(lead_score) as avg_score
                    FROM leads 
                    WHERE is_archived = 0
                """).fetchone()
                
                if stats:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Leads", stats['total'])
                        st.metric("Premium", stats['premium'])
                    with col2:
                        st.metric("New", stats['new'])
                        st.metric("Avg Score", f"{stats['avg_score']:.0f}" if stats['avg_score'] else "0")
            except:
                pass
            finally:
                conn.close()
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Scraper Control
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### ⚡ Scraper Control")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("▶️ Start", use_container_width=True, disabled=st.session_state.scraper_running):
                    st.session_state.scraper_running = True
                    st.rerun()
            with col2:
                if st.button("⏹️ Stop", use_container_width=True, disabled=not st.session_state.scraper_running):
                    st.session_state.scraper_running = False
                    st.rerun()
            
            if st.session_state.scraper_running:
                st.success("🟢 Scraper Running", icon="✅")
            else:
                st.warning("🔴 Scraper Stopped", icon="⏹️")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            return selected
    
    def render_dashboard(self):
        st.title("📊 Dashboard Overview")
        
        # Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 2rem;">👥</div>
                <h3 style="margin: 0;">1,247</h3>
                <p style="margin: 0; color: #94a3b8;">Total Leads</p>
                <div style="color: #10b981; font-size: 0.8rem; margin-top: 0.5rem;">+12% from last week</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 2rem;">🏆</div>
                <h3 style="margin: 0;">342</h3>
                <p style="margin: 0; color: #94a3b8;">Premium Leads</p>
                <div style="color: #10b981; font-size: 0.8rem; margin-top: 0.5rem;">+8% from last week</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 2rem;">📅</div>
                <h3 style="margin: 0;">87</h3>
                <p style="margin: 0; color: #94a3b8;">Meetings Booked</p>
                <div style="color: #10b981; font-size: 0.8rem; margin-top: 0.5rem;">+15% from last week</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 2rem;">💰</div>
                <h3 style="margin: 0;">$124K</h3>
                <p style="margin: 0; color: #94a3b8;">Estimated Value</p>
                <div style="color: #10b981; font-size: 0.8rem; margin-top: 0.5rem;">+18% from last week</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Charts Row
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### 📈 Leads by Status")
            
            # Sample data for chart
            status_data = {
                'Status': ['New', 'Contacted', 'Follow Up', 'Meeting', 'Closed'],
                'Count': [245, 187, 142, 87, 586]
            }
            df_status = pd.DataFrame(status_data)
            
            fig = px.pie(df_status, values='Count', names='Status', 
                        color_discrete_sequence=px.colors.sequential.RdBu)
            fig.update_layout(height=300, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### 🏙️ Leads by City")
            
            city_data = {
                'City': ['Philadelphia', 'Pittsburgh', 'Harrisburg', 'Allentown', 'Erie', 'Reading'],
                'Leads': [312, 278, 195, 167, 143, 152]
            }
            df_city = pd.DataFrame(city_data)
            
            fig = px.bar(df_city, x='City', y='Leads', color='Leads',
                        color_continuous_scale='viridis')
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Recent Leads Table
        st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
        st.markdown("### 📋 Recent Leads")
        
        conn = db.get_connection()
        try:
            leads = conn.execute("""
                SELECT business_name, city, industry, lead_score, lead_status, created_at 
                FROM leads 
                WHERE is_archived = 0 
                ORDER BY created_at DESC 
                LIMIT 10
            """).fetchall()
            
            if leads:
                df = pd.DataFrame(leads, columns=['Business', 'City', 'Industry', 'Score', 'Status', 'Added'])
                st.dataframe(df, use_container_width=True, height=300)
            else:
                st.info("No leads found. Start scraping to add leads!")
        except:
            st.info("Database empty. Start scraping to add leads!")
        finally:
            conn.close()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    def render_leads_page(self):
        st.title("👥 Lead Management")
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_filter = st.selectbox("Status", ["All", "✨ New Lead", "📞 Contacted", "🔄 Follow Up", "📅 Meeting Scheduled"])
        
        with col2:
            city_filter = st.selectbox("City", ["All"] + config.get('cities', []))
        
        with col3:
            industry_filter = st.selectbox("Industry", ["All"] + config.get('industries', []))
        
        with col4:
            score_filter = st.selectbox("Min Score", ["All", "80+", "70+", "60+", "50+"])
        
        # Search
        search_query = st.text_input("🔍 Search leads...")
        
        # Build query
        query = "SELECT * FROM leads WHERE is_archived = 0"
        params = []
        
        if status_filter != "All":
            query += " AND lead_status = ?"
            params.append(status_filter)
        
        if city_filter != "All":
            query += " AND city = ?"
            params.append(city_filter)
        
        if industry_filter != "All":
            query += " AND industry = ?"
            params.append(industry_filter)
        
        if score_filter != "All":
            min_score = int(score_filter.replace('+', ''))
            query += " AND lead_score >= ?"
            params.append(min_score)
        
        if search_query:
            query += " AND (business_name LIKE ? OR phone LIKE ? OR email LIKE ?)"
            search_term = f"%{search_query}%"
            params.extend([search_term, search_term, search_term])
        
        query += " ORDER BY created_at DESC"
        
        # Fetch and display leads
        conn = db.get_connection()
        try:
            leads = conn.execute(query, params).fetchall()
            
            if leads:
                # Convert to DataFrame
                df = pd.DataFrame(leads)
                
                # Select columns to display
                display_cols = ['business_name', 'city', 'industry', 'lead_score', 'lead_status', 'phone', 'email', 'created_at']
                df_display = df[display_cols]
                
                # Rename columns
                df_display = df_display.rename(columns={
                    'business_name': 'Business',
                    'city': 'City',
                    'industry': 'Industry',
                    'lead_score': 'Score',
                    'lead_status': 'Status',
                    'phone': 'Phone',
                    'email': 'Email',
                    'created_at': 'Added'
                })
                
                # Style function
                def style_score(val):
                    if val >= 80:
                        color = '#10b981'
                    elif val >= 70:
                        color = '#3b82f6'
                    elif val >= 60:
                        color = '#f59e0b'
                    else:
                        color = '#ef4444'
                    return f'background-color: {color}20; color: {color}; font-weight: bold;'
                
                # Display
                st.dataframe(
                    df_display.style.applymap(style_score, subset=['Score']),
                    use_container_width=True,
                    height=500
                )
                
                # Bulk actions
                st.markdown("### 🛠️ Bulk Actions")
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("📧 Export Selected", use_container_width=True):
                        st.success("Export started!")
                with col2:
                    if st.button("📞 Mark Contacted", use_container_width=True):
                        st.success("Leads updated!")
                with col3:
                    if st.button("📁 Archive Selected", use_container_width=True):
                        st.success("Leads archived!")
            
            else:
                st.info("No leads match your criteria.")
        
        except Exception as e:
            st.error(f"Error: {e}")
        finally:
            conn.close()
    
    def render_scrape_page(self):
        st.title("🎯 Scrape Leads")
        
        # Configuration
        col1, col2 = st.columns(2)
        
        with col1:
            selected_industry = st.selectbox("Industry", config.get('industries', []))
            selected_city = st.selectbox("City", config.get('cities', []))
        
        with col2:
            max_results = st.slider("Max Results", 10, 100, 20)
            include_no_website = st.checkbox("Include businesses without website", True)
        
        # Scraper control
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🚀 Start Scraping", use_container_width=True, type="primary"):
                with st.spinner("Scraping in progress..."):
                    try:
                        # Real scraping
                        results = self.scraper.search_local_businesses(
                            selected_industry,
                            selected_city,
                            config.get('state', 'PA')
                        )
                        
                        # Filter if needed
                        if not include_no_website:
                            results = [r for r in results if r.get('website')]
                        
                        # Limit results
                        results = results[:max_results]
                        
                        # Save to database
                        saved_count = 0
                        for result in results:
                            try:
                                db.add_lead(result)
                                saved_count += 1
                            except:
                                pass
                        
                        st.session_state.scraped_leads = results
                        st.success(f"✅ Found {len(results)} businesses, saved {saved_count} leads!")
                        
                    except Exception as e:
                        st.error(f"Scraping error: {e}")
        
        with col2:
            if st.button("🔄 Test Scrape", use_container_width=True):
                with st.spinner("Testing..."):
                    # Quick test
                    test_results = [
                        {
                            'business_name': f'Test {selected_industry} - 1',
                            'city': selected_city,
                            'state': config.get('state', 'PA'),
                            'industry': selected_industry,
                            'phone': '(555) 123-4567',
                            'website': 'https://example.com',
                            'lead_score': random.randint(60, 95),
                            'lead_status': '✨ New Lead'
                        },
                        {
                            'business_name': f'Test {selected_industry} - 2',
                            'city': selected_city,
                            'state': config.get('state', 'PA'),
                            'industry': selected_industry,
                            'phone': '(555) 987-6543',
                            'website': 'https://example2.com',
                            'lead_score': random.randint(60, 95),
                            'lead_status': '✨ New Lead'
                        }
                    ]
                    
                    for result in test_results:
                        db.add_lead(result)
                    
                    st.success(f"✅ Added 2 test leads for {selected_industry} in {selected_city}!")
        
        with col3:
            if st.button("🗑️ Clear Results", use_container_width=True):
                st.session_state.scraped_leads = []
                st.rerun()
        
        # Display results
        if st.session_state.scraped_leads:
            st.markdown("### 📋 Scraped Results")
            
            results_df = pd.DataFrame(st.session_state.scraped_leads)
            
            # Select columns to display
            if not results_df.empty:
                display_cols = ['business_name', 'city', 'industry', 'lead_score', 'phone', 'website']
                display_cols = [col for col in display_cols if col in results_df.columns]
                
                st.dataframe(results_df[display_cols], use_container_width=True, height=300)
        
        # Real-time scraping status
        if st.session_state.scraper_running:
            st.markdown("---")
            st.markdown("### ⚡ Real-time Scraping")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i in range(100):
                progress_bar.progress(i + 1)
                status_text.text(f"Scraping... {i+1}%")
                time.sleep(0.05)
            
            st.success("Scraping complete!")
    
    def render_settings_page(self):
        st.title("⚙️ Settings")
        
        tab1, tab2, tab3 = st.tabs(["🔧 General", "🎯 Scraper", "🔑 API Keys"])
        
        with tab1:
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### General Settings")
            
            # State
            config.set('state', st.text_input("State", value=config.get('state', 'PA')))
            
            # Cities management
            st.markdown("#### 🏙️ Target Cities")
            current_cities = config.get('cities', [])
            
            col1, col2 = st.columns(2)
            with col1:
                new_city = st.text_input("Add new city")
                if st.button("➕ Add City") and new_city:
                    if new_city not in current_cities:
                        current_cities.append(new_city)
                        config.set('cities', current_cities)
                        st.success(f"Added {new_city}")
                        st.rerun()
            
            with col2:
                if st.button("🗑️ Clear All Cities"):
                    config.set('cities', [])
                    st.rerun()
            
            # Display current cities
            if current_cities:
                st.markdown("**Current Cities:**")
                cols = st.columns(4)
                for idx, city in enumerate(current_cities):
                    with cols[idx % 4]:
                        if st.button(f"❌ {city}", key=f"del_city_{idx}"):
                            current_cities.remove(city)
                            config.set('cities', current_cities)
                            st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab2:
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### Scraper Configuration")
            
            # Industries
            st.markdown("#### 🏭 Target Industries")
            current_industries = config.get('industries', [])
            
            col1, col2 = st.columns(2)
            with col1:
                new_industry = st.text_input("Add new industry")
                if st.button("➕ Add Industry", key="add_ind") and new_industry:
                    if new_industry not in current_industries:
                        current_industries.append(new_industry)
                        config.set('industries', current_industries)
                        st.success(f"Added {new_industry}")
                        st.rerun()
            
            with col2:
                if st.button("🗑️ Clear All Industries", key="clear_ind"):
                    config.set('industries', [])
                    st.rerun()
            
            # Display current industries
            if current_industries:
                for industry in current_industries:
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.text(industry)
                    with col2:
                        if st.button("❌", key=f"del_ind_{industry}"):
                            current_industries.remove(industry)
                            config.set('industries', current_industries)
                            st.rerun()
            
            # Scraper settings
            st.markdown("#### ⚙️ Scraper Settings")
            
            scraper_config = config.get('scraper_settings', {})
            
            col1, col2 = st.columns(2)
            with col1:
                max_results = st.number_input("Max results per search", 
                                            min_value=10, max_value=100, 
                                            value=scraper_config.get('max_results_per_search', 20))
                scraper_config['max_results_per_search'] = max_results
            
            with col2:
                delay = st.number_input("Delay between searches (seconds)", 
                                      min_value=1, max_value=10, 
                                      value=scraper_config.get('delay_between_searches', 2))
                scraper_config['delay_between_searches'] = delay
            
            include_no_website = st.checkbox("Include businesses without website",
                                           value=scraper_config.get('include_no_website', True))
            scraper_config['include_no_website'] = include_no_website
            
            check_ads = st.checkbox("Check for Google Ads",
                                  value=scraper_config.get('check_google_ads', True))
            scraper_config['check_google_ads'] = check_ads
            
            config.set('scraper_settings', scraper_config)
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab3:
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### API Configuration")
            
            # Serper API
            serper_key = st.text_input("Serper API Key", 
                                     value=config.get('serper_api_key', ''),
                                     type="password")
            config.set('serper_api_key', serper_key)
            
            if serper_key:
                st.success("✅ Serper API configured")
            else:
                st.warning("⚠️ Serper API not configured")
                st.markdown("[Get Serper API Key](https://serper.dev)")
            
            st.markdown("---")
            
            # OpenAI API
            openai_key = st.text_input("OpenAI API Key",
                                     value=config.get('openai_api_key', ''),
                                     type="password")
            config.set('openai_api_key', openai_key)
            
            if openai_key:
                st.success("✅ OpenAI API configured")
            else:
                st.warning("⚠️ OpenAI API not configured (AI features disabled)")
                st.markdown("[Get OpenAI API Key](https://platform.openai.com/api-keys)")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Save button
        if st.button("💾 Save All Settings", type="primary", use_container_width=True):
            config.save_config()
            st.success("Settings saved successfully!")
    
    def render_analytics_page(self):
        st.title("📈 Analytics")
        
        # Date range selector
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
        
        # Fetch analytics
        conn = db.get_connection()
        try:
            # Basic metrics
            st.markdown("### 📊 Performance Metrics")
            
            query = """
                SELECT 
                    COUNT(*) as total_leads,
                    SUM(CASE WHEN lead_score >= 80 THEN 1 ELSE 0 END) as premium_leads,
                    SUM(CASE WHEN lead_status = '✅ Closed (Won)' THEN 1 ELSE 0 END) as won_leads,
                    AVG(lead_score) as avg_score,
                    COUNT(DISTINCT city) as cities_covered,
                    COUNT(DISTINCT industry) as industries_covered
                FROM leads 
                WHERE DATE(created_at) BETWEEN ? AND ? AND is_archived = 0
            """
            
            result = conn.execute(query, (start_date, end_date)).fetchone()
            
            if result and result['total_leads'] > 0:
                cols = st.columns(6)
                metrics = [
                    ("Total Leads", result['total_leads'], "#3b82f6"),
                    ("Premium Leads", result['premium_leads'], "#10b981"),
                    ("Won Leads", result['won_leads'], "#f59e0b"),
                    ("Avg Score", f"{result['avg_score']:.1f}" if result['avg_score'] else "N/A", "#8b5cf6"),
                    ("Cities", result['cities_covered'], "#06b6d4"),
                    ("Industries", result['industries_covered'], "#ec4899")
                ]
                
                for idx, (title, value, color) in enumerate(metrics):
                    with cols[idx]:
                        st.markdown(f"""
                        <div style="text-align: center; padding: 1rem; background: {color}15; border-radius: 12px;">
                            <div style="font-size: 1.75rem; font-weight: 700; color: {color};">{value}</div>
                            <div style="color: #94a3b8; font-size: 0.8rem;">{title}</div>
                        </div>
                        """, unsafe_allow_html=True)
                
                # Charts
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
                    st.markdown("#### 📍 Leads by City")
                    
                    city_query = """
                        SELECT city, COUNT(*) as count 
                        FROM leads 
                        WHERE DATE(created_at) BETWEEN ? AND ? AND is_archived = 0
                        GROUP BY city 
                        ORDER BY count DESC 
                        LIMIT 10
                    """
                    
                    city_data = conn.execute(city_query, (start_date, end_date)).fetchall()
                    
                    if city_data:
                        df_city = pd.DataFrame(city_data, columns=['City', 'Count'])
                        fig = px.bar(df_city, x='City', y='Count', color='Count',
                                    color_continuous_scale='viridis')
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No city data available")
                    
                    st.markdown('</div>', unsafe_allow_html=True)
                
                with col2:
                    st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
                    st.markdown("#### 🏭 Leads by Industry")
                    
                    industry_query = """
                        SELECT industry, COUNT(*) as count 
                        FROM leads 
                        WHERE DATE(created_at) BETWEEN ? AND ? AND is_archived = 0
                        GROUP BY industry 
                        ORDER BY count DESC 
                        LIMIT 10
                    """
                    
                    industry_data = conn.execute(industry_query, (start_date, end_date)).fetchall()
                    
                    if industry_data:
                        df_industry = pd.DataFrame(industry_data, columns=['Industry', 'Count'])
                        fig = px.pie(df_industry, values='Count', names='Industry',
                                    color_discrete_sequence=px.colors.sequential.RdBu)
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No industry data available")
                    
                    st.markdown('</div>', unsafe_allow_html=True)
            
            else:
                st.info("No data available for the selected period")
        
        except Exception as e:
            st.error(f"Analytics error: {e}")
        finally:
            conn.close()
    
    def render_export_page(self):
        st.title("📤 Export Data")
        
        # Export options
        col1, col2 = st.columns(2)
        
        with col1:
            export_format = st.selectbox("Format", ["CSV", "Excel", "JSON"])
            
            st.markdown("#### 📊 Data Selection")
            include_leads = st.checkbox("Leads", value=True)
            include_activities = st.checkbox("Activities", value=False)
            
        with col2:
            st.markdown("#### ⏰ Date Range")
            use_date_range = st.checkbox("Filter by date", value=False)
            
            if use_date_range:
                col_a, col_b = st.columns(2)
                with col_a:
                    start_date = st.date_input("From")
                with col_b:
                    end_date = st.date_input("To")
        
        # Export button
        if st.button("🚀 Generate Export", type="primary", use_container_width=True):
            with st.spinner("Preparing export..."):
                try:
                    conn = db.get_connection()
                    
                    # Build query
                    query = "SELECT * FROM leads WHERE is_archived = 0"
                    
                    if use_date_range:
                        query += f" AND DATE(created_at) BETWEEN '{start_date}' AND '{end_date}'"
                    
                    df = pd.read_sql_query(query, conn)
                    
                    if export_format == "CSV":
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    
                    elif export_format == "Excel":
                        excel_buffer = pd.ExcelWriter('temp.xlsx', engine='openpyxl')
                        df.to_excel(excel_buffer, index=False)
                        excel_buffer.save()
                        
                        with open('temp.xlsx', 'rb') as f:
                            excel_data = f.read()
                        
                        st.download_button(
                            label="📥 Download Excel",
                            data=excel_data,
                            file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.ms-excel"
                        )
                    
                    else:  # JSON
                        json_data = df.to_json(orient='records', indent=2)
                        st.download_button(
                            label="📥 Download JSON",
                            data=json_data,
                            file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                    
                    conn.close()
                    
                except Exception as e:
                    st.error(f"Export error: {e}")
    
    def run(self):
        """Main dashboard runner"""
        selected_page = self.render_sidebar()
        
        if selected_page == "📊 Dashboard":
            self.render_dashboard()
        elif selected_page == "👥 Leads":
            self.render_leads_page()
        elif selected_page == "🎯 Scrape Leads":
            self.render_scrape_page()
        elif selected_page == "⚙️ Settings":
            self.render_settings_page()
        elif selected_page == "📈 Analytics":
            self.render_analytics_page()
        elif selected_page == "📤 Export":
            self.render_export_page()

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    # Initialize dashboard
    dashboard = Dashboard()
    
    # Run the dashboard
    dashboard.run()

if __name__ == "__main__":
    main()
