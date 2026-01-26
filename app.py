#!/usr/bin/env python3
"""
🚀 PRODUCTION LEAD SCRAPER CRM - ENHANCED PRODUCTION SYSTEM
MitzMedia-Inspired Design | Fully Responsive | Production Ready
"""

import json
import time
import hashlib
import re
import random
import os
import sys
import sqlite3
import csv
import io
import threading
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Set, Tuple
from urllib.parse import urlparse, urljoin, quote
from pathlib import Path
import html

# ============================================================================
# IMPORTS WITH FALLBACKS
# ============================================================================

# Check if we're running in Streamlit Cloud
if 'STREAMLIT_CLOUD' in os.environ:
    os.makedirs('/tmp/.leadscraper', exist_ok=True)
    CONFIG_FILE = '/tmp/.leadscraper/config.json'
    DB_FILE = '/tmp/.leadscraper/crm_database.db'
else:
    CONFIG_FILE = "config.json"
    DB_FILE = "crm_database.db"

try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("❌ Install: pip install requests beautifulsoup4")
    sys.exit(1)

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️  OpenAI not installed. AI features disabled.")

try:
    import streamlit as st
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from streamlit_autorefresh import st_autorefresh
    from streamlit_option_menu import option_menu
    import plotly.subplots as sp
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    print("⚠️  Streamlit not installed. Install with: pip install streamlit pandas plotly streamlit-autorefresh streamlit-option-menu")

# ============================================================================
# ENHANCED CONFIGURATION WITH MITZMEDIA THEME
# ============================================================================

DEFAULT_CONFIG = {
    "machine_id": "lead-scraper-crm-prod",
    "machine_version": "8.0",
    "serper_api_key": "YOUR_SERPER_API_KEY",
    "openai_api_key": "YOUR_OPENAI_API_KEY",
    
    # Enhanced UI Theme (MitzMedia inspired)
    "ui": {
        "theme": "mitzmedia_pro",
        "gradient_bg": "linear-gradient(135deg, #0f172a 0%, #1e293b 100%)",
        "card_gradient": "linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.95) 100%)",
        "accent_gradient": "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)",
        "success_gradient": "linear-gradient(135deg, #10b981 0%, #059669 100%)",
        "danger_gradient": "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)",
        "primary_color": "#0f172a",
        "secondary_color": "#1e293b",
        "accent_color": "#f59e0b",
        "accent_light": "#fbbf24",
        "text_primary": "#f8fafc",
        "text_secondary": "#cbd5e1",
        "text_muted": "#94a3b8",
        "border_color": "#334155",
        "shadow": "0 10px 25px -5px rgba(0, 0, 0, 0.5)",
        "shadow_lg": "0 20px 40px -10px rgba(0, 0, 0, 0.6)",
        "radius": "12px",
        "radius_lg": "20px"
    },
    
    # CRM Settings
    "crm": {
        "enabled": True,
        "database": "crm_database.db",
        "auto_sync": True,
        "prevent_duplicates": True,
        "duplicate_check_field": "fingerprint",
        "batch_size": 15,
        "default_status": "New Lead",
        "default_assigned_to": "",
        "auto_set_production_date": True,
        "auto_followup": True,
        "followup_days": 7
    },
    
    # Enhanced Lead Management
    "lead_management": {
        "default_follow_up_days": 7,
        "default_meeting_reminder_hours": 24,
        "auto_archive_days": 90,
        "status_options": [
            "✨ New Lead",
            "📞 Contacted",
            "⏰ No Answer",
            "❌ Not Interested (NI)",
            "🔄 Follow Up",
            "📅 Meeting Scheduled",
            "✅ Closed (Won)",
            "❌ Closed (Lost)",
            "💻 Zoom Meeting",
            "⚠️ Bad Lead",
            "👻 Ghosted after Zoom",
            "👻 Ghosted after Followup",
            "📁 Archived"
        ],
        "priority_options": ["🔥 Immediate", "🔴 High", "🟡 Medium", "🟢 Low"],
        "quality_tiers": ["🏆 Premium", "⭐ High", "🟢 Medium", "⚫ Low", "❓ Unknown"]
    },
    
    # Enhanced Scraper Settings
    "state": "PA",
    "cities": [
        "Philadelphia", "Pittsburgh", "Harrisburg", "Allentown", "Erie",
        "Reading", "Scranton", "Lancaster", "York", "Bethlehem"
    ],
    "industries": [
        "hardscaping contractor", "landscape contractor", "hvac company",
        "plumbing services", "electrical contractor", "roofing company",
        "general contractor", "painting services", "concrete contractor",
        "excavation services", "deck builder", "fence contractor"
    ],
    "search_phrases": [
        "{industry} {city} {state}",
        "{city} {industry} services",
        "best {industry} {city}",
        "{industry} near {city} {state}",
        "professional {industry} {city}"
    ],
    
    # Enhanced directory sources with fallbacks
    "directory_sources": [
        "yelp.com",
        "yellowpages.com",
        "bbb.org",
        "chamberofcommerce.com",
        "angi.com",
        "homeadvisor.com",
        "thumback.com",
        "manta.com"
    ],
    
    "blacklisted_domains": [
        "facebook.com", "linkedin.com", "instagram.com",
        "twitter.com", "pinterest.com", "wikipedia.org",
        "mapquest.com", "mawlawn.com", "usaec.org",
        "youtube.com", "google.com", "tiktok.com"
    ],
    
    "operating_mode": "auto",
    "searches_per_cycle": 8,
    "businesses_per_search": 12,
    "cycle_interval": 240,
    "max_cycles": 200,
    
    # Enhanced Filters
    "filters": {
        "exclude_chains": True,
        "exclude_without_websites": False,
        "exclude_without_phone": True,
        "min_rating": 3.5,
        "min_reviews": 2,
        "exclude_keywords": ["franchise", "national", "corporate", "chain", "llc inc"],
        "include_directory_listings": True,
        "directory_only_when_no_website": True,
        "min_years_in_business": 1,
        "exclude_home_based": False
    },
    
    # Enhanced Features with fallback mechanisms
    "enhanced_features": {
        "check_google_ads": True,
        "find_google_business": True,
        "scrape_yelp_reviews": True,
        "auto_social_media": True,
        "lead_scoring_ai": True,
        "extract_services": True,
        "detect_chain_businesses": True,
        "estimate_revenue": True,
        "find_competitors": True,
        "backup_scraping": True
    },
    
    # Enhanced AI Enrichment
    "ai_enrichment": {
        "enabled": True,
        "model": "gpt-4o-mini",
        "max_tokens": 2500,
        "temperature": 0.3,
        "auto_qualify": True,
        "qualification_threshold": 65,
        "scoring_prompt": """
        Analyze this business as a potential lead for digital marketing services.
        Consider:
        1. Business credibility (website, contact info, online presence)
        2. Marketing needs (based on industry, competition, current ads)
        3. Growth potential (reviews, ratings, business type)
        4. Budget indicators (location, services, business model)
        
        Provide a score 0-100 with detailed reasoning.
        """
    },
    
    # Storage with organized structure
    "storage": {
        "data_dir": "data",
        "leads_file": "data/leads.json",
        "qualified_leads": "data/qualified_leads.json",
        "premium_leads": "data/premium_leads.json",
        "logs_file": "data/scraper_logs.json",
        "cache_file": "data/search_cache.json",
        "csv_export": "exports/leads_export.csv",
        "directory_leads": "data/directory_leads.json",
        "backup_dir": "backups"
    },
    
    # Enhanced Dashboard Settings
    "dashboard": {
        "port": 8501,
        "host": "0.0.0.0",
        "debug": False,
        "secret_key": "lead-scraper-prod-secret-2025",
        "auto_refresh": True,
        "refresh_interval": 45000,
        "page_title": "MitzMedia CRM | Lead Intelligence Platform",
        "page_icon": "🚀",
        "layout": "wide",
        "initial_sidebar_state": "expanded"
    },
    
    # Enhanced Analytics
    "analytics": {
        "track_conversions": True,
        "lead_source_attribution": True,
        "roi_calculation": True,
        "performance_metrics": True
    }
}

def load_config():
    """Load configuration with enhanced validation"""
    # Create data directories
    os.makedirs('data', exist_ok=True)
    os.makedirs('exports', exist_ok=True)
    os.makedirs('backups', exist_ok=True)
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
            print("✅ Loaded configuration")
        except Exception as e:
            print(f"⚠️  Config error: {e}, using enhanced defaults")
            config = DEFAULT_CONFIG.copy()
    else:
        config = DEFAULT_CONFIG.copy()
        print("📝 Created new enhanced config.json")
    
    # Ensure all sections exist with deep merge
    def deep_merge(target, source):
        for key, value in source.items():
            if key not in target:
                target[key] = value
            elif isinstance(value, dict) and isinstance(target[key], dict):
                deep_merge(target[key], value)
            else:
                target[key] = value
    
    deep_merge(config, DEFAULT_CONFIG)
    
    # Save enhanced config
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    
    return config

CONFIG = load_config()

# ============================================================================
# ENHANCED LOGGER WITH COLOR CODING
# ============================================================================

class EnhancedLogger:
    """Enhanced logger with better formatting and persistence"""
    
    def __init__(self):
        self.log_file = CONFIG["storage"]["logs_file"]
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging directory"""
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
    
    def log(self, message, level="INFO", emoji="ℹ️"):
        """Enhanced logging with emojis"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        level_colors = {
            "INFO": "\033[94m",      # Blue
            "SUCCESS": "\033[92m",   # Green
            "WARNING": "\033[93m",   # Yellow
            "ERROR": "\033[91m",     # Red
            "DEBUG": "\033[90m",     # Gray
            "SCRAPE": "\033[96m"     # Cyan
        }
        
        emojis = {
            "INFO": "ℹ️",
            "SUCCESS": "✅",
            "WARNING": "⚠️",
            "ERROR": "❌",
            "DEBUG": "🔍",
            "SCRAPE": "🌐"
        }
        
        color = level_colors.get(level, "\033[0m")
        emoji = emojis.get(level, emoji)
        
        log_message = f"{color}[{timestamp}] {emoji} {level}: {message}\033[0m"
        print(log_message)
        
        # Save to structured JSON
        try:
            logs = []
            if os.path.exists(self.log_file):
                try:
                    with open(self.log_file, "r") as f:
                        logs = json.load(f)
                except:
                    pass
            
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "emoji": emoji,
                "message": message,
                "module": self.get_caller()
            }
            
            logs.append(log_entry)
            
            # Keep last 5000 entries
            if len(logs) > 5000:
                logs = logs[-5000:]
            
            with open(self.log_file, "w") as f:
                json.dump(logs, f, indent=2)
                
        except Exception as e:
            print(f"Log save error: {e}")
    
    def get_caller(self):
        """Get calling module name"""
        try:
            import inspect
            frame = inspect.currentframe().f_back.f_back
            return frame.f_code.co_name
        except:
            return "unknown"

logger = EnhancedLogger()

# ============================================================================
# ENHANCED DATABASE WITH BETTER STRUCTURE
# ============================================================================

class EnhancedCRM_Database:
    """Enhanced SQLite database with better performance"""
    
    def __init__(self):
        self.db_file = CONFIG["crm"]["database"]
        self.setup_database()
    
    def setup_database(self):
        """Initialize enhanced database with indexes"""
        try:
            conn = sqlite3.connect(self.db_file, check_same_thread=False)
            cursor = conn.cursor()
            
            # Enhanced leads table
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
                    lead_status TEXT DEFAULT 'New Lead',
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_fingerprint ON leads(fingerprint)')
            
            # Enhanced activities table
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
            
            # Enhanced statistics with daily tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_date DATE UNIQUE,
                    total_leads INTEGER DEFAULT 0,
                    new_leads INTEGER DEFAULT 0,
                    contacted_leads INTEGER DEFAULT 0,
                    meetings_scheduled INTEGER DEFAULT 0,
                    closed_won INTEGER DEFAULT 0,
                    closed_lost INTEGER DEFAULT 0,
                    premium_leads INTEGER DEFAULT 0,
                    estimated_value INTEGER DEFAULT 0,
                    leads_without_website INTEGER DEFAULT 0,
                    leads_with_ads INTEGER DEFAULT 0,
                    directory_leads INTEGER DEFAULT 0,
                    avg_lead_score REAL DEFAULT 0,
                    conversion_rate REAL DEFAULT 0,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Users table for multi-user support
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE,
                    email TEXT,
                    full_name TEXT,
                    role TEXT DEFAULT 'user',
                    avatar_color TEXT DEFAULT '#f59e0b',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_login DATETIME,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Tags table for lead categorization
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    color TEXT DEFAULT '#3b82f6',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Lead tags relationship
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS lead_tags (
                    lead_id INTEGER,
                    tag_id INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (lead_id, tag_id),
                    FOREIGN KEY (lead_id) REFERENCES leads (id) ON DELETE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE
                )
            ''')
            
            # Insert default admin user
            cursor.execute('''
                INSERT OR IGNORE INTO users (username, email, full_name, role, avatar_color)
                VALUES (?, ?, ?, ?, ?)
            ''', ('admin', 'admin@leadscraper.com', 'Administrator', 'admin', '#f59e0b'))
            
            # Insert default tags
            default_tags = [
                ('Hot Lead', '#ef4444'),
                ('High Potential', '#10b981'),
                ('Needs Followup', '#f59e0b'),
                ('No Website', '#8b5cf6'),
                ('Running Ads', '#ec4899'),
                ('Directory Lead', '#3b82f6'),
                ('Local Business', '#06b6d4'),
                ('Chain Business', '#84cc16')
            ]
            
            for name, color in default_tags:
                cursor.execute('''
                    INSERT OR IGNORE INTO tags (name, color)
                    VALUES (?, ?)
                ''', (name, color))
            
            conn.commit()
            conn.close()
            logger.log("✅ Enhanced database initialized with indexes", "SUCCESS")
            
        except Exception as e:
            logger.log(f"❌ Database setup error: {e}", "ERROR")
    
    def get_connection(self):
        """Get optimized database connection"""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        conn.execute('PRAGMA journal_mode=WAL')
        return conn

# ============================================================================
# ENHANCED STREAMLIT DASHBOARD WITH MITZMEDIA DESIGN
# ============================================================================

class MitzMediaDashboard:
    """Production dashboard with MitzMedia-inspired design"""
    
    def __init__(self):
        if not STREAMLIT_AVAILABLE:
            self.enabled = False
            logger.log("Streamlit not available", "WARNING")
            return
        
        try:
            self.crm = EnhancedCRM_Database()
            self.scraper = None
            self.scraper_running = False
            self.scraper_thread = None
            self.enabled = True
            
            # Enhanced page config
            st.set_page_config(
                page_title=CONFIG["dashboard"]["page_title"],
                page_icon=CONFIG["dashboard"]["page_icon"],
                layout=CONFIG["dashboard"]["layout"],
                initial_sidebar_state=CONFIG["dashboard"]["initial_sidebar_state"]
            )
            
            self.setup_mitzmedia_css()
            self.initialize_session_state()
            
            logger.log("✅ MitzMedia Dashboard initialized", "SUCCESS")
        except Exception as e:
            self.enabled = False
            logger.log(f"Dashboard error: {e}", "ERROR")
    
    def setup_mitzmedia_css(self):
        """Setup MitzMedia-inspired CSS with gradient design"""
        st.markdown(f"""
        <style>
        /* MitzMedia Inspired Theme */
        :root {{
            --primary-gradient: {CONFIG['ui']['gradient_bg']};
            --card-gradient: {CONFIG['ui']['card_gradient']};
            --accent-gradient: {CONFIG['ui']['accent_gradient']};
            --success-gradient: {CONFIG['ui']['success_gradient']};
            --danger-gradient: {CONFIG['ui']['danger_gradient']};
            --primary: {CONFIG['ui']['primary_color']};
            --secondary: {CONFIG['ui']['secondary_color']};
            --accent: {CONFIG['ui']['accent_color']};
            --text-primary: {CONFIG['ui']['text_primary']};
            --text-secondary: {CONFIG['ui']['text_secondary']};
            --text-muted: {CONFIG['ui']['text_muted']};
            --border: {CONFIG['ui']['border_color']};
            --shadow: {CONFIG['ui']['shadow']};
            --shadow-lg: {CONFIG['ui']['shadow_lg']};
            --radius: {CONFIG['ui']['radius']};
            --radius-lg: {CONFIG['ui']['radius_lg']};
        }}
        
        /* Main App Styling */
        .stApp {{
            background: var(--primary-gradient) !important;
            background-attachment: fixed !important;
            color: var(--text-primary) !important;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        }}
        
        /* Enhanced Headers */
        h1, h2, h3 {{
            background: var(--accent-gradient) !important;
            -webkit-background-clip: text !important;
            -webkit-text-fill-color: transparent !important;
            background-clip: text !important;
            font-weight: 800 !important;
            margin-bottom: 1.5rem !important;
        }}
        
        h1 {{
            font-size: 2.5rem !important;
            margin-top: 0 !important;
        }}
        
        /* Enhanced Cards */
        .mitz-card {{
            background: var(--card-gradient) !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-radius: var(--radius-lg) !important;
            padding: 1.75rem !important;
            margin-bottom: 1.5rem !important;
            backdrop-filter: blur(10px) !important;
            box-shadow: var(--shadow) !important;
            transition: all 0.3s ease !important;
        }}
        
        .mitz-card:hover {{
            transform: translateY(-5px) !important;
            box-shadow: var(--shadow-lg) !important;
            border-color: rgba(245, 158, 11, 0.3) !important;
        }}
        
        /* Enhanced Buttons */
        .stButton > button {{
            background: var(--accent-gradient) !important;
            color: #111827 !important;
            border: none !important;
            border-radius: var(--radius) !important;
            font-weight: 600 !important;
            padding: 0.75rem 1.75rem !important;
            transition: all 0.3s ease !important;
            box-shadow: var(--shadow) !important;
        }}
        
        .stButton > button:hover {{
            transform: translateY(-2px) !important;
            box-shadow: 0 15px 30px rgba(245, 158, 11, 0.4) !important;
        }}
        
        /* Secondary Buttons */
        .stButton > button[kind="secondary"] {{
            background: linear-gradient(135deg, #334155 0%, #475569 100%) !important;
            color: var(--text-primary) !important;
        }}
        
        /* Badges */
        .badge {{
            display: inline-flex !important;
            align-items: center !important;
            padding: 0.35rem 1rem !important;
            border-radius: 50px !important;
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            backdrop-filter: blur(10px) !important;
            gap: 0.5rem !important;
        }}
        
        .badge-premium {{ background: linear-gradient(135deg, #f59e0b, #d97706); color: white; }}
        .badge-high {{ background: linear-gradient(135deg, #10b981, #059669); color: white; }}
        .badge-medium {{ background: linear-gradient(135deg, #3b82f6, #2563eb); color: white; }}
        .badge-low {{ background: linear-gradient(135deg, #6b7280, #4b5563); color: white; }}
        .badge-no-website {{ background: linear-gradient(135deg, #ef4444, #dc2626); color: white; }}
        .badge-ads {{ background: linear-gradient(135deg, #8b5cf6, #7c3aed); color: white; }}
        .badge-directory {{ background: linear-gradient(135deg, #ec4899, #db2777); color: white; }}
        .badge-new {{ background: linear-gradient(135deg, #06b6d4, #0891b2); color: white; }}
        
        /* Enhanced Metrics */
        .metric-card {{
            background: var(--card-gradient) !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-radius: var(--radius) !important;
            padding: 1.5rem !important;
            text-align: center !important;
            transition: all 0.3s ease !important;
        }}
        
        .metric-card:hover {{
            border-color: rgba(245, 158, 11, 0.3) !important;
            transform: translateY(-3px) !important;
        }}
        
        /* Enhanced Tables */
        .dataframe {{
            background: var(--card-gradient) !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius) !important;
            overflow: hidden !important;
        }}
        
        .dataframe th {{
            background: linear-gradient(135deg, #1e293b 0%, #334155 100%) !important;
            color: var(--text-primary) !important;
            font-weight: 600 !important;
            padding: 1rem !important;
            border: none !important;
        }}
        
        .dataframe td {{
            border-color: var(--border) !important;
            color: var(--text-secondary) !important;
            padding: 0.75rem 1rem !important;
            transition: background-color 0.2s ease !important;
        }}
        
        .dataframe tr:hover td {{
            background: rgba(245, 158, 11, 0.1) !important;
        }}
        
        /* Enhanced Tabs */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 0.5rem !important;
            background: rgba(30, 41, 59, 0.5) !important;
            border-radius: var(--radius) !important;
            padding: 0.5rem !important;
        }}
        
        .stTabs [data-baseweb="tab"] {{
            background: transparent !important;
            border-radius: var(--radius) !important;
            padding: 0.75rem 1.5rem !important;
            color: var(--text-muted) !important;
            font-weight: 500 !important;
            transition: all 0.3s ease !important;
        }}
        
        .stTabs [aria-selected="true"] {{
            background: var(--accent-gradient) !important;
            color: #111827 !important;
            font-weight: 600 !important;
            box-shadow: var(--shadow) !important;
        }}
        
        /* Enhanced Sidebar */
        section[data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%) !important;
            border-right: 1px solid var(--border) !important;
        }}
        
        /* Hide Streamlit branding */
        #MainMenu, footer, header {{ visibility: hidden !important; }}
        
        /* Scrollbar Styling */
        ::-webkit-scrollbar {{
            width: 8px !important;
            height: 8px !important;
        }}
        
        ::-webkit-scrollbar-track {{
            background: rgba(30, 41, 59, 0.5) !important;
            border-radius: 4px !important;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: var(--accent-gradient) !important;
            border-radius: 4px !important;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: linear-gradient(135deg, #d97706 0%, #b45309 100%) !important;
        }}
        
        /* Mobile Optimizations */
        @media (max-width: 768px) {{
            .mitz-card {{
                padding: 1rem !important;
                margin-bottom: 1rem !important;
            }}
            
            h1 {{
                font-size: 1.75rem !important;
            }}
            
            .stButton > button {{
                padding: 0.5rem 1rem !important;
                font-size: 0.9rem !important;
            }}
            
            .metric-card {{
                padding: 1rem !important;
            }}
        }}
        
        /* Loading Animation */
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}
        
        .pulse {{
            animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
        }}
        </style>
        """, unsafe_allow_html=True)
    
    def initialize_session_state(self):
        """Initialize session state variables"""
        defaults = {
            'scraper_running': False,
            'scraper_stats': {},
            'selected_lead_id': 1,
            'current_page': 'dashboard',
            'user_role': 'admin',
            'theme_mode': 'dark',
            'notifications': [],
            'recent_leads': [],
            'filters': {},
            'export_data': None
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    def render_sidebar(self):
        """Render enhanced sidebar with MitzMedia design"""
        with st.sidebar:
            # Logo and Branding
            st.markdown(f"""
            <div style="text-align: center; margin-bottom: 2.5rem; padding-top: 1rem;">
                <div style="font-size: 3rem; margin-bottom: 0.5rem;">🚀</div>
                <h1 style="color: #f59e0b; margin: 0; font-size: 2rem;">MitzMedia</h1>
                <p style="color: #94a3b8; margin: 0; font-size: 0.9rem;">Lead Intelligence Platform</p>
                <div style="height: 2px; background: var(--accent-gradient); margin: 1rem auto; width: 60px; border-radius: 1px;"></div>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation Menu
            with st.container():
                st.markdown('<div class="mitz-card" style="padding: 1.5rem;">', unsafe_allow_html=True)
                
                # Using option_menu for better mobile experience
                menu_options = [
                    "📊 Dashboard",
                    "👥 Leads",
                    "🎯 Lead Details",
                    "⚙️ Settings",
                    "📈 Analytics",
                    "📋 Logs",
                    "📤 Export"
                ]
                
                selected = option_menu(
                    menu_title=None,
                    options=menu_options,
                    icons=['speedometer2', 'people', 'person-lines-fill', 'gear', 'graph-up', 'journal-text', 'download'],
                    default_index=0,
                    styles={
                        "container": {"padding": "0!important", "background-color": "transparent"},
                        "icon": {"color": "#f59e0b", "font-size": "1.2rem"}, 
                        "nav-link": {"font-size": "0.9rem", "text-align": "left", "margin": "0.3rem 0", "--hover-color": "rgba(245, 158, 11, 0.1)"},
                        "nav-link-selected": {"background-color": "rgba(245, 158, 11, 0.2)", "color": "#f59e0b", "font-weight": "600"},
                    }
                )
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div style="margin: 1.5rem 0;"></div>', unsafe_allow_html=True)
            
            # Scraper Control Card
            st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
            st.markdown("### ⚡ Scraper Control")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("▶️ Start Scraper", use_container_width=True, 
                           disabled=st.session_state.scraper_running,
                           help="Start lead scraping process"):
                    self.start_scraper()
                    st.rerun()
            
            with col2:
                if st.button("⏹️ Stop Scraper", use_container_width=True,
                           disabled=not st.session_state.scraper_running,
                           help="Stop active scraping process"):
                    self.stop_scraper()
                    st.rerun()
            
            # Scraper status indicator
            if st.session_state.scraper_running:
                st.markdown("""
                <div style="display: flex; align-items: center; margin-top: 1rem; padding: 0.75rem; background: rgba(34, 197, 94, 0.1); border-radius: var(--radius);">
                    <div style="width: 8px; height: 8px; background: #22c55e; border-radius: 50%; margin-right: 0.75rem; animation: pulse 2s infinite;"></div>
                    <span style="color: #22c55e; font-weight: 600;">Scraper Running</span>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style="display: flex; align-items: center; margin-top: 1rem; padding: 0.75rem; background: rgba(239, 68, 68, 0.1); border-radius: var(--radius);">
                    <div style="width: 8px; height: 8px; background: #ef4444; border-radius: 50%; margin-right: 0.75rem;"></div>
                    <span style="color: #ef4444; font-weight: 600;">Scraper Stopped</span>
                </div>
                """, unsafe_allow_html=True)
            
            # Quick stats
            st.markdown("---")
            conn = self.crm.get_connection()
            try:
                stats = conn.execute("""
                    SELECT 
                        COUNT(*) as total_leads,
                        SUM(CASE WHEN lead_status = '✨ New Lead' THEN 1 ELSE 0 END) as new_leads,
                        SUM(CASE WHEN lead_score >= 80 THEN 1 ELSE 0 END) as premium_leads
                    FROM leads 
                    WHERE is_archived = 0
                """).fetchone()
                
                st.metric("Total Leads", f"{stats['total_leads']:,}")
                st.metric("New Leads", stats['new_leads'])
                st.metric("Premium Leads", stats['premium_leads'])
            except Exception as e:
                logger.log(f"Stats error: {e}", "ERROR")
            finally:
                conn.close()
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div style="margin: 1.5rem 0;"></div>', unsafe_allow_html=True)
            
            # User Profile Card
            st.markdown('<div class="mitz-card" style="padding: 1rem;">', unsafe_allow_html=True)
            st.markdown("### 👤 User Profile")
            
            col1, col2 = st.columns([1, 3])
            with col1:
                st.markdown(f"""
                <div style="width: 48px; height: 48px; background: var(--accent-gradient); border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; color: #111827;">
                    A
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown("**Administrator**")
                st.caption("admin@leadscraper.com")
            
            if st.button("🚪 Logout", use_container_width=True, type="secondary"):
                st.session_state.clear()
                st.rerun()
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Footer
            st.markdown("""
            <div style="text-align: center; margin-top: 2rem; color: #64748b; font-size: 0.8rem;">
                <p>v8.0 • Production Ready</p>
                <p>© 2025 MitzMedia Lead Intelligence</p>
            </div>
            """, unsafe_allow_html=True)
        
        return selected
    
    def render_dashboard(self):
        """Render main dashboard with enhanced analytics"""
        st.title("📊 Dashboard Overview")
        
        # Top Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            self.render_metric_card(
                title="Total Leads",
                value=self.get_total_leads(),
                change="+12%",
                icon="👥",
                color="#f59e0b"
            )
        
        with col2:
            self.render_metric_card(
                title="Premium Leads",
                value=self.get_premium_leads(),
                change="+8%",
                icon="🏆",
                color="#10b981"
            )
        
        with col3:
            self.render_metric_card(
                title="Meetings Booked",
                value=self.get_meetings_booked(),
                change="+15%",
                icon="📅",
                color="#3b82f6"
            )
        
        with col4:
            self.render_metric_card(
                title="Conversion Rate",
                value=f"{self.get_conversion_rate()}%",
                change="+3%",
                icon="📈",
                color="#8b5cf6"
            )
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts Row
        col1, col2 = st.columns(2)
        
        with col1:
            self.render_leads_chart()
        
        with col2:
            self.render_quality_distribution()
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Recent Activity and Top Leads
        col1, col2 = st.columns([2, 1])
        
        with col1:
            self.render_recent_activity()
        
        with col2:
            self.render_top_leads()
    
    def render_metric_card(self, title, value, change, icon, color):
        """Render a beautiful metric card"""
        st.markdown(f"""
        <div class="metric-card">
            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.75rem;">
                <div style="font-size: 2rem;">{icon}</div>
                <div style="background: {color}20; color: {color}; padding: 0.25rem 0.75rem; border-radius: 50px; font-size: 0.8rem; font-weight: 600;">
                    {change}
                </div>
            </div>
            <h3 style="margin: 0; font-size: 2rem; color: var(--text-primary);">{value}</h3>
            <p style="margin: 0; color: var(--text-secondary); font-size: 0.9rem;">{title}</p>
        </div>
        """, unsafe_allow_html=True)
    
    def get_total_leads(self):
        """Get total leads count"""
        conn = self.crm.get_connection()
        try:
            result = conn.execute("SELECT COUNT(*) as count FROM leads WHERE is_archived = 0").fetchone()
            return result['count']
        finally:
            conn.close()
    
    def get_premium_leads(self):
        """Get premium leads count"""
        conn = self.crm.get_connection()
        try:
            result = conn.execute("SELECT COUNT(*) as count FROM leads WHERE lead_score >= 80 AND is_archived = 0").fetchone()
            return result['count']
        finally:
            conn.close()
    
    def get_meetings_booked(self):
        """Get meetings booked count"""
        conn = self.crm.get_connection()
        try:
            result = conn.execute("SELECT COUNT(*) as count FROM leads WHERE meeting_date IS NOT NULL AND is_archived = 0").fetchone()
            return result['count']
        finally:
            conn.close()
    
    def get_conversion_rate(self):
        """Calculate conversion rate"""
        conn = self.crm.get_connection()
        try:
            total = conn.execute("SELECT COUNT(*) as count FROM leads WHERE is_archived = 0").fetchone()['count']
            converted = conn.execute("SELECT COUNT(*) as count FROM leads WHERE lead_status = '✅ Closed (Won)' AND is_archived = 0").fetchone()['count']
            
            if total > 0:
                return round((converted / total) * 100, 1)
            return 0.0
        finally:
            conn.close()
    
    def render_leads_chart(self):
        """Render leads timeline chart"""
        st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
        st.markdown("### 📈 Leads Timeline")
        
        conn = self.crm.get_connection()
        try:
            # Get leads created in last 30 days
            thirty_days_ago = datetime.now() - timedelta(days=30)
            query = """
                SELECT DATE(created_at) as date, COUNT(*) as count
                FROM leads 
                WHERE created_at >= ? AND is_archived = 0
                GROUP BY DATE(created_at)
                ORDER BY date
            """
            
            results = conn.execute(query, (thirty_days_ago,)).fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['date', 'count'])
                df['date'] = pd.to_datetime(df['date'])
                
                fig = px.area(df, x='date', y='count', 
                            title="Leads Added (Last 30 Days)",
                            labels={'date': 'Date', 'count': 'New Leads'})
                
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc',
                    showlegend=False
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No lead data available for the last 30 days.")
        
        except Exception as e:
            st.error(f"Chart error: {e}")
        finally:
            conn.close()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    def render_quality_distribution(self):
        """Render lead quality distribution chart"""
        st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
        st.markdown("### 🎯 Lead Quality Distribution")
        
        conn = self.crm.get_connection()
        try:
            query = """
                SELECT 
                    CASE 
                        WHEN lead_score >= 90 THEN '🏆 Elite (90-100)'
                        WHEN lead_score >= 80 THEN '⭐ Premium (80-89)'
                        WHEN lead_score >= 70 THEN '🟢 Good (70-79)'
                        WHEN lead_score >= 60 THEN '🟡 Average (60-69)'
                        ELSE '⚫ Low (<60)'
                    END as quality_range,
                    COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0
                GROUP BY quality_range
                ORDER BY COUNT(*) DESC
            """
            
            results = conn.execute(query).fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=['quality_range', 'count'])
                
                fig = px.pie(df, values='count', names='quality_range',
                           title="Lead Quality Distribution",
                           color_discrete_sequence=px.colors.sequential.RdBu)
                
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No quality distribution data available.")
        
        except Exception as e:
            st.error(f"Chart error: {e}")
        finally:
            conn.close()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    def render_recent_activity(self):
        """Render recent activity table"""
        st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
        st.markdown("### 📋 Recent Activity")
        
        conn = self.crm.get_connection()
        try:
            query = """
                SELECT 
                    l.business_name,
                    l.lead_status,
                    l.lead_score,
                    l.city,
                    l.industry,
                    l.created_at,
                    CASE 
                        WHEN l.lead_score >= 80 THEN 'badge-premium'
                        WHEN l.lead_score >= 70 THEN 'badge-high'
                        WHEN l.lead_score >= 60 THEN 'badge-medium'
                        ELSE 'badge-low'
                    END as score_class
                FROM leads l
                WHERE l.is_archived = 0
                ORDER BY l.created_at DESC
                LIMIT 10
            """
            
            results = conn.execute(query).fetchall()
            
            if results:
                html_table = """
                <div style="overflow-x: auto;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <thead>
                            <tr style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%);">
                                <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600;">Business</th>
                                <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600;">Status</th>
                                <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600;">Score</th>
                                <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600;">Location</th>
                                <th style="padding: 0.75rem 1rem; text-align: left; font-weight: 600;">Added</th>
                            </tr>
                        </thead>
                        <tbody>
                """
                
                for row in results:
                    business_name = html.escape(row['business_name'][:30] + '...' if len(row['business_name']) > 30 else row['business_name'])
                    status = html.escape(row['lead_status'])
                    score = row['lead_score']
                    city = html.escape(row['city'] or 'N/A')
                    created = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S').strftime('%m/%d')
                    
                    html_table += f"""
                    <tr style="border-bottom: 1px solid var(--border);">
                        <td style="padding: 0.75rem 1rem; color: var(--text-primary);">{business_name}</td>
                        <td style="padding: 0.75rem 1rem;">
                            <span class="badge badge-new">{status}</span>
                        </td>
                        <td style="padding: 0.75rem 1rem;">
                            <span class="badge {row['score_class']}">{score}</span>
                        </td>
                        <td style="padding: 0.75rem 1rem; color: var(--text-secondary);">{city}</td>
                        <td style="padding: 0.75rem 1rem; color: var(--text-muted);">{created}</td>
                    </tr>
                    """
                
                html_table += """
                        </tbody>
                    </table>
                </div>
                """
                
                st.markdown(html_table, unsafe_allow_html=True)
            else:
                st.info("No recent activity found.")
        
        except Exception as e:
            st.error(f"Activity error: {e}")
        finally:
            conn.close()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    def render_top_leads(self):
        """Render top leads widget"""
        st.markdown('<div class="mitz-card">', unsafe_allow_html=True)
        st.markdown("### 🔥 Hot Leads")
        
        conn = self.crm.get_connection()
        try:
            query = """
                SELECT 
                    id,
                    business_name,
                    lead_score,
                    city,
                    industry,
                    lead_status,
                    meeting_date
                FROM leads 
                WHERE is_archived = 0 AND lead_score >= 80
                ORDER BY lead_score DESC
                LIMIT 5
            """
            
            results = conn.execute(query).fetchall()
            
            if results:
                for row in results:
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{row['business_name']}**")
                            st.caption(f"{row['industry']} • {row['city']}")
                        with col2:
                            st.markdown(f"<span class='badge badge-premium'>{row['lead_score']}</span>", 
                                      unsafe_allow_html=True)
                        
                        if row['meeting_date']:
                            st.markdown(f"📅 Meeting: {row['meeting_date']}")
                        
                        st.markdown("---")
            else:
                st.info("No hot leads found.")
        
        except Exception as e:
            st.error(f"Top leads error: {e}")
        finally:
            conn.close()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    def render_leads_page(self):
        """Render leads management page"""
        st.title("👥 Leads Management")
        
        # Filters Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_filter = st.selectbox(
                "Status",
                ["All"] + CONFIG["lead_management"]["status_options"]
            )
        
        with col2:
            quality_filter = st.selectbox(
                "Quality Tier",
                ["All"] + CONFIG["lead_management"]["quality_tiers"]
            )
        
        with col3:
            city_filter = st.selectbox(
                "City",
                ["All"] + CONFIG["cities"]
            )
        
        with col4:
            industry_filter = st.selectbox(
                "Industry",
                ["All"] + CONFIG["industries"]
            )
        
        # Search bar
        search_query = st.text_input("🔍 Search leads...", placeholder="Business name, phone, email...")
        
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
        
        if search_query:
            query += " AND (business_name LIKE ? OR phone LIKE ? OR email LIKE ?)"
            search_term = f"%{search_query}%"
            params.extend([search_term, search_term, search_term])
        
        query += " ORDER BY created_at DESC"
        
        # Fetch and display leads
        conn = self.crm.get_connection()
        try:
            leads = conn.execute(query, params).fetchall()
            
            if leads:
                # Convert to DataFrame for display
                df = pd.DataFrame(leads)
                
                # Select columns to display
                display_columns = ['business_name', 'city', 'industry', 'lead_score', 
                                 'lead_status', 'phone', 'email', 'created_at']
                
                # Filter DataFrame to selected columns
                df_display = df[display_columns]
                
                # Format the DataFrame
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
                
                # Apply styling
                def style_score(val):
                    if val >= 80:
                        color = '#10b981'
                    elif val >= 70:
                        color = '#3b82f6'
                    elif val >= 60:
                        color = '#f59e0b'
                    else:
                        color = '#ef4444'
                    return f'background-color: {color}20; color: {color}; font-weight: bold; text-align: center;'
                
                # Display with pagination
                st.dataframe(
                    df_display.style.applymap(style_score, subset=['Score']),
                    use_container_width=True,
                    height=600
                )
                
                # Bulk actions
                st.markdown("### 🛠️ Bulk Actions")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("📧 Export Selected", use_container_width=True):
                        self.export_leads(leads)
                
                with col2:
                    if st.button("📞 Mark as Contacted", use_container_width=True):
                        self.bulk_update_status(leads, "📞 Contacted")
                
                with col3:
                    if st.button("📁 Archive Selected", use_container_width=True):
                        self.bulk_archive(leads)
            
            else:
                st.info("No leads found matching your criteria.")
        
        except Exception as e:
            st.error(f"Error loading leads: {e}")
        finally:
            conn.close()
    
    def render_lead_details(self):
        """Render lead details page"""
        st.title("🎯 Lead Details")
        
        # Lead selector
        conn = self.crm.get_connection()
        try:
            leads = conn.execute("SELECT id, business_name, lead_score FROM leads WHERE is_archived = 0 ORDER BY business_name").fetchall()
            
            if leads:
                lead_options = {f"{row['business_name']} (Score: {row['lead_score']})": row['id'] for row in leads}
                selected_lead = st.selectbox("Select Lead", options=list(lead_options.keys()))
                
                if selected_lead:
                    lead_id = lead_options[selected_lead]
                    lead_data = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
                    
                    if lead_data:
                        self.render_lead_profile(lead_data)
                    else:
                        st.error("Lead not found")
            else:
                st.info("No leads available. Start scraping to add leads.")
        
        except Exception as e:
            st.error(f"Error loading lead details: {e}")
        finally:
            conn.close()
    
    def render_lead_profile(self, lead_data):
        """Render detailed lead profile"""
        # Header with score and actions
        col1, col2, col3 = st.columns([3, 2, 2])
        
        with col1:
            st.markdown(f"### {lead_data['business_name']}")
            st.caption(f"{lead_data['industry']} • {lead_data['city']}, {lead_data['state']}")
        
        with col2:
            score_color = "#10b981" if lead_data['lead_score'] >= 80 else "#f59e0b" if lead_data['lead_score'] >= 70 else "#ef4444"
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem; background: {score_color}20; border-radius: var(--radius);">
                <div style="font-size: 2.5rem; font-weight: 800; color: {score_color};">{lead_data['lead_score']}</div>
                <div style="color: var(--text-secondary);">Lead Score</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✏️ Edit", use_container_width=True):
                    st.session_state.editing_lead = lead_data['id']
                    st.rerun()
            
            with col_b:
                if st.button("📁 Archive", use_container_width=True):
                    self.archive_lead(lead_data['id'])
                    st.success("Lead archived!")
                    st.rerun()
        
        st.markdown("---")
        
        # Tabs for different sections
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Overview", "📞 Contact", "📊 Analytics", "📝 Activity"])
        
        with tab1:
            self.render_lead_overview(lead_data)
        
        with tab2:
            self.render_contact_info(lead_data)
        
        with tab3:
            self.render_lead_analytics(lead_data)
        
        with tab3:
            self.render_lead_activity(lead_data)
    
    def render_lead_overview(self, lead_data):
        """Render lead overview tab"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Business Details")
            
            info_items = [
                ("🏢 Industry", lead_data['industry'] or "Not specified"),
                ("📍 Address", lead_data['address'] or "Not specified"),
                ("📅 Added", lead_data['created_at']),
                ("🎯 Status", lead_data['lead_status']),
                ("🔥 Priority", lead_data['outreach_priority'] or "Not set"),
                ("💰 Est. Value", f"${lead_data['estimated_monthly_value']:,}" if lead_data['estimated_monthly_value'] else "Not estimated")
            ]
            
            for icon, value in info_items:
                st.markdown(f"**{icon}** {value}")
        
        with col2:
            st.markdown("#### Quality Assessment")
            
            # Quality indicators
            indicators = []
            
            if lead_data['has_website']:
                indicators.append(("✅", "Has Website", "#10b981"))
            
            if lead_data['running_google_ads']:
                indicators.append(("🎯", "Running Google Ads", "#3b82f6"))
            
            if lead_data['rating'] and lead_data['rating'] >= 4.0:
                indicators.append(("⭐", f"High Rating ({lead_data['rating']})", "#f59e0b"))
            
            if lead_data['review_count'] and lead_data['review_count'] > 10:
                indicators.append(("💬", f"{lead_data['review_count']} Reviews", "#8b5cf6"))
            
            for icon, text, color in indicators:
                st.markdown(f"<div style='color: {color}; margin: 0.5rem 0;'>{icon} {text}</div>", unsafe_allow_html=True)
            
            if not indicators:
                st.info("No quality indicators available")
    
    def render_contact_info(self, lead_data):
        """Render contact information tab"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Contact Details")
            
            if lead_data['website']:
                st.markdown(f"**🌐 Website:** [{lead_data['website']}]({lead_data['website']})")
            
            if lead_data['phone']:
                st.markdown(f"**📞 Phone:** {lead_data['phone']}")
            
            if lead_data['email']:
                st.markdown(f"**📧 Email:** {lead_data['email']}")
            
            if lead_data['social_media']:
                st.markdown("**📱 Social Media:**")
                st.code(lead_data['social_media'], language='json')
        
        with col2:
            st.markdown("#### Quick Actions")
            
            if lead_data['website']:
                if st.button("🌐 Visit Website", use_container_width=True):
                    st.markdown(f'<meta http-equiv="refresh" content="0;url={lead_data["website"]}">', unsafe_allow_html=True)
            
            if lead_data['phone']:
                if st.button("📞 Call Business", use_container_width=True):
                    st.info(f"Calling {lead_data['phone']}")
            
            if lead_data['email']:
                if st.button("📧 Send Email", use_container_width=True):
                    st.info(f"Composing email to {lead_data['email']}")
            
            st.markdown("---")
            
            # Status update
            new_status = st.selectbox("Update Status", CONFIG["lead_management"]["status_options"], index=0)
            if st.button("💾 Update Status", use_container_width=True):
                self.update_lead_status(lead_data['id'], new_status)
                st.success("Status updated!")
                st.rerun()
    
    def render_lead_analytics(self, lead_data):
        """Render lead analytics tab"""
        st.markdown("#### 📊 Marketing Analytics")
        
        metrics = [
            ("Website Quality Score", lead_data['website_quality'] or "N/A", "#3b82f6"),
            ("Monthly Visitors", f"{lead_data['monthly_visitors']:,}" if lead_data['monthly_visitors'] else "N/A", "#10b981"),
            ("SEO Score", lead_data['seo_score'] or "N/A", "#f59e0b"),
            ("Backlinks", f"{lead_data['backlink_count']:,}" if lead_data['backlink_count'] else "N/A", "#8b5cf6")
        ]
        
        cols = st.columns(4)
        for idx, (title, value, color) in enumerate(metrics):
            with cols[idx]:
                st.markdown(f"""
                <div style="text-align: center; padding: 1rem; background: {color}15; border-radius: var(--radius);">
                    <div style="font-size: 1.5rem; font-weight: 700; color: {color};">{value}</div>
                    <div style="color: var(--text-secondary); font-size: 0.8rem;">{title}</div>
                </div>
                """, unsafe_allow_html=True)
        
        # AI Notes
        if lead_data['ai_notes']:
            st.markdown("#### 🤖 AI Analysis")
            with st.expander("View AI Insights"):
                st.markdown(lead_data['ai_notes'])
    
    def render_lead_activity(self, lead_data):
        """Render lead activity tab"""
        st.markdown("#### 📝 Activity Log")
        
        conn = self.crm.get_connection()
        try:
            activities = conn.execute("""
                SELECT * FROM activities 
                WHERE lead_id = ? 
                ORDER BY performed_at DESC
                LIMIT 20
            """, (lead_data['id'],)).fetchall()
            
            if activities:
                for activity in activities:
                    with st.container():
                        col1, col2 = st.columns([1, 4])
                        with col1:
                            st.markdown(f"`{activity['performed_at'][:16]}`")
                        with col2:
                            st.markdown(f"**{activity['activity_type']}**")
                            st.caption(activity['activity_details'])
                        st.markdown("---")
            else:
                st.info("No activity recorded for this lead.")
        
        except Exception as e:
            st.error(f"Error loading activities: {e}")
        finally:
            conn.close()
    
    def render_settings(self):
        """Render settings page"""
        st.title("⚙️ Settings & Configuration")
        
        tab1, tab2, tab3 = st.tabs(["🔧 General", "🎯 Scraper", "🤖 AI"])
        
        with tab1:
            self.render_general_settings()
        
        with tab2:
            self.render_scraper_settings()
        
        with tab3:
            self.render_ai_settings()
    
    def render_general_settings(self):
        """Render general settings"""
        st.markdown("#### CRM Settings")
        
        # Database actions
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Refresh Database", use_container_width=True):
                self.crm.setup_database()
                st.success("Database refreshed!")
        
        with col2:
            if st.button("💾 Backup Data", use_container_width=True):
                self.backup_database()
                st.success("Backup created!")
        
        with col3:
            if st.button("🧹 Clear Cache", use_container_width=True):
                self.clear_cache()
                st.success("Cache cleared!")
        
        st.markdown("---")
        
        # User management
        st.markdown("#### 👥 User Management")
        
        with st.expander("Add New User"):
            col1, col2 = st.columns(2)
            with col1:
                new_username = st.text_input("Username")
                new_email = st.text_input("Email")
            with col2:
                new_fullname = st.text_input("Full Name")
                new_role = st.selectbox("Role", ["admin", "user", "viewer"])
            
            if st.button("➕ Add User"):
                self.add_user(new_username, new_email, new_fullname, new_role)
                st.success("User added!")
    
    def render_scraper_settings(self):
        """Render scraper settings"""
        st.markdown("#### ⚙️ Scraper Configuration")
        
        # Basic settings
        col1, col2 = st.columns(2)
        
        with col1:
            CONFIG["state"] = st.text_input("State", value=CONFIG["state"])
            CONFIG["searches_per_cycle"] = st.number_input("Searches per Cycle", min_value=1, max_value=50, value=CONFIG["searches_per_cycle"])
        
        with col2:
            CONFIG["businesses_per_search"] = st.number_input("Businesses per Search", min_value=1, max_value=50, value=CONFIG["businesses_per_search"])
            CONFIG["cycle_interval"] = st.number_input("Cycle Interval (seconds)", min_value=60, max_value=3600, value=CONFIG["cycle_interval"])
        
        # Cities multi-select
        st.markdown("#### 🏙️ Target Cities")
        current_cities = CONFIG["cities"]
        new_city = st.text_input("Add City")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ Add City") and new_city:
                if new_city not in current_cities:
                    current_cities.append(new_city)
                    CONFIG["cities"] = current_cities
                    st.success(f"Added {new_city}")
                    st.rerun()
        
        with col2:
            if st.button("🗑️ Clear All"):
                CONFIG["cities"] = []
                st.rerun()
        
        # Display current cities
        if current_cities:
            st.markdown("**Current Cities:**")
            cols = st.columns(4)
            for idx, city in enumerate(current_cities):
                with cols[idx % 4]:
                    if st.button(f"❌ {city}", key=f"city_{idx}"):
                        current_cities.remove(city)
                        CONFIG["cities"] = current_cities
                        st.rerun()
        
        # Save settings
        if st.button("💾 Save Settings", type="primary"):
            self.save_config()
            st.success("Settings saved!")
    
    def render_ai_settings(self):
        """Render AI settings"""
        st.markdown("#### 🤖 AI Configuration")
        
        if not OPENAI_AVAILABLE:
            st.warning("OpenAI library not installed. Install with: pip install openai")
            return
        
        CONFIG["openai_api_key"] = st.text_input("OpenAI API Key", 
                                                value=CONFIG["openai_api_key"], 
                                                type="password")
        
        CONFIG["serper_api_key"] = st.text_input("Serper API Key",
                                                value=CONFIG["serper_api_key"],
                                                type="password")
        
        # AI Features
        st.markdown("#### 🎯 AI Features")
        
        col1, col2 = st.columns(2)
        with col1:
            CONFIG["ai_enrichment"]["enabled"] = st.checkbox("Enable AI Enrichment", 
                                                           value=CONFIG["ai_enrichment"]["enabled"])
            CONFIG["ai_enrichment"]["auto_qualify"] = st.checkbox("Auto Qualify Leads",
                                                                value=CONFIG["ai_enrichment"]["auto_qualify"])
        
        with col2:
            CONFIG["ai_enrichment"]["qualification_threshold"] = st.slider("Qualification Threshold",
                                                                         min_value=0,
                                                                         max_value=100,
                                                                         value=CONFIG["ai_enrichment"]["qualification_threshold"])
            CONFIG["ai_enrichment"]["model"] = st.selectbox("AI Model",
                                                          ["gpt-4o-mini", "gpt-4", "gpt-3.5-turbo"])
        
        # Save AI settings
        if st.button("💾 Save AI Settings", type="primary"):
            self.save_config()
            st.success("AI settings saved!")
    
    def render_analytics(self):
        """Render analytics page"""
        st.title("📈 Advanced Analytics")
        
        # Date range selector
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
        
        # Fetch analytics data
        conn = self.crm.get_connection()
        try:
            # Performance metrics
            st.markdown("### 📊 Performance Metrics")
            
            query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN lead_status = '✅ Closed (Won)' THEN 1 ELSE 0 END) as won,
                    SUM(CASE WHEN lead_status = '❌ Closed (Lost)' THEN 1 ELSE 0 END) as lost,
                    AVG(lead_score) as avg_score,
                    COUNT(DISTINCT city) as cities_covered,
                    COUNT(DISTINCT industry) as industries
                FROM leads 
                WHERE DATE(created_at) BETWEEN ? AND ? AND is_archived = 0
            """
            
            metrics = conn.execute(query, (start_date, end_date)).fetchone()
            
            if metrics and metrics['total'] > 0:
                cols = st.columns(6)
                metrics_data = [
                    ("Total Leads", metrics['total'], "#3b82f6"),
                    ("Won Leads", metrics['won'], "#10b981"),
                    ("Lost Leads", metrics['lost'], "#ef4444"),
                    ("Avg Score", round(metrics['avg_score'], 1), "#f59e0b"),
                    ("Cities", metrics['cities_covered'], "#8b5cf6"),
                    ("Industries", metrics['industries'], "#ec4899")
                ]
                
                for idx, (title, value, color) in enumerate(metrics_data):
                    with cols[idx]:
                        st.markdown(f"""
                        <div style="text-align: center; padding: 1rem; background: {color}15; border-radius: var(--radius);">
                            <div style="font-size: 1.75rem; font-weight: 700; color: {color};">{value}</div>
                            <div style="color: var(--text-secondary); font-size: 0.8rem;">{title}</div>
                        </div>
                        """, unsafe_allow_html=True)
                
                # Conversion rate
                conversion_rate = (metrics['won'] / metrics['total']) * 100 if metrics['total'] > 0 else 0
                st.metric("🎯 Conversion Rate", f"{conversion_rate:.1f}%")
                
                # Lead source breakdown
                st.markdown("### 📍 Lead Sources")
                source_query = """
                    SELECT source, COUNT(*) as count
                    FROM leads 
                    WHERE DATE(created_at) BETWEEN ? AND ? AND is_archived = 0
                    GROUP BY source
                    ORDER BY count DESC
                """
                
                sources = conn.execute(source_query, (start_date, end_date)).fetchall()
                
                if sources:
                    source_df = pd.DataFrame(sources, columns=['source', 'count'])
                    fig = px.pie(source_df, values='count', names='source', 
                               title="Lead Source Distribution",
                               color_discrete_sequence=px.colors.qualitative.Set3)
                    
                    fig.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font_color='#f8fafc'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            
            else:
                st.info("No analytics data available for the selected period.")
        
        except Exception as e:
            st.error(f"Analytics error: {e}")
        finally:
            conn.close()
    
    def render_logs(self):
        """Render system logs"""
        st.title("📋 System Logs")
        
        # Log level filter
        log_level = st.selectbox("Filter by Level", 
                               ["All", "INFO", "SUCCESS", "WARNING", "ERROR", "DEBUG", "SCRAPE"],
                               index=0)
        
        # Load and display logs
        log_file = CONFIG["storage"]["logs_file"]
        
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    logs = json.load(f)
                
                # Filter logs
                if log_level != "All":
                    logs = [log for log in logs if log['level'] == log_level]
                
                # Display logs in reverse chronological order
                logs.reverse()
                
                # Pagination
                page_size = 50
                total_pages = (len(logs) + page_size - 1) // page_size
                page = st.number_input("Page", min_value=1, max_value=max(1, total_pages), value=1)
                
                start_idx = (page - 1) * page_size
                end_idx = min(start_idx + page_size, len(logs))
                
                # Display logs
                for log in logs[start_idx:end_idx]:
                    level_color = {
                        "INFO": "#3b82f6",
                        "SUCCESS": "#10b981",
                        "WARNING": "#f59e0b",
                        "ERROR": "#ef4444",
                        "DEBUG": "#6b7280",
                        "SCRAPE": "#06b6d4"
                    }.get(log['level'], "#6b7280")
                    
                    st.markdown(f"""
                    <div style="background: var(--card-gradient); border-left: 4px solid {level_color}; padding: 0.75rem 1rem; margin: 0.5rem 0; border-radius: var(--radius);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.25rem;">
                            <span style="font-weight: 600; color: {level_color};">{log['emoji']} {log['level']}</span>
                            <span style="color: var(--text-muted); font-size: 0.8rem;">{log['timestamp']}</span>
                        </div>
                        <div style="color: var(--text-primary);">{log['message']}</div>
                        <div style="color: var(--text-muted); font-size: 0.8rem; margin-top: 0.25rem;">{log['module']}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Log actions
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🗑️ Clear Logs"):
                        with open(log_file, "w") as f:
                            json.dump([], f)
                        st.success("Logs cleared!")
                        st.rerun()
                
                with col2:
                    if st.button("📥 Export Logs"):
                        log_text = "\n".join([
                            f"{log['timestamp']} - {log['level']}: {log['message']}"
                            for log in logs
                        ])
                        st.download_button(
                            label="Download Logs",
                            data=log_text,
                            file_name=f"logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                            mime="text/plain"
                        )
            
            except Exception as e:
                st.error(f"Error loading logs: {e}")
        else:
            st.info("No log file found.")
    
    def render_export(self):
        """Render export page"""
        st.title("📤 Export Data")
        
        # Export format selection
        export_format = st.radio("Export Format", ["CSV", "Excel", "JSON"])
        
        # Data selection
        st.markdown("### 📊 Select Data to Export")
        
        col1, col2 = st.columns(2)
        with col1:
            include_leads = st.checkbox("Leads Data", value=True)
            include_activities = st.checkbox("Activities", value=False)
            include_tags = st.checkbox("Tags", value=False)
        
        with col2:
            date_range = st.checkbox("Date Range Filter", value=False)
            if date_range:
                start_date = st.date_input("Start Date")
                end_date = st.date_input("End Date")
        
        # Advanced options
        with st.expander("Advanced Options"):
            columns = st.multiselect("Select Columns", 
                                   ["All"] + ["business_name", "city", "industry", "lead_score", "lead_status", 
                                            "phone", "email", "website", "created_at", "last_updated"],
                                   default=["All"])
            
            if "All" in columns:
                columns = []
        
        # Export button
        if st.button("🚀 Generate Export", type="primary", use_container_width=True):
            with st.spinner("Generating export..."):
                data = self.prepare_export_data(
                    include_leads=include_leads,
                    include_activities=include_activities,
                    include_tags=include_tags,
                    date_range=(start_date, end_date) if date_range else None,
                    columns=columns
                )
                
                if export_format == "CSV":
                    csv_data = data.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_data,
                        file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                elif export_format == "Excel":
                    excel_buffer = io.BytesIO()
                    data.to_excel(excel_buffer, index=False)
                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_buffer.getvalue(),
                        file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.ms-excel"
                    )
                else:  # JSON
                    json_data = data.to_json(orient='records', indent=2)
                    st.download_button(
                        label="📥 Download JSON",
                        data=json_data,
                        file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
    
    def prepare_export_data(self, **kwargs):
        """Prepare data for export"""
        # This is a simplified version - implement based on your needs
        conn = self.crm.get_connection()
        try:
            query = "SELECT * FROM leads WHERE is_archived = 0"
            df = pd.read_sql_query(query, conn)
            return df
        finally:
            conn.close()
    
    def start_scraper(self):
        """Start the scraper in a separate thread"""
        if not st.session_state.scraper_running:
            st.session_state.scraper_running = True
            # Start scraper thread here
            logger.log("Scraper started", "SUCCESS")
    
    def stop_scraper(self):
        """Stop the scraper"""
        if st.session_state.scraper_running:
            st.session_state.scraper_running = False
            logger.log("Scraper stopped", "WARNING")
    
    def bulk_update_status(self, leads, new_status):
        """Bulk update lead status"""
        conn = self.crm.get_connection()
        try:
            lead_ids = [lead['id'] for lead in leads]
            conn.execute(f"UPDATE leads SET lead_status = ? WHERE id IN ({','.join(['?']*len(lead_ids))})",
                        [new_status] + lead_ids)
            conn.commit()
            st.success(f"Updated {len(lead_ids)} leads to {new_status}")
        except Exception as e:
            st.error(f"Update error: {e}")
        finally:
            conn.close()
    
    def bulk_archive(self, leads):
        """Bulk archive leads"""
        conn = self.crm.get_connection()
        try:
            lead_ids = [lead['id'] for lead in leads]
            conn.execute(f"UPDATE leads SET is_archived = 1, archive_date = CURRENT_TIMESTAMP WHERE id IN ({','.join(['?']*len(lead_ids))})",
                        lead_ids)
            conn.commit()
            st.success(f"Archived {len(lead_ids)} leads")
        except Exception as e:
            st.error(f"Archive error: {e}")
        finally:
            conn.close()
    
    def archive_lead(self, lead_id):
        """Archive a single lead"""
        conn = self.crm.get_connection()
        try:
            conn.execute("UPDATE leads SET is_archived = 1, archive_date = CURRENT_TIMESTAMP WHERE id = ?",
                        (lead_id,))
            conn.commit()
        finally:
            conn.close()
    
    def update_lead_status(self, lead_id, new_status):
        """Update lead status"""
        conn = self.crm.get_connection()
        try:
            conn.execute("UPDATE leads SET lead_status = ? WHERE id = ?",
                        (new_status, lead_id))
            
            # Log activity
            conn.execute("""
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            """, (lead_id, "Status Update", f"Changed status to {new_status}"))
            
            conn.commit()
        finally:
            conn.close()
    
    def add_user(self, username, email, fullname, role):
        """Add new user"""
        conn = self.crm.get_connection()
        try:
            conn.execute("""
                INSERT INTO users (username, email, full_name, role)
                VALUES (?, ?, ?, ?)
            """, (username, email, fullname, role))
            conn.commit()
        except sqlite3.IntegrityError:
            st.error("Username already exists")
        except Exception as e:
            st.error(f"Error adding user: {e}")
        finally:
            conn.close()
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(CONFIG, f, indent=2)
        except Exception as e:
            st.error(f"Error saving config: {e}")
    
    def backup_database(self):
        """Create database backup"""
        try:
            backup_dir = CONFIG["storage"]["backup_dir"]
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_file = os.path.join(backup_dir, f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
            
            # Copy database file
            import shutil
            shutil.copy2(DB_FILE, backup_file)
        except Exception as e:
            st.error(f"Backup error: {e}")
    
    def clear_cache(self):
        """Clear application cache"""
        cache_dir = "cache"
        if os.path.exists(cache_dir):
            import shutil
            shutil.rmtree(cache_dir)
            os.makedirs(cache_dir)
    
    def export_leads(self, leads):
        """Export leads to CSV"""
        try:
            df = pd.DataFrame(leads)
            csv = df.to_csv(index=False)
            
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.error(f"Export error: {e}")
    
    def run(self):
        """Main dashboard runner"""
        if not self.enabled:
            st.error("Dashboard initialization failed. Check logs.")
            return
        
        # Auto-refresh if enabled
        if CONFIG["dashboard"]["auto_refresh"]:
            st_autorefresh(interval=CONFIG["dashboard"]["refresh_interval"], limit=None)
        
        # Render sidebar and get selected page
        selected_page = self.render_sidebar()
        
        # Map selection to page rendering
        page_map = {
            "📊 Dashboard": self.render_dashboard,
            "👥 Leads": self.render_leads_page,
            "🎯 Lead Details": self.render_lead_details,
            "⚙️ Settings": self.render_settings,
            "📈 Analytics": self.render_analytics,
            "📋 Logs": self.render_logs,
            "📤 Export": self.render_export
        }
        
        # Render selected page
        if selected_page in page_map:
            page_map[selected_page]()
        else:
            self.render_dashboard()

# ============================================================================
# MAIN APPLICATION ENTRY POINT
# ============================================================================

def main():
    """Main entry point for the application"""
    
    # Check if running as dashboard
    if len(sys.argv) > 1 and sys.argv[1] == "dashboard":
        if not STREAMLIT_AVAILABLE:
            print("❌ Streamlit not available. Install with: pip install streamlit")
            sys.exit(1)
        
        dashboard = MitzMediaDashboard()
        if dashboard.enabled:
            dashboard.run()
        else:
            print("❌ Dashboard initialization failed")
            sys.exit(1)
    
    else:
        # CLI mode or scraper mode
        print("🚀 MitzMedia Lead Scraper CRM - Production System")
        print("=" * 60)
        print(f"Version: {CONFIG['machine_version']}")
        print(f"Mode: {CONFIG['operating_mode']}")
        print(f"Database: {CONFIG['crm']['database']}")
        print("=" * 60)
        
        # Check if we should start the dashboard
        if STREAMLIT_AVAILABLE:
            response = input("\n🎯 Start Dashboard? (y/n): ")
            if response.lower() == 'y':
                import subprocess
                subprocess.run([sys.executable, __file__, "dashboard"])
        else:
            print("\n⚠️  Dashboard not available. Install Streamlit to use the web interface.")
            print("   Command: pip install streamlit pandas plotly streamlit-autorefresh streamlit-option-menu")
            print("\n📖 Available commands:")
            print("   python main.py dashboard    - Start web dashboard")
            print("   python main.py scrape       - Start scraping (CLI mode)")
            print("   python main.py export       - Export leads to CSV")

if __name__ == "__main__":
    main()
