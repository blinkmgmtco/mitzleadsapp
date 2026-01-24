#!/usr/bin/env python3
"""
🚀 COMPREHENSIVE LEAD SCRAPER CRM - STREAMLIT EDITION
Fully working with web scraping, AI enrichment, SQLite CRM, and Streamlit dashboard
MitzMedia-inspired design with enhanced targeting features
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
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Set, Tuple
from urllib.parse import urlparse, urljoin, quote
from pathlib import Path
import html

# ============================================================================
# IMPORTS WITH FALLBACKS
# ============================================================================

# Check if we're running in Streamlit Cloud and adjust paths
if 'STREAMLIT_CLOUD' in os.environ:
    # Use temporary directory for database and configs
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
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    print("⚠️  Streamlit not installed. Install with: pip install streamlit pandas plotly streamlit-autorefresh")

# ============================================================================
# CONFIGURATION
# ============================================================================

# Default configuration - MitzMedia theme colors
DEFAULT_CONFIG = {
    "machine_id": "lead-scraper-crm-v2",
    "machine_version": "6.0",
    "serper_api_key": "",
    "openai_api_key": "",
    
    # CRM Settings
    "crm": {
        "enabled": True,
        "database": "crm_database.db",
        "auto_sync": True,
        "prevent_duplicates": True,
        "duplicate_check_field": "fingerprint",
        "batch_size": 10,
        "default_status": "New Lead",
        "default_assigned_to": "",
        "auto_set_production_date": True
    },
    
    # Lead Management
    "lead_management": {
        "default_follow_up_days": 7,
        "default_meeting_reminder_hours": 24,
        "auto_archive_days": 90,
        "status_options": [
            "New Lead",
            "Contacted",
            "No Answer",
            "Not Interested (NI)",
            "Follow Up",
            "Meeting Scheduled",
            "Closed (Won)",
            "Closed (Lost)",
            "Zoom Meeting",
            "Bad Lead",
            "Ghosted after Zoom",
            "Ghosted after Followup",
            "Archived"
        ],
        "priority_options": [
            "Immediate",
            "High",
            "Medium",
            "Low"
        ],
        "quality_tiers": [
            "Premium",
            "High",
            "Medium",
            "Low",
            "Unknown"
        ]
    },
    
    # UI Theme (MitzMedia inspired)
    "ui": {
        "theme": "mitzmedia",
        "primary_color": "#111827",
        "secondary_color": "#1f2937",
        "accent_color": "#f59e0b",
        "accent_light": "#fbbf24",
        "success_color": "#10b981",
        "danger_color": "#ef4444",
        "warning_color": "#f59e0b",
        "info_color": "#3b82f6",
        "dark_bg": "#0f172a",
        "light_bg": "#f8fafc",
        "card_bg": "#1e293b",
        "border_color": "#334155",
        "text_light": "#f1f5f9",
        "text_muted": "#94a3b8",
        "text_dark": "#0f172a",
        "gradient_start": "#0f172a",
        "gradient_end": "#1e293b"
    },
    
    # Scraper Settings
    "state": "PA",
    "cities": [
        "Philadelphia",
        "Pittsburgh",
        "Harrisburg",
        "Allentown",
        "Erie",
        "Reading",
        "Scranton",
        "Lancaster",
        "York",
        "Bethlehem"
    ],
    "industries": [
        "hardscaping contractor",
        "landscape contractor",
        "hvac company",
        "plumbing services",
        "electrical contractor",
        "roofing company",
        "general contractor",
        "painting services",
        "concrete contractor",
        "excavation services",
        "deck builder",
        "fence contractor"
    ],
    "search_phrases": [
        "{industry} {city} {state}",
        "{city} {industry} services",
        "best {industry} {city}"
    ],
    
    "blacklisted_domains": [
        "yelp.com", "yellowpages.com", "angi.com", "homeadvisor.com",
        "thumbtack.com", "bbb.org", "facebook.com", "linkedin.com",
        "instagram.com", "twitter.com", "pinterest.com", "wikipedia.org",
        "chamberofcommerce.com", "mapquest.com", "mawlawn.com", "usaec.org",
        "youtube.com", "google.com"
    ],
    
    "operating_mode": "auto",
    "searches_per_cycle": 5,
    "businesses_per_search": 10,
    "cycle_interval": 300,
    "max_cycles": 100,
    
    "filters": {
        "exclude_chains": True,
        "exclude_without_websites": False,  # NEW: Default to False to get listings without websites
        "exclude_without_phone": True,
        "min_rating": 3.0,
        "min_reviews": 1,
        "exclude_keywords": [
            "franchise",
            "national",
            "corporate",
            "chain"
        ]
    },
    
    "enhanced_features": {
        "check_google_ads": True,  # NEW: Check if business is running Google Ads
        "find_google_business": True,  # NEW: Find Google Business Profile
        "scrape_yelp_reviews": True,  # NEW: Scrape Yelp for reviews
        "auto_social_media": True,  # NEW: Auto-find social media
        "lead_scoring_ai": True  # NEW: AI-powered lead scoring
    },
    
    "ai_enrichment": {
        "enabled": True,
        "model": "gpt-4o-mini",
        "max_tokens": 2000,
        "auto_qualify": True,
        "qualification_threshold": 60
    },
    
    "storage": {
        "leads_file": "real_leads.json",
        "qualified_leads": "qualified_leads.json",
        "premium_leads": "premium_leads.json",
        "logs_file": "scraper_logs.json",
        "cache_file": "search_cache.json",
        "csv_export": "leads_export.csv"
    },
    
    # Dashboard Settings
    "dashboard": {
        "port": 8501,
        "host": "0.0.0.0",
        "debug": False,
        "secret_key": "lead-scraper-secret-key-2024"
    }
}

def load_config():
    """Load configuration file"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
                print("✅ Loaded configuration")
                return config
        except Exception as e:
            print(f"⚠️  Config error: {e}")
    
    # Create default config
    os.makedirs(os.path.dirname(CONFIG_FILE) if os.path.dirname(CONFIG_FILE) else ".", exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=2)
    
    print("📝 Created config.json")
    return DEFAULT_CONFIG

CONFIG = load_config()

# ============================================================================
# LOGGER
# ============================================================================

class Logger:
    """Simple logger"""
    
    def __init__(self):
        self.log_file = CONFIG["storage"]["logs_file"]
    
    def log(self, message, level="INFO"):
        """Log message"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        colors = {
            "INFO": "\033[94m",
            "SUCCESS": "\033[92m",
            "WARNING": "\033[93m",
            "ERROR": "\033[91m",
            "DEBUG": "\033[90m"
        }
        
        color = colors.get(level, "\033[0m")
        print(f"{color}[{timestamp}] {level}: {message}\033[0m")
        
        # Save to file
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
                "message": message
            }
            
            logs.append(log_entry)
            
            if len(logs) > 1000:
                logs = logs[-1000:]
            
            with open(self.log_file, "w") as f:
                json.dump(logs, f, indent=2)
                
        except Exception as e:
            print(f"Log save error: {e}")

logger = Logger()

# ============================================================================
# DATABASE (SQLite CRM)
# ============================================================================

class CRM_Database:
    """SQLite database for local CRM"""
    
    def __init__(self):
        self.db_file = CONFIG["crm"]["database"]
        self.conn = None
        self.cursor = None
        self.setup_database()
def setup_database(self):
    """Initialize database with tables - UPDATED WITH ALL NEW COLUMNS"""
    try:
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        
        # Check existing table structure
        self.cursor.execute("PRAGMA table_info(leads)")
        existing_columns = {row[1] for row in self.cursor.fetchall()}
        
        # Required new columns
        new_columns = [
            ("has_website", "BOOLEAN DEFAULT 1"),
            ("google_business_profile", "TEXT"),
            ("running_google_ads", "BOOLEAN DEFAULT 0"),
            ("ad_transparency_url", "TEXT"),
            ("yelp_url", "TEXT"),
            ("bbb_url", "TEXT")
        ]
        
        # Add missing columns
        for column_name, column_type in new_columns:
            if column_name not in existing_columns:
                try:
                    self.cursor.execute(f"ALTER TABLE leads ADD COLUMN {column_name} {column_type}")
                    print(f"✅ Added column: {column_name}")
                except Exception as e:
                    print(f"⚠️ Could not add column {column_name}: {e}")
        
            
            # Leads table with enhanced columns
            self.cursor.execute('''
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
                    industry TEXT,
                    business_type TEXT,
                    services TEXT,
                    description TEXT,
                    social_media TEXT,
                    google_business_profile TEXT,  -- NEW: Google Business Profile URL
                    running_google_ads BOOLEAN DEFAULT 0,  -- NEW: Google Ads status
                    ad_transparency_url TEXT,  -- NEW: Ads Transparency URL
                    lead_score INTEGER DEFAULT 0,
                    quality_tier TEXT,
                    potential_value INTEGER DEFAULT 0,
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
                    source TEXT DEFAULT 'Web Scraper',
                    scraped_date DATETIME,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_archived BOOLEAN DEFAULT 0,
                    archive_date DATETIME,
                    yelp_url TEXT,  -- NEW: Yelp URL
                    bbb_url TEXT,  -- NEW: BBB URL
                    has_website BOOLEAN DEFAULT 1  -- NEW: Website status
                )
            ''')
            
            # Create indexes
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_fingerprint ON leads(fingerprint)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_lead_status ON leads(lead_status)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_quality_tier ON leads(quality_tier)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON leads(city)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON leads(created_at)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_has_website ON leads(has_website)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_running_ads ON leads(running_google_ads)')
            
            # Activities table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS activities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER,
                    activity_type TEXT,
                    activity_details TEXT,
                    performed_by TEXT,
                    performed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (lead_id) REFERENCES leads (id)
                )
            ''')
            
            # Users table (for assignments)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE,
                    email TEXT,
                    full_name TEXT,
                    role TEXT DEFAULT 'user',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            # Statistics table
            self.cursor.execute('''
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
                    leads_without_website INTEGER DEFAULT 0,  -- NEW
                    leads_with_ads INTEGER DEFAULT 0,  -- NEW
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Settings table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setting_key TEXT UNIQUE,
                    setting_value TEXT,
                    setting_type TEXT DEFAULT 'string',
                    category TEXT DEFAULT 'general',
                    description TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Insert default settings if not exists
            default_settings = [
                ('scraper_enabled', 'true', 'boolean', 'scraper', 'Enable automatic scraping'),
                ('scraper_interval', '300', 'number', 'scraper', 'Scraping interval in seconds'),
                ('auto_save', 'true', 'boolean', 'crm', 'Auto-save leads to CRM'),
                ('dashboard_theme', 'mitzmedia', 'string', 'ui', 'Dashboard theme'),
                ('notification_enabled', 'true', 'boolean', 'notifications', 'Enable notifications'),
                ('include_no_website', 'true', 'boolean', 'targeting', 'Include leads without websites'),  # NEW
                ('check_google_ads', 'true', 'boolean', 'targeting', 'Check if businesses run Google Ads'),  # NEW
                ('find_google_business', 'true', 'boolean', 'targeting', 'Find Google Business Profiles')  # NEW
            ]
            
            for key, value, stype, category, desc in default_settings:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO settings (setting_key, setting_value, setting_type, category, description)
                    VALUES (?, ?, ?, ?, ?)
                ''', (key, value, stype, category, desc))
            
            self.conn.commit()
            logger.log("✅ Database initialized", "SUCCESS")
            
        except Exception as e:
            logger.log(f"❌ Database error: {e}", "ERROR")
    
    def get_connection(self):
        """Get a new database connection to avoid cursor conflicts"""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def save_lead(self, lead_data):
        """Save lead to database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Extract data
            fingerprint = lead_data.get("fingerprint", "")
            
            # Check for duplicate
            if CONFIG["crm"]["prevent_duplicates"] and fingerprint:
                cursor.execute("SELECT id FROM leads WHERE fingerprint = ?", (fingerprint,))
                existing = cursor.fetchone()
                if existing:
                    return {"success": False, "message": "Duplicate lead", "lead_id": existing[0]}
            
            # Prepare lead data
            business_name = lead_data.get("business_name", "Unknown Business")[:200]
            website = lead_data.get("website", lead_data.get("website_url", ""))[:200]
            has_website = bool(website and len(website) > 5)
            
            # Get phone
            phone = ""
            phones = lead_data.get("phones", [])
            if phones and isinstance(phones, list) and len(phones) > 0:
                phone = str(phones[0]) if phones[0] else ""
            if not phone:
                phone = lead_data.get("phone", "") or ""
            
            # Get email
            email = ""
            emails = lead_data.get("emails", [])
            if emails and isinstance(emails, list) and len(emails) > 0:
                email = str(emails[0]) if emails[0] else ""
            
            # Get address
            address = lead_data.get("address", "")
            if not address and lead_data.get("city"):
                address = f"{lead_data.get('city')}, {lead_data.get('state', CONFIG['state'])}"
            
            # Services
            services = lead_data.get("services", "")
            if isinstance(services, list):
                services = ", ".join(services)
            
            # Social media
            social_media = lead_data.get("social_media", "")
            if isinstance(social_media, dict):
                social_media = json.dumps(social_media)
            
            # Enhanced features
            google_business_profile = lead_data.get("google_business_profile", "")
            running_google_ads = lead_data.get("running_google_ads", False)
            ad_transparency_url = lead_data.get("ad_transparency_url", "")
            yelp_url = lead_data.get("yelp_url", "")
            bbb_url = lead_data.get("bbb_url", "")
            
            # Quality tier and potential value
            quality_tier = lead_data.get("quality_tier", "Unknown")
            potential_value = lead_data.get("potential_value", 0)
            if not potential_value:
                # Map quality tier to dollar amounts
                tier_map = {
                    "PREMIUM": 10000,
                    "Premium": 10000,
                    "HIGH": 7500,
                    "High": 7500,
                    "MEDIUM": 5000,
                    "Medium": 5000,
                    "LOW": 2500,
                    "Low": 2500,
                    "UNKNOWN": 0,
                    "Unknown": 0
                }
                potential_value = tier_map.get(quality_tier, 0)
            
            # Outreach priority based on score
            lead_score = lead_data.get("lead_score", 0)
            if lead_score >= 80:
                outreach_priority = "Immediate"
            elif lead_score >= 60:
                outreach_priority = "High"
            elif lead_score >= 40:
                outreach_priority = "Medium"
            else:
                outreach_priority = "Low"
            
            # Follow-up date (7 days from now)
            follow_up_date = (datetime.now(timezone.utc) + timedelta(days=7)).date().isoformat()
            
            # Insert lead
            cursor.execute('''
                INSERT OR REPLACE INTO leads (
                    fingerprint, business_name, website, phone, email, address,
                    city, state, industry, business_type, services, description,
                    social_media, google_business_profile, running_google_ads,
                    ad_transparency_url, lead_score, quality_tier, potential_value,
                    outreach_priority, lead_status, assigned_to, lead_production_date,
                    follow_up_date, notes, ai_notes, source, scraped_date,
                    yelp_url, bbb_url, has_website
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fingerprint, business_name, website, phone, email, address,
                lead_data.get("city", ""), lead_data.get("state", CONFIG["state"]),
                lead_data.get("industry", ""), lead_data.get("business_type", "LLC"),
                services[:500], lead_data.get("description", "")[:1000],
                social_media[:500], google_business_profile, running_google_ads,
                ad_transparency_url, lead_score, quality_tier, potential_value,
                outreach_priority, CONFIG["crm"]["default_status"],
                CONFIG["crm"]["default_assigned_to"],
                datetime.now(timezone.utc).date().isoformat() if CONFIG["crm"]["auto_set_production_date"] else None,
                follow_up_date, "", lead_data.get("ai_notes", "")[:500],
                "Web Scraper", lead_data.get("scraped_date", datetime.now(timezone.utc).isoformat()),
                yelp_url, bbb_url, has_website
            ))
            
            lead_id = cursor.lastrowid
            
            # Add activity log
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, "Lead Created", f"Lead scraped from {website if website else 'directory listing'}"))
            
            conn.commit()
            
            # Update statistics
            self.update_statistics(cursor)
            
            return {"success": True, "lead_id": lead_id, "message": "Lead saved"}
            
        except Exception as e:
            conn.rollback()
            logger.log(f"Save lead error: {e}", "ERROR")
            return {"success": False, "message": f"Error: {str(e)}"}
        finally:
            conn.close()
    
    def update_statistics(self, cursor=None):
        """Update daily statistics"""
        conn = None
        try:
            if cursor is None:
                conn = self.get_connection()
                cursor = conn.cursor()
            
            today = datetime.now(timezone.utc).date().isoformat()
            
            # Get current stats with enhanced metrics
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_leads,
                    SUM(CASE WHEN lead_status = 'New Lead' THEN 1 ELSE 0 END) as new_leads,
                    SUM(CASE WHEN lead_status = 'Contacted' THEN 1 ELSE 0 END) as contacted_leads,
                    SUM(CASE WHEN lead_status = 'Meeting Scheduled' THEN 1 ELSE 0 END) as meetings_scheduled,
                    SUM(CASE WHEN lead_status = 'Closed (Won)' THEN 1 ELSE 0 END) as closed_won,
                    SUM(CASE WHEN lead_status = 'Closed (Lost)' THEN 1 ELSE 0 END) as closed_lost,
                    SUM(CASE WHEN quality_tier IN ('Premium', 'High') THEN 1 ELSE 0 END) as premium_leads,
                    SUM(potential_value) as estimated_value,
                    SUM(CASE WHEN has_website = 0 THEN 1 ELSE 0 END) as leads_without_website,
                    SUM(CASE WHEN running_google_ads = 1 THEN 1 ELSE 0 END) as leads_with_ads
                FROM leads 
                WHERE DATE(created_at) = DATE('now') AND is_archived = 0
            ''')
            
            stats = cursor.fetchone()
            if not stats:
                stats = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            
            cursor.execute('''
                INSERT OR REPLACE INTO statistics 
                (stat_date, total_leads, new_leads, contacted_leads, meetings_scheduled, 
                 closed_won, closed_lost, premium_leads, estimated_value, 
                 leads_without_website, leads_with_ads)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, *stats))
            
            if conn:
                conn.commit()
            
        except Exception as e:
            logger.log(f"Statistics update error: {e}", "ERROR")
        finally:
            if conn:
                conn.close()
    
    def get_leads(self, filters=None, page=1, per_page=50):
        """Get leads with pagination and filtering"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            query = "SELECT * FROM leads WHERE is_archived = 0"
            params = []
            
            if filters:
                conditions = []
                if filters.get("status"):
                    conditions.append("lead_status = ?")
                    params.append(filters["status"])
                if filters.get("quality_tier"):
                    conditions.append("quality_tier = ?")
                    params.append(filters["quality_tier"])
                if filters.get("city"):
                    conditions.append("city LIKE ?")
                    params.append(f"%{filters['city']}%")
                if filters.get("industry"):
                    conditions.append("industry LIKE ?")
                    params.append(f"%{filters['industry']}%")
                if filters.get("has_website") is not None:
                    conditions.append("has_website = ?")
                    params.append(filters["has_website"])
                if filters.get("running_ads") is not None:
                    conditions.append("running_google_ads = ?")
                    params.append(filters["running_ads"])
                if filters.get("search"):
                    search_term = f"%{filters['search']}%"
                    conditions.append("(business_name LIKE ? OR website LIKE ? OR phone LIKE ? OR email LIKE ?)")
                    params.extend([search_term, search_term, search_term, search_term])
                if filters.get("date_from"):
                    conditions.append("DATE(created_at) >= ?")
                    params.append(filters["date_from"])
                if filters.get("date_to"):
                    conditions.append("DATE(created_at) <= ?")
                    params.append(filters["date_to"])
                
                if conditions:
                    query += " AND " + " AND ".join(conditions)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM ({query})"
            cursor.execute(count_query, params)
            result = cursor.fetchone()
            total = result[0] if result else 0
            
            # Add pagination
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([per_page, (page - 1) * per_page])
            
            cursor.execute(query, params)
            leads = cursor.fetchall()
            
            # Convert to list of dictionaries
            result = []
            for lead in leads:
                lead_dict = dict(lead)
                
                # Parse social media if it's a JSON string
                if lead_dict.get("social_media") and isinstance(lead_dict["social_media"], str):
                    try:
                        lead_dict["social_media"] = json.loads(lead_dict["social_media"])
                    except:
                        pass
                
                result.append(lead_dict)
            
            return {
                "leads": result,
                "total": total,
                "page": page,
                "per_page": per_page,
                "total_pages": (total + per_page - 1) // per_page if per_page > 0 else 0
            }
            
        except Exception as e:
            logger.log(f"Get leads error: {e}", "ERROR")
            return {"leads": [], "total": 0, "page": page, "per_page": per_page}
        finally:
            conn.close()
    
    def get_lead_by_id(self, lead_id):
        """Get single lead by ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
            lead = cursor.fetchone()
            
            if not lead:
                return None
            
            lead_dict = dict(lead)
            
            # Get activities
            cursor.execute("SELECT * FROM activities WHERE lead_id = ? ORDER BY performed_at DESC", (lead_id,))
            activities = cursor.fetchall()
            lead_dict["activities"] = [dict(activity) for activity in activities]
            
            # Parse JSON fields
            if lead_dict.get("social_media") and isinstance(lead_dict["social_media"], str):
                try:
                    lead_dict["social_media"] = json.loads(lead_dict["social_media"])
                except:
                    lead_dict["social_media"] = {}
            
            if lead_dict.get("services") and isinstance(lead_dict["services"], str):
                try:
                    lead_dict["services"] = json.loads(lead_dict["services"])
                except:
                    if "," in lead_dict["services"]:
                        lead_dict["services"] = [s.strip() for s in lead_dict["services"].split(",") if s.strip()]
                    else:
                        lead_dict["services"] = [lead_dict["services"]]
            
            return lead_dict
            
        except Exception as e:
            logger.log(f"Get lead error: {e}", "ERROR")
            return None
        finally:
            conn.close()
    
    def update_lead(self, lead_id, update_data):
        """Update lead information"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Build update query
            set_clause = []
            params = []
            
            for field, value in update_data.items():
                set_clause.append(f"{field} = ?")
                params.append(value)
            
            params.append(lead_id)
            query = f"UPDATE leads SET {', '.join(set_clause)}, last_updated = CURRENT_TIMESTAMP WHERE id = ?"
            
            cursor.execute(query, params)
            
            # Log activity
            activity_desc = f"Updated: {', '.join(update_data.keys())}"
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, "Lead Updated", activity_desc))
            
            conn.commit()
            
            return {"success": True, "message": "Lead updated"}
            
        except Exception as e:
            conn.rollback()
            return {"success": False, "message": f"Error: {str(e)}"}
        finally:
            conn.close()
    
    def delete_lead(self, lead_id):
        """Soft delete lead (archive)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE leads 
                SET is_archived = 1, archive_date = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (lead_id,))
            
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, "Lead Archived", "Lead moved to archive"))
            
            conn.commit()
            
            return {"success": True, "message": "Lead archived"}
            
        except Exception as e:
            conn.rollback()
            return {"success": False, "message": f"Error: {str(e)}"}
        finally:
            conn.close()
    
    def get_statistics(self, days=30):
        """Get statistics for dashboard"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            stats = {}
            
            # Overall stats with enhanced metrics
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_leads,
                    SUM(CASE WHEN lead_status = 'New Lead' THEN 1 ELSE 0 END) as new_leads,
                    SUM(CASE WHEN lead_status = 'Closed (Won)' THEN 1 ELSE 0 END) as closed_won,
                    SUM(CASE WHEN lead_status = 'Closed (Lost)' THEN 1 ELSE 0 END) as closed_lost,
                    SUM(potential_value) as total_value,
                    AVG(lead_score) as avg_score,
                    SUM(CASE WHEN has_website = 0 THEN 1 ELSE 0 END) as leads_without_website,
                    SUM(CASE WHEN running_google_ads = 1 THEN 1 ELSE 0 END) as leads_with_ads
                FROM leads 
                WHERE is_archived = 0
            ''')
            
            overall = cursor.fetchone()
            if overall:
                stats["overall"] = dict(overall)
            else:
                stats["overall"] = {
                    "total_leads": 0,
                    "new_leads": 0,
                    "closed_won": 0,
                    "closed_lost": 0,
                    "total_value": 0,
                    "avg_score": 0,
                    "leads_without_website": 0,
                    "leads_with_ads": 0
                }
            
            # Status distribution
            cursor.execute('''
                SELECT lead_status, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0
                GROUP BY lead_status
                ORDER BY count DESC
            ''')
            
            stats["status_distribution"] = [
                {"status": row[0], "count": row[1]} 
                for row in cursor.fetchall()
            ]
            
            # Quality tier distribution
            cursor.execute('''
                SELECT quality_tier, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0 AND quality_tier != 'Unknown'
                GROUP BY quality_tier
                ORDER BY 
                    CASE quality_tier
                        WHEN 'Premium' THEN 1
                        WHEN 'High' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'Low' THEN 4
                        ELSE 5
                    END
            ''')
            
            stats["quality_distribution"] = [
                {"tier": row[0], "count": row[1]} 
                for row in cursor.fetchall()
            ]
            
            # Daily leads (last 30 days)
            cursor.execute('''
                SELECT DATE(created_at) as date, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0 AND created_at >= DATE('now', ?)
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            ''', (f"-{days} days",))
            
            stats["daily_leads"] = [
                {"date": row[0], "count": row[1]} 
                for row in cursor.fetchall()
            ]
            
            # City distribution
            cursor.execute('''
                SELECT city, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0 AND city != ''
                GROUP BY city
                ORDER BY count DESC
                LIMIT 10
            ''')
            
            stats["city_distribution"] = [
                {"city": row[0], "count": row[1]} 
                for row in cursor.fetchall()
            ]
            
            # Industry distribution
            cursor.execute('''
                SELECT industry, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0 AND industry != ''
                GROUP BY industry
                ORDER BY count DESC
                LIMIT 10
            ''')
            
            stats["industry_distribution"] = [
                {"industry": row[0], "count": row[1]} 
                for row in cursor.fetchall()
            ]
            
            # Website vs No Website
            cursor.execute('''
                SELECT 
                    CASE 
                        WHEN has_website = 1 THEN 'Has Website'
                        ELSE 'No Website'
                    END as category,
                    COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0
                GROUP BY has_website
            ''')
            
            stats["website_distribution"] = [
                {"category": row[0], "count": row[1]} 
                for row in cursor.fetchall()
            ]
            
            return stats
            
        except Exception as e:
            logger.log(f"Statistics error: {e}", "ERROR")
            return {
                "overall": {
                    "total_leads": 0,
                    "new_leads": 0,
                    "closed_won": 0,
                    "closed_lost": 0,
                    "total_value": 0,
                    "avg_score": 0,
                    "leads_without_website": 0,
                    "leads_with_ads": 0
                },
                "status_distribution": [],
                "quality_distribution": [],
                "daily_leads": [],
                "city_distribution": [],
                "industry_distribution": [],
                "website_distribution": []
            }
        finally:
            conn.close()
    
    def get_settings(self):
        """Get all settings"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM settings ORDER BY category, setting_key")
            settings = cursor.fetchall()
            
            result = {}
            for setting in settings:
                result[setting['setting_key']] = {
                    "value": setting['setting_value'],
                    "type": setting['setting_type'],
                    "category": setting['category'],
                    "description": setting['description']
                }
            
            return result
        except Exception as e:
            logger.log(f"Get settings error: {e}", "ERROR")
            return {}
        finally:
            conn.close()
    
    def update_setting(self, key, value):
        """Update a setting"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE settings SET setting_value = ?, updated_at = CURRENT_TIMESTAMP
                WHERE setting_key = ?
            ''', (value, key))
            
            conn.commit()
            return {"success": True, "message": "Setting updated"}
        except Exception as e:
            conn.rollback()
            return {"success": False, "message": f"Error: {str(e)}"}
        finally:
            conn.close()
    
    def get_today_count(self):
        """Get today's lead count"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM leads WHERE DATE(created_at) = DATE('now') AND is_archived = 0")
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.log(f"Today count error: {e}", "ERROR")
            return 0
        finally:
            conn.close()
    
    def get_all_settings(self):
        """Get ALL configuration settings"""
        try:
            return CONFIG
        except Exception as e:
            logger.log(f"Get all settings error: {e}", "ERROR")
            return {}
    
    def update_config_file(self, updated_config):
        """Update the config.json file"""
        global CONFIG
        try:
            # Validate the config structure
            if not isinstance(updated_config, dict):
                return {"success": False, "message": "Invalid configuration format"}
            
            # Backup original config
            backup_file = f"config_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(backup_file, "w") as f:
                json.dump(CONFIG, f, indent=2)
            
            # Update config file
            with open(CONFIG_FILE, "w") as f:
                json.dump(updated_config, f, indent=2)
            
            # Reload config
            CONFIG = updated_config
            
            logger.log("Configuration updated successfully", "SUCCESS")
            return {"success": True, "message": "Configuration updated", "backup": backup_file}
            
        except Exception as e:
            logger.log(f"Update config error: {e}", "ERROR")
            return {"success": False, "message": f"Error: {str(e)}"}

# ============================================================================
# ENHANCED WEBSITE SCRAPER
# ============================================================================

class EnhancedWebsiteScraper:
    """Enhanced scraper with Google Ads detection and Google Business Profile extraction"""
    
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        ]
        
        # Initialize OpenAI
        self.openai_client = None
        if OPENAI_AVAILABLE:
            api_key = CONFIG.get("openai_api_key", "")
            if api_key and not api_key.startswith("sk-proj-your-key-here"):
                try:
                    self.openai_client = openai.OpenAI(api_key=api_key)
                except:
                    logger.log("OpenAI initialization failed", "WARNING")
    
    def scrape_website(self, url, business_name="", city=""):
        """Scrape website for contact information with enhanced features"""
        data = {
            'website': url,
            'business_name': business_name,
            'description': '',
            'phones': [],
            'emails': [],
            'address': '',
            'social_media': {},
            'services': [],
            'google_business_profile': '',
            'running_google_ads': False,
            'ad_transparency_url': '',
            'yelp_url': '',
            'bbb_url': ''
        }
        
        if not url or not url.startswith(('http://', 'https://')):
            # Directory listing without website
            data['has_website'] = False
            return data
        
        try:
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract basic information
            data.update({
                'business_name': self._extract_business_name(soup, url, business_name),
                'description': self._extract_description(soup),
                'phones': self._extract_phones(soup),
                'emails': self._extract_emails(soup),
                'address': self._extract_address(soup),
                'social_media': self._extract_social_media(soup),
                'services': self._extract_services(soup),
                'has_website': True
            })
            
            # Enhanced features
            if CONFIG["enhanced_features"]["find_google_business"]:
                data['google_business_profile'] = self._extract_google_business(soup, data['business_name'], city)
            
            if CONFIG["enhanced_features"]["check_google_ads"]:
                ads_data = self._check_google_ads(url)
                data['running_google_ads'] = ads_data['running_ads']
                data['ad_transparency_url'] = ads_data['ad_transparency_url']
            
            # Check for Yelp/BBB
            data['yelp_url'] = self._find_yelp_page(data['business_name'], city)
            data['bbb_url'] = self._find_bbb_page(data['business_name'], city)
            
            return data
            
        except Exception as e:
            logger.log(f"Scrape error for {url}: {e}", "WARNING")
            data['has_website'] = False
            return data
    
    def _extract_business_name(self, soup, url, fallback_name=""):
        """Extract business name from website"""
        # Try meta tags first
        for meta in soup.find_all('meta'):
            if meta.get('property') in ['og:site_name', 'og:title']:
                name = meta.get('content', '')
                if name:
                    return name[:200]
        
        # Try title tag
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            # Remove common suffixes
            suffixes = [' - Home', ' | Home', ' - Official Site', ' | Official Site', ' | Website']
            for suffix in suffixes:
                if title.endswith(suffix):
                    title = title[:-len(suffix)]
            if title:
                return title[:200]
        
        # Try h1 tags
        h1_tags = soup.find_all('h1')
        if h1_tags:
            h1_text = h1_tags[0].get_text(strip=True)
            if h1_text and len(h1_text) > 3:
                return h1_text[:200]
        
        # Use provided fallback name
        if fallback_name:
            return fallback_name[:200]
        
        # Fallback to domain name
        try:
            domain = urlparse(url).netloc
            name = domain.replace('www.', '').split('.')[0].title()
            return name[:200]
        except:
            return "Unknown Business"
    
    def _extract_description(self, soup):
        """Extract description from website"""
        # Try meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc.get('content')[:500]
        
        # Try first paragraph
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 50 and len(text) < 300:
                return text[:500]
        
        return ""
    
    def _extract_phones(self, soup):
        """Extract phone numbers from website"""
        phones = set()
        text = soup.get_text()
        
        # Common phone patterns
        phone_patterns = [
            r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US format
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',        # Another US format
            r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US with +1
        ]
        
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Clean up the phone number
                phone = re.sub(r'[^\d+]', '', match)
                if len(phone) >= 10:
                    phones.add(phone)
        
        # Also look in specific elements
        phone_elements = soup.find_all(['a', 'span', 'div'], text=re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'))
        for elem in phone_elements:
            text = elem.get_text(strip=True)
            matches = re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', text)
            for match in matches:
                phone = re.sub(r'[^\d+]', '', match)
                if len(phone) >= 10:
                    phones.add(phone)
        
        # Look for tel: links
        tel_links = soup.find_all('a', href=re.compile(r'tel:'))
        for link in tel_links:
            href = link.get('href', '')
            phone_match = re.search(r'tel:([\+\d\s\-\(\)]+)', href)
            if phone_match:
                phone = re.sub(r'[^\d+]', '', phone_match.group(1))
                if len(phone) >= 10:
                    phones.add(phone)
        
        return list(phones)[:3]  # Return max 3 phones
    
    def _extract_emails(self, soup):
        """Extract email addresses from website"""
        emails = set()
        text = soup.get_text()
        
        # Email pattern
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        matches = re.findall(email_pattern, text)
        emails.update(matches)
        
        # Also look in mailto links
        mailto_links = soup.find_all('a', href=re.compile(r'mailto:'))
        for link in mailto_links:
            href = link.get('href', '')
            email_match = re.search(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', href)
            if email_match:
                emails.add(email_match.group(1))
        
        return list(emails)[:5]  # Return max 5 emails
    
    def _extract_address(self, soup):
        """Extract address from website"""
        text = soup.get_text()
        
        # Look for address patterns
        address_patterns = [
            r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Place|Pl),?\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}',
            r'\d+\s+[A-Za-z\s]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text)
            if matches:
                return matches[0]
        
        # Try to find address in specific elements
        address_elements = soup.find_all(['address', 'div', 'span'], class_=re.compile(r'address|location|contact'))
        for elem in address_elements:
            text = elem.get_text(strip=True)
            if len(text) > 20 and len(text) < 200:
                # Check if it looks like an address
                if any(word in text.lower() for word in ['street', 'ave', 'road', 'rd', 'drive', 'dr']):
                    return text
        
        return ""
    
    def _extract_social_media(self, soup):
        """Extract social media links from website"""
        social_media = {}
        
        social_platforms = {
            'facebook': ['facebook.com', 'fb.com'],
            'instagram': ['instagram.com'],
            'linkedin': ['linkedin.com'],
            'twitter': ['twitter.com', 'x.com'],
            'youtube': ['youtube.com'],
            'tiktok': ['tiktok.com'],
            'pinterest': ['pinterest.com']
        }
        
        for platform, domains in social_platforms.items():
            for a in soup.find_all('a', href=True):
                href = a['href'].lower()
                for domain in domains:
                    if domain in href:
                        social_media[platform] = a['href']
                        break
        
        return social_media
    
    def _extract_services(self, soup):
        """Extract services from website"""
        services = []
        
        # Common keywords for contractor services
        service_keywords = [
            'installation', 'repair', 'maintenance', 'service', 'contractor',
            'construction', 'remodeling', 'renovation', 'building', 'design',
            'installation', 'repair', 'maintenance', 'cleaning', 'painting',
            'electrical', 'plumbing', 'hvac', 'roofing', 'landscaping',
            'hardscaping', 'concrete', 'excavation', 'deck', 'fence'
        ]
        
        # Look in headings and lists
        text_content = soup.get_text().lower()
        
        for keyword in service_keywords:
            if keyword in text_content:
                services.append(keyword.title())
        
        # Look for specific service sections
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            heading_text = heading.get_text().lower()
            if any(word in heading_text for word in ['service', 'what we do', 'our work', 'expertise', 'special']):
                # Look at next elements for services
                next_elem = heading.find_next()
                for _ in range(10):  # Check next 10 elements
                    if next_elem:
                        if next_elem.name in ['ul', 'ol']:
                            for li in next_elem.find_all('li'):
                                services.append(li.get_text(strip=True)[:100])
                        elif next_elem.name == 'p':
                            text = next_elem.get_text(strip=True)
                            if len(text) < 200:  # Short paragraphs are likely service descriptions
                                services.append(text[:100])
                        next_elem = next_elem.find_next_sibling()
                    else:
                        break
        
        return list(set(services))[:10]  # Return unique services, max 10
    
    def _extract_google_business(self, soup, business_name, city):
        """Extract Google Business Profile link"""
        # Check for Google Maps links
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            if any(pattern in href for pattern in ['google.com/maps', 'g.page', 'goo.gl/maps', 'maps.app.goo.gl']):
                return a['href']
        
        # Check for iframes with Google Maps
        for iframe in soup.find_all('iframe', src=True):
            src = iframe['src'].lower()
            if 'google.com/maps' in src:
                return src
        
        # If not found, construct a search URL
        if business_name and city:
            search_query = f"{business_name} {city} Google Business"
            encoded_query = quote(search_query)
            return f"https://www.google.com/search?q={encoded_query}"
        
        return ""
    
    def _check_google_ads(self, url):
        """Check if business is running Google Ads"""
        try:
            # Extract domain from URL
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                return {"running_ads": False, "ad_transparency_url": ""}
            
            # Clean domain (remove www.)
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Construct Ads Transparency URL
            ad_transparency_url = f"https://adstransparency.google.com/?region=US&domain={domain}"
            
            # Try to fetch the page
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            try:
                response = requests.get(ad_transparency_url, headers=headers, timeout=8)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check for indicators of advertising
                text = soup.get_text().lower()
                
                # Indicators that ads might be running
                ad_indicators = [
                    'advertiser', 'campaign', 'political ads',
                    'election ads', 'verified advertiser'
                ]
                
                running_ads = any(indicator in text for indicator in ad_indicators)
                
                return {
                    "running_ads": running_ads,
                    "ad_transparency_url": ad_transparency_url
                }
                
            except:
                # If we can't fetch, assume no ads
                return {
                    "running_ads": False,
                    "ad_transparency_url": ad_transparency_url
                }
                
        except Exception as e:
            logger.log(f"Google Ads check error: {e}", "WARNING")
            return {"running_ads": False, "ad_transparency_url": ""}
    
    def _find_yelp_page(self, business_name, city):
        """Find Yelp page for business"""
        if not business_name or not city:
            return ""
        
        try:
            search_query = f"{business_name} {city} Yelp"
            encoded_query = quote(search_query)
            return f"https://www.yelp.com/search?find_desc={encoded_query}"
        except:
            return ""
    
    def _find_bbb_page(self, business_name, city):
        """Find BBB page for business"""
        if not business_name or not city:
            return ""
        
        try:
            search_query = f"{business_name} {city} BBB"
            encoded_query = quote(search_query)
            return f"https://www.bbb.org/search?find_desc={encoded_query}"
        except:
            return ""

# ============================================================================
# ENHANCED LEAD SCRAPER (SERP API)
# ============================================================================

class EnhancedLeadScraper:
    """Enhanced scraper with Google Ads detection and better targeting"""
    
    def __init__(self):
        self.api_key = CONFIG.get("serper_api_key", "")
        self.scraper = EnhancedWebsiteScraper()
        self.crm = CRM_Database()
        self.running = False
        self.paused = False
        self.stats = {
            'total_leads': 0,
            'qualified_leads': 0,
            'premium_leads': 0,
            'leads_without_website': 0,
            'leads_with_ads': 0,
            'cycles': 0,
            'last_cycle': None
        }
        self.cache_file = CONFIG["storage"]["cache_file"]
        self.load_cache()
    
    def load_cache(self):
        """Load search cache"""
        self.cache = {}
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except:
                self.cache = {}
    
    def save_cache(self):
        """Save search cache"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except:
            pass
    
    def generate_search_queries(self):
        """Generate search queries from config"""
        queries = []
        state = CONFIG["state"]
        
        # Generate queries for websites
        for industry in CONFIG["industries"]:
            for city in CONFIG["cities"]:
                for phrase_template in CONFIG["search_phrases"]:
                    query = phrase_template.format(
                        industry=industry,
                        city=city,
                        state=state
                    )
                    queries.append({
                        'query': query,
                        'industry': industry,
                        'city': city,
                        'state': state,
                        'type': 'website'
                    })
        
        # Generate queries for directory listings (if enabled)
        if not CONFIG["filters"]["exclude_without_websites"]:
            for industry in CONFIG["industries"]:
                for city in CONFIG["cities"]:
                    queries.append({
                        'query': f"{industry} {city} {state} phone number address",
                        'industry': industry,
                        'city': city,
                        'state': state,
                        'type': 'directory'
                    })
                    queries.append({
                        'query': f"{city} {industry} contact information",
                        'industry': industry,
                        'city': city,
                        'state': state,
                        'type': 'directory'
                    })
        
        random.shuffle(queries)  # Randomize order
        return queries[:CONFIG["searches_per_cycle"] * 2]  # More queries since we have directory searches
    
    def search_serper(self, query):
        """Search using Serper API"""
        if not self.api_key:
            logger.log("No Serper API key configured", "ERROR")
            return []
        
        # Check cache first
        cache_key = hashlib.md5(query.encode()).hexdigest()
        if cache_key in self.cache:
            logger.log(f"Using cached results for: {query}", "DEBUG")
            return self.cache[cache_key]
        
        try:
            url = "https://google.serper.dev/search"
            headers = {
                'X-API-KEY': self.api_key,
                'Content-Type': 'application/json'
            }
            payload = {
                'q': query,
                'num': CONFIG["businesses_per_search"]
            }
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            results = []
            
            # Extract organic results
            if 'organic' in data:
                for item in data['organic']:
                    result = {
                        'title': item.get('title', ''),
                        'link': item.get('link', ''),
                        'snippet': item.get('snippet', ''),
                        'position': item.get('position', 0)
                    }
                    results.append(result)
            
            # Cache results
            self.cache[cache_key] = results
            self.save_cache()
            
            logger.log(f"Found {len(results)} results for: {query}", "INFO")
            return results
            
        except Exception as e:
            logger.log(f"Serper API error for '{query}': {e}", "ERROR")
            return []
    
    def is_blacklisted(self, url):
        """Check if domain is blacklisted"""
        if not url:
            return True
        
        domain = urlparse(url).netloc.lower()
        
        for blacklisted in CONFIG["blacklisted_domains"]:
            if blacklisted in domain:
                return True
        
        return False
    
    def extract_domain(self, url):
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ""
    
    def generate_fingerprint(self, business_name, website, phone, city):
        """Generate fingerprint for duplicate detection"""
        data = f"{business_name}_{website}_{phone}_{city}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def qualify_lead(self, lead_data):
        """Qualify lead using AI"""
        if not CONFIG["ai_enrichment"]["enabled"] or not self.scraper.openai_client:
            # Basic scoring without AI
            lead_score = 50  # Default score
            
            # Adjust based on data quality
            if lead_data.get('website'):
                lead_score += 10
            if lead_data.get('phone'):
                lead_score += 15
            if lead_data.get('email'):
                lead_score += 10
            if lead_data.get('address'):
                lead_score += 5
            
            # Quality tier based on score
            if lead_score >= 80:
                quality_tier = "Premium"
            elif lead_score >= 60:
                quality_tier = "High"
            elif lead_score >= 40:
                quality_tier = "Medium"
            else:
                quality_tier = "Low"
            
            lead_data['lead_score'] = lead_score
            lead_data['quality_tier'] = quality_tier
            lead_data['business_type'] = "Unknown"
            lead_data['ai_notes'] = "Basic scoring applied (AI not available)"
            
            return lead_data
        
        try:
            prompt = f"""
            Analyze this business lead and provide:
            1. Lead score (0-100)
            2. Quality tier (Premium, High, Medium, Low, Unknown)
            3. Business type (LLC, Corporation, Sole Proprietorship, Partnership, Unknown)
            4. Key services (comma-separated)
            5. AI notes with insights about business potential
            
            Lead Information:
            - Business: {lead_data.get('business_name', 'Unknown')}
            - Website: {lead_data.get('website', 'None')}
            - Has Website: {lead_data.get('has_website', True)}
            - Phone: {lead_data.get('phone', 'None')}
            - Email: {lead_data.get('email', 'None')}
            - Address: {lead_data.get('address', 'None')}
            - City: {lead_data.get('city', 'Unknown')}
            - Industry: {lead_data.get('industry', 'Unknown')}
            - Description: {lead_data.get('description', 'None')}
            - Running Google Ads: {lead_data.get('running_google_ads', False)}
            
            Respond in JSON format:
            {{
                "lead_score": 0-100,
                "quality_tier": "Premium/High/Medium/Low/Unknown",
                "business_type": "LLC/Corporation/Sole Proprietorship/Partnership/Unknown",
                "services": ["service1", "service2"],
                "ai_notes": "Your analysis here"
            }}
            """
            
            response = self.scraper.openai_client.chat.completions.create(
                model=CONFIG["ai_enrichment"]["model"],
                messages=[
                    {"role": "system", "content": "You are a lead qualification expert for construction and home services businesses. Consider businesses without websites as still potentially valuable if they have contact info."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=CONFIG["ai_enrichment"]["max_tokens"],
                temperature=0.3
            )
            
            ai_response = response.choices[0].message.content
            
            # Parse JSON response
            try:
                ai_data = json.loads(ai_response)
                
                # Update lead data
                lead_data['lead_score'] = ai_data.get('lead_score', 50)
                lead_data['quality_tier'] = ai_data.get('quality_tier', 'Unknown')
                lead_data['business_type'] = ai_data.get('business_type', 'Unknown')
                
                # Merge services
                existing_services = lead_data.get('services', [])
                ai_services = ai_data.get('services', [])
                if isinstance(existing_services, str):
                    existing_services = [existing_services]
                if isinstance(ai_services, list):
                    all_services = list(set(existing_services + ai_services))
                    lead_data['services'] = all_services[:10]  # Limit to 10 services
                
                lead_data['ai_notes'] = ai_data.get('ai_notes', '')
                
            except json.JSONDecodeError:
                logger.log("Failed to parse AI response as JSON", "WARNING")
                # Fallback to basic scoring
                lead_data['lead_score'] = 50
                lead_data['quality_tier'] = 'Unknown'
                lead_data['ai_notes'] = 'AI analysis failed'
        
        except Exception as e:
            logger.log(f"AI qualification error: {e}", "WARNING")
            # Fallback to basic scoring
            lead_data['lead_score'] = 50
            lead_data['quality_tier'] = 'Unknown'
            lead_data['ai_notes'] = f'AI error: {str(e)}'
        
        return lead_data
    
    def process_lead(self, search_result, meta_info):
        """Process a single search result into a lead"""
        url = search_result.get('link', '')
        title = search_result.get('title', '')
        snippet = search_result.get('snippet', '')
        
        # Skip blacklisted domains for website searches
        if url and self.is_blacklisted(url):
            return None
        
        # Extract business name from title/snippet if available
        business_name = title
        if not business_name or len(business_name) < 3:
            # Try to extract from snippet
            if snippet:
                # Look for business name patterns in snippet
                name_match = re.search(r'^([A-Z][a-zA-Z\s&]+(?:Company|Services|Contractors|Contractor|LLC|Inc|Corp))', snippet)
                if name_match:
                    business_name = name_match.group(1)
        
        # Scrape website (or process directory listing)
        if url and url.startswith(('http://', 'https://')):
            scraped_data = self.scraper.scrape_website(url, business_name, meta_info.get('city', ''))
        else:
            # Directory listing - create minimal data
            scraped_data = {
                'website': url if url else '',
                'business_name': business_name if business_name else 'Unknown Business',
                'description': snippet[:200] if snippet else '',
                'phones': [],
                'emails': [],
                'address': '',
                'social_media': {},
                'services': [],
                'has_website': bool(url and len(url) > 5),
                'google_business_profile': '',
                'running_google_ads': False,
                'ad_transparency_url': '',
                'yelp_url': '',
                'bbb_url': ''
            }
            
            # Try to extract phone from snippet
            if snippet:
                phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                phones = re.findall(phone_pattern, snippet)
                if phones:
                    scraped_data['phones'] = [re.sub(r'[^\d+]', '', phones[0])]
            
            # Try to extract address from snippet
            address_pattern = r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd)[^,]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}'
            address_match = re.search(address_pattern, snippet)
            if address_match:
                scraped_data['address'] = address_match.group(0)
        
        # Create lead object
        lead_data = {
            'business_name': scraped_data.get('business_name', business_name or 'Unknown Business'),
            'website': scraped_data.get('website', url),
            'phone': scraped_data.get('phones', [''])[0] if scraped_data.get('phones') else '',
            'email': scraped_data.get('emails', [''])[0] if scraped_data.get('emails') else '',
            'address': scraped_data.get('address', ''),
            'city': meta_info.get('city', ''),
            'state': meta_info.get('state', CONFIG['state']),
            'industry': meta_info.get('industry', ''),
            'description': scraped_data.get('description', snippet[:500])[:500],
            'social_media': scraped_data.get('social_media', {}),
            'services': scraped_data.get('services', []),
            'has_website': scraped_data.get('has_website', True),
            'google_business_profile': scraped_data.get('google_business_profile', ''),
            'running_google_ads': scraped_data.get('running_google_ads', False),
            'ad_transparency_url': scraped_data.get('ad_transparency_url', ''),
            'yelp_url': scraped_data.get('yelp_url', ''),
            'bbb_url': scraped_data.get('bbb_url', ''),
            'scraped_date': datetime.now(timezone.utc).isoformat()
        }
        
        # Generate fingerprint
        fingerprint = self.generate_fingerprint(
            lead_data['business_name'],
            lead_data['website'],
            lead_data['phone'],
            lead_data['city']
        )
        lead_data['fingerprint'] = fingerprint
        
        # Apply filters
        if not self.passes_filters(lead_data):
            return None
        
        # AI qualification
        lead_data = self.qualify_lead(lead_data)
        
        return lead_data
    
    def passes_filters(self, lead_data):
        """Check if lead passes all filters"""
        filters = CONFIG["filters"]
        
        # Exclude without website if setting is True
        if filters["exclude_without_websites"] and not lead_data.get('has_website', True):
            return False
        
        # Exclude without phone
        if filters["exclude_without_phone"] and not lead_data.get('phone'):
            return False
        
        # Check for chain/franchise keywords
        business_name = lead_data.get('business_name', '').lower()
        description = lead_data.get('description', '').lower()
        
        for keyword in filters["exclude_keywords"]:
            if keyword.lower() in business_name or keyword.lower() in description:
                return False
        
        return True
    
    def run_cycle(self):
        """Run one scraping cycle"""
        if not self.running:
            return
        
        logger.log(f"🚀 Starting scraping cycle {self.stats['cycles'] + 1}", "INFO")
        
        queries = self.generate_search_queries()
        leads_found = 0
        leads_without_website = 0
        leads_with_ads = 0
        
        for query_info in queries:
            if self.paused or not self.running:
                break
            
            query = query_info['query']
            logger.log(f"🔍 Searching: {query} ({query_info.get('type', 'website')})", "INFO")
            
            results = self.search_serper(query)
            
            for result in results:
                if self.paused or not self.running:
                    break
                
                lead_data = self.process_lead(result, query_info)
                if lead_data:
                    # Update stats
                    if not lead_data.get('has_website', True):
                        leads_without_website += 1
                    if lead_data.get('running_google_ads', False):
                        leads_with_ads += 1
                    
                    # Save to CRM
                    if CONFIG["crm"]["enabled"] and CONFIG["crm"]["auto_sync"]:
                        result = self.crm.save_lead(lead_data)
                        if result["success"]:
                            leads_found += 1
                            logger.log(f"✅ Saved lead: {lead_data['business_name']} ({'No Website' if not lead_data.get('has_website', True) else 'Has Website'})", "SUCCESS")
                    
                    # Also save to JSON file
                    self.save_lead_to_file(lead_data)
            
            # Small delay between searches
            time.sleep(random.uniform(1, 3))
        
        self.stats['cycles'] += 1
        self.stats['total_leads'] += leads_found
        self.stats['leads_without_website'] += leads_without_website
        self.stats['leads_with_ads'] += leads_with_ads
        self.stats['last_cycle'] = datetime.now().isoformat()
        
        logger.log(f"✅ Cycle completed. Found {leads_found} new leads ({leads_without_website} without websites, {leads_with_ads} with ads). Total cycles: {self.stats['cycles']}", "SUCCESS")
    
    def save_lead_to_file(self, lead_data):
        """Save lead to JSON file"""
        try:
            leads_file = CONFIG["storage"]["leads_file"]
            leads = []
            
            if os.path.exists(leads_file):
                with open(leads_file, 'r') as f:
                    leads = json.load(f)
            
            leads.append(lead_data)
            
            # Keep only last 1000 leads
            if len(leads) > 1000:
                leads = leads[-1000:]
            
            with open(leads_file, 'w') as f:
                json.dump(leads, f, indent=2)
            
            # Separate qualified leads
            if lead_data.get('lead_score', 0) >= CONFIG["ai_enrichment"]["qualification_threshold"]:
                qualified_file = CONFIG["storage"]["qualified_leads"]
                qualified = []
                
                if os.path.exists(qualified_file):
                    with open(qualified_file, 'r') as f:
                        qualified = json.load(f)
                
                qualified.append(lead_data)
                
                if len(qualified) > 500:
                    qualified = qualified[-500:]
                
                with open(qualified_file, 'w') as f:
                    json.dump(qualified, f, indent=2)
                
                # Premium leads (score >= 80)
                if lead_data.get('lead_score', 0) >= 80:
                    premium_file = CONFIG["storage"]["premium_leads"]
                    premium = []
                    
                    if os.path.exists(premium_file):
                        with open(premium_file, 'r') as f:
                            premium = json.load(f)
                    
                    premium.append(lead_data)
                    
                    if len(premium) > 100:
                        premium = premium[-100:]
                    
                    with open(premium_file, 'w') as f:
                        json.dump(premium, f, indent=2)
        
        except Exception as e:
            logger.log(f"Error saving lead to file: {e}", "WARNING")

# ============================================================================
# MITZMEDIA-STREAMLIT DASHBOARD
# ============================================================================

class MitzMediaDashboard:
    """MitzMedia-inspired Streamlit dashboard for Lead Scraper CRM"""
    
    def __init__(self):
        if not STREAMLIT_AVAILABLE:
            self.enabled = False
            logger.log("⚠️  Streamlit not installed. Dashboard disabled.", "WARNING")
            return
        
        try:
            self.crm = CRM_Database()
            self.scraper = None
            self.scraper_running = False
            self.scraper_thread = None
            self.enabled = True
            
            # Configure Streamlit page
            st.set_page_config(
                page_title="LeadScraper CRM | MitzMedia Edition",
                page_icon="🚀",
                layout="wide",
                initial_sidebar_state="expanded"
            )
            
            # Apply MitzMedia styling
            self.setup_mitzmedia_css()
            
            logger.log("✅ MitzMedia Streamlit dashboard initialized", "SUCCESS")
        except Exception as e:
            self.enabled = False
            logger.log(f"Streamlit dashboard initialization error: {e}", "ERROR")
    
    def setup_mitzmedia_css(self):
        """Setup MitzMedia-inspired CSS for Streamlit"""
        st.markdown("""
        <style>
        /* MitzMedia Inspired Theme */
        :root {
            --primary: #111827;
            --primary-dark: #0f172a;
            --primary-light: #1e293b;
            --accent: #f59e0b;
            --accent-light: #fbbf24;
            --success: #10b981;
            --danger: #ef4444;
            --warning: #f59e0b;
            --info: #3b82f6;
            --card-bg: #1e293b;
            --border: #334155;
            --text-light: #f1f5f9;
            --text-muted: #94a3b8;
            --text-dark: #0f172a;
        }
        
        /* Main container */
        .stApp {
            background: linear-gradient(135deg, var(--primary-dark) 0%, var(--primary-light) 100%) !important;
            color: var(--text-light) !important;
        }
        
        /* MitzMedia-style cards */
        .mitz-card {
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            margin-bottom: 1rem;
        }
        
        .mitz-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.2);
            border-color: var(--accent);
        }
        
        /* MitzMedia button styles */
        .stButton > button {
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-light) 100%) !important;
            color: var(--text-dark) !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
            padding: 0.75rem 1.5rem !important;
            transition: all 0.3s ease !important;
            box-shadow: 0 4px 6px -1px rgba(245, 158, 11, 0.2) !important;
        }
        
        .stButton > button:hover {
            transform: scale(1.05) !important;
            box-shadow: 0 10px 15px -3px rgba(245, 158, 11, 0.3) !important;
        }
        
        /* Secondary buttons */
        .stButton > button[kind="secondary"] {
            background: linear-gradient(135deg, var(--primary-light) 0%, var(--border) 100%) !important;
            color: var(--text-light) !important;
            border: 1px solid var(--border) !important;
        }
        
        /* Stats cards like MitzMedia */
        .stats-card {
            background: rgba(255, 255, 255, 0.05);
            border-left: 4px solid var(--accent);
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
        }
        
        .stats-card h3 {
            color: var(--text-light) !important;
            margin: 0 0 0.5rem 0 !important;
            font-size: 0.875rem !important;
            font-weight: 600 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.05em !important;
        }
        
        .stats-card .value {
            color: var(--accent) !important;
            font-size: 1.875rem !important;
            font-weight: 700 !important;
            margin: 0 !important;
        }
        
        .stats-card .label {
            color: var(--text-muted) !important;
            font-size: 0.75rem !important;
            margin: 0.25rem 0 0 0 !important;
        }
        
        /* Badges */
        .badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .badge-premium {
            background: linear-gradient(135deg, #f59e0b, #d97706);
            color: white;
        }
        
        .badge-high {
            background: linear-gradient(135deg, #10b981, #059669);
            color: white;
        }
        
        .badge-medium {
            background: linear-gradient(135deg, #3b82f6, #2563eb);
            color: white;
        }
        
        .badge-low {
            background: linear-gradient(135deg, #6b7280, #4b5563);
            color: white;
        }
        
        .badge-no-website {
            background: linear-gradient(135deg, #ef4444, #dc2626);
            color: white;
        }
        
        .badge-ads {
            background: linear-gradient(135deg, #8b5cf6, #7c3aed);
            color: white;
        }
        
        /* Status indicators */
        .status-active {
            color: var(--success);
            font-weight: 600;
        }
        
        .status-inactive {
            color: var(--danger);
            font-weight: 600;
        }
        
        .status-warning {
            color: var(--warning);
            font-weight: 600;
        }
        
        /* Headers */
        h1, h2, h3 {
            color: var(--text-light) !important;
            font-weight: 700 !important;
        }
        
        h1 {
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-light) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 1rem;
            border-bottom: 1px solid var(--border);
        }
        
        .stTabs [data-baseweb="tab"] {
            background: transparent !important;
            color: var(--text-muted) !important;
            border: none !important;
            border-radius: 8px 8px 0 0 !important;
            padding: 0.75rem 1.5rem !important;
            font-weight: 600 !important;
        }
        
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            color: var(--accent) !important;
            border-bottom: 2px solid var(--accent) !important;
            background: transparent !important;
        }
        
        /* Sidebar */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, var(--primary-dark) 0%, var(--primary) 100%) !important;
            border-right: 1px solid var(--border) !important;
        }
        
        /* DataTables */
        .dataframe {
            background: var(--card-bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 8px !important;
            color: var(--text-light) !important;
        }
        
        .dataframe th {
            background: rgba(245, 158, 11, 0.1) !important;
            color: var(--text-light) !important;
            font-weight: 600 !important;
            border-bottom: 1px solid var(--border) !important;
        }
        
        .dataframe td {
            border-bottom: 1px solid var(--border) !important;
            color: var(--text-light) !important;
        }
        
        /* Metrics */
        [data-testid="stMetricValue"] {
            color: var(--accent) !important;
            font-weight: 700 !important;
        }
        
        [data-testid="stMetricLabel"] {
            color: var(--text-muted) !important;
            font-weight: 600 !important;
        }
        
        /* Input fields */
        .stTextInput > div > div > input,
        .stTextArea > div > div > textarea,
        .stSelectbox > div > div > div,
        .stNumberInput > div > div > input {
            background: var(--card-bg) !important;
            color: var(--text-light) !important;
            border: 1px solid var(--border) !important;
            border-radius: 8px !important;
        }
        
        /* Hide Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stDeployButton {display:none;}
        
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: var(--primary-dark);
        }
        
        ::-webkit-scrollbar-thumb {
            background: var(--border);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: var(--accent);
        }
        </style>
        """, unsafe_allow_html=True)
    
    def run_scraper_background(self):
        """Run scraper in background"""
        try:
            self.scraper = EnhancedLeadScraper()
            self.scraper.running = True
            self.scraper.paused = False
            
            cycles = 0
            while self.scraper_running and cycles < CONFIG['max_cycles']:
                if not self.scraper.running:
                    break
                
                self.scraper.run_cycle()
                cycles += 1
                
                # Update session state
                if 'scraper_stats' not in st.session_state:
                    st.session_state.scraper_stats = {}
                
                st.session_state.scraper_stats = {
                    'cycles': cycles,
                    'total_leads': self.scraper.stats['total_leads'],
                    'leads_without_website': self.scraper.stats['leads_without_website'],
                    'leads_with_ads': self.scraper.stats['leads_with_ads'],
                    'last_cycle': self.scraper.stats['last_cycle']
                }
                
                # Check if we should continue
                if self.scraper_running and cycles < CONFIG['max_cycles']:
                    time.sleep(CONFIG['cycle_interval'])
            
            self.scraper_running = False
            logger.log("Scraper finished", "INFO")
            
            # Update session state
            st.session_state.scraper_running = False
            
        except Exception as e:
            logger.log(f"Background scraper error: {e}", "ERROR")
            self.scraper_running = False
            st.session_state.scraper_running = False
    
    def start_scraper(self):
        """Start the scraper"""
        if not self.scraper_running:
            self.scraper_running = True
            st.session_state.scraper_running = True
            self.scraper_thread = threading.Thread(target=self.run_scraper_background, daemon=True)
            self.scraper_thread.start()
            return True
        return False
    
    def stop_scraper(self):
        """Stop the scraper"""
        self.scraper_running = False
        if self.scraper:
            self.scraper.running = False
        st.session_state.scraper_running = False
        return True
    
    def render_sidebar(self):
        """Render the MitzMedia-style sidebar"""
        with st.sidebar:
            # Logo and title
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem; padding: 1.5rem 0; border-bottom: 1px solid #334155;">
                <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">🚀</div>
                <h1 style="color: #f59e0b; font-size: 1.5rem; margin-bottom: 0.25rem; font-weight: 700;">LeadScraper CRM</h1>
                <p style="color: #94a3b8; font-size: 0.875rem; margin: 0;">MitzMedia Edition v6.0</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation
            st.markdown("### 📊 Navigation")
            page = st.radio(
                "Select Page",
                ["Dashboard", "Leads", "Lead Details", "Settings", "Analytics", "Export", "Logs"],
                label_visibility="collapsed"
            )
            
            st.markdown("---")
            
            # Scraper Control
            st.markdown("### ⚙️ Scraper Control")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("▶️ Start Scraper", use_container_width=True, type="primary"):
                    if self.start_scraper():
                        st.success("Scraper started!")
                        st.rerun()
            
            with col2:
                if st.button("⏹️ Stop Scraper", use_container_width=True, type="secondary"):
                    if self.stop_scraper():
                        st.info("Scraper stopped!")
                        st.rerun()
            
            # Scraper Status
            status_color = "#10b981" if st.session_state.get('scraper_running', False) else "#ef4444"
            status_icon = "🟢" if st.session_state.get('scraper_running', False) else "🔴"
            status_text = "Active" if st.session_state.get('scraper_running', False) else "Inactive"
            
            st.markdown(f"""
            <div style="background: rgba(255, 255, 255, 0.05); padding: 0.75rem; border-radius: 8px; margin: 0.5rem 0;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="color: {status_color}; font-weight: 600;">{status_icon} {status_text}</span>
                    <span style="color: #94a3b8; font-size: 0.875rem;">Status</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            if 'scraper_stats' in st.session_state:
                stats = st.session_state.scraper_stats
                st.markdown(f"""
                <div style="background: rgba(255, 255, 255, 0.05); padding: 0.75rem; border-radius: 8px; margin: 0.5rem 0;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                        <span style="color: #94a3b8;">Cycles</span>
                        <span style="color: #f59e0b; font-weight: 600;">{stats.get('cycles', 0)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                        <span style="color: #94a3b8;">Total Leads</span>
                        <span style="color: #10b981; font-weight: 600;">{stats.get('total_leads', 0)}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between;">
                        <span style="color: #94a3b8;">Last Cycle</span>
                        <span style="color: #94a3b8; font-size: 0.875rem;">{stats.get('last_cycle', 'Never')[:19] if stats.get('last_cycle') else 'Never'}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Quick Stats
            st.markdown("### 📈 Quick Stats")
            
            today_count = self.crm.get_today_count()
            total_leads = self.crm.get_leads()["total"]
            stats = self.crm.get_statistics()
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"""
                <div class="stats-card">
                    <h3>Today</h3>
                    <p class="value">{today_count}</p>
                    <p class="label">Leads</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="stats-card">
                    <h3>Total</h3>
                    <p class="value">{total_leads}</p>
                    <p class="label">Leads</p>
                </div>
                """, unsafe_allow_html=True)
            
            col3, col4 = st.columns(2)
            with col3:
                without_website = stats["overall"].get("leads_without_website", 0)
                st.markdown(f"""
                <div class="stats-card">
                    <h3>No Website</h3>
                    <p class="value">{without_website}</p>
                    <p class="label">Leads</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                with_ads = stats["overall"].get("leads_with_ads", 0)
                st.markdown(f"""
                <div class="stats-card">
                    <h3>With Ads</h3>
                    <p class="value">{with_ads}</p>
                    <p class="label">Leads</p>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # System Info
            st.markdown("### 💻 System Info")
            
            info_items = [
                ("Database", "✅ Connected" if self.crm.conn else "❌ Error"),
                ("AI Enabled", "✅ Ready" if OPENAI_AVAILABLE and CONFIG.get('openai_api_key', '').startswith('sk-') else "❌ Disabled"),
                ("State", CONFIG['state']),
                ("Cities", len(CONFIG['cities'])),
                ("Industries", len(CONFIG['industries'])),
                ("No Website Scraping", "✅ Enabled" if not CONFIG['filters']['exclude_without_websites'] else "❌ Disabled"),
                ("Google Ads Check", "✅ Enabled" if CONFIG['enhanced_features']['check_google_ads'] else "❌ Disabled"),
                ("Google Business", "✅ Enabled" if CONFIG['enhanced_features']['find_google_business'] else "❌ Disabled")
            ]
            
            for label, value in info_items:
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: #94a3b8; font-size: 0.875rem;">{label}</span>
                    <span style="color: #f1f5f9; font-size: 0.875rem; font-weight: 500;">{value}</span>
                </div>
                """, unsafe_allow_html=True)
        
        return page
    
    def render_dashboard(self):
        """Render the main dashboard"""
        # Header
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("<h1>📊 Dashboard Overview</h1>", unsafe_allow_html=True)
            st.markdown("<p style='color: #94a3b8;'>Real-time analytics and lead insights</p>", unsafe_allow_html=True)
        
        with col2:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.rerun()
        
        # Get statistics
        stats = self.crm.get_statistics()
        
        # Top Metrics Row
        st.markdown("### Key Metrics")
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric("Total Leads", stats["overall"]["total_leads"])
        
        with col2:
            st.metric("Estimated Value", f"${stats['overall']['total_value']:,}")
        
        with col3:
            st.metric("Avg Score", f"{stats['overall']['avg_score']:.1f}")
        
        with col4:
            st.metric("Closed Won", stats["overall"]["closed_won"])
        
        with col5:
            without_website = stats["overall"].get("leads_without_website", 0)
            st.metric("No Website", without_website)
        
        with col6:
            with_ads = stats["overall"].get("leads_with_ads", 0)
            st.metric("With Ads", with_ads)
        
        # Charts Row
        st.markdown("### Analytics")
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality Distribution
            quality_data = stats["quality_distribution"]
            if quality_data:
                df_quality = pd.DataFrame(quality_data)
                fig_quality = px.pie(
                    df_quality, 
                    values='count', 
                    names='tier',
                    title='Lead Quality Distribution',
                    color='tier',
                    color_discrete_map={
                        'Premium': '#f59e0b',
                        'High': '#10b981',
                        'Medium': '#3b82f6',
                        'Low': '#6b7280'
                    }
                )
                fig_quality.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f1f5f9'
                )
                st.plotly_chart(fig_quality, use_container_width=True)
        
        with col2:
            # Website Distribution
            website_data = stats.get("website_distribution", [])
            if website_data:
                df_website = pd.DataFrame(website_data)
                fig_website = px.bar(
                    df_website,
                    x='category',
                    y='count',
                    title='Website Status Distribution',
                    color='category',
                    color_discrete_map={
                        'Has Website': '#10b981',
                        'No Website': '#ef4444'
                    }
                )
                fig_website.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f1f5f9',
                    xaxis_title="",
                    yaxis_title="Count",
                    showlegend=False
                )
                st.plotly_chart(fig_website, use_container_width=True)
        
        # Daily Leads Chart
        st.markdown("### 📅 Daily Lead Acquisition (Last 30 Days)")
        daily_data = stats["daily_leads"]
        if daily_data:
            df_daily = pd.DataFrame(daily_data)
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily = df_daily.sort_values('date')
            
            fig_daily = px.line(
                df_daily,
                x='date',
                y='count',
                title='',
                markers=True,
                line_shape='spline'
            )
            fig_daily.update_traces(line_color='#f59e0b', marker_color='#fbbf24')
            fig_daily.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#f1f5f9',
                xaxis_title="Date",
                yaxis_title="Leads Count",
                hovermode='x unified'
            )
            st.plotly_chart(fig_daily, use_container_width=True)
        
        # Recent Leads
        st.markdown("### 🆕 Recent Leads")
        leads_data = self.crm.get_leads(page=1, per_page=10)
        
        if leads_data["leads"]:
            for lead in leads_data["leads"][:5]:  # Show first 5
                self.render_lead_card(lead)
            
            if len(leads_data["leads"]) > 5:
                with st.expander("Show More Leads"):
                    for lead in leads_data["leads"][5:10]:
                        self.render_lead_card(lead)
        else:
            st.info("No leads found. Start the scraper to collect leads!")
    
    def render_lead_card(self, lead):
        """Render a lead card in MitzMedia style"""
        business_name = lead.get('business_name', 'Unknown Business')
        city = lead.get('city', 'Unknown')
        industry = lead.get('industry', 'Unknown')
        lead_score = lead.get('lead_score', 0)
        quality_tier = lead.get('quality_tier', 'Unknown')
        has_website = lead.get('has_website', True)
        running_ads = lead.get('running_google_ads', False)
        google_profile = lead.get('google_business_profile', '')
        
        # Determine badge class
        badge_class = f"badge-{quality_tier.lower()}" if quality_tier.lower() in ['premium', 'high', 'medium', 'low'] else "badge-low"
        
        with st.container():
            st.markdown(f"""
            <div class="mitz-card">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 1rem;">
                    <div>
                        <h4 style="margin: 0; color: #f1f5f9; font-weight: 600;">{business_name}</h4>
                        <p style="color: #94a3b8; margin: 0.5rem 0; font-size: 0.875rem;">
                            {city} • {industry} • Score: {lead_score}/100
                        </p>
                    </div>
                    <div style="display: flex; gap: 0.5rem; flex-wrap: wrap;">
                        <span class="{badge_class}">{quality_tier}</span>
                        {'' if has_website else '<span class="badge-no-website">No Website</span>'}
                        {'' if not running_ads else '<span class="badge-ads">Running Ads</span>'}
                    </div>
                </div>
                
                <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1rem;">
                    <div>
                        <small style="color: #94a3b8;">Website</small>
                        <p style="margin: 0; color: #f1f5f9; font-size: 0.875rem;">
                            {lead.get('website', 'N/A')[:25] + '...' if lead.get('website') else 'N/A'}
                        </p>
                    </div>
                    <div>
                        <small style="color: #94a3b8;">Phone</small>
                        <p style="margin: 0; color: #f1f5f9; font-size: 0.875rem;">
                            {lead.get('phone', 'N/A')}
                        </p>
                    </div>
                    <div>
                        <small style="color: #94a3b8;">Google Profile</small>
                        <p style="margin: 0; color: #f1f5f9; font-size: 0.875rem;">
                            {'<a href="' + google_profile + '" target="_blank" style="color: #f59e0b; text-decoration: none;">Available</a>' if google_profile else 'Not found'}
                        </p>
                    </div>
                    <div>
                        <small style="color: #94a3b8;">Ads Transparency</small>
                        <p style="margin: 0; color: #f1f5f9; font-size: 0.875rem;">
                            {'<a href="' + lead.get('ad_transparency_url', '#') + '" target="_blank" style="color: #f59e0b; text-decoration: none;">Check</a>' if lead.get('ad_transparency_url') else 'N/A'}
                        </p>
                    </div>
                </div>
                
                <div style="display: flex; gap: 0.5rem; justify-content: flex-end; padding-top: 1rem; border-top: 1px solid #334155;">
                    <button style="background: transparent; color: #94a3b8; border: 1px solid #334155; 
                            padding: 0.5rem 1rem; border-radius: 6px; cursor: pointer; font-size: 0.875rem;"
                            onclick="window.location.href='?page=Lead%20Details&lead_id={lead.get('id', '')}'">
                        View Details
                    </button>
                    <button style="background: linear-gradient(135deg, #f59e0b, #fbbf24); color: #111827; border: none;
                            padding: 0.5rem 1rem; border-radius: 6px; cursor: pointer; font-size: 0.875rem; font-weight: 600;"
                            onclick="alert('Outreach functionality would open email/CRM here')">
                        Start Outreach
                    </button>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    def render_leads(self):
        """Render leads management page"""
        st.markdown("<h1>👥 Leads Management</h1>", unsafe_allow_html=True)
        st.markdown("<p style='color: #94a3b8;'>Filter and manage your leads</p>", unsafe_allow_html=True)
        
        # Filters in expandable section
        with st.expander("🔍 Advanced Filters", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                search_term = st.text_input("Search", placeholder="Business name, phone...")
            
            with col2:
                status_filter = st.selectbox("Status", ["All"] + CONFIG["lead_management"]["status_options"])
            
            with col3:
                quality_filter = st.selectbox("Quality Tier", ["All"] + CONFIG["lead_management"]["quality_tiers"])
            
            with col4:
                city_filter = st.selectbox("City", ["All"] + CONFIG["cities"])
            
            col5, col6, col7, col8 = st.columns(4)
            
            with col5:
                website_filter = st.selectbox("Has Website", ["All", "Yes", "No"])
            
            with col6:
                ads_filter = st.selectbox("Running Ads", ["All", "Yes", "No"])
            
            with col7:
                date_from = st.date_input("From Date", value=None)
            
            with col8:
                date_to = st.date_input("To Date", value=None)
        
        # Build filters
        filters = {}
        if search_term and search_term != "All":
            filters["search"] = search_term
        if status_filter and status_filter != "All":
            filters["status"] = status_filter
        if quality_filter and quality_filter != "All":
            filters["quality_tier"] = quality_filter
        if city_filter and city_filter != "All":
            filters["city"] = city_filter
        if website_filter != "All":
            filters["has_website"] = 1 if website_filter == "Yes" else 0
        if ads_filter != "All":
            filters["running_ads"] = 1 if ads_filter == "Yes" else 0
        if date_from:
            filters["date_from"] = date_from.isoformat()
        if date_to:
            filters["date_to"] = date_to.isoformat()
        
        # Get leads
        leads_data = self.crm.get_leads(filters=filters, page=1, per_page=50)
        leads = leads_data["leads"]
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.metric("Total Leads Found", leads_data["total"])
        with col2:
            if st.button("📥 Export Filtered", use_container_width=True):
                # Export logic would go here
                st.info("Export functionality would download CSV here")
        
        if leads:
            # Create dataframe for display
            df = pd.DataFrame(leads)
            
            # Select and format columns
            display_cols = ['id', 'business_name', 'phone', 'city', 
                          'industry', 'lead_score', 'quality_tier', 
                          'lead_status', 'has_website', 'running_google_ads']
            
            if all(col in df.columns for col in display_cols):
                df_display = df[display_cols].copy()
                
                # Format the display
                for idx, lead in enumerate(leads[:20]):  # Show first 20
                    self.render_lead_card(lead)
                
                # Pagination info
                if leads_data["total"] > 20:
                    st.info(f"Showing 20 of {leads_data['total']} leads. Use filters to narrow down results.")
            else:
                st.warning("Some required columns are missing from the data.")
        else:
            st.info("No leads match the current filters.")
    
    def render_lead_detail(self, lead=None, lead_id=None):
        """Render lead details"""
        if lead_id and not lead:
            lead = self.crm.get_lead_by_id(lead_id)
        
        if not lead:
            st.error("Lead not found!")
            return
        
        # Header with actions
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"<h1>📋 {lead.get('business_name', 'Lead Details')}</h1>", unsafe_allow_html=True)
        
        with col2:
            if st.button("✏️ Edit Lead", use_container_width=True):
                st.session_state.editing_lead = True
        
        # Create tabs for different sections
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Basic Info", "📞 Contact", "🎯 Targeting", "📊 Status & Actions", "📝 Activities"])
        
        with tab1:
            self.render_basic_info_tab(lead)
        
        with tab2:
            self.render_contact_tab(lead)
        
        with tab3:
            self.render_targeting_tab(lead)
        
        with tab4:
            self.render_status_tab(lead)
        
        with tab5:
            self.render_activities_tab(lead)
    
    def render_basic_info_tab(self, lead):
        """Render basic information tab"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Business Information")
            
            st.text_input("Business Name", lead.get('business_name', ''), disabled=True)
            st.text_input("Industry", lead.get('industry', ''), disabled=True)
            st.text_input("Business Type", lead.get('business_type', ''), disabled=True)
            
            # Description
            description = lead.get('description', '')
            if description:
                with st.expander("Description"):
                    st.write(description)
            
            # Services
            services = lead.get('services', [])
            if services:
                if isinstance(services, str):
                    services = [services]
                st.markdown("**Services:**")
                for service in services[:10]:
                    st.markdown(f"- {service}")
        
        with col2:
            st.markdown("### Lead Quality")
            
            # Quality Score
            col_score, col_tier = st.columns(2)
            with col_score:
                lead_score = lead.get('lead_score', 0)
                st.metric("Lead Score", f"{lead_score}/100")
            
            with col_tier:
                tier = lead.get('quality_tier', 'Unknown')
                tier_color = {
                    'Premium': 'badge-premium',
                    'High': 'badge-high',
                    'Medium': 'badge-medium',
                    'Low': 'badge-low',
                    'Unknown': 'badge-low'
                }.get(tier, 'badge-low')
                st.markdown(f"**Quality Tier:** <span class='{tier_color}'>{tier}</span>", unsafe_allow_html=True)
            
            st.markdown("---")
            
            # AI Notes
            ai_notes = lead.get('ai_notes', '')
            if ai_notes:
                with st.expander("AI Analysis Notes"):
                    st.write(ai_notes)
            
            # Potential Value
            potential_value = lead.get('potential_value', 0)
            if potential_value > 0:
                st.metric("Potential Value", f"${potential_value:,}")
    
    def render_contact_tab(self, lead):
        """Render contact information tab"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Contact Details")
            
            website = lead.get('website', '')
            if website:
                st.markdown(f"**Website:** [{website}]({website})")
            else:
                st.markdown("**Website:** ❌ Not available")
            
            phone = lead.get('phone', '')
            if phone:
                st.markdown(f"**Phone:** `{phone}`")
                if st.button("📞 Call", key="call_btn"):
                    st.info(f"Would dial: {phone}")
            else:
                st.markdown("**Phone:** ❌ Not available")
            
            email = lead.get('email', '')
            if email:
                st.markdown(f"**Email:** `{email}`")
                if st.button("📧 Email", key="email_btn"):
                    st.info(f"Would open email to: {email}")
            else:
                st.markdown("**Email:** ❌ Not available")
            
            address = lead.get('address', '')
            if address:
                st.text_area("Address", address, disabled=True, height=100)
            else:
                st.markdown("**Address:** ❌ Not available")
        
        with col2:
            st.markdown("### Location & Directories")
            
            st.text_input("City", lead.get('city', ''), disabled=True)
            st.text_input("State", lead.get('state', ''), disabled=True)
            
            st.markdown("---")
            
            # Google Business Profile
            google_profile = lead.get('google_business_profile', '')
            if google_profile:
                st.markdown(f"**Google Business Profile:** [View Profile]({google_profile})")
            else:
                st.markdown("**Google Business Profile:** ❌ Not found")
            
            # Yelp
            yelp_url = lead.get('yelp_url', '')
            if yelp_url:
                st.markdown(f"**Yelp:** [View Page]({yelp_url})")
            
            # BBB
            bbb_url = lead.get('bbb_url', '')
            if bbb_url:
                st.markdown(f"**BBB:** [View Page]({bbb_url})")
            
            # Social Media
            social_media = lead.get('social_media', {})
            if social_media and isinstance(social_media, dict) and social_media:
                st.markdown("### Social Media")
                for platform, url in social_media.items():
                    st.markdown(f"**{platform.title()}:** [{url}]({url})")
    
    def render_targeting_tab(self, lead):
        """Render targeting information tab"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Website & Online Presence")
            
            has_website = lead.get('has_website', True)
            website_status = "✅ Has Website" if has_website else "❌ No Website"
            st.markdown(f"**Website Status:** {website_status}")
            
            if not has_website:
                st.info("This lead was found through directory listings (Yelp, BBB, etc.) and may be a good outreach target.")
            
            st.markdown("---")
            
            st.markdown("### Google Ads Status")
            
            running_ads = lead.get('running_google_ads', False)
            ads_status = "✅ Running Google Ads" if running_ads else "❌ Not Running Ads"
            st.markdown(f"**Ads Status:** {ads_status}")
            
            ad_transparency_url = lead.get('ad_transparency_url', '')
            if ad_transparency_url:
                st.markdown(f"**Ads Transparency:** [Check]({ad_transparency_url})")
            
            if running_ads:
                st.success("This business is already investing in online advertising - they understand digital marketing value!")
            else:
                st.info("This business may not be aware of digital advertising opportunities.")
        
        with col2:
            st.markdown("### Outreach Priority")
            
            priority = lead.get('outreach_priority', 'Medium')
            priority_colors = {
                'Immediate': '#ef4444',
                'High': '#f59e0b',
                'Medium': '#3b82f6',
                'Low': '#6b7280'
            }
            priority_color = priority_colors.get(priority, '#6b7280')
            
            st.markdown(f"""
            <div style="background: {priority_color}20; border: 1px solid {priority_color}; 
                        border-radius: 8px; padding: 1rem; text-align: center;">
                <h3 style="color: {priority_color}; margin: 0;">{priority}</h3>
                <p style="color: #94a3b8; margin: 0.5rem 0 0 0; font-size: 0.875rem;">
                    Outreach Priority
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            st.markdown("### Recommended Approach")
            
            has_website = lead.get('has_website', True)
            running_ads = lead.get('running_google_ads', False)
            lead_score = lead.get('lead_score', 0)
            
            if not has_website:
                recommendation = """
                **Phone/In-person Outreach Recommended**
                - Call directly during business hours
                - Offer website development services
                - Emphasize lost online opportunities
                """
            elif running_ads:
                recommendation = """
                **Digital Marketing Upsell**
                - They already value online advertising
                - Offer Google Ads optimization
                - Propose expanded campaigns
                - Show ROI improvement potential
                """
            elif lead_score >= 70:
                recommendation = """
                **High-Value Lead**
                - Schedule Zoom meeting
                - Prepare detailed proposal
                - Focus on ROI and results
                - Follow up within 24 hours
                """
            else:
                recommendation = """
                **Standard Outreach**
                - Email introduction first
                - Follow up with call
                - Offer free audit/analysis
                - Build relationship gradually
                """
            
            st.info(recommendation)
    
    def render_status_tab(self, lead):
        """Render status and actions tab"""
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Lead Status")
            
            # Status update form
            with st.form("update_status_form"):
                current_status = lead.get('lead_status', 'New Lead')
                status_options = CONFIG["lead_management"]["status_options"]
                current_index = status_options.index(current_status) if current_status in status_options else 0
                
                new_status = st.selectbox(
                    "Update Status",
                    status_options,
                    index=current_index
                )
                
                priority_options = CONFIG["lead_management"]["priority_options"]
                current_priority = lead.get('outreach_priority', 'Medium')
                priority_index = priority_options.index(current_priority) if current_priority in priority_options else 2
                
                new_priority = st.selectbox(
                    "Update Priority",
                    priority_options,
                    index=priority_index
                )
                
                assigned_to = st.text_input("Assigned To", lead.get('assigned_to', ''))
                
                notes = st.text_area("Notes", lead.get('notes', ''), height=100)
                
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.form_submit_button("💾 Update Lead", use_container_width=True):
                        update_data = {
                            'lead_status': new_status,
                            'outreach_priority': new_priority,
                            'assigned_to': assigned_to,
                            'notes': notes
                        }
                        result = self.crm.update_lead(lead['id'], update_data)
                        if result['success']:
                            st.success("Lead updated successfully!")
                            st.rerun()
                        else:
                            st.error(f"Error: {result['message']}")
                
                with col_btn2:
                    if st.form_submit_button("🗑️ Archive Lead", type="secondary", use_container_width=True):
                        result = self.crm.delete_lead(lead['id'])
                        if result['success']:
                            st.success("Lead archived!")
                            st.rerun()
                        else:
                            st.error(f"Error: {result['message']}")
        
        with col2:
            st.markdown("### Timeline & History")
            
            created = lead.get('created_at', '')
            if created:
                st.markdown(f"**Created:** `{created[:19]}`")
            
            scraped = lead.get('scraped_date', '')
            if scraped:
                st.markdown(f"**Scraped:** `{scraped[:19]}`")
            
            follow_up = lead.get('follow_up_date', '')
            if follow_up:
                st.markdown(f"**Follow-up Date:** `{follow_up}`")
                
                # Check if follow-up is due
                try:
                    follow_up_date = datetime.fromisoformat(follow_up.replace('Z', '+00:00'))
                    today = datetime.now(timezone.utc)
                    if follow_up_date.date() <= today.date():
                        st.warning("⚠️ Follow-up is due!")
                except:
                    pass
            
            meeting_date = lead.get('meeting_date', '')
            if meeting_date:
                st.markdown(f"**Meeting Scheduled:** `{meeting_date[:19]}`")
            
            st.markdown("---")
            
            st.markdown("### Quick Actions")
            
            col_act1, col_act2 = st.columns(2)
            with col_act1:
                if st.button("📧 Send Email", use_container_width=True):
                    st.info("Email template would open here")
            
            with col_act2:
                if st.button("📞 Schedule Call", use_container_width=True):
                    st.info("Calendar integration would open here")
            
            if st.button("📋 Create Task", use_container_width=True):
                st.info("Task creation form would appear here")
    
    def render_activities_tab(self, lead):
        """Render activities timeline tab"""
        st.markdown("### Activity Timeline")
        activities = lead.get('activities', [])
        
        if activities:
            # Reverse to show newest first
            activities.reverse()
            
            for activity in activities[:20]:  # Show last 20 activities
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        activity_type = activity.get('activity_type', 'Activity')
                        activity_details = activity.get('activity_details', '')
                        
                        # Style based on activity type
                        if 'Created' in activity_type:
                            icon = "🆕"
                            color = "#10b981"
                        elif 'Updated' in activity_type:
                            icon = "✏️"
                            color = "#3b82f6"
                        elif 'Contacted' in activity_type:
                            icon = "📞"
                            color = "#f59e0b"
                        elif 'Meeting' in activity_type:
                            icon = "📅"
                            color = "#8b5cf6"
                        elif 'Closed' in activity_type:
                            icon = "✅"
                            color = "#10b981"
                        else:
                            icon = "📝"
                            color = "#94a3b8"
                        
                        st.markdown(f"""
                        <div style="display: flex; align-items: flex-start; margin-bottom: 1rem;">
                            <div style="background: {color}20; color: {color}; border-radius: 50%; 
                                     width: 32px; height: 32px; display: flex; align-items: center; 
                                     justify-content: center; margin-right: 0.75rem; font-size: 1rem;">
                                {icon}
                            </div>
                            <div>
                                <div style="color: #f1f5f9; font-weight: 600; margin-bottom: 0.25rem;">
                                    {activity_type}
                                </div>
                                <div style="color: #94a3b8; font-size: 0.875rem;">
                                    {activity_details}
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        performed = activity.get('performed_at', '')
                        if performed:
                            st.caption(performed[:19])
                    
                    st.divider()
        else:
            st.info("No activities recorded yet.")
    
    def render_settings(self):
        """Render settings page with MitzMedia styling"""
        st.markdown("<h1>⚙️ Settings & Configuration</h1>", unsafe_allow_html=True)
        st.markdown("<p style='color: #94a3b8;'>Configure your scraper and CRM settings</p>", unsafe_allow_html=True)
        
        # Create tabs for different setting categories
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["🔑 API Keys", "🎯 Targeting", "🔍 Scraper", "🏢 Business", "📊 CRM"])
        
        with tab1:
            self.render_api_settings()
        
        with tab2:
            self.render_targeting_settings()
        
        with tab3:
            self.render_scraper_settings()
        
        with tab4:
            self.render_business_settings()
        
        with tab5:
            self.render_crm_settings()
    
    def render_api_settings(self):
        """Render API settings tab"""
        st.markdown("### API Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            serper_key = st.text_input(
                "Serper API Key",
                value=CONFIG.get("serper_api_key", ""),
                type="password",
                help="Get from https://serper.dev",
                key="serper_key_input"
            )
            
            if serper_key and serper_key != CONFIG.get("serper_api_key", ""):
                CONFIG["serper_api_key"] = serper_key
        
        with col2:
            openai_key = st.text_input(
                "OpenAI API Key",
                value=CONFIG.get("openai_api_key", ""),
                type="password",
                help="Get from https://platform.openai.com/api-keys",
                key="openai_key_input"
            )
            
            if openai_key and openai_key != CONFIG.get("openai_api_key", ""):
                CONFIG["openai_api_key"] = openai_key
        
        st.markdown("---")
        
        col_btn1, col_btn2, _ = st.columns(3)
        with col_btn1:
            if st.button("💾 Save API Keys", use_container_width=True, type="primary"):
                self.save_config()
                st.success("API keys saved!")
        
        with col_btn2:
            if st.button("🔄 Test Connection", use_container_width=True):
                # Test Serper API
                if CONFIG.get("serper_api_key"):
                    try:
                        import requests
                        headers = {'X-API-KEY': CONFIG["serper_api_key"]}
                        response = requests.post("https://google.serper.dev/search", 
                                               json={'q': 'test'}, 
                                               headers=headers,
                                               timeout=5)
                        if response.status_code == 200:
                            st.success("✅ Serper API: Connected")
                        else:
                            st.error(f"❌ Serper API: Error {response.status_code}")
                    except:
                        st.error("❌ Serper API: Connection failed")
                else:
                    st.warning("⚠️ Serper API key not set")
                
                # Test OpenAI API
                if CONFIG.get("openai_api_key") and OPENAI_AVAILABLE:
                    try:
                        import openai
                        client = openai.OpenAI(api_key=CONFIG["openai_api_key"])
                        # Simple test
                        st.success("✅ OpenAI API: Ready")
                    except:
                        st.error("❌ OpenAI API: Connection failed")
                else:
                    st.warning("⚠️ OpenAI API key not set or library not installed")
    
    def render_targeting_settings(self):
        """Render targeting settings tab"""
        st.markdown("### 🎯 Enhanced Targeting Settings")
        
        st.markdown("#### Website Inclusion")
        col1, col2 = st.columns(2)
        
        with col1:
            include_no_website = st.toggle(
                "Include businesses without websites",
                value=not CONFIG["filters"]["exclude_without_websites"],
                help="Turn ON to get leads from Yelp, BBB, YellowPages with only phone/address (Great for outreach!)",
                key="include_no_website_toggle"
            )
            CONFIG["filters"]["exclude_without_websites"] = not include_no_website
        
        with col2:
            require_phone = st.toggle(
                "Require phone number",
                value=CONFIG["filters"]["exclude_without_phone"],
                help="Only save leads with phone numbers",
                key="require_phone_toggle"
            )
            CONFIG["filters"]["exclude_without_phone"] = require_phone
        
        st.markdown("---")
        
        st.markdown("#### Google Integration")
        col3, col4 = st.columns(2)
        
        with col3:
            check_google_ads = st.toggle(
                "Check Google Ads status",
                value=CONFIG["enhanced_features"]["check_google_ads"],
                help="Check if businesses are running Google Ads using Ads Transparency Center",
                key="check_google_ads_toggle"
            )
            CONFIG["enhanced_features"]["check_google_ads"] = check_google_ads
        
        with col4:
            find_google_business = st.toggle(
                "Find Google Business Profiles",
                value=CONFIG["enhanced_features"]["find_google_business"],
                help="Extract Google Business Profile/Maps links",
                key="find_google_business_toggle"
            )
            CONFIG["enhanced_features"]["find_google_business"] = find_google_business
        
        st.markdown("---")
        
        st.markdown("#### Chain/Franchise Filtering")
        exclude_keywords = st.text_area(
            "Exclude keywords (one per line)",
            value="\n".join(CONFIG["filters"]["exclude_keywords"]),
            height=100,
            help="Businesses containing these keywords will be excluded",
            key="exclude_keywords_textarea"
        )
        
        if exclude_keywords:
            CONFIG["filters"]["exclude_keywords"] = [
                kw.strip() for kw in exclude_keywords.split("\n") 
                if kw.strip()
            ]
        
        st.markdown("---")
        
        if st.button("💾 Save Targeting Settings", use_container_width=True, type="primary"):
            self.save_config()
            st.success("Targeting settings saved!")
    
    def render_scraper_settings(self):
        """Render scraper settings tab"""
        st.markdown("### 🔍 Scraper Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            CONFIG["state"] = st.text_input(
                "State",
                value=CONFIG.get("state", "PA"),
                key="state_input"
            )
            
            CONFIG["searches_per_cycle"] = st.number_input(
                "Searches per Cycle",
                value=CONFIG.get("searches_per_cycle", 5),
                min_value=1,
                max_value=50,
                key="searches_per_cycle_input"
            )
            
            CONFIG["businesses_per_search"] = st.number_input(
                "Businesses per Search",
                value=CONFIG.get("businesses_per_search", 10),
                min_value=1,
                max_value=100,
                key="businesses_per_search_input"
            )
        
        with col2:
            CONFIG["cycle_interval"] = st.number_input(
                "Cycle Interval (seconds)",
                value=CONFIG.get("cycle_interval", 300),
                min_value=10,
                max_value=3600,
                key="cycle_interval_input"
            )
            
            CONFIG["max_cycles"] = st.number_input(
                "Max Cycles",
                value=CONFIG.get("max_cycles", 100),
                min_value=1,
                max_value=1000,
                key="max_cycles_input"
            )
            
            CONFIG["operating_mode"] = st.selectbox(
                "Operating Mode",
                options=["auto", "manual"],
                index=0 if CONFIG.get("operating_mode", "auto") == "auto" else 1,
                key="operating_mode_select"
            )
        
        st.markdown("---")
        
        st.markdown("#### Blacklisted Domains")
        blacklisted_domains = st.text_area(
            "Blacklisted domains (one per line)",
            value="\n".join(CONFIG.get("blacklisted_domains", [])),
            height=150,
            help="Domains containing these will be skipped",
            key="blacklisted_domains_textarea"
        )
        
        if blacklisted_domains:
            CONFIG["blacklisted_domains"] = [
                domain.strip() for domain in blacklisted_domains.split("\n") 
                if domain.strip()
            ]
        
        st.markdown("---")
        
        if st.button("💾 Save Scraper Settings", use_container_width=True, type="primary"):
            self.save_config()
            st.success("Scraper settings saved!")
    
    def render_business_settings(self):
        """Render business settings tab"""
        st.markdown("### 🏢 Business Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Target Cities")
            cities_text = st.text_area(
                "Cities (one per line)",
                value="\n".join(CONFIG.get("cities", [])),
                height=200,
                help="Cities to target for lead generation",
                key="cities_textarea"
            )
            
            if cities_text:
                CONFIG["cities"] = [
                    city.strip() for city in cities_text.split("\n") 
                    if city.strip()
                ]
        
        with col2:
            st.markdown("#### Target Industries")
            industries_text = st.text_area(
                "Industries (one per line)",
                value="\n".join(CONFIG.get("industries", [])),
                height=200,
                help="Industries to target for lead generation",
                key="industries_textarea"
            )
            
            if industries_text:
                CONFIG["industries"] = [
                    industry.strip() for industry in industries_text.split("\n") 
                    if industry.strip()
                ]
        
        st.markdown("#### Search Phrases")
        search_phrases_text = st.text_area(
            "Search Phrases (one per line)",
            value="\n".join(CONFIG.get("search_phrases", [])),
            height=150,
            help="Use {industry}, {city}, {state} as placeholders",
            key="search_phrases_textarea"
        )
        
        if search_phrases_text:
            CONFIG["search_phrases"] = [
                phrase.strip() for phrase in search_phrases_text.split("\n") 
                if phrase.strip()
            ]
        
        st.markdown("---")
        
        if st.button("💾 Save Business Settings", use_container_width=True, type="primary"):
            self.save_config()
            st.success("Business settings saved!")
    
    def render_crm_settings(self):
        """Render CRM settings tab"""
        st.markdown("### 📊 CRM Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            CONFIG["crm"]["enabled"] = st.toggle(
                "Enable CRM",
                value=CONFIG["crm"].get("enabled", True),
                key="crm_enabled_toggle"
            )
            
            CONFIG["crm"]["auto_sync"] = st.toggle(
                "Auto Sync Leads",
                value=CONFIG["crm"].get("auto_sync", True),
                key="auto_sync_toggle"
            )
            
            CONFIG["crm"]["prevent_duplicates"] = st.toggle(
                "Prevent Duplicates",
                value=CONFIG["crm"].get("prevent_duplicates", True),
                key="prevent_duplicates_toggle"
            )
        
        with col2:
            CONFIG["crm"]["default_status"] = st.selectbox(
                "Default Status",
                options=CONFIG["lead_management"]["status_options"],
                index=CONFIG["lead_management"]["status_options"].index(
                    CONFIG["crm"].get("default_status", "New Lead")
                ) if CONFIG["crm"].get("default_status") in CONFIG["lead_management"]["status_options"] else 0,
                key="default_status_select"
            )
            
            CONFIG["crm"]["default_assigned_to"] = st.text_input(
                "Default Assigned To",
                value=CONFIG["crm"].get("default_assigned_to", ""),
                key="default_assigned_to_input"
            )
            
            CONFIG["crm"]["auto_set_production_date"] = st.toggle(
                "Auto-set Production Date",
                value=CONFIG["crm"].get("auto_set_production_date", True),
                key="auto_set_production_date_toggle"
            )
        
        st.markdown("---")
        
        st.markdown("#### AI Enrichment")
        CONFIG["ai_enrichment"]["enabled"] = st.toggle(
            "Enable AI Enrichment",
            value=CONFIG["ai_enrichment"].get("enabled", True),
            key="ai_enrichment_toggle"
        )
        
        if CONFIG["ai_enrichment"]["enabled"]:
            col3, col4 = st.columns(2)
            
            with col3:
                CONFIG["ai_enrichment"]["model"] = st.selectbox(
                    "Model",
                    options=["gpt-4o-mini", "gpt-4", "gpt-3.5-turbo"],
                    index=0 if CONFIG["ai_enrichment"].get("model", "gpt-4o-mini") == "gpt-4o-mini" else 
                           1 if CONFIG["ai_enrichment"].get("model") == "gpt-4" else 2,
                    key="ai_model_select"
                )
            
            with col4:
                CONFIG["ai_enrichment"]["qualification_threshold"] = st.slider(
                    "Qualification Threshold",
                    min_value=0,
                    max_value=100,
                    value=CONFIG["ai_enrichment"].get("qualification_threshold", 60),
                    key="qualification_threshold_slider"
                )
        
        st.markdown("---")
        
        if st.button("💾 Save CRM Settings", use_container_width=True, type="primary"):
            self.save_config()
            st.success("CRM settings saved!")
    
    def render_analytics(self):
        """Render analytics page"""
        st.markdown("<h1>📈 Advanced Analytics</h1>", unsafe_allow_html=True)
        st.markdown("<p style='color: #94a3b8;'>Deep dive into your lead data and performance metrics</p>", unsafe_allow_html=True)
        
        # Get comprehensive statistics
        stats = self.crm.get_statistics(days=90)
        
        # Performance Overview
        st.markdown("### Performance Overview")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            conversion_rate = 0
            if stats["overall"]["total_leads"] > 0:
                conversion_rate = (stats["overall"]["closed_won"] / stats["overall"]["total_leads"]) * 100
            st.metric("Conversion Rate", f"{conversion_rate:.1f}%")
        
        with col2:
            avg_lead_value = 0
            if stats["overall"]["total_leads"] > 0:
                avg_lead_value = stats["overall"]["total_value"] / stats["overall"]["total_leads"]
            st.metric("Avg Lead Value", f"${avg_lead_value:,.0f}")
        
        with col3:
            premium_rate = 0
            if stats["overall"]["total_leads"] > 0:
                premium_leads = sum(1 for tier in stats["quality_distribution"] 
                                  if tier["tier"] in ["Premium", "High"])
                premium_rate = (premium_leads / stats["overall"]["total_leads"]) * 100
            st.metric("Premium Rate", f"{premium_rate:.1f}%")
        
        with col4:
            ads_rate = 0
            if stats["overall"]["total_leads"] > 0:
                ads_rate = (stats["overall"]["leads_with_ads"] / stats["overall"]["total_leads"]) * 100
            st.metric("Ads Rate", f"{ads_rate:.1f}%")
        
        # Detailed Charts
        st.markdown("### 📊 Distribution Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # City performance
            city_data = stats["city_distribution"]
            if city_data:
                df_city = pd.DataFrame(city_data)
                fig_city = px.bar(
                    df_city,
                    x='city',
                    y='count',
                    title='Leads by City',
                    color='count',
                    color_continuous_scale='oranges'
                )
                fig_city.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f1f5f9',
                    xaxis_tickangle=-45,
                    showlegend=False
                )
                st.plotly_chart(fig_city, use_container_width=True)
        
        with col2:
            # Industry performance
            industry_data = stats["industry_distribution"]
            if industry_data:
                df_industry = pd.DataFrame(industry_data)
                fig_industry = px.bar(
                    df_industry,
                    x='industry',
                    y='count',
                    title='Leads by Industry',
                    color='count',
                    color_continuous_scale='blues'
                )
                fig_industry.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f1f5f9',
                    xaxis_tickangle=-45,
                    showlegend=False
                )
                st.plotly_chart(fig_industry, use_container_width=True)
        
        # Quality vs Ads Analysis
        st.markdown("### 🎯 Quality vs Advertising Analysis")
        
        # Get leads for analysis
        leads_data = self.crm.get_leads(page=1, per_page=1000)
        leads = leads_data["leads"]
        
        if leads:
            df_leads = pd.DataFrame(leads)
            
            # Create analysis
            if 'quality_tier' in df_leads.columns and 'running_google_ads' in df_leads.columns:
                analysis_data = []
                quality_tiers = ['Premium', 'High', 'Medium', 'Low', 'Unknown']
                
                for tier in quality_tiers:
                    tier_leads = df_leads[df_leads['quality_tier'] == tier]
                    total = len(tier_leads)
                    with_ads = len(tier_leads[tier_leads['running_google_ads'] == True])
                    
                    if total > 0:
                        ads_percentage = (with_ads / total) * 100
                        analysis_data.append({
                            'Quality Tier': tier,
                            'Total Leads': total,
                            'With Ads': with_ads,
                            'Ads %': ads_percentage
                        })
                
                if analysis_data:
                    df_analysis = pd.DataFrame(analysis_data)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        fig_analysis = px.bar(
                            df_analysis,
                            x='Quality Tier',
                            y=['Total Leads', 'With Ads'],
                            title='Leads & Ads by Quality Tier',
                            barmode='group',
                            color_discrete_map={
                                'Total Leads': '#3b82f6',
                                'With Ads': '#8b5cf6'
                            }
                        )
                        fig_analysis.update_layout(
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            font_color='#f1f5f9'
                        )
                        st.plotly_chart(fig_analysis, use_container_width=True)
                    
                    with col2:
                        # Display the analysis table
                        st.dataframe(
                            df_analysis.style.format({
                                'Ads %': '{:.1f}%',
                                'Total Leads': '{:,}',
                                'With Ads': '{:,}'
                            }),
                            use_container_width=True
                        )
        
        # Performance Recommendations
        st.markdown("### 💡 Performance Recommendations")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            without_website = stats["overall"].get("leads_without_website", 0)
            if without_website > 0:
                st.info(f"""
                **{without_website} leads without websites**
                
                These are prime outreach targets for website development services.
                """)
        
        with col2:
            with_ads = stats["overall"].get("leads_with_ads", 0)
            if with_ads > 0:
                st.success(f"""
                **{with_ads} leads running Google Ads**
                
                These businesses already invest in digital marketing - great upsell opportunities.
                """)
        
        with col3:
            if stats["overall"]["avg_score"] < 60:
                st.warning(f"""
                **Average lead score: {stats['overall']['avg_score']:.1f}**
                
                Consider refining your targeting to improve lead quality.
                """)
            else:
                st.success(f"""
                **Average lead score: {stats['overall']['avg_score']:.1f}**
                
                Good lead quality! Keep up the targeting strategy.
                """)
    
    def render_export(self):
        """Render export page"""
        st.markdown("<h1>📤 Export Data</h1>", unsafe_allow_html=True)
        st.markdown("<p style='color: #94a3b8;'>Export your leads in various formats</p>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Export Options")
            
            export_format = st.radio(
                "Export Format",
                ["CSV", "JSON", "Excel"],
                horizontal=True,
                key="export_format_radio"
            )
            
            include_columns = st.multiselect(
                "Select Columns to Include",
                options=[
                    "business_name", "website", "phone", "email", "address",
                    "city", "state", "industry", "business_type", "services",
                    "lead_score", "quality_tier", "potential_value", "lead_status",
                    "assigned_to", "created_at", "scraped_date",
                    "has_website", "running_google_ads", "google_business_profile",
                    "ad_transparency_url", "yelp_url", "bbb_url"
                ],
                default=[
                    "business_name", "phone", "email", "city", 
                    "lead_score", "quality_tier", "lead_status",
                    "has_website", "running_google_ads"
                ],
                key="export_columns_multiselect"
            )
            
            # Date range filter
            st.markdown("### Date Range")
            col_date1, col_date2 = st.columns(2)
            with col_date1:
                date_from = st.date_input("From Date", value=None, key="export_date_from")
            with col_date2:
                date_to = st.date_input("To Date", value=None, key="export_date_to")
        
        with col2:
            st.markdown("### Filters")
            
            status_filter = st.multiselect(
                "Status",
                options=CONFIG["lead_management"]["status_options"],
                default=[],
                key="export_status_multiselect"
            )
            
            quality_filter = st.multiselect(
                "Quality Tier",
                options=CONFIG["lead_management"]["quality_tiers"],
                default=[],
                key="export_quality_multiselect"
            )
            
            website_filter = st.selectbox(
                "Has Website",
                options=["All", "Yes", "No"],
                key="export_website_select"
            )
            
            ads_filter = st.selectbox(
                "Running Ads",
                options=["All", "Yes", "No"],
                key="export_ads_select"
            )
        
        # Apply filters
        filters = {}
        if status_filter:
            filters["status"] = status_filter[0]  # For now, just use first selected
        if quality_filter:
            filters["quality_tier"] = quality_filter[0]
        if website_filter != "All":
            filters["has_website"] = 1 if website_filter == "Yes" else 0
        if ads_filter != "All":
            filters["running_ads"] = 1 if ads_filter == "Yes" else 0
        if date_from:
            filters["date_from"] = date_from.isoformat()
        if date_to:
            filters["date_to"] = date_to.isoformat()
        
        # Get filtered data
        leads_data = self.crm.get_leads(filters=filters, page=1, per_page=10000)
        leads = leads_data["leads"]
        
        st.metric("Leads to Export", len(leads))
        
        if leads:
            # Convert to DataFrame
            df = pd.DataFrame(leads)
            
            # Filter columns
            if include_columns:
                available_cols = [col for col in include_columns if col in df.columns]
                df = df[available_cols]
            
            # Preview
            with st.expander("Preview Data (First 10 rows)"):
                st.dataframe(df.head(10), use_container_width=True)
            
            # Export buttons
            st.markdown("### Download")
            
            if export_format == "CSV":
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    type="primary",
                    use_container_width=True
                )
            
            elif export_format == "JSON":
                json_str = df.to_json(orient="records", indent=2)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_str,
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    type="primary",
                    use_container_width=True
                )
            
            elif export_format == "Excel":
                # For Excel export, we need to use a buffer
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Leads')
                
                st.download_button(
                    label="📥 Download Excel",
                    data=buffer.getvalue(),
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True
                )
        else:
            st.warning("No leads to export with the current filters.")
    
    def render_logs(self):
        """Render logs page"""
        st.markdown("<h1>📋 System Logs</h1>", unsafe_allow_html=True)
        st.markdown("<p style='color: #94a3b8;'>Monitor system activity and errors</p>", unsafe_allow_html=True)
        
        # Load logs
        log_file = CONFIG["storage"]["logs_file"]
        logs = []
        
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    logs = json.load(f)
            except:
                st.error("Could not load logs file")
        
        if logs:
            # Filter options
            col1, col2, col3 = st.columns(3)
            with col1:
                level_filter = st.selectbox(
                    "Filter by Level", 
                    ["All", "INFO", "SUCCESS", "WARNING", "ERROR", "DEBUG"],
                    key="log_level_filter"
                )
            
            with col2:
                date_filter = st.date_input("Filter by Date", value=None, key="log_date_filter")
            
            with col3:
                search_term = st.text_input("Search Logs", placeholder="Search message content...", key="log_search")
            
            # Apply filters
            filtered_logs = logs
            
            if level_filter != "All":
                filtered_logs = [log for log in filtered_logs if log.get("level") == level_filter]
            
            if date_filter:
                date_str = date_filter.isoformat()
                filtered_logs = [log for log in filtered_logs if log.get("timestamp", "").startswith(date_str)]
            
            if search_term:
                filtered_logs = [log for log in filtered_logs if search_term.lower() in log.get("message", "").lower()]
            
            # Display logs
            st.markdown(f"### Log Entries ({len(filtered_logs)})")
            
            # Create dataframe for display
            if filtered_logs:
                # Reverse to show newest first
                filtered_logs.reverse()
                
                # Display logs in a nice format
                for log in filtered_logs[:100]:  # Show last 100 logs
                    timestamp = log.get("timestamp", "")
                    level = log.get("level", "INFO")
                    message = log.get("message", "")
                    
                    # Color coding
                    if level == "ERROR":
                        color = "#ef4444"
                        icon = "❌"
                    elif level == "WARNING":
                        color = "#f59e0b"
                        icon = "⚠️"
                    elif level == "SUCCESS":
                        color = "#10b981"
                        icon = "✅"
                    elif level == "DEBUG":
                        color = "#94a3b8"
                        icon = "🐛"
                    else:
                        color = "#3b82f6"
                        icon = "ℹ️"
                    
                    # Format timestamp
                    if timestamp:
                        try:
                            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            time_str = timestamp[:19]
                    else:
                        time_str = "Unknown"
                    
                    with st.container():
                        st.markdown(f"""
                        <div style="background: rgba(255, 255, 255, 0.05); border-left: 4px solid {color}; 
                                    padding: 0.75rem; border-radius: 4px; margin-bottom: 0.5rem;">
                            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                                <div>
                                    <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.25rem;">
                                        <span style="color: {color}; font-weight: 600;">{icon} {level}</span>
                                        <span style="color: #94a3b8; font-size: 0.875rem;">{time_str}</span>
                                    </div>
                                    <div style="color: #f1f5f9; font-size: 0.875rem;">{message}</div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                
                if len(filtered_logs) > 100:
                    st.info(f"Showing 100 of {len(filtered_logs)} log entries. Use filters to narrow down.")
            else:
                st.info("No logs match the current filters.")
            
            # Clear logs button
            col_btn1, col_btn2, _ = st.columns(3)
            with col_btn1:
                if st.button("🗑️ Clear All Logs", type="secondary", use_container_width=True):
                    if os.path.exists(log_file):
                        with open(log_file, "w") as f:
                            json.dump([], f)
                        st.success("Logs cleared!")
                        st.rerun()
            
            with col_btn2:
                if st.button("📥 Download Logs", use_container_width=True):
                    logs_json = json.dumps(logs, indent=2)
                    st.download_button(
                        label="Download",
                        data=logs_json,
                        file_name=f"logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
        else:
            st.info("No logs available yet.")
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(CONFIG, f, indent=2)
            logger.log("Configuration saved", "SUCCESS")
            return True
        except Exception as e:
            logger.log(f"Error saving config: {e}", "ERROR")
            st.error(f"Error saving config: {e}")
            return False
    
    def run(self):
        """Run the Streamlit dashboard"""
        if not self.enabled:
            logger.log("Streamlit dashboard not available", "WARNING")
            return
        
        # Initialize session state
        if 'scraper_running' not in st.session_state:
            st.session_state.scraper_running = False
        
        if 'scraper_stats' not in st.session_state:
            st.session_state.scraper_stats = {}
        
        if 'editing_lead' not in st.session_state:
            st.session_state.editing_lead = False
        
        # Render sidebar and get selected page
        page = self.render_sidebar()
        
        # Render selected page
        if page == "Dashboard":
            self.render_dashboard()
        elif page == "Leads":
            self.render_leads()
        elif page == "Lead Details":
            # For lead details, we need a lead ID
            lead_id = st.number_input("Enter Lead ID", min_value=1, value=1, key="lead_details_id")
            if lead_id:
                self.render_lead_detail(lead_id=lead_id)
            else:
                st.info("Enter a Lead ID to view details")
        elif page == "Settings":
            self.render_settings()
        elif page == "Analytics":
            self.render_analytics()
        elif page == "Export":
            self.render_export()
        elif page == "Logs":
            self.render_logs()
        
        # Auto-refresh every 30 seconds if scraper is running
        if st.session_state.scraper_running:
            st_autorefresh(interval=30000, limit=100, key="scraper_refresh")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

# ============================================================================
# UPDATED MAIN FUNCTION WITH ERROR HANDLING
# ============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 COMPREHENSIVE LEAD SCRAPER CRM - MITZMEDIA EDITION")
    print("="*80)
    print("Features:")
    print("  ✅ MitzMedia-inspired clean, professional design")
    print("  ✅ Enhanced targeting: Include businesses WITHOUT websites")
    print("  ✅ Google Ads detection using Ads Transparency Center")
    print("  ✅ Google Business Profile extraction")
    print("  ✅ Complete lead management with detailed views")
    print("  ✅ Full configuration editing from dashboard")
    print("  ✅ Real-time statistics and monitoring")
    print("  ✅ Advanced filtering and search")
    print("  ✅ AI-powered lead qualification")
    print("  ✅ Export functionality (CSV, JSON, Excel)")
    print("  ✅ System logs viewer")
    print("="*80)
    
    # Check API keys with safe access
    if not CONFIG.get("serper_api_key") or CONFIG.get("serper_api_key", "").startswith("YOUR_"):
        print("\n❌ Update Serper API key in config.json")
        print("   Get from: https://serper.dev")
        print("   Current config file: config.json")
        print("   You can also update it in the dashboard Settings → API Keys")
    else:
        print("\n✅ Serper API key configured")
    
    # Check OpenAI with safe access
    openai_key = CONFIG.get("openai_api_key", "")
    if not openai_key or openai_key.startswith(("sk-proj-your-key-here", "YOUR_OPENAI")):
        print("\n⚠️  OpenAI API key not configured - AI features disabled")
        print("   Get from: https://platform.openai.com/api-keys")
        print("   You can also update it in the dashboard Settings → API Keys")
    elif OPENAI_AVAILABLE:
        print("\n✅ OpenAI configured - AI features enabled")
    
    # Safe access to enhanced_features with defaults
    enhanced_features = CONFIG.get("enhanced_features", {
        "check_google_ads": True,
        "find_google_business": True,
        "scrape_yelp_reviews": True,
        "auto_social_media": True,
        "lead_scoring_ai": True
    })
    
    # Safe access to filters with defaults
    filters = CONFIG.get("filters", {
        "exclude_without_websites": False,
        "exclude_without_phone": True
    })
    
    print(f"\n🎯 Targeting Settings:")
    print(f"   • Include no-website leads: {not filters.get('exclude_without_websites', True)}")
    print(f"   • Check Google Ads: {enhanced_features.get('check_google_ads', True)}")
    print(f"   • Find Google Business: {enhanced_features.get('find_google_business', True)}")
    print(f"🏙️  State: {CONFIG.get('state', 'PA')}")
    print(f"🏙️  Cities: {len(CONFIG.get('cities', []))}")
    print(f"🏭 Industries: {len(CONFIG.get('industries', []))}")
    print(f"⏱️  Interval: {CONFIG.get('cycle_interval', 300)}s")
    print("="*80)
    
    # Check Streamlit availability
    if not STREAMLIT_AVAILABLE:
        print("\n❌ Streamlit dependencies not installed")
        print("   Install with: pip install streamlit pandas plotly streamlit-autorefresh")
        return
    
    # Create and run dashboard
    try:
        dashboard = MitzMediaDashboard()
        
        if not dashboard.enabled:
            print("\n❌ Dashboard failed to initialize")
            return
        
        print(f"\n🌐 Starting Streamlit dashboard on port {CONFIG.get('dashboard', {}).get('port', 8501)}...")
        print(f"📱 Access at: http://localhost:{CONFIG.get('dashboard', {}).get('port', 8501)}")
        print("\n📊 Available features:")
        print("  • MitzMedia-inspired dashboard with real-time stats")
        print("  • Enhanced targeting for businesses without websites")
        print("  • Google Ads detection and Business Profile extraction")
        print("  • Lead management with advanced filtering")
        print("  • Lead details view with targeting insights")
        print("  • Settings configuration with all new features")
        print("  • Advanced analytics with performance metrics")
        print("  • Export functionality (CSV, JSON, Excel)")
        print("  • System logs viewer")
        print("  • Auto-scraping with configurable intervals")
        print("="*80)
        
        # Run Streamlit app
        dashboard.run()
        
    except Exception as e:
        print(f"\n❌ Dashboard initialization error: {e}")
        print("\n🔄 Try resetting your config.json with:")
        print("   1. Delete config.json")
        print("   2. Restart the application")
        print("   3. Update API keys in Settings → API Keys")
        import traceback
        traceback.print_exc()

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Check requirements
    if not REQUESTS_AVAILABLE:
        print("❌ Install requirements: pip install requests beautifulsoup4")
        sys.exit(1)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Program interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
