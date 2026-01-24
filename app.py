#!/usr/bin/env python3
"""
🚀 PRODUCTION LEAD SCRAPER CRM - FULLY WORKING
MitzMedia-inspired with all features, no errors
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
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    print("⚠️  Streamlit not installed. Install with: pip install streamlit pandas plotly streamlit-autorefresh")

# ============================================================================
# CONFIGURATION - COMPLETE WITH ALL SECTIONS
# ============================================================================

# DEFAULT CONFIG WITH EVERYTHING
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
        "priority_options": ["Immediate", "High", "Medium", "Low"],
        "quality_tiers": ["Premium", "High", "Medium", "Low", "Unknown"]
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
    
    # FIXED: All filters present
    "filters": {
        "exclude_chains": True,
        "exclude_without_websites": False,  # CHANGED: Default to False to get listings without websites
        "exclude_without_phone": True,
        "min_rating": 3.0,
        "min_reviews": 1,
        "exclude_keywords": ["franchise", "national", "corporate", "chain"]
    },
    
    # FIXED: Enhanced features section added
    "enhanced_features": {
        "check_google_ads": True,
        "find_google_business": True,
        "scrape_yelp_reviews": True,
        "auto_social_media": True,
        "lead_scoring_ai": True
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
    """Load configuration with automatic fixes for missing sections"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
            print("✅ Loaded configuration")
        except Exception as e:
            print(f"⚠️  Config error: {e}, using defaults")
            config = DEFAULT_CONFIG.copy()
    else:
        config = DEFAULT_CONFIG.copy()
        print("📝 Created new config.json")
    
    # Ensure all sections exist
    def deep_update(target, source):
        for key, value in source.items():
            if key not in target:
                target[key] = value
            elif isinstance(value, dict) and isinstance(target[key], dict):
                deep_update(target[key], value)
    
    deep_update(config, DEFAULT_CONFIG)
    
    # Save fixed config
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    
    return config

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
# DATABASE (SQLite CRM) - FIXED WITH ALL COLUMNS
# ============================================================================

class CRM_Database:
    """SQLite database for local CRM - FIXED VERSION"""
    
    def __init__(self):
        self.db_file = CONFIG["crm"]["database"]
        self.conn = None
        self.cursor = None
        self.setup_database()
    
    def setup_database(self):
        """Initialize database with tables - ALL COLUMNS INCLUDED"""
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            
            # Check if table exists
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='leads'")
            table_exists = self.cursor.fetchone()
            
            if table_exists:
                # Check existing columns
                self.cursor.execute("PRAGMA table_info(leads)")
                existing_columns = {row[1] for row in self.cursor.fetchall()}
                
                # Define all required columns
                required_columns = {
                    'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                    'fingerprint': 'TEXT UNIQUE',
                    'business_name': 'TEXT NOT NULL',
                    'website': 'TEXT',
                    'phone': 'TEXT',
                    'email': 'TEXT',
                    'address': 'TEXT',
                    'city': 'TEXT',
                    'state': 'TEXT',
                    'industry': 'TEXT',
                    'business_type': 'TEXT',
                    'services': 'TEXT',
                    'description': 'TEXT',
                    'social_media': 'TEXT',
                    'google_business_profile': 'TEXT',
                    'running_google_ads': 'BOOLEAN DEFAULT 0',
                    'ad_transparency_url': 'TEXT',
                    'lead_score': 'INTEGER DEFAULT 0',
                    'quality_tier': 'TEXT',
                    'potential_value': 'INTEGER DEFAULT 0',
                    'outreach_priority': 'TEXT',
                    'lead_status': 'TEXT DEFAULT "New Lead"',
                    'assigned_to': 'TEXT',
                    'lead_production_date': 'DATE',
                    'meeting_type': 'TEXT',
                    'meeting_date': 'DATETIME',
                    'meeting_outcome': 'TEXT',
                    'follow_up_date': 'DATE',
                    'notes': 'TEXT',
                    'ai_notes': 'TEXT',
                    'source': 'TEXT DEFAULT "Web Scraper"',
                    'scraped_date': 'DATETIME',
                    'last_updated': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
                    'created_at': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
                    'is_archived': 'BOOLEAN DEFAULT 0',
                    'archive_date': 'DATETIME',
                    'yelp_url': 'TEXT',
                    'bbb_url': 'TEXT',
                    'has_website': 'BOOLEAN DEFAULT 1'
                }
                
                # Add missing columns
                for col_name, col_type in required_columns.items():
                    if col_name not in existing_columns:
                        try:
                            self.cursor.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}")
                            logger.log(f"Added column: {col_name}", "INFO")
                        except Exception as e:
                            logger.log(f"Could not add column {col_name}: {e}", "WARNING")
            else:
                # Create table with all columns
                self.cursor.execute('''
                    CREATE TABLE leads (
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
                        google_business_profile TEXT,
                        running_google_ads BOOLEAN DEFAULT 0,
                        ad_transparency_url TEXT,
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
                        yelp_url TEXT,
                        bbb_url TEXT,
                        has_website BOOLEAN DEFAULT 1
                    )
                ''')
            
            # Create indexes
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_fingerprint ON leads(fingerprint)',
                'CREATE INDEX IF NOT EXISTS idx_lead_status ON leads(lead_status)',
                'CREATE INDEX IF NOT EXISTS idx_quality_tier ON leads(quality_tier)',
                'CREATE INDEX IF NOT EXISTS idx_city ON leads(city)',
                'CREATE INDEX IF NOT EXISTS idx_created_at ON leads(created_at)',
                'CREATE INDEX IF NOT EXISTS idx_has_website ON leads(has_website)',
                'CREATE INDEX IF NOT EXISTS idx_running_ads ON leads(running_google_ads)'
            ]
            
            for index_sql in indexes:
                try:
                    self.cursor.execute(index_sql)
                except:
                    pass
            
            # Activities table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS activities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER,
                    activity_type TEXT,
                    activity_details TEXT,
                    performed_by TEXT,
                    performed_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
                    leads_without_website INTEGER DEFAULT 0,
                    leads_with_ads INTEGER DEFAULT 0
                )
            ''')
            
            self.conn.commit()
            logger.log("✅ Database initialized", "SUCCESS")
            
        except Exception as e:
            logger.log(f"❌ Database error: {e}", "ERROR")
    
    def get_connection(self):
        """Get a new database connection"""
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def save_lead(self, lead_data):
        """Save lead to database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            fingerprint = lead_data.get("fingerprint", "")
            
            # Check for duplicate
            if CONFIG["crm"]["prevent_duplicates"] and fingerprint:
                cursor.execute("SELECT id FROM leads WHERE fingerprint = ?", (fingerprint,))
                if cursor.fetchone():
                    return {"success": False, "message": "Duplicate lead"}
            
            # Prepare data with defaults
            business_name = lead_data.get("business_name", "Unknown Business")[:200]
            website = lead_data.get("website", "")[:200]
            phone = lead_data.get("phone", "") or ""
            email = lead_data.get("email", "") or ""
            address = lead_data.get("address", "") or ""
            city = lead_data.get("city", "") or ""
            state = lead_data.get("state", CONFIG["state"])
            industry = lead_data.get("industry", "") or ""
            
            # Enhanced features
            has_website = lead_data.get("has_website", bool(website))
            google_business = lead_data.get("google_business_profile", "") or ""
            running_ads = lead_data.get("running_google_ads", False)
            ads_url = lead_data.get("ad_transparency_url", "") or ""
            yelp_url = lead_data.get("yelp_url", "") or ""
            bbb_url = lead_data.get("bbb_url", "") or ""
            
            # Services
            services = lead_data.get("services", "")
            if isinstance(services, list):
                services = ", ".join(services)
            
            # Social media
            social_media = lead_data.get("social_media", "")
            if isinstance(social_media, dict):
                social_media = json.dumps(social_media)
            
            # Quality tier
            quality_tier = lead_data.get("quality_tier", "Unknown")
            lead_score = lead_data.get("lead_score", 50)
            
            # Outreach priority
            if lead_score >= 80:
                outreach_priority = "Immediate"
            elif lead_score >= 60:
                outreach_priority = "High"
            elif lead_score >= 40:
                outreach_priority = "Medium"
            else:
                outreach_priority = "Low"
            
            # Insert lead
            cursor.execute('''
                INSERT INTO leads (
                    fingerprint, business_name, website, phone, email, address,
                    city, state, industry, business_type, services, description,
                    social_media, google_business_profile, running_google_ads,
                    ad_transparency_url, lead_score, quality_tier, potential_value,
                    outreach_priority, lead_status, assigned_to, lead_production_date,
                    follow_up_date, notes, ai_notes, source, scraped_date,
                    yelp_url, bbb_url, has_website
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fingerprint, business_name, website, phone, email, address,
                city, state, industry, lead_data.get("business_type", "LLC"),
                services[:500], lead_data.get("description", "")[:1000],
                social_media[:500], google_business, running_ads,
                ads_url, lead_score, quality_tier, lead_data.get("potential_value", 0),
                outreach_priority, CONFIG["crm"]["default_status"],
                CONFIG["crm"]["default_assigned_to"],
                datetime.now(timezone.utc).date().isoformat() if CONFIG["crm"]["auto_set_production_date"] else None,
                (datetime.now(timezone.utc) + timedelta(days=7)).date().isoformat(),
                "", lead_data.get("ai_notes", "")[:500],
                "Web Scraper", lead_data.get("scraped_date", datetime.now(timezone.utc).isoformat()),
                yelp_url, bbb_url, has_website
            ))
            
            lead_id = cursor.lastrowid
            
            # Add activity
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, "Lead Created", f"Lead scraped from {website if website else 'directory'}"))
            
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
            
            return lead_dict
            
        except Exception as e:
            logger.log(f"Get lead error: {e}", "ERROR")
            return None
        finally:
            conn.close()
    
    def get_statistics(self, days=30):
        """Get statistics for dashboard"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            stats = {}
            
            # Overall stats
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
            
            # Website distribution
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
            
            # Daily leads
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
                "website_distribution": [],
                "daily_leads": []
            }
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

# ============================================================================
# ENHANCED WEBSITE SCRAPER
# ============================================================================

class EnhancedWebsiteScraper:
    """Enhanced scraper with all features"""
    
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        ]
        
        # Initialize OpenAI
        self.openai_client = None
        if OPENAI_AVAILABLE and CONFIG.get("openai_api_key"):
            try:
                self.openai_client = openai.OpenAI(api_key=CONFIG["openai_api_key"])
            except:
                logger.log("OpenAI initialization failed", "WARNING")
    
    def scrape_website(self, url, business_name="", city=""):
        """Scrape website with enhanced features"""
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
            'bbb_url': '',
            'has_website': True
        }
        
        if not url or not url.startswith(('http://', 'https://')):
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
            
            # Extract basic info
            data.update({
                'business_name': self._extract_business_name(soup, url, business_name),
                'description': self._extract_description(soup),
                'phones': self._extract_phones(soup),
                'emails': self._extract_emails(soup),
                'address': self._extract_address(soup),
                'social_media': self._extract_social_media(soup),
                'services': self._extract_services(soup)
            })
            
            # Enhanced features
            if CONFIG["enhanced_features"]["find_google_business"]:
                data['google_business_profile'] = self._extract_google_business(soup, data['business_name'], city)
            
            if CONFIG["enhanced_features"]["check_google_ads"]:
                ads_data = self._check_google_ads(url)
                data['running_google_ads'] = ads_data['running_ads']
                data['ad_transparency_url'] = ads_data['ad_transparency_url']
            
            # Directory links
            data['yelp_url'] = self._find_yelp_page(data['business_name'], city)
            data['bbb_url'] = self._find_bbb_page(data['business_name'], city)
            
            return data
            
        except Exception as e:
            logger.log(f"Scrape error for {url}: {e}", "WARNING")
            data['has_website'] = False
            return data
    
    def _extract_business_name(self, soup, url, fallback_name=""):
        """Extract business name"""
        # Try meta tags
        for meta in soup.find_all('meta'):
            if meta.get('property') in ['og:site_name', 'og:title']:
                name = meta.get('content', '')
                if name:
                    return name[:200]
        
        # Try title tag
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            suffixes = [' - Home', ' | Home', ' - Official Site', ' | Official Site']
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
        
        # Fallbacks
        if fallback_name:
            return fallback_name[:200]
        
        try:
            domain = urlparse(url).netloc
            name = domain.replace('www.', '').split('.')[0].title()
            return name[:200]
        except:
            return "Unknown Business"
    
    def _extract_description(self, soup):
        """Extract description"""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc.get('content')[:500]
        
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 50 and len(text) < 300:
                return text[:500]
        
        return ""
    
    def _extract_phones(self, soup):
        """Extract phone numbers"""
        phones = set()
        text = soup.get_text()
        
        phone_patterns = [
            r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        ]
        
        for pattern in phone_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                phone = re.sub(r'[^\d+]', '', match)
                if len(phone) >= 10:
                    phones.add(phone)
        
        # Look in tel: links
        tel_links = soup.find_all('a', href=re.compile(r'tel:'))
        for link in tel_links:
            href = link.get('href', '')
            phone_match = re.search(r'tel:([\+\d\s\-\(\)]+)', href)
            if phone_match:
                phone = re.sub(r'[^\d+]', '', phone_match.group(1))
                if len(phone) >= 10:
                    phones.add(phone)
        
        return list(phones)[:3]
    
    def _extract_emails(self, soup):
        """Extract email addresses"""
        emails = set()
        text = soup.get_text()
        
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        matches = re.findall(email_pattern, text)
        emails.update(matches)
        
        # Mailto links
        mailto_links = soup.find_all('a', href=re.compile(r'mailto:'))
        for link in mailto_links:
            href = link.get('href', '')
            email_match = re.search(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', href)
            if email_match:
                emails.add(email_match.group(1))
        
        return list(emails)[:5]
    
    def _extract_address(self, soup):
        """Extract address"""
        text = soup.get_text()
        
        address_patterns = [
            r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|Place|Pl),?\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}',
            r'\d+\s+[A-Za-z\s]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}\s+\d{5}',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text)
            if matches:
                return matches[0]
        
        # Look for address elements
        address_elements = soup.find_all(['address', 'div', 'span'], class_=re.compile(r'address|location|contact'))
        for elem in address_elements:
            text = elem.get_text(strip=True)
            if len(text) > 20 and len(text) < 200:
                if any(word in text.lower() for word in ['street', 'ave', 'road', 'rd', 'drive', 'dr']):
                    return text
        
        return ""
    
    def _extract_social_media(self, soup):
        """Extract social media links"""
        social_media = {}
        
        social_platforms = {
            'facebook': ['facebook.com', 'fb.com'],
            'instagram': ['instagram.com'],
            'linkedin': ['linkedin.com'],
            'twitter': ['twitter.com', 'x.com'],
            'youtube': ['youtube.com'],
            'tiktok': ['tiktok.com']
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
        """Extract services"""
        services = []
        
        service_keywords = [
            'installation', 'repair', 'maintenance', 'service', 'contractor',
            'construction', 'remodeling', 'renovation', 'building', 'design',
            'cleaning', 'painting', 'electrical', 'plumbing', 'hvac', 'roofing',
            'landscaping', 'hardscaping', 'concrete', 'excavation', 'deck', 'fence'
        ]
        
        text_content = soup.get_text().lower()
        
        for keyword in service_keywords:
            if keyword in text_content:
                services.append(keyword.title())
        
        # Look for service sections
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            heading_text = heading.get_text().lower()
            if any(word in heading_text for word in ['service', 'what we do', 'our work', 'expertise']):
                next_elem = heading.find_next()
                for _ in range(10):
                    if next_elem:
                        if next_elem.name in ['ul', 'ol']:
                            for li in next_elem.find_all('li'):
                                services.append(li.get_text(strip=True)[:100])
                        elif next_elem.name == 'p':
                            text = next_elem.get_text(strip=True)
                            if len(text) < 200:
                                services.append(text[:100])
                        next_elem = next_elem.find_next_sibling()
                    else:
                        break
        
        return list(set(services))[:10]
    
    def _extract_google_business(self, soup, business_name, city):
        """Extract Google Business Profile link"""
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            if any(pattern in href for pattern in ['google.com/maps', 'g.page', 'goo.gl/maps', 'maps.app.goo.gl']):
                return a['href']
        
        for iframe in soup.find_all('iframe', src=True):
            src = iframe['src'].lower()
            if 'google.com/maps' in src:
                return src
        
        # Construct search URL
        if business_name and city:
            search_query = f"{business_name} {city} Google Business"
            encoded_query = quote(search_query)
            return f"https://www.google.com/search?q={encoded_query}"
        
        return ""
    
    def _check_google_ads(self, url):
        """Check if business is running Google Ads"""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                return {"running_ads": False, "ad_transparency_url": ""}
            
            if domain.startswith('www.'):
                domain = domain[4:]
            
            ad_transparency_url = f"https://adstransparency.google.com/?region=US&domain={domain}"
            
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            try:
                response = requests.get(ad_transparency_url, headers=headers, timeout=8)
                soup = BeautifulSoup(response.text, 'html.parser')
                text = soup.get_text().lower()
                
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
                return {
                    "running_ads": False,
                    "ad_transparency_url": ad_transparency_url
                }
                
        except Exception as e:
            logger.log(f"Google Ads check error: {e}", "WARNING")
            return {"running_ads": False, "ad_transparency_url": ""}
    
    def _find_yelp_page(self, business_name, city):
        """Find Yelp page"""
        if not business_name or not city:
            return ""
        
        try:
            search_query = f"{business_name} {city} Yelp"
            encoded_query = quote(search_query)
            return f"https://www.yelp.com/search?find_desc={encoded_query}"
        except:
            return ""
    
    def _find_bbb_page(self, business_name, city):
        """Find BBB page"""
        if not business_name or not city:
            return ""
        
        try:
            search_query = f"{business_name} {city} BBB"
            encoded_query = quote(search_query)
            return f"https://www.bbb.org/search?find_desc={encoded_query}"
        except:
            return ""

# ============================================================================
# ENHANCED LEAD SCRAPER
# ============================================================================

class EnhancedLeadScraper:
    """Main scraper with all features"""
    
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
        """Generate search queries"""
        queries = []
        state = CONFIG["state"]
        
        # Website searches
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
        
        # Directory searches (if enabled)
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
        
        random.shuffle(queries)
        return queries[:CONFIG["searches_per_cycle"] * 2]
    
    def search_serper(self, query):
        """Search using Serper API"""
        if not self.api_key:
            logger.log("No Serper API key configured", "ERROR")
            return []
        
        # Check cache
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
    
    def generate_fingerprint(self, business_name, website, phone, city):
        """Generate fingerprint for duplicate detection"""
        data = f"{business_name}_{website}_{phone}_{city}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def qualify_lead(self, lead_data):
        """Qualify lead using AI"""
        if not CONFIG["ai_enrichment"]["enabled"] or not self.scraper.openai_client:
            # Basic scoring without AI
            lead_score = 50
            
            if lead_data.get('website'):
                lead_score += 10
            if lead_data.get('phone'):
                lead_score += 15
            if lead_data.get('email'):
                lead_score += 10
            if lead_data.get('address'):
                lead_score += 5
            if lead_data.get('running_google_ads'):
                lead_score += 20
            
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
            lead_data['ai_notes'] = "Basic scoring applied"
            
            return lead_data
        
        try:
            prompt = f"""
            Analyze this business lead and provide:
            1. Lead score (0-100)
            2. Quality tier (Premium, High, Medium, Low, Unknown)
            3. Business type (LLC, Corporation, Sole Proprietorship, Partnership, Unknown)
            4. Key services
            5. AI notes with insights
            
            Lead Information:
            - Business: {lead_data.get('business_name', 'Unknown')}
            - Website: {lead_data.get('website', 'None')}
            - Has Website: {lead_data.get('has_website', True)}
            - Phone: {lead_data.get('phone', 'None')}
            - Email: {lead_data.get('email', 'None')}
            - City: {lead_data.get('city', 'Unknown')}
            - Industry: {lead_data.get('industry', 'Unknown')}
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
                    {"role": "system", "content": "You are a lead qualification expert."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=CONFIG["ai_enrichment"]["max_tokens"],
                temperature=0.3
            )
            
            ai_response = response.choices[0].message.content
            
            try:
                ai_data = json.loads(ai_response)
                
                lead_data['lead_score'] = ai_data.get('lead_score', 50)
                lead_data['quality_tier'] = ai_data.get('quality_tier', 'Unknown')
                lead_data['business_type'] = ai_data.get('business_type', 'Unknown')
                
                existing_services = lead_data.get('services', [])
                ai_services = ai_data.get('services', [])
                if isinstance(existing_services, str):
                    existing_services = [existing_services]
                if isinstance(ai_services, list):
                    all_services = list(set(existing_services + ai_services))
                    lead_data['services'] = all_services[:10]
                
                lead_data['ai_notes'] = ai_data.get('ai_notes', '')
                
            except:
                lead_data['lead_score'] = 50
                lead_data['quality_tier'] = 'Unknown'
                lead_data['ai_notes'] = 'AI analysis failed'
        
        except Exception as e:
            logger.log(f"AI qualification error: {e}", "WARNING")
            lead_data['lead_score'] = 50
            lead_data['quality_tier'] = 'Unknown'
            lead_data['ai_notes'] = f'AI error: {str(e)}'
        
        return lead_data
    
    def process_lead(self, search_result, meta_info):
        """Process a single search result"""
        url = search_result.get('link', '')
        title = search_result.get('title', '')
        snippet = search_result.get('snippet', '')
        
        # Skip blacklisted
        if url and self.is_blacklisted(url):
            return None
        
        # Extract business name
        business_name = title
        if not business_name or len(business_name) < 3:
            if snippet:
                name_match = re.search(r'^([A-Z][a-zA-Z\s&]+(?:Company|Services|Contractors|Contractor|LLC|Inc|Corp))', snippet)
                if name_match:
                    business_name = name_match.group(1)
        
        # Scrape or create directory listing
        if url and url.startswith(('http://', 'https://')):
            scraped_data = self.scraper.scrape_website(url, business_name, meta_info.get('city', ''))
        else:
            # Directory listing
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
            
            # Extract phone from snippet
            if snippet:
                phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
                phones = re.findall(phone_pattern, snippet)
                if phones:
                    scraped_data['phones'] = [re.sub(r'[^\d+]', '', phones[0])]
            
            # Extract address from snippet
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
        """Check if lead passes filters"""
        filters = CONFIG["filters"]
        
        # Exclude without website if setting is True
        if filters["exclude_without_websites"] and not lead_data.get('has_website', True):
            return False
        
        # Exclude without phone
        if filters["exclude_without_phone"] and not lead_data.get('phone'):
            return False
        
        # Check for chain keywords
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
            logger.log(f"🔍 Searching: {query}", "INFO")
            
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
                            logger.log(f"✅ Saved lead: {lead_data['business_name']}", "SUCCESS")
            
            time.sleep(random.uniform(1, 3))
        
        self.stats['cycles'] += 1
        self.stats['total_leads'] += leads_found
        self.stats['leads_without_website'] += leads_without_website
        self.stats['leads_with_ads'] += leads_with_ads
        self.stats['last_cycle'] = datetime.now().isoformat()
        
        logger.log(f"✅ Cycle completed. Found {leads_found} new leads ({leads_without_website} without websites, {leads_with_ads} with ads)", "SUCCESS")

# ============================================================================
# STREAMLIT DASHBOARD - FIXED
# ============================================================================

class StreamlitDashboard:
    """Production-ready dashboard"""
    
    def __init__(self):
        if not STREAMLIT_AVAILABLE:
            self.enabled = False
            logger.log("Streamlit not available", "WARNING")
            return
        
        try:
            self.crm = CRM_Database()
            self.scraper = None
            self.scraper_running = False
            self.scraper_thread = None
            self.enabled = True
            
            st.set_page_config(
                page_title="LeadScraper CRM | Production",
                page_icon="🚀",
                layout="wide",
                initial_sidebar_state="expanded"
            )
            
            self.setup_custom_css()
            
            logger.log("✅ Dashboard initialized", "SUCCESS")
        except Exception as e:
            self.enabled = False
            logger.log(f"Dashboard error: {e}", "ERROR")
    
    def setup_custom_css(self):
        """Setup MitzMedia styling"""
        st.markdown("""
        <style>
        :root {
            --primary: #111827;
            --primary-dark: #0f172a;
            --accent: #f59e0b;
            --accent-light: #fbbf24;
            --success: #10b981;
            --danger: #ef4444;
            --card-bg: #1e293b;
            --border: #334155;
            --text-light: #f1f5f9;
            --text-muted: #94a3b8;
        }
        
        .stApp {
            background: linear-gradient(135deg, var(--primary-dark) 0%, var(--card-bg) 100%) !important;
            color: var(--text-light) !important;
        }
        
        .mitz-card {
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1rem;
        }
        
        .stButton > button {
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-light) 100%) !important;
            color: #111827 !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
            padding: 0.75rem 1.5rem !important;
        }
        
        .badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .badge-premium { background: linear-gradient(135deg, #f59e0b, #d97706); color: white; }
        .badge-high { background: linear-gradient(135deg, #10b981, #059669); color: white; }
        .badge-medium { background: linear-gradient(135deg, #3b82f6, #2563eb); color: white; }
        .badge-low { background: linear-gradient(135deg, #6b7280, #4b5563); color: white; }
        .badge-no-website { background: linear-gradient(135deg, #ef4444, #dc2626); color: white; }
        .badge-ads { background: linear-gradient(135deg, #8b5cf6, #7c3aed); color: white; }
        
        h1, h2, h3 {
            color: var(--text-light) !important;
        }
        
        h1 {
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-light) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        </style>
        """, unsafe_allow_html=True)
    
    def run_scraper_background(self):
        """Run scraper in background"""
        try:
            self.scraper = EnhancedLeadScraper()
            self.scraper.running = True
            
            cycles = 0
            while self.scraper_running and cycles < CONFIG['max_cycles']:
                if not self.scraper.running:
                    break
                
                self.scraper.run_cycle()
                cycles += 1
                
                if 'scraper_stats' not in st.session_state:
                    st.session_state.scraper_stats = {}
                
                st.session_state.scraper_stats = {
                    'cycles': cycles,
                    'total_leads': self.scraper.stats['total_leads'],
                    'leads_without_website': self.scraper.stats['leads_without_website'],
                    'leads_with_ads': self.scraper.stats['leads_with_ads'],
                    'last_cycle': self.scraper.stats['last_cycle']
                }
                
                if self.scraper_running and cycles < CONFIG['max_cycles']:
                    time.sleep(CONFIG['cycle_interval'])
            
            self.scraper_running = False
            st.session_state.scraper_running = False
            
        except Exception as e:
            logger.log(f"Background scraper error: {e}", "ERROR")
            self.scraper_running = False
            st.session_state.scraper_running = False
    
    def start_scraper(self):
        """Start scraper"""
        if not self.scraper_running:
            self.scraper_running = True
            st.session_state.scraper_running = True
            self.scraper_thread = threading.Thread(target=self.run_scraper_background, daemon=True)
            self.scraper_thread.start()
            return True
        return False
    
    def stop_scraper(self):
        """Stop scraper"""
        self.scraper_running = False
        if self.scraper:
            self.scraper.running = False
        st.session_state.scraper_running = False
        return True
    
    def render_sidebar(self):
        """Render sidebar - SAFE CONFIG ACCESS"""
        with st.sidebar:
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <div style="font-size: 2.5rem;">🚀</div>
                <h1 style="color: #f59e0b; margin: 0;">LeadScraper CRM</h1>
                <p style="color: #94a3b8; margin: 0;">Production v6.0</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation
            page = st.radio(
                "Navigation",
                ["Dashboard", "Leads", "Settings", "Logs"],
                label_visibility="collapsed"
            )
            
            st.divider()
            
            # Scraper Control
            st.markdown("### ⚙️ Scraper Control")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("▶️ Start", use_container_width=True, type="primary"):
                    if self.start_scraper():
                        st.success("Scraper started!")
                        st.rerun()
            
            with col2:
                if st.button("⏹️ Stop", use_container_width=True, type="secondary"):
                    if self.stop_scraper():
                        st.info("Scraper stopped!")
                        st.rerun()
            
            # Status
            if 'scraper_running' not in st.session_state:
                st.session_state.scraper_running = False
            
            status_color = "#10b981" if st.session_state.scraper_running else "#ef4444"
            status_text = "Active" if st.session_state.scraper_running else "Stopped"
            
            st.markdown(f"""
            <div style="background: rgba(255, 255, 255, 0.05); padding: 0.75rem; border-radius: 8px; margin: 0.5rem 0;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: {status_color}; font-weight: 600;">{status_text}</span>
                    <span style="color: #94a3b8;">Status</span>
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
                    <div style="display: flex; justify-content: space-between;">
                        <span style="color: #94a3b8;">Total Leads</span>
                        <span style="color: #10b981; font-weight: 600;">{stats.get('total_leads', 0)}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.divider()
            
            # Quick Stats
            st.markdown("### 📈 Quick Stats")
            
            today_count = self.crm.get_today_count()
            total_leads = self.crm.get_leads()["total"]
            stats = self.crm.get_statistics()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Today", today_count)
            with col2:
                st.metric("Total", total_leads)
            
            col3, col4 = st.columns(2)
            with col3:
                without_website = stats["overall"].get("leads_without_website", 0)
                st.metric("No Website", without_website)
            with col4:
                with_ads = stats["overall"].get("leads_with_ads", 0)
                st.metric("With Ads", with_ads)
            
            st.divider()
            
            # System Info - SAFE ACCESS
            st.markdown("### 💻 System Info")
            
            # Use .get() with defaults for safe access
            enhanced_features = CONFIG.get('enhanced_features', {})
            filters = CONFIG.get('filters', {})
            
            info_items = [
                ("Database", "✅" if self.crm.conn else "❌"),
                ("No Website Scraping", "✅" if not filters.get('exclude_without_websites', True) else "❌"),
                ("Google Ads Check", "✅" if enhanced_features.get('check_google_ads', True) else "❌"),
                ("State", CONFIG.get('state', 'PA')),
                ("Cities", len(CONFIG.get('cities', []))),
                ("Industries", len(CONFIG.get('industries', [])))
            ]
            
            for label, value in info_items:
                st.markdown(f"**{label}:** {value}")
            
            return page
    
    def render_dashboard(self):
        """Render dashboard"""
        st.title("📊 Dashboard")
        
        stats = self.crm.get_statistics()
        
        # Metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        with col1:
            st.metric("Total Leads", stats["overall"]["total_leads"])
        with col2:
            st.metric("New Leads", stats["overall"]["new_leads"])
        with col3:
            st.metric("Closed Won", stats["overall"]["closed_won"])
        with col4:
            st.metric("Avg Score", f"{stats['overall']['avg_score']:.1f}")
        with col5:
            without_website = stats["overall"].get("leads_without_website", 0)
            st.metric("No Website", without_website)
        with col6:
            with_ads = stats["overall"].get("leads_with_ads", 0)
            st.metric("With Ads", with_ads)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality distribution
            quality_data = stats["quality_distribution"]
            if quality_data:
                df_quality = pd.DataFrame(quality_data)
                fig_quality = px.pie(
                    df_quality, 
                    values='count', 
                    names='tier',
                    title='Lead Quality',
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
            # Website distribution
            website_data = stats.get("website_distribution", [])
            if website_data:
                df_website = pd.DataFrame(website_data)
                fig_website = px.bar(
                    df_website,
                    x='category',
                    y='count',
                    title='Website Status',
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
                    showlegend=False
                )
                st.plotly_chart(fig_website, use_container_width=True)
        
        # Recent leads
        st.subheader("Recent Leads")
        leads_data = self.crm.get_leads(page=1, per_page=10)
        
        if leads_data["leads"]:
            for lead in leads_data["leads"][:5]:
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**{lead.get('business_name', 'Unknown')}**")
                        st.caption(f"{lead.get('city', '')} • {lead.get('industry', '')}")
                    with col2:
                        score = lead.get('lead_score', 0)
                        tier = lead.get('quality_tier', 'Unknown')
                        tier_class = f"badge-{tier.lower()}" if tier.lower() in ['premium', 'high', 'medium', 'low'] else "badge-low"
                        st.markdown(f'<span class="{tier_class}">{tier}</span>', unsafe_allow_html=True)
                    st.divider()
        else:
            st.info("No leads found")
    
    def render_leads(self):
        """Render leads page"""
        st.title("👥 Leads")
        
        # Filters
        with st.expander("🔍 Filters", expanded=True):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                search_term = st.text_input("Search")
            
            with col2:
                status_options = ["All"] + CONFIG["lead_management"]["status_options"]
                status_filter = st.selectbox("Status", status_options)
            
            with col3:
                quality_options = ["All"] + CONFIG["lead_management"]["quality_tiers"]
                quality_filter = st.selectbox("Quality", quality_options)
            
            with col4:
                city_options = ["All"] + CONFIG["cities"]
                city_filter = st.selectbox("City", city_options)
        
        # Build filters
        filters = {}
        if search_term:
            filters["search"] = search_term
        if status_filter != "All":
            filters["status"] = status_filter
        if quality_filter != "All":
            filters["quality_tier"] = quality_filter
        if city_filter != "All":
            filters["city"] = city_filter
        
        # Get leads
        leads_data = self.crm.get_leads(filters=filters, page=1, per_page=50)
        
        st.metric("Total Leads", leads_data["total"])
        
        if leads_data["leads"]:
            # Create dataframe for display
            leads = leads_data["leads"]
            display_data = []
            
            for lead in leads[:20]:  # Show first 20
                display_data.append({
                    "ID": lead.get("id"),
                    "Business": lead.get("business_name", "")[:30],
                    "Phone": lead.get("phone", ""),
                    "City": lead.get("city", ""),
                    "Score": lead.get("lead_score", 0),
                    "Quality": lead.get("quality_tier", "Unknown"),
                    "Status": lead.get("lead_status", "New Lead"),
                    "Website": "✅" if lead.get("has_website") else "❌",
                    "Ads": "✅" if lead.get("running_google_ads") else "❌"
                })
            
            if display_data:
                df = pd.DataFrame(display_data)
                st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No leads match the filters")
    
    def render_settings(self):
        """Render settings page"""
        st.title("⚙️ Settings")
        
        tab1, tab2, tab3 = st.tabs(["API Keys", "Targeting", "Scraper"])
        
        with tab1:
            st.subheader("API Configuration")
            
            # Serper API
            current_serper = CONFIG.get("serper_api_key", "")
            new_serper = st.text_input("Serper API Key", value=current_serper, type="password")
            if new_serper != current_serper:
                CONFIG["serper_api_key"] = new_serper
            
            # OpenAI API
            current_openai = CONFIG.get("openai_api_key", "")
            new_openai = st.text_input("OpenAI API Key", value=current_openai, type="password")
            if new_openai != current_openai:
                CONFIG["openai_api_key"] = new_openai
            
            if st.button("Save API Keys", type="primary"):
                try:
                    with open(CONFIG_FILE, "w") as f:
                        json.dump(CONFIG, f, indent=2)
                    st.success("API keys saved!")
                except Exception as e:
                    st.error(f"Error: {e}")
        
        with tab2:
            st.subheader("🎯 Targeting Settings")
            
            # Use safe access with .get()
            filters = CONFIG.get("filters", {})
            enhanced = CONFIG.get("enhanced_features", {})
            
            col1, col2 = st.columns(2)
            
            with col1:
                # No website scraping
                exclude_websites = filters.get("exclude_without_websites", False)
                new_setting = st.toggle(
                    "Include businesses without websites",
                    value=not exclude_websites,
                    help="Turn ON to get leads from Yelp/BBB with only phone/address"
                )
                CONFIG["filters"]["exclude_without_websites"] = not new_setting
            
            with col2:
                # Google Ads check
                check_ads = enhanced.get("check_google_ads", True)
                new_ads = st.toggle(
                    "Check Google Ads status",
                    value=check_ads,
                    help="Check if businesses run Google Ads"
                )
                CONFIG["enhanced_features"]["check_google_ads"] = new_ads
            
            # Google Business
            find_business = enhanced.get("find_google_business", True)
            new_business = st.toggle(
                "Find Google Business Profiles",
                value=find_business,
                help="Extract Google Business Profile/Maps links"
            )
            CONFIG["enhanced_features"]["find_google_business"] = new_business
            
            if st.button("Save Targeting Settings", type="primary"):
                try:
                    with open(CONFIG_FILE, "w") as f:
                        json.dump(CONFIG, f, indent=2)
                    st.success("Targeting settings saved!")
                except Exception as e:
                    st.error(f"Error: {e}")
        
        with tab3:
            st.subheader("🔍 Scraper Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                CONFIG["state"] = st.text_input("State", value=CONFIG.get("state", "PA"))
                CONFIG["searches_per_cycle"] = st.number_input(
                    "Searches per Cycle",
                    value=CONFIG.get("searches_per_cycle", 5),
                    min_value=1, max_value=20
                )
            
            with col2:
                CONFIG["cycle_interval"] = st.number_input(
                    "Cycle Interval (seconds)",
                    value=CONFIG.get("cycle_interval", 300),
                    min_value=10, max_value=3600
                )
                CONFIG["max_cycles"] = st.number_input(
                    "Max Cycles",
                    value=CONFIG.get("max_cycles", 100),
                    min_value=1, max_value=1000
                )
            
            if st.button("Save Scraper Settings", type="primary"):
                try:
                    with open(CONFIG_FILE, "w") as f:
                        json.dump(CONFIG, f, indent=2)
                    st.success("Scraper settings saved!")
                except Exception as e:
                    st.error(f"Error: {e}")
    
    def render_logs(self):
        """Render logs page"""
        st.title("📋 System Logs")
        
        log_file = CONFIG["storage"]["logs_file"]
        
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    logs = json.load(f)
                
                # Filter options
                col1, col2 = st.columns(2)
                with col1:
                    level_filter = st.selectbox("Filter Level", ["All", "INFO", "SUCCESS", "WARNING", "ERROR"])
                with col2:
                    search_term = st.text_input("Search logs")
                
                # Apply filters
                filtered_logs = logs
                
                if level_filter != "All":
                    filtered_logs = [log for log in filtered_logs if log.get("level") == level_filter]
                
                if search_term:
                    filtered_logs = [log for log in filtered_logs if search_term.lower() in log.get("message", "").lower()]
                
                # Display logs
                st.subheader(f"Logs ({len(filtered_logs)})")
                
                for log in reversed(filtered_logs[-50:]):  # Last 50
                    timestamp = log.get("timestamp", "")[:19]
                    level = log.get("level", "INFO")
                    message = log.get("message", "")
                    
                    if level == "ERROR":
                        color = "#ef4444"
                        icon = "❌"
                    elif level == "WARNING":
                        color = "#f59e0b"
                        icon = "⚠️"
                    elif level == "SUCCESS":
                        color = "#10b981"
                        icon = "✅"
                    else:
                        color = "#3b82f6"
                        icon = "ℹ️"
                    
                    st.markdown(f"""
                    <div style='border-left: 3px solid {color}; padding-left: 10px; margin: 5px 0;'>
                        <small style='color: #94a3b8'>{timestamp}</small><br/>
                        <strong style='color: {color}'>{icon} {level}:</strong> {message}
                    </div>
                    """, unsafe_allow_html=True)
                
            except:
                st.error("Could not load logs")
        else:
            st.info("No logs available")
    
    def run(self):
        """Run dashboard"""
        if not self.enabled:
            st.error("Dashboard not available")
            return
        
        # Initialize session state
        if 'scraper_running' not in st.session_state:
            st.session_state.scraper_running = False
        if 'scraper_stats' not in st.session_state:
            st.session_state.scraper_stats = {}
        
        # Render
        page = self.render_sidebar()
        
        if page == "Dashboard":
            self.render_dashboard()
        elif page == "Leads":
            self.render_leads()
        elif page == "Settings":
            self.render_settings()
        elif page == "Logs":
            self.render_logs()
        
        # Auto-refresh
        if st.session_state.scraper_running:
            st_autorefresh(interval=30000, key="scraper_refresh")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 PRODUCTION LEAD SCRAPER CRM - FULLY WORKING")
    print("="*80)
    print("Features:")
    print("  ✅ MitzMedia-inspired professional design")
    print("  ✅ Enhanced targeting: Businesses WITHOUT websites")
    print("  ✅ Google Ads detection (Ads Transparency Center)")
    print("  ✅ Google Business Profile extraction")
    print("  ✅ Complete lead management CRM")
    print("  ✅ Real-time statistics and monitoring")
    print("  ✅ AI-powered lead qualification")
    print("  ✅ All database columns included")
    print("  ✅ No more KeyError or missing column errors")
    print("="*80)
    
    # Check API status
    serper_key = CONFIG.get("serper_api_key", "")
    openai_key = CONFIG.get("openai_api_key", "")
    
    if not serper_key:
        print("\n❌ Serper API key not configured")
        print("   Get from: https://serper.dev")
        print("   Update in Settings → API Keys")
    else:
        print("\n✅ Serper API: Configured")
    
    if not openai_key:
        print("⚠️  OpenAI API: Not configured (AI features disabled)")
    else:
        print("✅ OpenAI API: Configured")
    
    # Show targeting settings
    filters = CONFIG.get("filters", {})
    enhanced = CONFIG.get("enhanced_features", {})
    
    print(f"\n🎯 Targeting Settings:")
    print(f"   • Include no-website leads: {not filters.get('exclude_without_websites', True)}")
    print(f"   • Check Google Ads: {enhanced.get('check_google_ads', True)}")
    print(f"   • Find Google Business: {enhanced.get('find_google_business', True)}")
    print(f"🏙️  State: {CONFIG.get('state', 'PA')}")
    print(f"🏙️  Cities: {len(CONFIG.get('cities', []))}")
    print(f"🏭 Industries: {len(CONFIG.get('industries', []))}")
    print(f"⏱️  Interval: {CONFIG.get('cycle_interval', 300)}s")
    print("="*80)
    
    if not STREAMLIT_AVAILABLE:
        print("\n❌ Streamlit dependencies not installed")
        print("   Install with: pip install streamlit pandas plotly streamlit-autorefresh")
        return
    
    print(f"\n🌐 Dashboard: http://localhost:{CONFIG.get('dashboard', {}).get('port', 8501)}")
    print("="*80)
    
    # Run dashboard
    try:
        dashboard = StreamlitDashboard()
        dashboard.run()
    except Exception as e:
        print(f"\n❌ Dashboard error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Check requirements
    if not REQUESTS_AVAILABLE:
        print("❌ Install requirements: pip install requests beautifulsoup4")
        sys.exit(1)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
