#!/usr/bin/env python3
"""
🚀 COMPREHENSIVE LEAD SCRAPER CRM - STREAMLIT EDITION
Fully working with web scraping, AI enrichment, SQLite CRM, and Streamlit dashboard
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
from urllib.parse import urlparse, urljoin
from pathlib import Path
import html

# ============================================================================
# IMPORTS WITH FALLBACKS
# ============================================================================
import os
import sys

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

CONFIG_FILE = "config.json"
DB_FILE = "crm_database.db"

# Default configuration - Matches MitzMedia theme colors
DEFAULT_CONFIG = {
    "machine_id": "lead-scraper-crm-v1",
    "machine_version": "5.0",
    "serper_api_key": "bab72f11620025db8aee1df5b905b9d9b6872a00",
    "openai_api_key": "sk-proj-your-key-here",
    
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
        "primary_color": "#2563eb",
        "secondary_color": "#1e40af",
        "accent_color": "#f59e0b",
        "success_color": "#10b981",
        "danger_color": "#ef4444",
        "dark_bg": "#111827",
        "light_bg": "#f9fafb",
        "text_light": "#f9fafb",
        "text_dark": "#111827"
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
        "exclude_without_websites": True,
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
        """Initialize database with tables"""
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            
            # Leads table
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
                    archive_date DATETIME
                )
            ''')
            
            # Create indexes
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_fingerprint ON leads(fingerprint)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_lead_status ON leads(lead_status)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_quality_tier ON leads(quality_tier)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_city ON leads(city)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON leads(created_at)')
            
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
                ('notification_enabled', 'true', 'boolean', 'notifications', 'Enable notifications')
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
                    social_media, lead_score, quality_tier, potential_value,
                    outreach_priority, lead_status, assigned_to, lead_production_date,
                    follow_up_date, notes, ai_notes, source, scraped_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fingerprint, business_name, website, phone, email, address,
                lead_data.get("city", ""), lead_data.get("state", CONFIG["state"]),
                lead_data.get("industry", ""), lead_data.get("business_type", "LLC"),
                services[:500], lead_data.get("description", "")[:1000],
                social_media[:500], lead_score, quality_tier, potential_value,
                outreach_priority, CONFIG["crm"]["default_status"],
                CONFIG["crm"]["default_assigned_to"],
                datetime.now(timezone.utc).date().isoformat() if CONFIG["crm"]["auto_set_production_date"] else None,
                follow_up_date, "", lead_data.get("ai_notes", "")[:500],
                "Web Scraper", lead_data.get("scraped_date", datetime.now(timezone.utc).isoformat())
            ))
            
            lead_id = cursor.lastrowid
            
            # Add activity log
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, "Lead Created", f"Lead scraped from {website}"))
            
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
            
            # Get current stats
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_leads,
                    SUM(CASE WHEN lead_status = 'New Lead' THEN 1 ELSE 0 END) as new_leads,
                    SUM(CASE WHEN lead_status = 'Contacted' THEN 1 ELSE 0 END) as contacted_leads,
                    SUM(CASE WHEN lead_status = 'Meeting Scheduled' THEN 1 ELSE 0 END) as meetings_scheduled,
                    SUM(CASE WHEN lead_status = 'Closed (Won)' THEN 1 ELSE 0 END) as closed_won,
                    SUM(CASE WHEN lead_status = 'Closed (Lost)' THEN 1 ELSE 0 END) as closed_lost,
                    SUM(CASE WHEN quality_tier IN ('Premium', 'High') THEN 1 ELSE 0 END) as premium_leads,
                    SUM(potential_value) as estimated_value
                FROM leads 
                WHERE DATE(created_at) = DATE('now') AND is_archived = 0
            ''')
            
            stats = cursor.fetchone()
            if not stats:
                stats = (0, 0, 0, 0, 0, 0, 0, 0)
            
            cursor.execute('''
                INSERT OR REPLACE INTO statistics 
                (stat_date, total_leads, new_leads, contacted_leads, meetings_scheduled, 
                 closed_won, closed_lost, premium_leads, estimated_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            
            # Overall stats
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_leads,
                    SUM(CASE WHEN lead_status = 'New Lead' THEN 1 ELSE 0 END) as new_leads,
                    SUM(CASE WHEN lead_status = 'Closed (Won)' THEN 1 ELSE 0 END) as closed_won,
                    SUM(CASE WHEN lead_status = 'Closed (Lost)' THEN 1 ELSE 0 END) as closed_lost,
                    SUM(potential_value) as total_value,
                    AVG(lead_score) as avg_score
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
                    "avg_score": 0
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
                    "avg_score": 0
                },
                "status_distribution": [],
                "quality_distribution": [],
                "daily_leads": [],
                "city_distribution": [],
                "industry_distribution": []
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
# WEBSITE SCRAPER
# ============================================================================

class WebsiteScraper:
    """Scrape websites for contact information"""
    
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
    
    def scrape_website(self, url):
        """Scrape website for contact information"""
        if not url or not url.startswith(('http://', 'https://')):
            return {}
        
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
            
            # Extract information
            data = {
                'website': url,
                'business_name': self._extract_business_name(soup, url),
                'description': self._extract_description(soup),
                'phones': self._extract_phones(soup),
                'emails': self._extract_emails(soup),
                'address': self._extract_address(soup),
                'social_media': self._extract_social_media(soup),
                'services': self._extract_services(soup)
            }
            
            return data
            
        except Exception as e:
            logger.log(f"Scrape error for {url}: {e}", "WARNING")
            return {}
    
    def _extract_business_name(self, soup, url):
        """Extract business name from website"""
        # Try meta tags first
        for meta in soup.find_all('meta'):
            if meta.get('property') in ['og:site_name', 'og:title']:
                return meta.get('content', '')[:200]
        
        # Try title tag
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            # Remove common suffixes
            for suffix in [' - Home', ' | Home', ' - Official Site', ' | Official Site']:
                if title.endswith(suffix):
                    title = title[:-len(suffix)]
            return title[:200]
        
        # Try h1 tags
        h1_tags = soup.find_all('h1')
        if h1_tags:
            return h1_tags[0].get_text(strip=True)[:200]
        
        # Fallback to domain name
        domain = urlparse(url).netloc
        return domain.replace('www.', '').split('.')[0].title()
    
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
            if len(text) > 50:
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
        """Extract services from website"""
        services = []
        
        # Common keywords for contractor services
        service_keywords = [
            'installation', 'repair', 'maintenance', 'service', 'contractor',
            'construction', 'remodeling', 'renovation', 'building', 'design',
            'installation', 'repair', 'maintenance', 'cleaning', 'painting',
            'electrical', 'plumbing', 'hvac', 'roofing', 'landscaping'
        ]
        
        # Look in headings and lists
        text_content = soup.get_text().lower()
        
        for keyword in service_keywords:
            if keyword in text_content:
                services.append(keyword.title())
        
        # Look for specific service sections
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            heading_text = heading.get_text().lower()
            if any(word in heading_text for word in ['service', 'what we do', 'our work', 'expertise']):
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

# ============================================================================
# LEAD SCRAPER (SERP API)
# ============================================================================

class ModernLeadScraper:
    """Main scraper using Serper API"""
    
    def __init__(self):
        self.api_key = CONFIG.get("serper_api_key", "")
        self.scraper = WebsiteScraper()
        self.crm = CRM_Database()
        self.running = False
        self.paused = False
        self.stats = {
            'total_leads': 0,
            'qualified_leads': 0,
            'premium_leads': 0,
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
                        'state': state
                    })
        
        random.shuffle(queries)  # Randomize order
        return queries[:CONFIG["searches_per_cycle"]]
    
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
            return lead_data
        
        try:
            prompt = f"""
            Analyze this business lead and provide:
            1. Lead score (0-100)
            2. Quality tier (Premium, High, Medium, Low, Unknown)
            3. Business type (LLC, Corporation, Sole Proprietorship, Partnership)
            4. Key services (comma-separated)
            5. AI notes with insights
            
            Lead Information:
            - Business: {lead_data.get('business_name', 'Unknown')}
            - Website: {lead_data.get('website', 'None')}
            - Phone: {lead_data.get('phone', 'None')}
            - Email: {lead_data.get('email', 'None')}
            - Address: {lead_data.get('address', 'None')}
            - City: {lead_data.get('city', 'Unknown')}
            - Industry: {lead_data.get('industry', 'Unknown')}
            - Description: {lead_data.get('description', 'None')}
            
            Respond in JSON format:
            {{
                "lead_score": 0-100,
                "quality_tier": "Premium/High/Medium/Low/Unknown",
                "business_type": "LLC/Corporation/Sole Proprietorship/Partnership",
                "services": ["service1", "service2"],
                "ai_notes": "Your analysis here"
            }}
            """
            
            response = self.scraper.openai_client.chat.completions.create(
                model=CONFIG["ai_enrichment"]["model"],
                messages=[
                    {"role": "system", "content": "You are a lead qualification expert for construction and home services businesses."},
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
                lead_data['business_type'] = ai_data.get('business_type', 'LLC')
                
                # Merge services
                existing_services = lead_data.get('services', [])
                ai_services = ai_data.get('services', [])
                if isinstance(existing_services, str):
                    existing_services = [existing_services]
                if isinstance(ai_services, list):
                    lead_data['services'] = list(set(existing_services + ai_services))
                
                lead_data['ai_notes'] = ai_data.get('ai_notes', '')
                
            except:
                logger.log("Failed to parse AI response", "WARNING")
        
        except Exception as e:
            logger.log(f"AI qualification error: {e}", "WARNING")
        
        return lead_data
    
    def process_lead(self, search_result, meta_info):
        """Process a single search result into a lead"""
        url = search_result.get('link', '')
        
        # Skip blacklisted domains
        if self.is_blacklisted(url):
            return None
        
        # Scrape website
        scraped_data = self.scraper.scrape_website(url)
        if not scraped_data:
            return None
        
        # Create lead object
        lead_data = {
            'business_name': scraped_data.get('business_name', search_result.get('title', 'Unknown Business')),
            'website': url,
            'phone': scraped_data.get('phones', [''])[0] if scraped_data.get('phones') else '',
            'email': scraped_data.get('emails', [''])[0] if scraped_data.get('emails') else '',
            'address': scraped_data.get('address', ''),
            'city': meta_info.get('city', ''),
            'state': meta_info.get('state', CONFIG['state']),
            'industry': meta_info.get('industry', ''),
            'description': scraped_data.get('description', search_result.get('snippet', ''))[:500],
            'social_media': scraped_data.get('social_media', {}),
            'services': scraped_data.get('services', []),
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
        
        # Exclude without website
        if filters["exclude_without_websites"] and not lead_data.get('website'):
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
                    # Save to CRM
                    if CONFIG["crm"]["enabled"] and CONFIG["crm"]["auto_sync"]:
                        result = self.crm.save_lead(lead_data)
                        if result["success"]:
                            leads_found += 1
                            logger.log(f"✅ Saved lead: {lead_data['business_name']}", "SUCCESS")
                    
                    # Also save to JSON file
                    self.save_lead_to_file(lead_data)
            
            # Small delay between searches
            time.sleep(random.uniform(1, 3))
        
        self.stats['cycles'] += 1
        self.stats['total_leads'] += leads_found
        self.stats['last_cycle'] = datetime.now().isoformat()
        
        logger.log(f"✅ Cycle completed. Found {leads_found} new leads. Total cycles: {self.stats['cycles']}", "SUCCESS")
    
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
# STREAMLIT DASHBOARD
# ============================================================================

class StreamlitDashboard:
    """Streamlit-based dashboard for Lead Scraper CRM"""
    
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
                page_title="LeadScraper CRM",
                page_icon="🚀",
                layout="wide",
                initial_sidebar_state="expanded"
            )
            
            # Custom CSS
            self.setup_custom_css()
            
            logger.log("✅ Streamlit dashboard initialized", "SUCCESS")
        except Exception as e:
            self.enabled = False
            logger.log(f"Streamlit dashboard initialization error: {e}", "ERROR")
    
    def setup_custom_css(self):
        """Setup custom CSS for Streamlit"""
        st.markdown("""
        <style>
        /* Main theme colors */
        :root {
            --primary: #2563eb;
            --primary-dark: #1e40af;
            --accent: #f59e0b;
            --success: #10b981;
            --danger: #ef4444;
            --dark: #111827;
            --light: #f9fafb;
        }
        
        /* Main container */
        .main .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        
        /* Cards */
        .card {
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 0.75rem;
            padding: 1.5rem;
            background: rgba(255, 255, 255, 0.05);
            margin-bottom: 1rem;
        }
        
        /* Headers */
        h1, h2, h3 {
            color: var(--light) !important;
        }
        
        /* Badges */
        .badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
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
            color: var(--accent);
            font-weight: 600;
        }
        
        /* DataTables */
        .dataframe {
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-radius: 0.5rem !important;
        }
        
        .dataframe th {
            background: rgba(37, 99, 235, 0.2) !important;
            color: white !important;
            font-weight: 600 !important;
        }
        
        .dataframe td {
            border-color: rgba(255, 255, 255, 0.1) !important;
        }
        
        /* Metrics */
        .metric-card {
            background: linear-gradient(135deg, rgba(37, 99, 235, 0.1) 0%, rgba(30, 64, 175, 0.1) 100%);
            border-left: 4px solid var(--primary);
            border-radius: 0.5rem;
            padding: 1rem;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2rem;
        }
        
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            white-space: pre-wrap;
            border-radius: 0.5rem 0.5rem 0 0;
            gap: 1rem;
            padding: 1rem;
        }
        
        /* Sidebar */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, var(--dark) 0%, #1f2937 100%);
        }
        
        /* Hide Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
        """, unsafe_allow_html=True)
    
    def run_scraper_background(self):
        """Run scraper in background"""
        try:
            self.scraper = ModernLeadScraper()
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
        """Render the sidebar"""
        with st.sidebar:
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1 style="color: #2563eb; font-size: 2rem; margin-bottom: 0.5rem;">🚀 LeadScraper</h1>
                <p style="color: #9ca3af; font-size: 0.875rem;">v5.0 - Streamlit Edition</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation
            st.markdown("### 📊 Navigation")
            page = st.radio(
                "Select Page",
                ["Dashboard", "Leads", "Lead Details", "Settings", "Logs", "Export"],
                label_visibility="collapsed"
            )
            
            st.markdown("---")
            
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
            
            # Scraper Status
            status = "🟢 Active" if st.session_state.get('scraper_running', False) else "🔴 Inactive"
            st.markdown(f"**Status:** {status}")
            
            if 'scraper_stats' in st.session_state:
                stats = st.session_state.scraper_stats
                st.markdown(f"**Cycles:** {stats.get('cycles', 0)}")
                st.markdown(f"**Total Leads:** {stats.get('total_leads', 0)}")
            
            st.markdown("---")
            
            # Quick Stats
            st.markdown("### 📈 Quick Stats")
            today_count = self.crm.get_today_count()
            total_leads = self.crm.get_leads()["total"]
            
            st.metric("Today's Leads", today_count)
            st.metric("Total Leads", total_leads)
            
            st.markdown("---")
            
            # System Info
            st.markdown("### 💻 System Info")
            st.markdown(f"**Database:** {'✅ Connected' if self.crm.conn else '❌ Error'}")
            st.markdown(f"**AI Enabled:** {'✅ Ready' if OPENAI_AVAILABLE and CONFIG.get('openai_api_key', '').startswith('sk-') else '❌ Disabled'}")
            st.markdown(f"**State:** {CONFIG['state']}")
            st.markdown(f"**Cities:** {len(CONFIG['cities'])}")
            st.markdown(f"**Industries:** {len(CONFIG['industries'])}")
        
        return page
    
    def render_dashboard(self):
        """Render the main dashboard"""
        st.title("📊 Dashboard Overview")
        
        # Get statistics
        stats = self.crm.get_statistics()
        
        # Top Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Leads", stats["overall"]["total_leads"])
        
        with col2:
            st.metric("Estimated Value", f"${stats['overall']['total_value']:,}")
        
        with col3:
            st.metric("Average Score", f"{stats['overall']['avg_score']:.1f}")
        
        with col4:
            st.metric("Closed Won", stats["overall"]["closed_won"])
        
        # Charts Row
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
                st.plotly_chart(fig_quality, use_container_width=True)
        
        with col2:
            # Status Distribution
            status_data = stats["status_distribution"][:8]  # Top 8 statuses
            if status_data:
                df_status = pd.DataFrame(status_data)
                fig_status = px.bar(
                    df_status,
                    x='status',
                    y='count',
                    title='Lead Status Distribution',
                    color='count',
                    color_continuous_scale='blues'
                )
                fig_status.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_status, use_container_width=True)
        
        # Daily Leads Chart
        st.subheader("📅 Daily Leads (Last 30 Days)")
        daily_data = stats["daily_leads"]
        if daily_data:
            df_daily = pd.DataFrame(daily_data)
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily = df_daily.sort_values('date')
            
            fig_daily = px.line(
                df_daily,
                x='date',
                y='count',
                title='Daily Lead Acquisition',
                markers=True
            )
            st.plotly_chart(fig_daily, use_container_width=True)
        
        # Recent Leads
        st.subheader("🆕 Recent Leads")
        leads_data = self.crm.get_leads(page=1, per_page=10)
        
        if leads_data["leads"]:
            df_recent = pd.DataFrame(leads_data["leads"])
            
            # Select and rename columns for display
            display_cols = ['business_name', 'city', 'industry', 'lead_score', 'quality_tier', 'lead_status']
            df_display = df_recent[display_cols].copy()
            df_display.columns = ['Business', 'City', 'Industry', 'Score', 'Quality', 'Status']
            
            # Format the dataframe
            def format_quality_tier(tier):
                color_map = {
                    'Premium': 'badge-premium',
                    'High': 'badge-high',
                    'Medium': 'badge-medium',
                    'Low': 'badge-low'
                }
                return f'<span class="{color_map.get(tier, "badge-low")}">{tier}</span>'
            
            st.markdown(df_display.to_html(escape=False, formatters={'Quality': format_quality_tier}), unsafe_allow_html=True)
        else:
            st.info("No leads found. Start the scraper to collect leads!")
    
    def render_leads(self):
        """Render leads management page"""
        st.title("👥 Leads Management")
        
        # Filters
        with st.expander("🔍 Filters", expanded=False):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                search_term = st.text_input("Search", placeholder="Business name, phone, email...")
            
            with col2:
                status_filter = st.selectbox("Status", ["All"] + CONFIG["lead_management"]["status_options"])
            
            with col3:
                quality_filter = st.selectbox("Quality Tier", ["All"] + CONFIG["lead_management"]["quality_tiers"])
            
            with col4:
                city_filter = st.selectbox("City", ["All"] + CONFIG["cities"])
        
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
        
        # Get leads
        leads_data = self.crm.get_leads(filters=filters, page=1, per_page=50)
        leads = leads_data["leads"]
        
        st.metric("Total Leads Found", leads_data["total"])
        
        if leads:
            # Create dataframe
            df = pd.DataFrame(leads)
            
            # Select columns for display
            display_cols = ['id', 'business_name', 'phone', 'email', 'city', 
                          'industry', 'lead_score', 'quality_tier', 'lead_status']
            
            if all(col in df.columns for col in display_cols):
                df_display = df[display_cols].copy()
                df_display.columns = ['ID', 'Business', 'Phone', 'Email', 'City', 
                                    'Industry', 'Score', 'Quality', 'Status']
                
                # Add action buttons
                df_display['Actions'] = df_display['ID'].apply(
                    lambda x: f'<button onclick="viewLead({x})">View</button>'
                )
                
                # Display as HTML table with styling
                st.markdown("""
                <style>
                .dataframe-table {
                    width: 100%;
                    border-collapse: collapse;
                }
                .dataframe-table th {
                    background: rgba(37, 99, 235, 0.2);
                    padding: 12px;
                    text-align: left;
                    color: white;
                    font-weight: 600;
                }
                .dataframe-table td {
                    padding: 12px;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
                }
                .dataframe-table tr:hover {
                    background: rgba(255, 255, 255, 0.05);
                }
                </style>
                """, unsafe_allow_html=True)
                
                st.markdown(df_display.to_html(escape=False, index=False, classes='dataframe-table'), unsafe_allow_html=True)
                
                # Lead detail viewer
                st.subheader("📋 Lead Details")
                selected_id = st.selectbox("Select Lead ID to View Details", df_display['ID'].tolist())
                
                if selected_id:
                    lead = self.crm.get_lead_by_id(selected_id)
                    if lead:
                        self.render_lead_detail(lead)
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
        
        # Create tabs for different sections
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Basic Info", "📞 Contact", "📊 Status & Actions", "📝 Activities"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Business Information")
                st.text_input("Business Name", lead.get('business_name', ''), disabled=True)
                st.text_input("Industry", lead.get('industry', ''), disabled=True)
                st.text_input("Business Type", lead.get('business_type', ''), disabled=True)
                
                # Quality Score
                col_score, col_tier = st.columns(2)
                with col_score:
                    st.metric("Lead Score", lead.get('lead_score', 0))
                with col_tier:
                    tier = lead.get('quality_tier', 'Unknown')
                    tier_color = {
                        'Premium': 'badge-premium',
                        'High': 'badge-high',
                        'Medium': 'badge-medium',
                        'Low': 'badge-low'
                    }.get(tier, 'badge-low')
                    st.markdown(f"**Quality Tier:** <span class='{tier_color}'>{tier}</span>", unsafe_allow_html=True)
            
            with col2:
                st.subheader("Description & Services")
                st.text_area("Description", lead.get('description', ''), height=150, disabled=True)
                
                # Services
                services = lead.get('services', [])
                if isinstance(services, str):
                    services = [services]
                
                if services:
                    st.markdown("**Services:**")
                    for service in services[:5]:  # Show first 5 services
                        st.markdown(f"- {service}")
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Contact Details")
                website = lead.get('website', '')
                if website:
                    st.markdown(f"**Website:** [{website}]({website})")
                else:
                    st.text("Website: Not available")
                
                phone = lead.get('phone', '')
                if phone:
                    st.markdown(f"**Phone:** {phone}")
                else:
                    st.text("Phone: Not available")
                
                email = lead.get('email', '')
                if email:
                    st.markdown(f"**Email:** {email}")
                else:
                    st.text("Email: Not available")
                
                address = lead.get('address', '')
                if address:
                    st.text_area("Address", address, disabled=True)
            
            with col2:
                st.subheader("Location")
                st.text_input("City", lead.get('city', ''), disabled=True)
                st.text_input("State", lead.get('state', ''), disabled=True)
                
                # Social Media
                social_media = lead.get('social_media', {})
                if social_media and isinstance(social_media, dict):
                    st.subheader("Social Media")
                    for platform, url in social_media.items():
                        st.markdown(f"**{platform.title()}:** [{url}]({url})")
        
        with tab3:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Lead Status")
                
                # Status update form
                with st.form("update_status_form"):
                    new_status = st.selectbox(
                        "Update Status",
                        CONFIG["lead_management"]["status_options"],
                        index=CONFIG["lead_management"]["status_options"].index(lead.get('lead_status', 'New Lead')) 
                        if lead.get('lead_status') in CONFIG["lead_management"]["status_options"] else 0
                    )
                    
                    new_priority = st.selectbox(
                        "Update Priority",
                        CONFIG["lead_management"]["priority_options"],
                        index=CONFIG["lead_management"]["priority_options"].index(lead.get('outreach_priority', 'Medium')) 
                        if lead.get('outreach_priority') in CONFIG["lead_management"]["priority_options"] else 2
                    )
                    
                    assigned_to = st.text_input("Assigned To", lead.get('assigned_to', ''))
                    
                    notes = st.text_area("Notes", lead.get('notes', ''), height=100)
                    
                    if st.form_submit_button("💾 Update Lead"):
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
            
            with col2:
                st.subheader("Financial Information")
                st.metric("Potential Value", f"${lead.get('potential_value', 0):,}")
                
                st.subheader("Timeline")
                created = lead.get('created_at', '')
                if created:
                    st.text(f"Created: {created[:19]}")
                
                scraped = lead.get('scraped_date', '')
                if scraped:
                    st.text(f"Scraped: {scraped[:19]}")
                
                follow_up = lead.get('follow_up_date', '')
                if follow_up:
                    st.text(f"Follow-up: {follow_up}")
        
        with tab4:
            st.subheader("Activity Timeline")
            activities = lead.get('activities', [])
            
            if activities:
                for activity in activities[:10]:  # Show last 10 activities
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{activity.get('activity_type', 'Activity')}**")
                            st.caption(activity.get('activity_details', ''))
                        with col2:
                            performed = activity.get('performed_at', '')
                            if performed:
                                st.caption(performed[:19])
                        st.divider()
            else:
                st.info("No activities recorded yet.")
    
    def render_settings(self):
        """Render settings page"""
        st.title("⚙️ Settings")
        
        # Create tabs for different setting categories
        tab1, tab2, tab3, tab4 = st.tabs(["🔑 API Keys", "🔍 Scraper Settings", "🏢 Business Settings", "📊 CRM Settings"])
        
        with tab1:
            st.subheader("API Configuration")
            
            serper_key = st.text_input(
                "Serper API Key",
                value=CONFIG.get("serper_api_key", ""),
                type="password",
                help="Get from https://serper.dev"
            )
            
            openai_key = st.text_input(
                "OpenAI API Key",
                value=CONFIG.get("openai_api_key", ""),
                type="password",
                help="Get from https://platform.openai.com/api-keys"
            )
            
            if st.button("Save API Keys", type="primary"):
                CONFIG["serper_api_key"] = serper_key
                CONFIG["openai_api_key"] = openai_key
                self.save_config()
                st.success("API keys saved!")
        
        with tab2:
            st.subheader("Scraper Configuration")
            
            col1, col2 = st.columns(2)
            
            with col1:
                CONFIG["state"] = st.text_input("State", value=CONFIG.get("state", "PA"))
                CONFIG["searches_per_cycle"] = st.number_input("Searches per Cycle", 
                                                              value=CONFIG.get("searches_per_cycle", 5),
                                                              min_value=1, max_value=50)
                CONFIG["businesses_per_search"] = st.number_input("Businesses per Search",
                                                                 value=CONFIG.get("businesses_per_search", 10),
                                                                 min_value=1, max_value=100)
            
            with col2:
                CONFIG["cycle_interval"] = st.number_input("Cycle Interval (seconds)",
                                                          value=CONFIG.get("cycle_interval", 300),
                                                          min_value=10, max_value=3600)
                CONFIG["max_cycles"] = st.number_input("Max Cycles",
                                                      value=CONFIG.get("max_cycles", 100),
                                                      min_value=1, max_value=1000)
                CONFIG["operating_mode"] = st.selectbox("Operating Mode",
                                                       options=["auto", "manual"],
                                                       index=0 if CONFIG.get("operating_mode", "auto") == "auto" else 1)
            
            if st.button("Save Scraper Settings", type="primary"):
                self.save_config()
                st.success("Scraper settings saved!")
        
        with tab3:
            st.subheader("Business Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Cities")
                cities_text = st.text_area("Cities (one per line)",
                                          value="\n".join(CONFIG.get("cities", [])),
                                          height=200)
                if cities_text:
                    CONFIG["cities"] = [city.strip() for city in cities_text.split("\n") if city.strip()]
            
            with col2:
                st.subheader("Industries")
                industries_text = st.text_area("Industries (one per line)",
                                              value="\n".join(CONFIG.get("industries", [])),
                                              height=200)
                if industries_text:
                    CONFIG["industries"] = [industry.strip() for industry in industries_text.split("\n") if industry.strip()]
            
            st.subheader("Search Phrases")
            search_phrases_text = st.text_area("Search Phrases (one per line)",
                                              value="\n".join(CONFIG.get("search_phrases", [])),
                                              height=150,
                                              help="Use {industry}, {city}, {state} as placeholders")
            if search_phrases_text:
                CONFIG["search_phrases"] = [phrase.strip() for phrase in search_phrases_text.split("\n") if phrase.strip()]
            
            if st.button("Save Business Settings", type="primary"):
                self.save_config()
                st.success("Business settings saved!")
        
        with tab4:
            st.subheader("CRM Configuration")
            
            col1, col2 = st.columns(2)
            
            with col1:
                CONFIG["crm"]["enabled"] = st.checkbox("Enable CRM", value=CONFIG["crm"].get("enabled", True))
                CONFIG["crm"]["auto_sync"] = st.checkbox("Auto Sync Leads", value=CONFIG["crm"].get("auto_sync", True))
                CONFIG["crm"]["prevent_duplicates"] = st.checkbox("Prevent Duplicates", 
                                                                 value=CONFIG["crm"].get("prevent_duplicates", True))
            
            with col2:
                CONFIG["crm"]["default_status"] = st.selectbox("Default Status",
                                                              options=CONFIG["lead_management"]["status_options"],
                                                              index=CONFIG["lead_management"]["status_options"].index(
                                                                  CONFIG["crm"].get("default_status", "New Lead"))
                                                              if CONFIG["crm"].get("default_status") in CONFIG["lead_management"]["status_options"] else 0)
                
                CONFIG["crm"]["default_assigned_to"] = st.text_input("Default Assigned To",
                                                                     value=CONFIG["crm"].get("default_assigned_to", ""))
            
            # AI Settings
            st.subheader("AI Enrichment")
            CONFIG["ai_enrichment"]["enabled"] = st.checkbox("Enable AI Enrichment",
                                                            value=CONFIG["ai_enrichment"].get("enabled", True))
            
            if CONFIG["ai_enrichment"]["enabled"]:
                col1, col2 = st.columns(2)
                with col1:
                    CONFIG["ai_enrichment"]["model"] = st.selectbox("Model",
                                                                   options=["gpt-4o-mini", "gpt-4", "gpt-3.5-turbo"],
                                                                   index=0 if CONFIG["ai_enrichment"].get("model", "gpt-4o-mini") == "gpt-4o-mini" else 
                                                                          1 if CONFIG["ai_enrichment"].get("model") == "gpt-4" else 2)
                with col2:
                    CONFIG["ai_enrichment"]["qualification_threshold"] = st.slider("Qualification Threshold",
                                                                                  min_value=0, max_value=100,
                                                                                  value=CONFIG["ai_enrichment"].get("qualification_threshold", 60))
            
            if st.button("Save CRM Settings", type="primary"):
                self.save_config()
                st.success("CRM settings saved!")
    
    def render_logs(self):
        """Render logs page"""
        st.title("📋 System Logs")
        
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
                level_filter = st.selectbox("Filter by Level", 
                                           ["All", "INFO", "SUCCESS", "WARNING", "ERROR", "DEBUG"])
            
            with col2:
                date_filter = st.date_input("Filter by Date", value=None)
            
            with col3:
                search_term = st.text_input("Search Logs", placeholder="Search message content...")
            
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
            st.subheader(f"Log Entries ({len(filtered_logs)})")
            
            # Create dataframe for display
            if filtered_logs:
                # Reverse to show newest first
                filtered_logs.reverse()
                
                df_logs = pd.DataFrame(filtered_logs[:100])  # Show last 100 logs
                
                # Format timestamps
                if 'timestamp' in df_logs.columns:
                    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Color code levels
                def color_level(level):
                    colors = {
                        'INFO': 'blue',
                        'SUCCESS': 'green',
                        'WARNING': 'orange',
                        'ERROR': 'red',
                        'DEBUG': 'gray'
                    }
                    color = colors.get(level, 'black')
                    return f'<span style="color: {color}; font-weight: bold;">{level}</span>'
                
                # Display as HTML table
                html = df_logs.to_html(escape=False, 
                                      formatters={'level': color_level},
                                      index=False,
                                      classes='dataframe-table')
                st.markdown(html, unsafe_allow_html=True)
            else:
                st.info("No logs match the current filters.")
            
            # Clear logs button
            if st.button("🗑️ Clear All Logs", type="secondary"):
                if os.path.exists(log_file):
                    with open(log_file, "w") as f:
                        json.dump([], f)
                    st.success("Logs cleared!")
                    st.rerun()
        else:
            st.info("No logs available yet.")
    
    def render_export(self):
        """Render export page"""
        st.title("📤 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Export Options")
            
            export_format = st.radio(
                "Export Format",
                ["CSV", "JSON", "Excel"],
                horizontal=True
            )
            
            include_columns = st.multiselect(
                "Select Columns to Include",
                options=[
                    "business_name", "website", "phone", "email", "address",
                    "city", "state", "industry", "business_type", "services",
                    "lead_score", "quality_tier", "potential_value", "lead_status",
                    "assigned_to", "created_at", "scraped_date"
                ],
                default=["business_name", "phone", "email", "city", "lead_score", "quality_tier", "lead_status"]
            )
            
            # Date range filter
            st.subheader("Date Range")
            col_date1, col_date2 = st.columns(2)
            with col_date1:
                date_from = st.date_input("From Date", value=None)
            with col_date2:
                date_to = st.date_input("To Date", value=None)
        
        with col2:
            st.subheader("Filters")
            
            status_filter = st.multiselect(
                "Status",
                options=CONFIG["lead_management"]["status_options"],
                default=[]
            )
            
            quality_filter = st.multiselect(
                "Quality Tier",
                options=CONFIG["lead_management"]["quality_tiers"],
                default=[]
            )
            
            city_filter = st.multiselect(
                "City",
                options=CONFIG["cities"],
                default=[]
            )
        
        # Apply filters
        filters = {}
        if status_filter:
            filters["status"] = status_filter[0]  # For now, just use first selected
        if quality_filter:
            filters["quality_tier"] = quality_filter[0]
        if city_filter:
            filters["city"] = city_filter[0]
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
            st.subheader("Preview")
            st.dataframe(df.head(10), use_container_width=True)
            
            # Export buttons
            st.subheader("Download")
            
            if export_format == "CSV":
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    type="primary"
                )
            
            elif export_format == "JSON":
                json_str = df.to_json(orient="records", indent=2)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_str,
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    type="primary"
                )
            
            elif export_format == "Excel":
                # For Excel export, we need to use a buffer
                import io
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Leads')
                
                st.download_button(
                    label="📥 Download Excel",
                    data=buffer.getvalue(),
                    file_name=f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
        else:
            st.warning("No leads to export with the current filters.")
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(CONFIG, f, indent=2)
            logger.log("Configuration saved", "SUCCESS")
            return True
        except Exception as e:
            logger.log(f"Error saving config: {e}", "ERROR")
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
        
        # Render sidebar and get selected page
        page = self.render_sidebar()
        
        # Render selected page
        if page == "Dashboard":
            self.render_dashboard()
        elif page == "Leads":
            self.render_leads()
        elif page == "Lead Details":
            # For lead details, we need a lead ID
            lead_id = st.number_input("Enter Lead ID", min_value=1, value=1)
            if lead_id:
                self.render_lead_detail(lead_id=lead_id)
            else:
                st.info("Enter a Lead ID to view details")
        elif page == "Settings":
            self.render_settings()
        elif page == "Logs":
            self.render_logs()
        elif page == "Export":
            self.render_export()
        
        # Auto-refresh every 30 seconds if scraper is running
        if st.session_state.scraper_running:
            st_autorefresh(interval=30000, limit=100, key="scraper_refresh")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 COMPREHENSIVE LEAD SCRAPER CRM - STREAMLIT EDITION")
    print("="*80)
    print("Features:")
    print("  ✅ Complete lead management with detailed views")
    print("  ✅ Full configuration editing from dashboard")
    print("  ✅ Real-time statistics and monitoring")
    print("  ✅ Advanced filtering and search")
    print("  ✅ AI-powered lead qualification")
    print("  ✅ Export functionality (CSV, JSON, Excel)")
    print("  ✅ System logs viewer")
    print("  ✅ Beautiful Streamlit interface")
    print("="*80)
    
    # Check API keys
    if not CONFIG.get("serper_api_key") or CONFIG.get("serper_api_key") == "bab72f11620025db8aee1df5b905b9d9b6872a00":
        print("\n❌ Update Serper API key in config.json")
        print("   Get from: https://serper.dev")
        print("   Current config file: config.json")
    
    if CONFIG.get("openai_api_key", "").startswith("sk-proj-your-key-here"):
        print("\n⚠️  OpenAI API key not configured - AI features disabled")
        print("   Get from: https://platform.openai.com/api-keys")
    elif OPENAI_AVAILABLE:
        print("\n✅ OpenAI configured - AI features enabled")
    
    print(f"\n🎯 State: {CONFIG['state']}")
    print(f"🏙️  Cities: {len(CONFIG['cities'])}")
    print(f"🏭 Industries: {len(CONFIG['industries'])}")
    print(f"⏱️  Interval: {CONFIG['cycle_interval']}s")
    print("="*80)
    
    # Check Streamlit availability
    if not STREAMLIT_AVAILABLE:
        print("\n❌ Streamlit dependencies not installed")
        print("   Install with: pip install streamlit pandas plotly streamlit-autorefresh")
        return
    
    # Create and run dashboard
    dashboard = StreamlitDashboard()
    
    if not dashboard.enabled:
        print("\n❌ Dashboard failed to initialize")
        return
    
    print(f"\n🌐 Starting Streamlit dashboard on port {CONFIG['dashboard']['port']}...")
    print(f"📱 Access at: http://localhost:{CONFIG['dashboard']['port']}")
    print("\n📊 Available features:")
    print("  • Dashboard with real-time stats")
    print("  • Lead management with filtering")
    print("  • Lead details view")
    print("  • Settings configuration")
    print("  • System logs viewer")
    print("  • Export functionality (CSV, JSON, Excel)")
    print("  • Auto-scraping with configurable intervals")
    print("="*80)
    
    # Run Streamlit app
    dashboard.run()

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
