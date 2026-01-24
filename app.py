#!/usr/bin/env python3
"""
🚀 COMPREHENSIVE LEAD SCRAPER CRM - SINGLE FILE
Fully working with web scraping, AI enrichment, SQLite CRM, and dashboard
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
    from flask import Flask, jsonify, request, make_response, render_template_string, send_from_directory
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("⚠️  Flask not installed. Dashboard disabled.")

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
        "port": 5000,
        "host": "0.0.0.0",
        "debug": False,
        "secret_key": "lead-scraper-secret-key-2024",
        "enable_socketio": False
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
# COMPREHENSIVE DASHBOARD
# ============================================================================

class ComprehensiveDashboard:
    """Comprehensive web dashboard with full lead details and config editing"""
    
    def __init__(self):
        if not FLASK_AVAILABLE:
            self.enabled = False
            logger.log("⚠️  Flask not installed. Dashboard disabled.", "WARNING")
            return
        
        try:
            self.app = Flask(__name__)
            self.app.config['SECRET_KEY'] = CONFIG["dashboard"]["secret_key"]
            self.crm = CRM_Database()
            self.scraper = None
            self.scraper_running = False
            self.scraper_thread = None
            self.setup_routes()
            self.enabled = True
            logger.log("✅ Comprehensive dashboard initialized", "SUCCESS")
        except Exception as e:
            self.enabled = False
            logger.log(f"Dashboard initialization error: {e}", "ERROR")
    
    def setup_routes(self):
        """Setup Flask routes"""
        
        @self.app.route('/')
        def index():
            return self.render_home()
        
        @self.app.route('/api/dashboard')
        def api_dashboard():
            """Dashboard API"""
            return self.render_dashboard()
        
        @self.app.route('/api/leads')
        def api_leads():
            """Leads list API"""
            return self.render_leads()
        
        @self.app.route('/api/lead/<int:lead_id>')
        def api_lead_detail(lead_id):
            """Lead detail page"""
            return self.render_lead_detail(lead_id)
        
        @self.app.route('/api/settings')
        def api_settings():
            """Settings page API"""
            return self.render_settings()
        
        @self.app.route('/api/advanced-settings')
        def api_advanced_settings():
            """Advanced settings page"""
            return self.render_advanced_settings()
        
        @self.app.route('/api/logs')
        def api_logs():
            """Logs page"""
            return self.render_logs()
        
        @self.app.route('/api/stats')
        def api_stats():
            """Get current stats"""
            try:
                stats = self.crm.get_statistics()
                today_count = self.crm.get_today_count()
                
                return jsonify({
                    'total_leads': stats.get('overall', {}).get('total_leads', 0),
                    'today_leads': today_count,
                    'scraper_status': 'active' if self.scraper_running else 'idle',
                    'avg_score': stats.get('overall', {}).get('avg_score', 0),
                    'total_value': stats.get('overall', {}).get('total_value', 0),
                    'new_leads': stats.get('overall', {}).get('new_leads', 0),
                    'closed_won': stats.get('overall', {}).get('closed_won', 0)
                })
            except Exception as e:
                logger.log(f"Stats API error: {e}", "ERROR")
                return jsonify({
                    'total_leads': 0,
                    'today_leads': 0,
                    'scraper_status': 'error'
                })
        
        @self.app.route('/api/lead-detail/<int:lead_id>')
        def api_lead_detail_data(lead_id):
            """Get lead detail data"""
            try:
                lead = self.crm.get_lead_by_id(lead_id)
                if not lead:
                    return jsonify({'error': 'Lead not found'}), 404
                return jsonify(lead)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/recent-leads')
        def api_recent_leads():
            """Get recent leads"""
            try:
                leads_data = self.crm.get_leads(page=1, per_page=10)
                return jsonify(leads_data['leads'])
            except Exception as e:
                logger.log(f"Recent leads error: {e}", "ERROR")
                return jsonify([])
        
        @self.app.route('/api/update-lead/<int:lead_id>', methods=['POST'])
        def api_update_lead(lead_id):
            """Update lead"""
            try:
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'message': 'No data provided'})
                
                result = self.crm.update_lead(lead_id, data)
                return jsonify(result)
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
        
        @self.app.route('/api/get-config')
        def api_get_config():
            """Get current configuration"""
            try:
                config = self.crm.get_all_settings()
                return jsonify(config)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/update-config', methods=['POST'])
        def api_update_config():
            """Update configuration"""
            try:
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'message': 'No data provided'})
                
                result = self.crm.update_config_file(data)
                return jsonify(result)
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500
        
        @self.app.route('/api/start-scraper', methods=['POST'])
        def api_start_scraper():
            """Start scraper"""
            try:
                if not self.scraper_running:
                    self.scraper_running = True
                    self.scraper_thread = threading.Thread(target=self.run_scraper_background, daemon=True)
                    self.scraper_thread.start()
                    return jsonify({'success': True, 'message': 'Scraper started'})
                else:
                    return jsonify({'success': False, 'message': 'Scraper already running'})
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
        
        @self.app.route('/api/stop-scraper', methods=['POST'])
        def api_stop_scraper():
            """Stop scraper"""
            self.scraper_running = False
            if self.scraper:
                self.scraper.running = False
            return jsonify({'success': True, 'message': 'Scraper stopped'})
        
        @self.app.route('/api/export-leads')
        def api_export_leads():
            """Export leads as CSV"""
            try:
                return self.export_leads()
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
        
        @self.app.route('/api/health')
        def api_health():
            """Health check endpoint"""
            try:
                return jsonify({
                    "status": "ok",
                    "database": "connected",
                    "leads_count": self.crm.get_leads()["total"],
                    "dashboard": "running",
                    "scraper": "running" if self.scraper_running else "stopped",
                    "openai_available": OPENAI_AVAILABLE and bool(CONFIG.get("openai_api_key", ""))
                })
            except Exception as e:
                return jsonify({
                    "status": "error",
                    "message": str(e)
                }), 500
        
        @self.app.route('/api/get-logs')
        def api_get_logs():
            """Get log data"""
            try:
                log_file = CONFIG["storage"]["logs_file"]
                if os.path.exists(log_file):
                    with open(log_file, "r") as f:
                        logs = json.load(f)
                    return jsonify(logs[-100:])  # Return last 100 logs
                return jsonify([])
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/clear-logs', methods=['POST'])
        def api_clear_logs():
            """Clear logs"""
            try:
                log_file = CONFIG["storage"]["logs_file"]
                if os.path.exists(log_file):
                    with open(log_file, "w") as f:
                        json.dump([], f)
                return jsonify({'success': True, 'message': 'Logs cleared'})
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500
        
        @self.app.route('/api/test-scrape', methods=['POST'])
        def api_test_scrape():
            """Test scrape a single URL"""
            try:
                data = request.get_json()
                url = data.get('url', '')
                
                if not url:
                    return jsonify({'success': False, 'message': 'No URL provided'})
                
                scraper = WebsiteScraper()
                result = scraper.scrape_website(url)
                
                if result:
                    return jsonify({'success': True, 'data': result})
                else:
                    return jsonify({'success': False, 'message': 'Failed to scrape website'})
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
    
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
                
                # Check if we should continue
                if self.scraper_running and cycles < CONFIG['max_cycles']:
                    time.sleep(CONFIG['cycle_interval'])
            
            self.scraper_running = False
            logger.log("Scraper finished", "INFO")
            
        except Exception as e:
            logger.log(f"Background scraper error: {e}", "ERROR")
            self.scraper_running = False
    
    def render_home(self):
        """Render home page"""
        return '''
        <!DOCTYPE html>
        <html lang="en" class="dark">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>LeadScraper CRM - Comprehensive Dashboard</title>
            <script src="https://cdn.tailwindcss.com"></script>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
            <style>
                :root {
                    --primary: #2563eb;
                    --primary-dark: #1e40af;
                    --accent: #f59e0b;
                    --success: #10b981;
                    --danger: #ef4444;
                    --dark: #111827;
                    --light: #f9fafb;
                }
                
                body {
                    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
                    background-color: var(--dark);
                    color: var(--light);
                }
                
                .sidebar {
                    background: linear-gradient(180deg, var(--dark) 0%, #1f2937 100%);
                }
                
                .card {
                    background: rgba(255, 255, 255, 0.05);
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 0.75rem;
                    backdrop-filter: blur(10px);
                }
                
                .btn-primary {
                    background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
                    color: white;
                    padding: 0.625rem 1.25rem;
                    border-radius: 0.5rem;
                    font-weight: 500;
                    transition: all 0.2s;
                }
                
                .btn-primary:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 10px 25px -5px rgba(37, 99, 235, 0.3);
                }
                
                .stat-card {
                    background: linear-gradient(135deg, rgba(37, 99, 235, 0.1) 0%, rgba(30, 64, 175, 0.1) 100%);
                    border-left: 4px solid var(--primary);
                }
                
                .nav-item {
                    display: flex;
                    align-items: center;
                    padding: 0.75rem 1rem;
                    border-radius: 0.5rem;
                    color: #d1d5db;
                    transition: all 0.2s;
                    cursor: pointer;
                    margin-bottom: 0.25rem;
                }
                
                .nav-item:hover {
                    background-color: rgba(255, 255, 255, 0.1);
                    color: white;
                }
                
                .nav-item.active {
                    background: linear-gradient(to right, rgba(37, 99, 235, 0.2), rgba(30, 64, 175, 0.1));
                    color: white;
                    border-left: 4px solid var(--primary);
                }
                
                .badge {
                    padding: 0.25rem 0.75rem;
                    border-radius: 9999px;
                    font-size: 0.75rem;
                    font-weight: 600;
                    display: inline-flex;
                    align-items: center;
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
                
                .loading {
                    display: inline-block;
                    width: 24px;
                    height: 24px;
                    border: 3px solid rgba(255,255,255,.3);
                    border-radius: 50%;
                    border-top-color: var(--primary);
                    animation: spin 1s ease-in-out infinite;
                }
                
                @keyframes spin {
                    to { transform: rotate(360deg); }
                }
                
                .tab {
                    padding: 0.75rem 1.5rem;
                    border-bottom: 2px solid transparent;
                    color: #9ca3af;
                    cursor: pointer;
                    transition: all 0.2s;
                }
                
                .tab:hover {
                    color: #e5e7eb;
                }
                
                .tab.active {
                    color: var(--primary);
                    border-bottom-color: var(--primary);
                }
                
                .form-input {
                    background-color: rgba(255, 255, 255, 0.05);
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 0.5rem;
                    color: white;
                    padding: 0.625rem 0.875rem;
                    width: 100%;
                    transition: border-color 0.2s;
                }
                
                .form-input:focus {
                    outline: none;
                    border-color: var(--primary);
                    box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
                }
                
                .form-label {
                    display: block;
                    color: #9ca3af;
                    font-size: 0.875rem;
                    font-weight: 500;
                    margin-bottom: 0.25rem;
                }
                
                .form-select {
                    background-color: rgba(255, 255, 255, 0.05);
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 0.5rem;
                    color: white;
                    padding: 0.625rem 2.5rem 0.625rem 0.875rem;
                    width: 100%;
                }
                
                .json-viewer {
                    background: rgba(0, 0, 0, 0.2);
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 0.5rem;
                    padding: 1rem;
                    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
                    font-size: 0.875rem;
                    max-height: 400px;
                    overflow-y: auto;
                }
                
                .scrollbar-thin {
                    scrollbar-width: thin;
                    scrollbar-color: rgba(255, 255, 255, 0.2) transparent;
                }
                
                .scrollbar-thin::-webkit-scrollbar {
                    width: 6px;
                }
                
                .scrollbar-thin::-webkit-scrollbar-track {
                    background: transparent;
                }
                
                .scrollbar-thin::-webkit-scrollbar-thumb {
                    background-color: rgba(255, 255, 255, 0.2);
                    border-radius: 3px;
                }
            </style>
        </head>
        <body class="min-h-screen">
            <div class="flex">
                <!-- Sidebar -->
                <div class="sidebar w-64 min-h-screen p-6 space-y-6 flex-shrink-0">
                    <div class="flex items-center space-x-3 mb-8">
                        <div class="w-10 h-10 rounded-lg bg-gradient-to-br from-blue-500 to-blue-700 flex items-center justify-center">
                            <i class="fas fa-robot text-white text-xl"></i>
                        </div>
                        <div>
                            <h1 class="text-xl font-bold">LeadScraper</h1>
                            <p class="text-xs text-gray-400">v5.0</p>
                        </div>
                    </div>
                    
                    <nav class="space-y-1">
       
                        <a href="#" data-view="leads" class="nav-item">
                            <i class="fas fa-users w-5 mr-3"></i>
                            <span>Leads</span>
                            <span id="leadsCount" class="ml-auto text-xs bg-blue-900 text-blue-200 px-2 py-1 rounded-full">0</span>
                        </a>
                        <a href="#" data-view="settings" class="nav-item">
                            <i class="fas fa-cog w-5 mr-3"></i>
                            <span>Settings</span>
                        </a>
                        <a href="#" data-view="advanced-settings" class="nav-item">
                            <i class="fas fa-sliders-h w-5 mr-3"></i>
                            <span>Advanced Settings</span>
                        </a>
                        <a href="#" data-view="logs" class="nav-item">
                            <i class="fas fa-clipboard-list w-5 mr-3"></i>
                            <span>Logs</span>
                        </a>
                    </nav>
                    
                    <div class="card p-4 mt-8">
                        <div class="flex items-center justify-between mb-3">
                            <span class="text-sm text-gray-400">Scraper Status</span>
                            <span id="scraperStatus" class="px-2 py-1 rounded text-xs bg-yellow-900 text-yellow-300">Idle</span>
                        </div>
                        <div class="space-y-2 text-sm">
                            <div class="flex justify-between">
                                <span>Today's Leads:</span>
                                <span id="todayLeads" class="font-semibold">0</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Total Leads:</span>
                                <span id="totalLeads" class="font-semibold">0</span>
                            </div>
                            <div class="flex justify-between">
                                <span>Active Scrapes:</span>
                                <span id="activeScrapes" class="font-semibold">0</span>
                            </div>
                        </div>
                        <div class="mt-4 space-y-2">
                            <button id="startBtn" onclick="toggleScraper()" class="w-full btn-primary">
                                <i class="fas fa-play mr-2"></i>Start Scraper
                            </button>
                            <button onclick="exportLeads()" class="w-full bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg transition-colors">
                                <i class="fas fa-download mr-2"></i>Export Leads
                            </button>
                        </div>
                    </div>
                    
                    <div class="mt-auto pt-6 border-t border-gray-800">
                        <div class="text-xs text-gray-500">
                            <p class="flex items-center">
                                <i class="fas fa-database mr-2"></i>
                                <span id="databaseStatus">Database: Connected</span>
                            </p>
                            <p class="flex items-center mt-1">
                                <i class="fas fa-bolt mr-2"></i>
                                <span id="aiStatus">AI: Ready</span>
                            </p>
                        </div>
                    </div>
                </div>
                
                <!-- Main Content -->
                <div class="flex-1 min-h-screen">
                    <div id="app" class="p-6">
                        <div class="flex items-center justify-center h-96">
                            <div class="text-center">
                                <div class="loading mx-auto mb-4"></div>
                                <p class="text-gray-400">Loading dashboard...</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Toast Notification -->
            <div id="toast" class="fixed bottom-4 right-4 p-4 rounded-lg shadow-lg transform translate-y-full transition-transform duration-300 hidden" style="min-width: 300px; z-index: 9999;">
                <div class="flex items-center">
                    <div id="toastIcon" class="mr-3"></div>
                    <div class="flex-1">
                        <p id="toastMessage" class="font-medium"></p>
                    </div>
                    <button onclick="hideToast()" class="ml-4 text-gray-400 hover:text-white">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
            </div>
            
            <script>
            // Global variables
            let scraperRunning = false;
            let currentView = 'dashboard';
            let currentLeadId = null;
            
            // Toast notification functions
            function showToast(message, type = 'info') {
                const toast = document.getElementById('toast');
                const toastMessage = document.getElementById('toastMessage');
                const toastIcon = document.getElementById('toastIcon');
                
                const types = {
                    success: { bg: 'bg-green-900', border: 'border-green-700', icon: '<i class="fas fa-check-circle text-green-400"></i>' },
                    error: { bg: 'bg-red-900', border: 'border-red-700', icon: '<i class="fas fa-exclamation-circle text-red-400"></i>' },
                    warning: { bg: 'bg-yellow-900', border: 'border-yellow-700', icon: '<i class="fas fa-exclamation-triangle text-yellow-400"></i>' },
                    info: { bg: 'bg-blue-900', border: 'border-blue-700', icon: '<i class="fas fa-info-circle text-blue-400"></i>' }
                };
                
                const config = types[type] || types.info;
                
                toast.className = `fixed bottom-4 right-4 p-4 rounded-lg shadow-lg transform translate-y-full transition-transform duration-300 ${config.bg} border ${config.border} text-white`;
                toastMessage.textContent = message;
                toastIcon.innerHTML = config.icon;
                
                toast.classList.remove('hidden');
                setTimeout(() => {
                    toast.classList.remove('translate-y-full');
                }, 10);
                
                // Auto-hide after 5 seconds
                setTimeout(hideToast, 5000);
            }
            
            function hideToast() {
                const toast = document.getElementById('toast');
                toast.classList.add('translate-y-full');
                setTimeout(() => toast.classList.add('hidden'), 300);
            }
            
            // View management
            async function loadView(view, params = {}) {
                try {
                    currentView = view;
                    
                    // Update active nav item
                    document.querySelectorAll('.nav-item').forEach(item => {
                        if (item.getAttribute('data-view') === view) {
                            item.classList.add('active');
                        } else {
                            item.classList.remove('active');
                        }
                    });
                    
                    // Show loading
                    document.getElementById('app').innerHTML = `
                        <div class="flex items-center justify-center h-96">
                            <div class="text-center">
                                <div class="loading mx-auto mb-4"></div>
                                <p class="text-gray-400">Loading ${view.replace('-', ' ')}...</p>
                            </div>
                        </div>
                    `;
                    
                    // Build URL with params
                    let url = `/api/${view}`;
                    if (params.leadId) {
                        url = `/api/lead/${params.leadId}`;
                    } else if (Object.keys(params).length > 0) {
                        const queryParams = new URLSearchParams(params).toString();
                        url += `?${queryParams}`;
                    }
                    
                    const response = await fetch(url);
                    if (!response.ok) throw new Error(`HTTP ${response.status}`);
                    const html = await response.text();
                    document.getElementById('app').innerHTML = html;
                    
                    // Initialize view-specific JavaScript
                    if (view === 'leads') {
                        initLeadsTable();
                    } else if (view === 'settings') {
                        loadSettings();
                    } else if (view === 'advanced-settings') {
                        loadAdvancedSettings();
                    } else if (view === 'logs') {
                        loadLogs();
                    }
                    
                } catch (error) {
                    console.error('Error loading view:', error);
                    document.getElementById('app').innerHTML = `
                        <div class="bg-red-900/20 border border-red-700 rounded-lg p-6">
                            <h3 class="text-red-300 font-semibold text-lg mb-2">Error Loading View</h3>
                            <p class="text-red-400 mb-4">${error.message}</p>
                            <button onclick="loadView('dashboard')" class="btn-primary">
                                <i class="fas fa-arrow-left mr-2"></i>Back to Dashboard
                            </button>
                        </div>
                    `;
                }
            }
            
            // Stats updating
            async function updateStats() {
                try {
                    const response = await fetch('/api/stats');
                    const data = await response.json();
                    
                    // Update sidebar stats
                    document.getElementById('totalLeads').textContent = data.total_leads || 0;
                    document.getElementById('todayLeads').textContent = data.today_leads || 0;
                    document.getElementById('leadsCount').textContent = data.total_leads || 0;
                    
                    // Update scraper status
                    const statusEl = document.getElementById('scraperStatus');
                    const startBtn = document.getElementById('startBtn');
                    
                    if (data.scraper_status === 'active') {
                        statusEl.textContent = 'Active';
                        statusEl.className = 'px-2 py-1 rounded text-xs bg-green-900 text-green-300';
                        startBtn.innerHTML = '<i class="fas fa-stop mr-2"></i>Stop Scraper';
                        scraperRunning = true;
                    } else if (data.scraper_status === 'error') {
                        statusEl.textContent = 'Error';
                        statusEl.className = 'px-2 py-1 rounded text-xs bg-red-900 text-red-300';
                    } else {
                        statusEl.textContent = 'Idle';
                        statusEl.className = 'px-2 py-1 rounded text-xs bg-yellow-900 text-yellow-300';
                        startBtn.innerHTML = '<i class="fas fa-play mr-2"></i>Start Scraper';
                        scraperRunning = false;
                    }
                    
                } catch (error) {
                    console.error('Error updating stats:', error);
                }
            }
            
            // Scraper control
            async function toggleScraper() {
                const btn = document.getElementById('startBtn');
                const originalHtml = btn.innerHTML;
                
                btn.disabled = true;
                btn.innerHTML = '<div class="loading"></div>';
                
                try {
                    if (scraperRunning) {
                        await fetch('/api/stop-scraper', { method: 'POST' });
                        showToast('Scraper stopped', 'info');
                    } else {
                        await fetch('/api/start-scraper', { method: 'POST' });
                        showToast('Scraper started', 'success');
                    }
                    
                    updateStats();
                    
                } catch (error) {
                    console.error('Error toggling scraper:', error);
                    showToast('Error: ' + error.message, 'error');
                } finally {
                    setTimeout(() => {
                        btn.disabled = false;
                        btn.innerHTML = originalHtml;
                        updateStats();
                    }, 2000);
                }
            }
            
            // Export functionality
            async function exportLeads() {
                try {
                    const response = await fetch('/api/export-leads');
                    if (!response.ok) throw new Error('Export failed');
                    
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `leads_export_${new Date().toISOString().slice(0,10)}.csv`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                    
                    showToast('Leads exported successfully', 'success');
                    
                } catch (error) {
                    console.error('Export error:', error);
                    showToast('Error exporting leads: ' + error.message, 'error');
                }
            }
            
            // Initialize on load
            document.addEventListener('DOMContentLoaded', function() {
                // Setup navigation
                document.querySelectorAll('.nav-item').forEach(item => {
                    item.addEventListener('click', function(e) {
                        e.preventDefault();
                        const view = this.getAttribute('data-view');
                        if (view) {
                            loadView(view);
                        }
                    });
                });
                
                // Load initial view
                loadView('leads');
                
                // Update stats every 10 seconds
                setInterval(updateStats, 10000);
                updateStats();
                
                // Check system status periodically
                setInterval(checkSystemStatus, 30000);
                checkSystemStatus();
            });
            
            // System status check
            async function checkSystemStatus() {
                try {
                    const response = await fetch('/api/health');
                    const data = await response.json();
                    
                    const dbStatus = document.getElementById('databaseStatus');
                    const aiStatus = document.getElementById('aiStatus');
                    
                    dbStatus.textContent = `Database: ${data.database === 'connected' ? 'Connected' : 'Error'}`;
                    dbStatus.className = `flex items-center ${data.database === 'connected' ? 'text-green-400' : 'text-red-400'}`;
                    
                    aiStatus.textContent = `AI: ${data.openai_available ? 'Ready' : 'Disabled'}`;
                    aiStatus.className = `flex items-center ${data.openai_available ? 'text-green-400' : 'text-yellow-400'}`;
                    
                } catch (error) {
                    console.error('Status check error:', error);
                }
            }
            
            // Utility functions for child views
            function initLeadsTable() {
                // Initialize select2 for filters
                if (window.$ && $.fn && $.fn.select2) {
                    $('.select2').select2({
                        theme: 'dark',
                        width: '100%'
                    });
                }
            }
            
            async function loadSettings() {
                try {
                    const response = await fetch('/api/get-config');
                    const config = await response.json();
                    
                    const container = document.getElementById('settingsContainer');
                    let html = `
                        <div class="space-y-6">
                            <div class="card p-6">
                                <h3 class="text-lg font-semibold mb-4">General Settings</h3>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    `;
                    
                    // API Keys
                    html += `
                        <div>
                            <label class="form-label">Serper API Key</label>
                            <input type="password" id="serper_api_key" class="form-input" value="${config.serper_api_key || ''}">
                        </div>
                        <div>
                            <label class="form-label">OpenAI API Key</label>
                            <input type="password" id="openai_api_key" class="form-input" value="${config.openai_api_key || ''}">
                        </div>
                    `;
                    
                    // Scraper Settings
                    html += `
                        <div>
                            <label class="form-label">State</label>
                            <input type="text" id="state" class="form-input" value="${config.state || ''}">
                        </div>
                        <div>
                            <label class="form-label">Operating Mode</label>
                            <select id="operating_mode" class="form-select">
                                <option value="auto" ${config.operating_mode === 'auto' ? 'selected' : ''}>Auto</option>
                                <option value="manual" ${config.operating_mode === 'manual' ? 'selected' : ''}>Manual</option>
                            </select>
                        </div>
                        <div>
                            <label class="form-label">Searches per Cycle</label>
                            <input type="number" id="searches_per_cycle" class="form-input" value="${config.searches_per_cycle || 5}">
                        </div>
                        <div>
                            <label class="form-label">Businesses per Search</label>
                            <input type="number" id="businesses_per_search" class="form-input" value="${config.businesses_per_search || 10}">
                        </div>
                        <div>
                            <label class="form-label">Cycle Interval (seconds)</label>
                            <input type="number" id="cycle_interval" class="form-input" value="${config.cycle_interval || 300}">
                        </div>
                        <div>
                            <label class="form-label">Max Cycles</label>
                            <input type="number" id="max_cycles" class="form-input" value="${config.max_cycles || 100}">
                        </div>
                    `;
                    
                    html += `
                                </div>
                            </div>
                            
                            <div class="card p-6">
                                <h3 class="text-lg font-semibold mb-4">AI Settings</h3>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <div>
                                        <label class="form-label">AI Enabled</label>
                                        <select id="ai_enabled" class="form-select">
                                            <option value="true" ${config.ai_enrichment?.enabled ? 'selected' : ''}>Enabled</option>
                                            <option value="false" ${!config.ai_enrichment?.enabled ? 'selected' : ''}>Disabled</option>
                                        </select>
                                    </div>
                                    <div>
                                        <label class="form-label">Model</label>
                                        <input type="text" id="ai_model" class="form-input" value="${config.ai_enrichment?.model || 'gpt-4o-mini'}">
                                    </div>
                                    <div>
                                        <label class="form-label">Qualification Threshold</label>
                                        <input type="number" id="qualification_threshold" class="form-input" value="${config.ai_enrichment?.qualification_threshold || 60}">
                                    </div>
                                </div>
                            </div>
                            
                            <div class="flex justify-end space-x-3">
                                <button onclick="saveSettings()" class="btn-primary">
                                    <i class="fas fa-save mr-2"></i>Save Settings
                                </button>
                                <button onclick="loadSettings()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                                    <i class="fas fa-sync-alt mr-2"></i>Reload
                                </button>
                            </div>
                        </div>
                    `;
                    
                    container.innerHTML = html;
                    
                } catch (error) {
                    console.error('Error loading settings:', error);
                    showToast('Error loading settings: ' + error.message, 'error');
                }
            }
            
            async function saveSettings() {
                try {
                    // Get current config
                    const response = await fetch('/api/get-config');
                    const config = await response.json();
                    
                    // Update with form values
                    config.serper_api_key = document.getElementById('serper_api_key').value;
                    config.openai_api_key = document.getElementById('openai_api_key').value;
                    config.state = document.getElementById('state').value;
                    config.operating_mode = document.getElementById('operating_mode').value;
                    config.searches_per_cycle = parseInt(document.getElementById('searches_per_cycle').value);
                    config.businesses_per_search = parseInt(document.getElementById('businesses_per_search').value);
                    config.cycle_interval = parseInt(document.getElementById('cycle_interval').value);
                    config.max_cycles = parseInt(document.getElementById('max_cycles').value);
                    
                    // Update AI settings
                    if (!config.ai_enrichment) config.ai_enrichment = {};
                    config.ai_enrichment.enabled = document.getElementById('ai_enabled').value === 'true';
                    config.ai_enrichment.model = document.getElementById('ai_model').value;
                    config.ai_enrichment.qualification_threshold = parseInt(document.getElementById('qualification_threshold').value);
                    
                    // Save to server
                    const saveResponse = await fetch('/api/update-config', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(config)
                    });
                    
                    const result = await saveResponse.json();
                    
                    if (result.success) {
                        showToast('Settings saved successfully', 'success');
                    } else {
                        showToast('Error saving settings: ' + result.message, 'error');
                    }
                    
                } catch (error) {
                    console.error('Error saving settings:', error);
                    showToast('Error saving settings: ' + error.message, 'error');
                }
            }
            
            async function loadAdvancedSettings() {
                try {
                    const response = await fetch('/api/get-config');
                    const config = await response.json();
                    
                    const container = document.getElementById('advancedSettingsContainer');
                    let html = `
                        <div class="space-y-6">
                            <div class="card p-6">
                                <h3 class="text-lg font-semibold mb-4">JSON Configuration Editor</h3>
                                <p class="text-gray-400 mb-4">Edit the complete configuration as JSON. Be careful when making changes.</p>
                                <textarea id="configEditor" class="form-input h-96 font-mono text-sm">${JSON.stringify(config, null, 2)}</textarea>
                            </div>
                            
                            <div class="card p-6">
                                <h3 class="text-lg font-semibold mb-4">Configuration Sections</h3>
                                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    `;
                    
                    // List all config sections
                    const sections = [
                        { name: 'CRM Settings', path: 'crm', icon: 'fa-database' },
                        { name: 'Lead Management', path: 'lead_management', icon: 'fa-users-cog' },
                        { name: 'UI Theme', path: 'ui', icon: 'fa-palette' },
                        { name: 'Cities', path: 'cities', icon: 'fa-city' },
                        { name: 'Industries', path: 'industries', icon: 'fa-industry' },
                        { name: 'Search Phrases', path: 'search_phrases', icon: 'fa-search' },
                        { name: 'Blacklisted Domains', path: 'blacklisted_domains', icon: 'fa-ban' },
                        { name: 'Filters', path: 'filters', icon: 'fa-filter' },
                        { name: 'Storage', path: 'storage', icon: 'fa-hdd' },
                        { name: 'Dashboard', path: 'dashboard', icon: 'fa-tachometer-alt' }
                    ];
                    
                    sections.forEach(section => {
                        const sectionData = getNestedValue(config, section.path);
                        html += `
                            <div class="p-4 rounded-lg bg-gray-800/30 hover:bg-gray-800/50 transition-colors cursor-pointer" onclick="editSection('${section.path}')">
                                <div class="flex items-center mb-2">
                                    <i class="fas ${section.icon} text-blue-400 mr-2"></i>
                                    <span class="font-medium">${section.name}</span>
                                </div>
                                <div class="text-sm text-gray-400">
                                    ${sectionData ? (Array.isArray(sectionData) ? `${sectionData.length} items` : 'Configured') : 'Not set'}
                                </div>
                            </div>
                        `;
                    });
                    
                    html += `
                                </div>
                            </div>
                            
                            <div class="flex justify-end space-x-3">
                                <button onclick="saveAdvancedSettings()" class="btn-primary">
                                    <i class="fas fa-save mr-2"></i>Save Configuration
                                </button>
                                <button onclick="loadAdvancedSettings()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                                    <i class="fas fa-sync-alt mr-2"></i>Reload
                                </button>
                                <button onclick="showBackupConfig()" class="bg-yellow-800 text-yellow-300 hover:bg-yellow-700 px-4 py-2 rounded-lg">
                                    <i class="fas fa-history mr-2"></i>Backup
                                </button>
                            </div>
                        </div>
                    `;
                    
                    container.innerHTML = html;
                    
                } catch (error) {
                    console.error('Error loading advanced settings:', error);
                    showToast('Error loading advanced settings: ' + error.message, 'error');
                }
            }
            
            function getNestedValue(obj, path) {
                return path.split('.').reduce((acc, part) => acc && acc[part], obj);
            }
            
            function editSection(path) {
                const editor = document.getElementById('configEditor');
                const config = JSON.parse(editor.value);
                const value = getNestedValue(config, path);
                
                // For now, just select the section in the editor
                const jsonString = JSON.stringify(config, null, 2);
                const lines = jsonString.split('\\n');
                let startLine = 0;
                let endLine = lines.length - 1;
                
                // Simple search for the section
                const searchTerm = `"${path.split('.').pop()}"`;
                for (let i = 0; i < lines.length; i++) {
                    if (lines[i].includes(searchTerm + ':')) {
                        startLine = Math.max(0, i - 1);
                        // Find matching closing brace
                        let braceCount = 0;
                        for (let j = i; j < lines.length; j++) {
                            if (lines[j].includes('{')) braceCount++;
                            if (lines[j].includes('}')) braceCount--;
                            if (braceCount === 0 && j > i) {
                                endLine = j;
                                break;
                            }
                        }
                        break;
                    }
                }
                
                editor.focus();
                const startPos = lines.slice(0, startLine).join('\\n').length + (startLine > 0 ? 1 : 0);
                const endPos = lines.slice(0, endLine + 1).join('\\n').length;
                editor.setSelectionRange(startPos, endPos);
                
                showToast(`Selected section: ${path}`, 'info');
            }
            
            async function saveAdvancedSettings() {
                try {
                    const editor = document.getElementById('configEditor');
                    const configText = editor.value;
                    
                    // Validate JSON
                    const config = JSON.parse(configText);
                    
                    // Save to server
                    const response = await fetch('/api/update-config', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(config)
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        showToast('Configuration saved successfully', 'success');
                        if (result.backup) {
                            showToast(`Backup created: ${result.backup}`, 'info');
                        }
                    } else {
                        showToast('Error saving configuration: ' + result.message, 'error');
                    }
                    
                } catch (error) {
                    console.error('Error saving advanced settings:', error);
                    if (error instanceof SyntaxError) {
                        showToast('Invalid JSON: ' + error.message, 'error');
                    } else {
                        showToast('Error: ' + error.message, 'error');
                    }
                }
            }
            
            function showBackupConfig() {
                // Show current config in a modal
                const configEditor = document.getElementById('configEditor');
                const config = JSON.parse(configEditor.value);
                
                const backupConfig = {
                    ...config,
                    backup_timestamp: new Date().toISOString(),
                    backup_note: 'Manual backup from dashboard'
                };
                
                const blob = new Blob([JSON.stringify(backupConfig, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `config_backup_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
                
                showToast('Backup downloaded', 'success');
            }
            
            async function loadLogs() {
                try {
                    const response = await fetch('/api/get-logs');
                    const logs = await response.json();
                    
                    const container = document.getElementById('logsContainer');
                    let html = `
                        <div class="space-y-6">
                            <div class="card p-6">
                                <div class="flex justify-between items-center mb-4">
                                    <h3 class="text-lg font-semibold">System Logs</h3>
                                    <div class="flex space-x-2">
                                        <button onclick="loadLogs()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                                            <i class="fas fa-sync-alt mr-2"></i>Refresh
                                        </button>
                                        <button onclick="clearLogs()" class="bg-red-800 text-red-300 hover:bg-red-700 px-4 py-2 rounded-lg">
                                            <i class="fas fa-trash mr-2"></i>Clear Logs
                                        </button>
                                    </div>
                                </div>
                                
                                <div class="overflow-x-auto">
                                    <table class="w-full text-sm">
                                        <thead class="text-xs text-gray-400 uppercase bg-gray-800/50">
                                            <tr>
                                                <th class="px-4 py-3">Time</th>
                                                <th class="px-4 py-3">Level</th>
                                                <th class="px-4 py-3">Message</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                    `;
                    
                    if (logs.length === 0) {
                        html += `
                            <tr>
                                <td colspan="3" class="px-4 py-8 text-center text-gray-500">
                                    <i class="fas fa-clipboard-list text-2xl mb-2"></i>
                                    <p>No logs available</p>
                                </td>
                            </tr>
                        `;
                    } else {
                        logs.reverse().forEach(log => {
                            const timestamp = new Date(log.timestamp).toLocaleString();
                            const levelColor = {
                                'INFO': 'text-blue-400',
                                'SUCCESS': 'text-green-400',
                                'WARNING': 'text-yellow-400',
                                'ERROR': 'text-red-400',
                                'DEBUG': 'text-gray-400'
                            }[log.level] || 'text-gray-400';
                            
                            html += `
                                <tr class="border-t border-gray-800 hover:bg-gray-800/30">
                                    <td class="px-4 py-3 text-gray-400">${timestamp}</td>
                                    <td class="px-4 py-3">
                                        <span class="${levelColor} font-medium">${log.level}</span>
                                    </td>
                                    <td class="px-4 py-3">${log.message}</td>
                                </tr>
                            `;
                        });
                    }
                    
                    html += `
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    `;
                    
                    container.innerHTML = html;
                    
                } catch (error) {
                    console.error('Error loading logs:', error);
                    showToast('Error loading logs: ' + error.message, 'error');
                }
            }
            
            function clearLogs() {
                if (confirm('Are you sure you want to clear all logs? This cannot be undone.')) {
                    fetch('/api/clear-logs', { method: 'POST' })
                        .then(response => response.json())
                        .then(data => {
                            if (data.success) {
                                showToast('Logs cleared', 'success');
                                loadLogs();
                            } else {
                                showToast('Error clearing logs: ' + data.message, 'error');
                            }
                        })
                        .catch(error => {
                            showToast('Error: ' + error.message, 'error');
                        });
                }
            }
            </script>
        </body>
        </html>
        '''
    
    def render_dashboard(self):
        """Render comprehensive dashboard"""
        try:
            stats = self.crm.get_statistics()
            
            html = f"""
            <div class="space-y-6">
                <!-- Header -->
                <div class="flex justify-between items-center">
                    <div>
                        <h1 class="text-2xl font-bold">Dashboard Overview</h1>
                        <p class="text-gray-400">Real-time insights and performance metrics</p>
                    </div>
                    <div class="flex space-x-3">
                        <button onclick="exportLeads()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                            <i class="fas fa-download mr-2"></i>Export
                        </button>
                        <button onclick="loadView('leads')" class="btn-primary">
                            <i class="fas fa-eye mr-2"></i>View All Leads
                        </button>
                    </div>
                </div>
                
                <!-- Stats Grid -->
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <div class="stat-card p-6">
                        <div class="flex items-center justify-between">
                            <div>
                                <p class="text-gray-400">Total Leads</p>
                                <p class="text-3xl font-bold">{stats.get('overall', {}).get('total_leads', 0)}</p>
                            </div>
                            <div class="w-12 h-12 rounded-full bg-blue-900/30 flex items-center justify-center">
                                <i class="fas fa-users text-blue-400 text-xl"></i>
                            </div>
                        </div>
                        <div class="mt-4 text-sm text-gray-400">
                            <i class="fas fa-arrow-up text-green-400 mr-1"></i>
                            <span>{stats.get('overall', {}).get('new_leads', 0)} new today</span>
                        </div>
                    </div>
                    
                    <div class="stat-card p-6">
                        <div class="flex items-center justify-between">
                            <div>
                                <p class="text-gray-400">Estimated Value</p>
                                <p class="text-3xl font-bold">${stats.get('overall', {}).get('total_value', 0) or 0:,}</p>
                            </div>
                            <div class="w-12 h-12 rounded-full bg-green-900/30 flex items-center justify-center">
                                <i class="fas fa-dollar-sign text-green-400 text-xl"></i>
                            </div>
                        </div>
                        <div class="mt-4 text-sm text-gray-400">
                            <i class="fas fa-chart-line text-green-400 mr-1"></i>
                            <span>Potential revenue</span>
                        </div>
                    </div>
                    
                    <div class="stat-card p-6">
                        <div class="flex items-center justify-between">
                            <div>
                                <p class="text-gray-400">Avg Lead Score</p>
                                <p class="text-3xl font-bold">{stats.get('overall', {}).get('avg_score', 0) or 0:.1f}</p>
                            </div>
                            <div class="w-12 h-12 rounded-full bg-yellow-900/30 flex items-center justify-center">
                                <i class="fas fa-chart-line text-yellow-400 text-xl"></i>
                            </div>
                        </div>
                        <div class="mt-4">
                            <div class="h-2 w-full bg-gray-700 rounded-full overflow-hidden">
                                <div class="h-full bg-gradient-to-r from-yellow-500 to-orange-500" style="width: {min((stats.get('overall', {}).get('avg_score', 0) or 0), 100)}%"></div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="stat-card p-6">
                        <div class="flex items-center justify-between">
                            <div>
                                <p class="text-gray-400">Closed Won</p>
                                <p class="text-3xl font-bold">{stats.get('overall', {}).get('closed_won', 0)}</p>
                            </div>
                            <div class="w-12 h-12 rounded-full bg-purple-900/30 flex items-center justify-center">
                                <i class="fas fa-trophy text-purple-400 text-xl"></i>
                            </div>
                        </div>
                        <div class="mt-4 text-sm text-gray-400">
                            <span>Success rate: {(stats.get('overall', {}).get('closed_won', 0) / max(stats.get('overall', {}).get('total_leads', 1) or 1) * 100):.1f}%</span>
                        </div>
                    </div>
                </div>
                
                <!-- Charts and Recent Leads -->
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <!-- Quality Distribution -->
                    <div class="card p-6">
                        <h3 class="text-lg font-semibold mb-6">Lead Quality Distribution</h3>
                        <div class="space-y-4">
            """
            
            quality_dist = stats.get('quality_distribution', [])
            total = max(sum(item['count'] for item in quality_dist), 1)
            
            for item in quality_dist:
                tier = item['tier']
                count = item['count']
                percentage = (count / total * 100)
                
                tier_info = {
                    'Premium': {'color': 'from-yellow-500 to-orange-500', 'bg': 'bg-yellow-900/20'},
                    'High': {'color': 'from-green-500 to-emerald-500', 'bg': 'bg-green-900/20'},
                    'Medium': {'color': 'from-blue-500 to-indigo-500', 'bg': 'bg-blue-900/20'},
                    'Low': {'color': 'from-gray-500 to-gray-700', 'bg': 'bg-gray-900/20'}
                }.get(tier, {'color': 'from-gray-500 to-gray-700', 'bg': 'bg-gray-900/20'})
                
                html += f"""
                            <div>
                                <div class="flex justify-between mb-1">
                                    <span class="font-medium">{tier}</span>
                                    <span class="text-gray-400">{count} ({percentage:.1f}%)</span>
                                </div>
                                <div class="h-2 w-full bg-gray-800 rounded-full overflow-hidden">
                                    <div class="h-full bg-gradient-to-r {tier_info['color']}" style="width: {percentage}%"></div>
                                </div>
                            </div>
                """
            
            html += """
                        </div>
                    </div>
                    
                    <!-- Recent Leads -->
                    <div class="card p-6">
                        <div class="flex justify-between items-center mb-6">
                            <h3 class="text-lg font-semibold">Recent Leads</h3>
                            <a href="#" onclick="loadView('leads')" class="text-blue-400 hover:text-blue-300 text-sm">View All</a>
                        </div>
                        <div id="recentLeads">
                            <div class="text-center py-8 text-gray-500">
                                <div class="loading mx-auto mb-2"></div>
                                <p>Loading recent leads...</p>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Status Distribution -->
                <div class="card p-6">
                    <h3 class="text-lg font-semibold mb-6">Lead Status Overview</h3>
                    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            """
            
            status_dist = stats.get('status_distribution', [])
            for item in status_dist[:6]:  # Show top 6 statuses
                status = item['status']
                count = item['count']
                
                status_colors = {
                    'New Lead': 'bg-blue-900/30 text-blue-400',
                    'Contacted': 'bg-purple-900/30 text-purple-400',
                    'Meeting Scheduled': 'bg-green-900/30 text-green-400',
                    'Closed (Won)': 'bg-emerald-900/30 text-emerald-400',
                    'Closed (Lost)': 'bg-red-900/30 text-red-400',
                    'Follow Up': 'bg-yellow-900/30 text-yellow-400'
                }
                
                color_class = status_colors.get(status, 'bg-gray-900/30 text-gray-400')
                
                html += f"""
                        <div class="text-center p-4 rounded-lg {color_class}">
                            <div class="text-2xl font-bold mb-1">{count}</div>
                            <div class="text-sm font-medium">{status}</div>
                        </div>
                """
            
            html += """
                    </div>
                </div>
            </div>
            
            <script>
            // Load recent leads
            fetch('/api/recent-leads')
                .then(res => res.json())
                .then(leads => {
                    const container = document.getElementById('recentLeads');
                    if (!leads || leads.length === 0) {
                        container.innerHTML = '<div class="text-center py-8 text-gray-500"><i class="fas fa-inbox text-2xl mb-2"></i><p>No leads yet</p></div>';
                        return;
                    }
                    
                    let html = '<div class="space-y-3">';
                    leads.forEach(lead => {
                        const tierColor = lead.quality_tier === 'Premium' ? 'badge-premium' : 
                                         lead.quality_tier === 'High' ? 'badge-high' :
                                         lead.quality_tier === 'Medium' ? 'badge-medium' : 'badge-low';
                        
                        html += `
                        <div class="p-4 rounded-lg bg-gray-800/30 hover:bg-gray-800/50 transition-colors cursor-pointer" onclick="viewLead(${lead.id})">
                            <div class="flex justify-between items-start">
                                <div class="flex-1">
                                    <h4 class="font-semibold text-lg">${lead.business_name || 'Unknown'}</h4>
                                    <div class="flex flex-wrap items-center gap-2 mt-2 text-sm text-gray-400">
                                        <span class="flex items-center"><i class="fas fa-globe mr-1"></i> ${lead.website || 'No website'}</span>
                                        <span class="flex items-center"><i class="fas fa-phone mr-1"></i> ${lead.phone || 'No phone'}</span>
                                        <span class="flex items-center"><i class="fas fa-map-marker mr-1"></i> ${lead.city || 'Unknown'}</span>
                                        <span class="flex items-center"><i class="fas fa-industry mr-1"></i> ${lead.industry || 'Unknown'}</span>
                                    </div>
                                </div>
                                <div class="flex items-center space-x-2 ml-4">
                                    <span class="badge ${tierColor}">${lead.quality_tier || 'Unknown'}</span>
                                    <span class="badge bg-gray-700">${lead.lead_score || 0}</span>
                                </div>
                            </div>
                        </div>
                        `;
                    });
                    html += '</div>';
                    container.innerHTML = html;
                });
            
            function viewLead(id) {
                loadView('lead', { leadId: id });
            }
            </script>
            """
            return html
        except Exception as e:
            return f"""
            <div class="bg-red-900/20 border border-red-700 rounded-lg p-6">
                <h3 class="text-red-300 font-semibold text-lg mb-2">Error Loading Dashboard</h3>
                <p class="text-red-400 mb-4">{str(e)}</p>
                <button onclick="location.reload()" class="btn-primary">
                    <i class="fas fa-sync-alt mr-2"></i>Reload Page
                </button>
            </div>
            """
    
    def render_leads(self):
        """Render leads page with filters and table"""
        try:
            page = request.args.get('page', 1, type=int)
            status = request.args.get('status', '')
            quality = request.args.get('quality', '')
            city = request.args.get('city', '')
            search = request.args.get('search', '')
            
            filters = {}
            if status:
                filters['status'] = status
            if quality:
                filters['quality_tier'] = quality
            if city:
                filters['city'] = city
            if search:
                filters['search'] = search
            
            leads_data = self.crm.get_leads(filters=filters, page=page, per_page=20)
            
            status_options = CONFIG["lead_management"]["status_options"]
            quality_tiers = CONFIG["lead_management"]["quality_tiers"]
            cities = CONFIG["cities"]
            
            html = f"""
            <div class="space-y-6">
                <!-- Header -->
                <div class="flex justify-between items-center">
                    <div>
                        <h1 class="text-2xl font-bold">Leads Management</h1>
                        <p class="text-gray-400">Total: {leads_data['total']} leads | Page {page} of {leads_data['total_pages']}</p>
                    </div>
                    <div class="flex space-x-3">
                        <button onclick="exportLeads()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                            <i class="fas fa-download mr-2"></i>Export
                        </button>
                        <button onclick="document.getElementById('filters').classList.toggle('hidden')" class="btn-primary">
                            <i class="fas fa-filter mr-2"></i>Filters
                        </button>
                    </div>
                </div>
                
                <!-- Filters -->
                <div id="filters" class="card p-6 hidden">
                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                        <div>
                            <label class="form-label">Search</label>
                            <input type="text" id="searchFilter" value="{search}" placeholder="Search leads..." class="form-input" onkeyup="if(event.key==='Enter') applyFilters()">
                        </div>
                        <div>
                            <label class="form-label">Status</label>
                            <select id="statusFilter" class="form-select">
                                <option value="">All Statuses</option>
            """
            
            for status_opt in status_options:
                selected = "selected" if status_opt == status else ""
                html += f'<option value="{status_opt}" {selected}>{status_opt}</option>'
            
            html += """
                            </select>
                        </div>
                        <div>
                            <label class="form-label">Quality Tier</label>
                            <select id="qualityFilter" class="form-select">
                                <option value="">All Tiers</option>
            """
            
            for tier in quality_tiers:
                selected = "selected" if tier == quality else ""
                html += f'<option value="{tier}" {selected}>{tier}</option>'
            
            html += """
                            </select>
                        </div>
                        <div>
                            <label class="form-label">City</label>
                            <select id="cityFilter" class="form-select">
                                <option value="">All Cities</option>
            """
            
            for city_opt in cities:
                selected = "selected" if city_opt == city else ""
                html += f'<option value="{city_opt}" {selected}>{city_opt}</option>'
            
            html += """
                            </select>
                        </div>
                    </div>
                    <div class="flex justify-end space-x-3 mt-4">
                        <button onclick="applyFilters()" class="btn-primary">
                            <i class="fas fa-search mr-2"></i>Apply Filters
                        </button>
                        <button onclick="resetFilters()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                            <i class="fas fa-times mr-2"></i>Reset
                        </button>
                    </div>
                </div>
                
                <!-- Leads Table -->
                <div class="card overflow-hidden">
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="text-xs text-gray-400 uppercase bg-gray-800/50">
                                <tr>
                                    <th class="px-6 py-4">Business</th>
                                    <th class="px-6 py-4">Contact</th>
                                    <th class="px-6 py-4">Location</th>
                                    <th class="px-6 py-4">Score</th>
                                    <th class="px-6 py-4">Status</th>
                                    <th class="px-6 py-4">Actions</th>
                                </tr>
                            </thead>
                            <tbody>
            """
            
            for lead in leads_data['leads']:
                tier_color = {
                    'Premium': 'badge-premium',
                    'High': 'badge-high',
                    'Medium': 'badge-medium',
                    'Low': 'badge-low',
                    'Unknown': 'badge-low'
                }.get(lead.get('quality_tier', 'Unknown'), 'badge-low')
                
                html += f"""
                <tr class="border-t border-gray-800 hover:bg-gray-800/30 transition-colors">
                    <td class="px-6 py-4">
                        <div class="font-semibold">{lead.get('business_name', 'Unknown')}</div>
                        <div class="text-sm text-gray-400">{lead.get('industry', '')}</div>
                    </td>
                    <td class="px-6 py-4">
                        <div class="font-medium">{lead.get('phone', 'No phone')}</div>
                        <div class="text-sm text-gray-400 truncate max-w-xs">{lead.get('email', 'No email')}</div>
                    </td>
                    <td class="px-6 py-4">
                        <div>{lead.get('city', 'Unknown')}, {lead.get('state', '')}</div>
                        <div class="text-sm text-gray-400 truncate max-w-xs">{lead.get('website', 'No website')}</div>
                    </td>
                    <td class="px-6 py-4">
                        <div class="flex items-center space-x-2">
                            <div class="font-semibold text-lg">{lead.get('lead_score', 0)}</div>
                            <span class="badge {tier_color}">{lead.get('quality_tier', 'Unknown')}</span>
                        </div>
                    </td>
                    <td class="px-6 py-4">
                        <span class="badge bg-gray-700">{lead.get('lead_status', 'New Lead')}</span>
                    </td>
                    <td class="px-6 py-4">
                        <div class="flex space-x-2">
                            <button onclick="viewLeadDetail({lead['id']})" class="text-blue-400 hover:text-blue-300 p-2 rounded hover:bg-blue-900/20" title="View Details">
                                <i class="fas fa-eye"></i>
                            </button>
                            <button onclick="editLead({lead['id']})" class="text-green-400 hover:text-green-300 p-2 rounded hover:bg-green-900/20" title="Edit Lead">
                                <i class="fas fa-edit"></i>
                            </button>
                            <button onclick="callLead('{lead.get('phone', '').replace("'", "\\'")}')" class="text-purple-400 hover:text-purple-300 p-2 rounded hover:bg-purple-900/20" title="Call">
                                <i class="fas fa-phone"></i>
                            </button>
                        </div>
                    </td>
                </tr>
                """
            
            if not leads_data['leads']:
                html += """
                <tr>
                    <td colspan="6" class="text-center py-12 text-gray-500">
                        <i class="fas fa-inbox text-3xl mb-3"></i>
                        <p class="text-lg">No leads found</p>
                        <p class="text-sm mt-2">Try adjusting your filters or run the scraper</p>
                    </td>
                </tr>
                """
            
            html += """
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <!-- Pagination -->
                <div class="flex justify-between items-center">
                    <div class="text-sm text-gray-400">
                        Showing {((page - 1) * 20) + 1} to {min(page * 20, leads_data['total'])} of {leads_data['total']} leads
                    </div>
                    <div class="flex space-x-2">
            """
            
            if page > 1:
                html += f"""
                        <button onclick="changePage({page - 1})" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                            <i class="fas fa-chevron-left mr-2"></i>Previous
                        </button>
                """
            
            if page < leads_data['total_pages']:
                html += f"""
                        <button onclick="changePage({page + 1})" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                            Next<i class="fas fa-chevron-right ml-2"></i>
                        </button>
                """
            
            html += """
                    </div>
                </div>
            </div>
            
            <script>
            function applyFilters() {
                const search = document.getElementById('searchFilter').value;
                const status = document.getElementById('statusFilter').value;
                const quality = document.getElementById('qualityFilter').value;
                const city = document.getElementById('cityFilter').value;
                
                let url = `/api/leads?page=1`;
                if (search) url += `&search=${encodeURIComponent(search)}`;
                if (status) url += `&status=${encodeURIComponent(status)}`;
                if (quality) url += `&quality=${encodeURIComponent(quality)}`;
                if (city) url += `&city=${encodeURIComponent(city)}`;
                
                loadView('leads', { search, status, quality, city });
            }
            
            function resetFilters() {
                document.getElementById('searchFilter').value = '';
                document.getElementById('statusFilter').value = '';
                document.getElementById('qualityFilter').value = '';
                document.getElementById('cityFilter').value = '';
                loadView('leads');
            }
            
            function changePage(newPage) {
                const search = document.getElementById('searchFilter')?.value || '';
                const status = document.getElementById('statusFilter')?.value || '';
                const quality = document.getElementById('qualityFilter')?.value || '';
                const city = document.getElementById('cityFilter')?.value || '';
                
                let url = `/api/leads?page=${newPage}`;
                if (search) url += `&search=${encodeURIComponent(search)}`;
                if (status) url += `&status=${encodeURIComponent(status)}`;
                if (quality) url += `&quality=${encodeURIComponent(quality)}`;
                if (city) url += `&city=${encodeURIComponent(city)}`;
                
                loadView('leads', { page: newPage, search, status, quality, city });
            }
            
            function viewLeadDetail(id) {
                loadView('lead', { leadId: id });
            }
            
            function editLead(id) {
                // In a real implementation, this would open an edit modal
                showToast('Edit feature coming soon!', 'info');
            }
            
            function callLead(phone) {
                if (phone && phone !== 'No phone') {
                    window.open(`tel:${phone}`, '_blank');
                } else {
                    showToast('No phone number available', 'warning');
                }
            }
            </script>
            """
            
            return html
        except Exception as e:
            return f"""
            <div class="bg-red-900/20 border border-red-700 rounded-lg p-6">
                <h3 class="text-red-300 font-semibold text-lg mb-2">Error Loading Leads</h3>
                <p class="text-red-400 mb-4">{str(e)}</p>
                <button onclick="loadView('leads')" class="btn-primary">
                    <i class="fas fa-sync-alt mr-2"></i>Retry
                </button>
            </div>
            """
    
    def render_lead_detail(self, lead_id):
        """Render detailed lead view"""
        try:
            lead = self.crm.get_lead_by_id(lead_id)
            if not lead:
                return """
                <div class="bg-red-900/20 border border-red-700 rounded-lg p-6">
                    <h3 class="text-red-300 font-semibold text-lg mb-2">Lead Not Found</h3>
                    <p class="text-red-400 mb-4">The requested lead does not exist.</p>
                    <button onclick="loadView('leads')" class="btn-primary">
                        <i class="fas fa-arrow-left mr-2"></i>Back to Leads
                    </button>
                </div>
                """
            
            status_options = CONFIG["lead_management"]["status_options"]
            priority_options = CONFIG["lead_management"]["priority_options"]
            quality_tiers = CONFIG["lead_management"]["quality_tiers"]
            
            # Format dates
            created_at = lead.get('created_at', '')
            if created_at:
                try:
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    created_at = str(created_at)[:19]
            
            scraped_date = lead.get('scraped_date', '')
            if scraped_date:
                try:
                    scraped_date = datetime.fromisoformat(scraped_date.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    scraped_date = str(scraped_date)[:19]
            
            # Start building the HTML
            html_parts = []
            html_parts.append(f"""
            <div class="space-y-6">
                <!-- Header -->
                <div class="flex justify-between items-start">
                    <div>
                        <button onclick="loadView('leads')" class="text-gray-400 hover:text-white mb-4">
                            <i class="fas fa-arrow-left mr-2"></i>Back to Leads
                        </button>
                        <h1 class="text-2xl font-bold">{html.escape(lead.get('business_name', 'Unknown Business'))}</h1>
                        <p class="text-gray-400">Lead ID: {lead_id} | Created: {created_at}</p>
                    </div>
                    <div class="flex space-x-3">
                        <button onclick="exportLead({lead_id})" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                            <i class="fas fa-download mr-2"></i>Export
                        </button>
                        <button onclick="showEditModal()" class="btn-primary">
                            <i class="fas fa-edit mr-2"></i>Edit Lead
                        </button>
                    </div>
                </div>
                
                <!-- Main Content Grid -->
                <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    <!-- Left Column - Lead Info -->
                    <div class="lg:col-span-2 space-y-6">
                        <!-- Basic Info Card -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-info-circle text-blue-400 mr-2"></i>Basic Information
                            </h3>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div>
                                    <label class="form-label">Business Name</label>
                                    <div class="form-input bg-gray-800/50">{html.escape(lead.get('business_name', 'N/A'))}</div>
                                </div>
                                <div>
                                    <label class="form-label">Industry</label>
                                    <div class="form-input bg-gray-800/50">{html.escape(lead.get('industry', 'N/A'))}</div>
                                </div>
                                <div>
                                    <label class="form-label">Business Type</label>
                                    <div class="form-input bg-gray-800/50">{html.escape(lead.get('business_type', 'N/A'))}</div>
                                </div>
                                <div>
                                    <label class="form-label">Lead Score</label>
                                    <div class="form-input bg-gray-800/50 flex items-center">
                                        <span class="font-semibold text-xl mr-2">{lead.get('lead_score', 0)}</span>
                                        <span class="badge {'badge-premium' if lead.get('quality_tier') == 'Premium' else 'badge-high' if lead.get('quality_tier') == 'High' else 'badge-medium' if lead.get('quality_tier') == 'Medium' else 'badge-low'}">{lead.get('quality_tier', 'Unknown')}</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Contact Info Card -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-address-book text-green-400 mr-2"></i>Contact Information
                            </h3>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <div>
                                    <label class="form-label">Website</label>
                                    <div class="form-input bg-gray-800/50 truncate">
                                        <a href="{lead.get('website', '#')}" target="_blank" class="text-blue-400 hover:text-blue-300">
                                            {html.escape(lead.get('website', 'N/A'))}
                                        </a>
                                    </div>
                                </div>
                                <div>
                                    <label class="form-label">Phone</label>
                                    <div class="form-input bg-gray-800/50">
                                        <a href="tel:{lead.get('phone', '')}" class="text-green-400 hover:text-green-300">
                                            {html.escape(lead.get('phone', 'N/A'))}
                                        </a>
                                    </div>
                                </div>
                                <div>
                                    <label class="form-label">Email</label>
                                    <div class="form-input bg-gray-800/50 truncate">
                                        <a href="mailto:{lead.get('email', '')}" class="text-purple-400 hover:text-purple-300">
                                            {html.escape(lead.get('email', 'N/A'))}
                                        </a>
                                    </div>
                                </div>
                                <div>
                                    <label class="form-label">Address</label>
                                    <div class="form-input bg-gray-800/50">{html.escape(lead.get('address', 'N/A'))}</div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Description & Services -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-file-alt text-yellow-400 mr-2"></i>Description & Services
                            </h3>
                            <div class="space-y-4">
                                <div>
                                    <label class="form-label">Description</label>
                                    <div class="form-input bg-gray-800/50 min-h-[100px]">{html.escape(lead.get('description', 'No description available'))}</div>
                                </div>
                                <div>
                                    <label class="form-label">Services</label>
                                    <div class="form-input bg-gray-800/50 min-h-[60px]">
            """)
            
            services = lead.get('services', [])
            if isinstance(services, list) and services:
                html_parts.append('<div class="flex flex-wrap gap-2">')
                for service in services:
                    html_parts.append(f'<span class="badge bg-blue-900/50 text-blue-300">{html.escape(str(service))}</span>')
                html_parts.append('</div>')
            elif isinstance(services, str) and services:
                html_parts.append(f'<div>{html.escape(services)}</div>')
            else:
                html_parts.append('<div class="text-gray-500">No services listed</div>')
            
            html_parts.append("""
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- AI Notes -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-robot text-purple-400 mr-2"></i>AI Analysis
                            </h3>
                            <div class="form-input bg-gray-800/50 min-h-[150px]">
            """)
            
            ai_notes = lead.get('ai_notes', '')
            if ai_notes:
                html_parts.append(f'<div class="whitespace-pre-wrap">{html.escape(ai_notes)}</div>')
            else:
                html_parts.append('<div class="text-gray-500 italic">No AI analysis available</div>')
            
            html_parts.append("""
                            </div>
                        </div>
                    </div>
                    
                    <!-- Right Column - Status & Actions -->
                    <div class="space-y-6">
                        <!-- Status Card -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-chart-line text-red-400 mr-2"></i>Lead Status
                            </h3>
                            <div class="space-y-4">
                                <div>
                                    <label class="form-label">Current Status</label>
                                    <div class="form-input bg-gray-800/50 font-semibold">""")
            
            html_parts.append(f"{lead.get('lead_status', 'New Lead')}</div>")
            
            html_parts.append("""
                                </div>
                                <div>
                                    <label class="form-label">Priority</label>
                                    <div class="form-input bg-gray-800/50">""")
            
            html_parts.append(f"{lead.get('outreach_priority', 'Medium')}</div>")
            
            html_parts.append("""
                                </div>
                                <div>
                                    <label class="form-label">Potential Value</label>
                                    <div class="form-input bg-gray-800/50 font-semibold text-green-400">${lead.get('potential_value', 0):,}</div>
                                </div>
                                <div>
                                    <label class="form-label">Follow-up Date</label>
                                    <div class="form-input bg-gray-800/50">{lead.get('follow_up_date', 'Not set')}</div>
                                </div>
                                <div>
                                    <label class="form-label">Assigned To</label>
                                    <div class="form-input bg-gray-800/50">{lead.get('assigned_to', 'Unassigned')}</div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Social Media Card -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-share-alt text-blue-400 mr-2"></i>Social Media
                            </h3>
                            <div class="space-y-2">
            """)
            
            social_media = lead.get('social_media', {})
            if isinstance(social_media, dict) and social_media:
                platforms = {
                    'facebook': {'icon': 'fa-facebook', 'color': 'text-blue-500'},
                    'instagram': {'icon': 'fa-instagram', 'color': 'text-pink-500'},
                    'linkedin': {'icon': 'fa-linkedin', 'color': 'text-blue-600'},
                    'twitter': {'icon': 'fa-twitter', 'color': 'text-blue-400'},
                    'youtube': {'icon': 'fa-youtube', 'color': 'text-red-500'}
                }
                
                for platform, url in social_media.items():
                    if platform in platforms:
                        platform_info = platforms[platform]
                        html_parts.append(f"""
                                <a href="{url}" target="_blank" class="flex items-center p-2 rounded hover:bg-gray-800/50 transition-colors">
                                    <i class="fab {platform_info['icon']} {platform_info['color']} mr-3 text-lg"></i>
                                    <span class="flex-1">{platform.title()}</span>
                                    <i class="fas fa-external-link-alt text-gray-400"></i>
                                </a>
                        """)
            else:
                html_parts.append('<div class="text-gray-500 text-center py-4">No social media links found</div>')
            
            html_parts.append("""
                            </div>
                        </div>
                        
                        <!-- Source Information -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-database text-green-400 mr-2"></i>Source Information
                            </h3>
                            <div class="space-y-3 text-sm">
                                <div class="flex justify-between">
                                    <span class="text-gray-400">Source:</span>
                                    <span>""")
            
            html_parts.append(f"{lead.get('source', 'Web Scraper')}</span>")
            
            html_parts.append("""
                                </div>
                                <div class="flex justify-between">
                                    <span class="text-gray-400">Scraped Date:</span>
                                    <span>""")
            
            html_parts.append(f"{scraped_date}</span>")
            
            html_parts.append("""
                                </div>
                                <div class="flex justify-between">
                                    <span class="text-gray-400">Last Updated:</span>
                                    <span>{lead.get('last_updated', 'N/A')}</span>
                                </div>
                                <div class="flex justify-between">
                                    <span class="text-gray-400">Fingerprint:</span>
                                    <span class="font-mono text-xs truncate max-w-[150px]" title="{lead.get('fingerprint', '')}">{lead.get('fingerprint', '')[:20]}...</span>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Quick Actions -->
                        <div class="card p-6">
                            <h3 class="text-lg font-semibold mb-4 flex items-center">
                                <i class="fas fa-bolt text-yellow-400 mr-2"></i>Quick Actions
                            </h3>
                            <div class="grid grid-cols-2 gap-3">
                                <button onclick="callLead('""")
            
            html_parts.append(f"{lead.get('phone', '').replace(chr(39), chr(92)+chr(39))}')")            
            html_parts.append(""" class="bg-green-900/30 text-green-400 hover:bg-green-900/50 p-3 rounded-lg flex flex-col items-center">
                                    <i class="fas fa-phone text-xl mb-1"></i>
                                    <span class="text-sm">Call</span>
                                </button>
                                <button onclick="window.open('mailto:""")
            
            html_parts.append(f"{lead.get('email', '')}'")
            
            html_parts.append(""", '_blank')" class="bg-purple-900/30 text-purple-400 hover:bg-purple-900/50 p-3 rounded-lg flex flex-col items-center">
                                    <i class="fas fa-envelope text-xl mb-1"></i>
                                    <span class="text-sm">Email</span>
                                </button>
                                <button onclick="window.open('""")
            
            html_parts.append(f"{lead.get('website', '#')}'")
            
            html_parts.append(""", '_blank')" class="bg-blue-900/30 text-blue-400 hover:bg-blue-900/50 p-3 rounded-lg flex flex-col items-center">
                                    <i class="fas fa-globe text-xl mb-1"></i>
                                    <span class="text-sm">Website</span>
                                </button>
                                <button onclick="showNotesModal()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 p-3 rounded-lg flex flex-col items-center">
                                    <i class="fas fa-sticky-note text-xl mb-1"></i>
                                    <span class="text-sm">Notes</span>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Activities Timeline -->
                <div class="card p-6">
                    <h3 class="text-lg font-semibold mb-6 flex items-center">
                        <i class="fas fa-history text-indigo-400 mr-2"></i>Activity Timeline
                    </h3>
                    <div class="space-y-4">
            """)
            
            activities = lead.get('activities', [])
            if activities:
                for activity in activities[:10]:  # Show last 10 activities
                    performed_at = activity.get('performed_at', '')
                    if performed_at:
                        try:
                            performed_at = datetime.fromisoformat(performed_at.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            performed_at = str(performed_at)[:19]
                    
                    html_parts.append(f"""
                        <div class="flex items-start">
                            <div class="w-3 h-3 rounded-full bg-blue-500 mt-2 mr-4"></div>
                            <div class="flex-1">
                                <div class="font-medium">{activity.get('activity_type', 'Activity')}</div>
                                <div class="text-sm text-gray-400">{activity.get('activity_details', '')}</div>
                                <div class="text-xs text-gray-500 mt-1">{performed_at}</div>
                            </div>
                        </div>
                    """)
            else:
                html_parts.append('<div class="text-gray-500 text-center py-4">No activity recorded yet</div>')
            
            # Build the edit form options
            status_options_html = ""
            for status_opt in status_options:
                selected = "selected" if status_opt == lead.get('lead_status', '') else ""
                status_options_html += f'<option value="{status_opt}" {selected}>{status_opt}</option>'
            
            priority_options_html = ""
            for priority_opt in priority_options:
                selected = "selected" if priority_opt == lead.get('outreach_priority', '') else ""
                priority_options_html += f'<option value="{priority_opt}" {selected}>{priority_opt}</option>'
            
            quality_tiers_html = ""
            for tier_opt in quality_tiers:
                selected = "selected" if tier_opt == lead.get('quality_tier', '') else ""
                quality_tiers_html += f'<option value="{tier_opt}" {selected}>{tier_opt}</option>'
            
            html_parts.append(f"""
                    </div>
                </div>
            </div>
            
            <!-- Edit Modal -->
            <div id="editModal" class="fixed inset-0 bg-black/70 flex items-center justify-center p-4 z-50 hidden">
                <div class="bg-gray-900 rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
                    <div class="p-6">
                        <div class="flex justify-between items-center mb-6">
                            <h3 class="text-xl font-bold">Edit Lead</h3>
                            <button onclick="closeEditModal()" class="text-gray-400 hover:text-white">
                                <i class="fas fa-times text-xl"></i>
                            </button>
                        </div>
                        <div id="editForm" class="space-y-4">
                            <!-- Form will be loaded dynamically -->
                        </div>
                        <div class="flex justify-end space-x-3 mt-6">
                            <button onclick="closeEditModal()" class="bg-gray-800 text-gray-300 hover:bg-gray-700 px-4 py-2 rounded-lg">
                                Cancel
                            </button>
                            <button onclick="saveLeadChanges({lead_id})" class="btn-primary">
                                <i class="fas fa-save mr-2"></i>Save Changes
                            </button>
                        </div>
                    </div>
                </div>
            </div>
            
            <script>
            function showEditModal() {{
                const modal = document.getElementById('editModal');
                const form = document.getElementById('editForm');
                
                let html = `
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <div>
                            <label class="form-label">Lead Status</label>
                            <select id="editLeadStatus" class="form-select">
                                {status_options_html}
                            </select>
                        </div>
                        <div>
                            <label class="form-label">Priority</label>
                            <select id="editPriority" class="form-select">
                                {priority_options_html}
                            </select>
                        </div>
                        <div>
                            <label class="form-label">Quality Tier</label>
                            <select id="editQualityTier" class="form-select">
                                {quality_tiers_html}
                            </select>
                        </div>
                        <div>
                            <label class="form-label">Assigned To</label>
                            <input type="text" id="editAssignedTo" class="form-input" value="{html.escape(lead.get('assigned_to', ''))}">
                        </div>
                        <div class="md:col-span-2">
                            <label class="form-label">Notes</label>
                            <textarea id="editNotes" class="form-input h-32">{html.escape(lead.get('notes', ''))}</textarea>
                        </div>
                    </div>
                `;
                
                form.innerHTML = html;
                modal.classList.remove('hidden');
            }}
            
            function closeEditModal() {{
                document.getElementById('editModal').classList.add('hidden');
            }}
            
            async function saveLeadChanges(leadId) {{
                try {{
                    const updatedData = {{
                        lead_status: document.getElementById('editLeadStatus').value,
                        outreach_priority: document.getElementById('editPriority').value,
                        quality_tier: document.getElementById('editQualityTier').value,
                        assigned_to: document.getElementById('editAssignedTo').value,
                        notes: document.getElementById('editNotes').value
                    }};
                    
                    const response = await fetch(`/api/update-lead/${{leadId}}`, {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify(updatedData)
                    }});
                    
                    const result = await response.json();
                    
                    if (result.success) {{
                        showToast('Lead updated successfully', 'success');
                        closeEditModal();
                        // Reload the lead view
                        loadView('lead', {{ leadId: leadId }});
                    }} else {{
                        showToast('Error: ' + result.message, 'error');
                    }}
                    
                }} catch (error) {{
                    showToast('Error: ' + error.message, 'error');
                }}
            }}
            
            function showNotesModal() {{
                showToast('Notes feature coming soon!', 'info');
            }}
            
            function exportLead(id) {{
                // In a real implementation, this would generate a PDF or detailed CSV
                showToast('Export feature coming soon!', 'info');
            }}
            
            // Close modal on escape key
            document.addEventListener('keydown', function(e) {{
                if (e.key === 'Escape') {{
                    closeEditModal();
                }}
            }});
            
            // Close modal when clicking outside
            document.getElementById('editModal').addEventListener('click', function(e) {{
                if (e.target === this) {{
                    closeEditModal();
                }}
            }});
            </script>
            """)
            
            return ''.join(html_parts)
        except Exception as e:
            return f"""
            <div class="bg-red-900/20 border border-red-700 rounded-lg p-6">
                <h3 class="text-red-300 font-semibold text-lg mb-2">Error Loading Lead Details</h3>
                <p class="text-red-400 mb-4">{str(e)}</p>
                <button onclick="loadView('leads')" class="btn-primary">
                    <i class="fas fa-arrow-left mr-2"></i>Back to Leads
                </button>
            </div>
            """
    
    def render_settings(self):
        """Render settings page"""
        return """
        <div class="space-y-6">
            <div class="flex justify-between items-center">
                <div>
                    <h1 class="text-2xl font-bold">Settings</h1>
                    <p class="text-gray-400">Configure your LeadScraper application</p>
                </div>
            </div>
            
            <div id="settingsContainer">
                <div class="flex items-center justify-center h-96">
                    <div class="text-center">
                        <div class="loading mx-auto mb-4"></div>
                        <p class="text-gray-400">Loading settings...</p>
                    </div>
                </div>
            </div>
        </div>
        """
    
    def render_advanced_settings(self):
        """Render advanced settings page"""
        return """
        <div class="space-y-6">
            <div class="flex justify-between items-center">
                <div>
                    <h1 class="text-2xl font-bold">Advanced Settings</h1>
                    <p class="text-gray-400">Edit configuration files and system settings</p>
                </div>
            </div>
            
            <div id="advancedSettingsContainer">
                <div class="flex items-center justify-center h-96">
                    <div class="text-center">
                        <div class="loading mx-auto mb-4"></div>
                        <p class="text-gray-400">Loading advanced settings...</p>
                    </div>
                </div>
            </div>
        </div>
        """
    
    def render_logs(self):
        """Render logs page"""
        return """
        <div class="space-y-6">
            <div class="flex justify-between items-center">
                <div>
                    <h1 class="text-2xl font-bold">System Logs</h1>
                    <p class="text-gray-400">Monitor application activity and errors</p>
                </div>
            </div>
            
            <div id="logsContainer">
                <div class="flex items-center justify-center h-96">
                    <div class="text-center">
                        <div class="loading mx-auto mb-4"></div>
                        <p class="text-gray-400">Loading logs...</p>
                    </div>
                </div>
            </div>
        </div>
        """
    
    def export_leads(self):
        """Export leads as CSV"""
        try:
            leads_data = self.crm.get_leads(filters={}, page=1, per_page=10000)
            
            # Create CSV
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header with all fields
            writer.writerow([
                'ID', 'Business Name', 'Website', 'Phone', 'Email', 'Address',
                'City', 'State', 'Industry', 'Business Type', 'Services',
                'Description', 'Lead Score', 'Quality Tier', 'Potential Value',
                'Outreach Priority', 'Lead Status', 'Assigned To',
                'Lead Production Date', 'Follow-up Date', 'Notes', 'AI Notes',
                'Source', 'Scraped Date', 'Created At'
            ])
            
            # Write data
            for lead in leads_data['leads']:
                writer.writerow([
                    lead.get('id', ''),
                    lead.get('business_name', ''),
                    lead.get('website', ''),
                    lead.get('phone', ''),
                    lead.get('email', ''),
                    lead.get('address', ''),
                    lead.get('city', ''),
                    lead.get('state', ''),
                    lead.get('industry', ''),
                    lead.get('business_type', ''),
                    lead.get('services', '') if isinstance(lead.get('services'), str) else ', '.join(lead.get('services', [])),
                    lead.get('description', ''),
                    lead.get('lead_score', 0),
                    lead.get('quality_tier', ''),
                    lead.get('potential_value', 0),
                    lead.get('outreach_priority', ''),
                    lead.get('lead_status', ''),
                    lead.get('assigned_to', ''),
                    lead.get('lead_production_date', ''),
                    lead.get('follow_up_date', ''),
                    lead.get('notes', ''),
                    lead.get('ai_notes', ''),
                    lead.get('source', ''),
                    lead.get('scraped_date', ''),
                    lead.get('created_at', '')
                ])
            
            response = make_response(output.getvalue())
            response.headers["Content-Disposition"] = "attachment; filename=leads_export.csv"
            response.headers["Content-type"] = "text/csv"
            return response
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)})
    
    def run(self):
        """Run the dashboard"""
        if not self.enabled:
            logger.log("Dashboard not available", "WARNING")
            return
        
        port = CONFIG["dashboard"]["port"]
        host = CONFIG["dashboard"]["host"]
        
        logger.log(f"🌐 Starting comprehensive dashboard on http://{host}:{port}", "INFO")
        
        # Disable reloader for Windows compatibility
        self.app.run(
            host=host, 
            port=port, 
            debug=CONFIG["dashboard"]["debug"],
            use_reloader=False
        )

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 COMPREHENSIVE LEAD SCRAPER CRM")
    print("="*80)
    print("Features:")
    print("  ✅ Complete lead management with detailed views")
    print("  ✅ Full configuration editing from dashboard")
    print("  ✅ Real-time statistics and monitoring")
    print("  ✅ Advanced filtering and search")
    print("  ✅ AI-powered lead qualification")
    print("  ✅ Export functionality (CSV)")
    print("  ✅ System logs viewer")
    print("  ✅ Beautiful MitzMedia-inspired UI")
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
    
    # Create dashboard instance
    dashboard = ComprehensiveDashboard()
    
    if not dashboard.enabled:
        print("\n❌ Dashboard dependencies not installed")
        print("   Install with: pip install flask flask-cors")
        return
    
    print(f"\n🌐 Dashboard starting on port {CONFIG['dashboard']['port']}...")
    print(f"📱 Access at: http://localhost:{CONFIG['dashboard']['port']}")
    print("\n📊 Available features:")
    print("  • Dashboard with real-time stats")
    print("  • Lead management with filtering")
    print("  • Lead details view")
    print("  • Settings configuration")
    print("  • Advanced JSON config editor")
    print("  • System logs viewer")
    print("  • CSV export functionality")
    print("  • Auto-scraping with configurable intervals")
    print("="*80)
    
    # Run dashboard
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
