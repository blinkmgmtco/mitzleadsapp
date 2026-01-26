#!/usr/bin/env python3
"""
🚀 PROFESSIONAL LEAD SCRAPER CRM - PRODUCTION READY
Modern Design | Mobile Responsive | Full Featured
Inspired by MitzMedia.com
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
# ENVIRONMENT SETUP
# ============================================================================

if 'STREAMLIT_CLOUD' in os.environ:
    os.makedirs('/tmp/.leadscraper', exist_ok=True)
    CONFIG_FILE = '/tmp/.leadscraper/config.json'
    DB_FILE = '/tmp/.leadscraper/crm_database.db'
else:
    CONFIG_FILE = "config.json"
    DB_FILE = "crm_database.db"

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

DEFAULT_CONFIG = {
    "machine_id": "lead-scraper-crm-pro",
    "machine_version": "8.0",
    "serper_api_key": "YOUR_SERPER_API_KEY",
    "openai_api_key": "YOUR_OPENAI_API_KEY",
    
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
    
    "lead_management": {
        "default_follow_up_days": 7,
        "default_meeting_reminder_hours": 24,
        "auto_archive_days": 90,
        "status_options": [
            "New Lead", "Contacted", "No Answer", "Not Interested (NI)",
            "Follow Up", "Meeting Scheduled", "Closed (Won)", "Closed (Lost)",
            "Zoom Meeting", "Bad Lead", "Ghosted after Zoom", 
            "Ghosted after Followup", "Archived"
        ],
        "priority_options": ["Immediate", "High", "Medium", "Low"],
        "quality_tiers": ["Premium", "High", "Medium", "Low", "Unknown"]
    },
    
    "ui": {
        "theme": "professional",
        "primary_color": "#0A0E27",
        "secondary_color": "#1A1F3A",
        "accent_color": "#6366F1",
        "accent_light": "#818CF8",
        "success_color": "#10B981",
        "danger_color": "#EF4444",
        "warning_color": "#F59E0B",
        "info_color": "#3B82F6",
        "gradient_start": "#6366F1",
        "gradient_end": "#8B5CF6"
    },
    
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
    
    "directory_sources": [
        "yelp.com", "yellowpages.com", "bbb.org",
        "chamberofcommerce.com", "angi.com", "homeadvisor.com"
    ],
    
    "blacklisted_domains": [
        "facebook.com", "linkedin.com", "instagram.com", "twitter.com",
        "pinterest.com", "wikipedia.org", "youtube.com", "google.com"
    ],
    
    "operating_mode": "auto",
    "searches_per_cycle": 5,
    "businesses_per_search": 10,
    "cycle_interval": 300,
    "max_cycles": 100,
    
    "filters": {
        "exclude_chains": True,
        "exclude_without_websites": False,
        "exclude_without_phone": True,
        "min_rating": 3.0,
        "min_reviews": 1,
        "exclude_keywords": ["franchise", "national", "corporate", "chain"],
        "include_directory_listings": True,
        "directory_only_when_no_website": True
    },
    
    "enhanced_features": {
        "check_google_ads": True,
        "find_google_business": True,
        "scrape_yelp_reviews": False,
        "auto_social_media": True,
        "lead_scoring_ai": True,
        "extract_services": True,
        "detect_chain_businesses": True
    },
    
    "ai_enrichment": {
        "enabled": True,
        "model": "gpt-4o-mini",
        "max_tokens": 2000,
        "auto_qualify": True,
        "qualification_threshold": 60,
        "scoring_prompt": "Score this business for potential as a home services lead."
    },
    
    "storage": {
        "leads_file": "real_leads.json",
        "qualified_leads": "qualified_leads.json",
        "premium_leads": "premium_leads.json",
        "logs_file": "scraper_logs.json",
        "cache_file": "search_cache.json",
        "csv_export": "leads_export.csv",
        "directory_leads": "directory_leads.json"
    },
    
    "dashboard": {
        "port": 8501,
        "host": "0.0.0.0",
        "debug": False,
        "auto_refresh": True,
        "refresh_interval": 30000
    }
}

def load_config():
    """Load configuration with automatic fixes"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
        except:
            config = DEFAULT_CONFIG.copy()
    else:
        config = DEFAULT_CONFIG.copy()
    
    def deep_update(target, source):
        for key, value in source.items():
            if key not in target:
                target[key] = value
            elif isinstance(value, dict) and isinstance(target[key], dict):
                deep_update(target[key], value)
    
    deep_update(config, DEFAULT_CONFIG)
    
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    
    return config

CONFIG = load_config()

# ============================================================================
# LOGGER
# ============================================================================

class Logger:
    def __init__(self):
        self.log_file = CONFIG["storage"]["logs_file"]
    
    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        colors = {
            "INFO": "\033[94m",
            "SUCCESS": "\033[92m",
            "WARNING": "\033[93m",
            "ERROR": "\033[91m"
        }
        color = colors.get(level, "\033[0m")
        print(f"{color}[{timestamp}] {level}: {message}\033[0m")
        
        try:
            logs = []
            if os.path.exists(self.log_file):
                try:
                    with open(self.log_file, "r") as f:
                        logs = json.load(f)
                except:
                    pass
            
            logs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "message": message
            })
            
            if len(logs) > 1000:
                logs = logs[-1000:]
            
            with open(self.log_file, "w") as f:
                json.dump(logs, f, indent=2)
        except:
            pass

logger = Logger()

# ============================================================================
# DATABASE (Keeping original CRM_Database class)
# ============================================================================

class CRM_Database:
    """SQLite database for CRM"""
    
    def __init__(self):
        self.db_file = CONFIG["crm"]["database"]
        self.setup_database()
    
    def setup_database(self):
        """Initialize database"""
        try:
            conn = sqlite3.connect(self.db_file, check_same_thread=False)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='leads'")
            if cursor.fetchone():
                cursor.execute("PRAGMA table_info(leads)")
                columns = [col[1] for col in cursor.fetchall()]
                
                required_columns = {
                    'is_directory_listing': 'BOOLEAN DEFAULT 0',
                    'directory_source': 'TEXT',
                    'rating': 'REAL DEFAULT 0',
                    'review_count': 'INTEGER DEFAULT 0'
                }
                
                for col_name, col_type in required_columns.items():
                    if col_name not in columns:
                        try:
                            cursor.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}")
                        except:
                            pass
            else:
                cursor.execute('''
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
                        has_website BOOLEAN DEFAULT 1,
                        is_directory_listing BOOLEAN DEFAULT 0,
                        directory_source TEXT,
                        rating REAL DEFAULT 0,
                        review_count INTEGER DEFAULT 0,
                        years_in_business INTEGER,
                        employee_count TEXT,
                        annual_revenue TEXT
                    )
                ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER,
                    activity_type TEXT,
                    activity_details TEXT,
                    performed_by TEXT,
                    performed_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
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
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
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
            
            cursor.execute('''
                INSERT OR IGNORE INTO users (username, email, full_name, role)
                VALUES (?, ?, ?, ?)
            ''', ('admin', 'admin@leadscraper.com', 'Administrator', 'admin'))
            
            conn.commit()
            conn.close()
            logger.log("✅ Database initialized", "SUCCESS")
        except Exception as e:
            logger.log(f"❌ Database error: {e}", "ERROR")
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_file, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def save_lead(self, lead_data):
        """Save lead to database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            fingerprint = lead_data.get("fingerprint", "")
            
            if CONFIG["crm"]["prevent_duplicates"] and fingerprint:
                cursor.execute("SELECT id FROM leads WHERE fingerprint = ?", (fingerprint,))
                if cursor.fetchone():
                    return {"success": False, "message": "Duplicate lead"}
            
            business_name = lead_data.get("business_name", "Unknown Business")[:200]
            website = lead_data.get("website", "")[:200]
            phone = lead_data.get("phone", "") or ""
            email = lead_data.get("email", "") or ""
            address = lead_data.get("address", "") or ""
            city = lead_data.get("city", "") or ""
            state = lead_data.get("state", CONFIG["state"])
            industry = lead_data.get("industry", "") or ""
            
            has_website = lead_data.get("has_website", bool(website))
            is_directory = lead_data.get("is_directory_listing", False)
            directory_source = lead_data.get("directory_source", "")
            google_business = lead_data.get("google_business_profile", "") or ""
            running_ads = lead_data.get("running_google_ads", False)
            ads_url = lead_data.get("ad_transparency_url", "") or ""
            yelp_url = lead_data.get("yelp_url", "") or ""
            bbb_url = lead_data.get("bbb_url", "") or ""
            
            rating = lead_data.get("rating", 0)
            review_count = lead_data.get("review_count", 0)
            
            services = lead_data.get("services", "")
            if isinstance(services, list):
                services = json.dumps(services)
            
            social_media = lead_data.get("social_media", {})
            if isinstance(social_media, dict):
                social_media = json.dumps(social_media)
            
            quality_tier = lead_data.get("quality_tier", "Unknown")
            lead_score = lead_data.get("lead_score", 50)
            
            if lead_score >= 80:
                outreach_priority = "Immediate"
            elif lead_score >= 60:
                outreach_priority = "High"
            elif lead_score >= 40:
                outreach_priority = "Medium"
            else:
                outreach_priority = "Low"
            
            potential_value = lead_data.get("potential_value", 0)
            if not potential_value:
                tier_map = {
                    "Premium": 10000,
                    "High": 7500,
                    "Medium": 5000,
                    "Low": 2500,
                    "Unknown": 0
                }
                potential_value = tier_map.get(quality_tier, 0)
            
            cursor.execute('''
                INSERT INTO leads (
                    fingerprint, business_name, website, phone, email, address,
                    city, state, industry, business_type, services, description,
                    social_media, google_business_profile, running_google_ads,
                    ad_transparency_url, lead_score, quality_tier, potential_value,
                    outreach_priority, lead_status, assigned_to, lead_production_date,
                    follow_up_date, notes, ai_notes, source, scraped_date,
                    yelp_url, bbb_url, has_website, is_directory_listing, directory_source,
                    rating, review_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fingerprint, business_name, website, phone, email, address,
                city, state, industry, lead_data.get("business_type", "Unknown"),
                services[:1000], lead_data.get("description", "")[:2000],
                social_media[:1000], google_business, running_ads,
                ads_url, lead_score, quality_tier, potential_value,
                outreach_priority, CONFIG["crm"]["default_status"],
                CONFIG["crm"]["default_assigned_to"],
                datetime.now(timezone.utc).date().isoformat() if CONFIG["crm"]["auto_set_production_date"] else None,
                (datetime.now(timezone.utc) + timedelta(days=7)).date().isoformat(),
                "", lead_data.get("ai_notes", "")[:1000],
                "Web Scraper", lead_data.get("scraped_date", datetime.now(timezone.utc).isoformat()),
                yelp_url, bbb_url, has_website, is_directory, directory_source,
                rating, review_count
            ))
            
            lead_id = cursor.lastrowid
            
            source_type = "Directory" if is_directory else "Website"
            cursor.execute('''
                INSERT INTO activities (lead_id, activity_type, activity_details)
                VALUES (?, ?, ?)
            ''', (lead_id, "Lead Created", f"Lead scraped from {source_type}"))
            
            conn.commit()
            self.update_statistics()
            
            return {"success": True, "lead_id": lead_id, "message": "Lead saved"}
        except Exception as e:
            conn.rollback()
            logger.log(f"Save lead error: {e}", "ERROR")
            return {"success": False, "message": f"Error: {str(e)}"}
        finally:
            conn.close()
    
    def update_statistics(self):
        """Update statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
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
                    SUM(CASE WHEN running_google_ads = 1 THEN 1 ELSE 0 END) as leads_with_ads,
                    SUM(CASE WHEN is_directory_listing = 1 THEN 1 ELSE 0 END) as directory_leads
                FROM leads 
                WHERE DATE(created_at) = DATE('now') AND is_archived = 0
            ''')
            
            stats = cursor.fetchone()
            stats_tuple = tuple(stats) if stats else (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            
            cursor.execute('''
                INSERT OR REPLACE INTO statistics 
                (stat_date, total_leads, new_leads, contacted_leads, meetings_scheduled, 
                 closed_won, closed_lost, premium_leads, estimated_value, 
                 leads_without_website, leads_with_ads, directory_leads)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, *stats_tuple))
            
            conn.commit()
        except Exception as e:
            logger.log(f"Statistics update error: {e}", "ERROR")
        finally:
            conn.close()
    
    def get_leads(self, filters=None, page=1, per_page=50):
        """Get leads with pagination"""
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
                if filters.get("search"):
                    search_term = f"%{filters['search']}%"
                    conditions.append("(business_name LIKE ? OR website LIKE ? OR phone LIKE ?)")
                    params.extend([search_term, search_term, search_term])
                
                if conditions:
                    query += " AND " + " AND ".join(conditions)
            
            count_query = f"SELECT COUNT(*) FROM ({query})"
            cursor.execute(count_query, params)
            result = cursor.fetchone()
            total = result[0] if result else 0
            
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([per_page, (page - 1) * per_page])
            
            cursor.execute(query, params)
            leads = cursor.fetchall()
            
            result = []
            for lead in leads:
                lead_dict = dict(lead)
                
                if lead_dict.get("social_media") and isinstance(lead_dict["social_media"], str):
                    try:
                        lead_dict["social_media"] = json.loads(lead_dict["social_media"])
                    except:
                        pass
                
                if lead_dict.get("services") and isinstance(lead_dict["services"], str):
                    try:
                        lead_dict["services"] = json.loads(lead_dict["services"])
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
        """Get single lead"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
            lead = cursor.fetchone()
            
            if not lead:
                return None
            
            lead_dict = dict(lead)
            
            cursor.execute("SELECT * FROM activities WHERE lead_id = ? ORDER BY performed_at DESC", (lead_id,))
            activities = cursor.fetchall()
            lead_dict["activities"] = [dict(activity) for activity in activities]
            
            if lead_dict.get("social_media") and isinstance(lead_dict["social_media"], str):
                try:
                    lead_dict["social_media"] = json.loads(lead_dict["social_media"])
                except:
                    lead_dict["social_media"] = {}
            
            if lead_dict.get("services") and isinstance(lead_dict["services"], str):
                try:
                    lead_dict["services"] = json.loads(lead_dict["services"])
                except:
                    pass
            
            return lead_dict
        except Exception as e:
            logger.log(f"Get lead error: {e}", "ERROR")
            return None
        finally:
            conn.close()
    
    def update_lead(self, lead_id, update_data):
        """Update lead"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            set_clause = []
            params = []
            
            for field, value in update_data.items():
                set_clause.append(f"{field} = ?")
                params.append(value)
            
            params.append(lead_id)
            query = f"UPDATE leads SET {', '.join(set_clause)}, last_updated = CURRENT_TIMESTAMP WHERE id = ?"
            
            cursor.execute(query, params)
            
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
        """Archive lead"""
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
        """Get statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            stats = {}
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_leads,
                    SUM(CASE WHEN lead_status = 'New Lead' THEN 1 ELSE 0 END) as new_leads,
                    SUM(CASE WHEN lead_status = 'Closed (Won)' THEN 1 ELSE 0 END) as closed_won,
                    SUM(potential_value) as total_value,
                    AVG(lead_score) as avg_score,
                    SUM(CASE WHEN has_website = 0 THEN 1 ELSE 0 END) as leads_without_website,
                    SUM(CASE WHEN running_google_ads = 1 THEN 1 ELSE 0 END) as leads_with_ads,
                    SUM(CASE WHEN is_directory_listing = 1 THEN 1 ELSE 0 END) as directory_leads
                FROM leads 
                WHERE is_archived = 0
            ''')
            
            row = cursor.fetchone()
            stats["overall"] = {
                "total_leads": row[0] or 0,
                "new_leads": row[1] or 0,
                "closed_won": row[2] or 0,
                "total_value": row[3] or 0,
                "avg_score": float(row[4] or 0),
                "leads_without_website": row[5] or 0,
                "leads_with_ads": row[6] or 0,
                "directory_leads": row[7] or 0
            }
            
            cursor.execute('''
                SELECT lead_status, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0
                GROUP BY lead_status
                ORDER BY count DESC
            ''')
            stats["status_distribution"] = [{"status": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            cursor.execute('''
                SELECT quality_tier, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0 AND quality_tier != 'Unknown'
                GROUP BY quality_tier
            ''')
            stats["quality_distribution"] = [{"tier": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            cursor.execute(f'''
                SELECT DATE(created_at) as date, COUNT(*) as count
                FROM leads 
                WHERE is_archived = 0 AND created_at >= DATE('now', '-{days} days')
                GROUP BY DATE(created_at)
                ORDER BY date DESC
            ''')
            stats["daily_leads"] = [{"date": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            return stats
        except Exception as e:
            logger.log(f"Statistics error: {e}", "ERROR")
            return {
                "overall": {"total_leads": 0, "new_leads": 0, "closed_won": 0, "total_value": 0, "avg_score": 0},
                "status_distribution": [],
                "quality_distribution": [],
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
        except:
            return 0
        finally:
            conn.close()

# ============================================================================
# STREAMLIT DASHBOARD - MODERN REDESIGN
# ============================================================================

class ModernDashboard:
    """Production-ready modern dashboard"""
    
    def __init__(self):
        if not STREAMLIT_AVAILABLE:
            self.enabled = False
            return
        
        try:
            self.crm = CRM_Database()
            self.enabled = True
            
            st.set_page_config(
                page_title="LeadScraper Pro",
                page_icon="🚀",
                layout="wide",
                initial_sidebar_state="expanded"
            )
            
            self.setup_modern_css()
        except Exception as e:
            self.enabled = False
            logger.log(f"Dashboard error: {e}", "ERROR")
    
    def setup_modern_css(self):
        """Modern CSS inspired by MitzMedia"""
        st.markdown("""
        <style>
        /* Import Google Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
        
        /* Root Variables */
        :root {
            --primary: #0A0E27;
            --secondary: #1A1F3A;
            --accent: #6366F1;
            --accent-glow: rgba(99, 102, 241, 0.3);
            --success: #10B981;
            --danger: #EF4444;
            --warning: #F59E0B;
            --text-primary: #F9FAFB;
            --text-secondary: #9CA3AF;
            --border: rgba(255, 255, 255, 0.1);
        }
        
        /* Global Styles */
        * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        
        .stApp {
            background: linear-gradient(135deg, #0A0E27 0%, #1A1F3A 100%);
            color: var(--text-primary);
        }
        
        /* Hide Streamlit Branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Custom Header */
        .custom-header {
            background: linear-gradient(135deg, var(--accent) 0%, #8B5CF6 100%);
            padding: 2rem;
            border-radius: 20px;
            margin-bottom: 2rem;
            box-shadow: 0 20px 60px var(--accent-glow);
        }
        
        .custom-header h1 {
            font-size: 2.5rem;
            font-weight: 800;
            margin: 0;
            background: linear-gradient(135deg, #fff 0%, rgba(255,255,255,0.8) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .custom-header p {
            color: rgba(255, 255, 255, 0.9);
            font-size: 1.1rem;
            margin: 0.5rem 0 0 0;
        }
        
        /* Modern Cards */
        .modern-card {
            background: rgba(26, 31, 58, 0.6);
            backdrop-filter: blur(20px);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            transition: all 0.3s ease;
        }
        
        .modern-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
            border-color: var(--accent);
        }
        
        /* Metric Cards */
        .metric-card {
            background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.5rem;
            text-align: center;
            transition: all 0.3s ease;
        }
        
        .metric-card:hover {
            transform: scale(1.05);
            box-shadow: 0 10px 30px var(--accent-glow);
        }
        
        .metric-value {
            font-size: 2.5rem;
            font-weight: 800;
            background: linear-gradient(135deg, var(--accent) 0%, #8B5CF6 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0.5rem 0;
        }
        
        .metric-label {
            color: var(--text-secondary);
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }
        
        .metric-change {
            font-size: 0.875rem;
            margin-top: 0.5rem;
        }
        
        .metric-change.positive {
            color: var(--success);
        }
        
        .metric-change.negative {
            color: var(--danger);
        }
        
        /* Buttons */
        .stButton > button {
            background: linear-gradient(135deg, var(--accent) 0%, #8B5CF6 100%);
            color: white;
            border: none;
            border-radius: 12px;
            padding: 0.75rem 2rem;
            font-weight: 600;
            font-size: 1rem;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px var(--accent-glow);
        }
        
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 30px var(--accent-glow);
        }
        
        /* Badges */
        .badge {
            display: inline-block;
            padding: 0.375rem 0.875rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        
        .badge-premium {
            background: linear-gradient(135deg, #F59E0B 0%, #D97706 100%);
            color: white;
            box-shadow: 0 4px 12px rgba(245, 158, 11, 0.3);
        }
        
        .badge-high {
            background: linear-gradient(135deg, #10B981 0%, #059669 100%);
            color: white;
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
        }
        
        .badge-medium {
            background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);
            color: white;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        }
        
        .badge-low {
            background: linear-gradient(135deg, #6B7280 0%, #4B5563 100%);
            color: white;
        }
        
        /* Tables */
        .stDataFrame {
            border-radius: 12px;
            overflow: hidden;
        }
        
        .dataframe {
            background: rgba(26, 31, 58, 0.6) !important;
            border: 1px solid var(--border) !important;
        }
        
        .dataframe th {
            background: rgba(99, 102, 241, 0.2) !important;
            color: var(--text-primary) !important;
            font-weight: 600 !important;
            padding: 1rem !important;
        }
        
        .dataframe td {
            border-color: var(--border) !important;
            color: var(--text-secondary) !important;
            padding: 0.875rem !important;
        }
        
        .dataframe tr:hover {
            background: rgba(99, 102, 241, 0.1) !important;
        }
        
        /* Sidebar */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0A0E27 0%, #1A1F3A 100%);
            border-right: 1px solid var(--border);
        }
        
        [data-testid="stSidebar"] .stMarkdown {
            color: var(--text-primary);
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
            background: transparent;
        }
        
        .stTabs [data-baseweb="tab"] {
            background: rgba(26, 31, 58, 0.6);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 0.75rem 1.5rem;
            color: var(--text-secondary);
            font-weight: 600;
        }
        
        .stTabs [aria-selected="true"] {
            background: linear-gradient(135deg, var(--accent) 0%, #8B5CF6 100%);
            color: white;
            border-color: var(--accent);
        }
        
        /* Input Fields */
        .stTextInput input, .stSelectbox select, .stTextArea textarea {
            background: rgba(26, 31, 58, 0.6) !important;
            border: 1px solid var(--border) !important;
            border-radius: 12px !important;
            color: var(--text-primary) !important;
            padding: 0.75rem 1rem !important;
        }
        
        .stTextInput input:focus, .stSelectbox select:focus, .stTextArea textarea:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 2px var(--accent-glow) !important;
        }
        
        /* Progress Bar */
        .stProgress > div > div {
            background: linear-gradient(90deg, var(--accent) 0%, #8B5CF6 100%);
        }
        
        /* Expander */
        .streamlit-expanderHeader {
            background: rgba(26, 31, 58, 0.6);
            border: 1px solid var(--border);
            border-radius: 12px;
            color: var(--text-primary);
            font-weight: 600;
        }
        
        /* Status Indicator */
        .status-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 0.5rem;
            animation: pulse 2s infinite;
        }
        
        .status-active {
            background: var(--success);
            box-shadow: 0 0 10px var(--success);
        }
        
        .status-inactive {
            background: var(--danger);
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        /* Glassmorphism Effect */
        .glass {
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        
        /* Mobile Responsive */
        @media (max-width: 768px) {
            .custom-header h1 {
                font-size: 1.75rem;
            }
            
            .custom-header p {
                font-size: 0.875rem;
            }
            
            .metric-value {
                font-size: 2rem;
            }
            
            .modern-card {
                padding: 1rem;
            }
        }
        
        /* Animations */
        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .animate-in {
            animation: fadeIn 0.6s ease-out;
        }
        
        /* Charts */
        .js-plotly-plot {
            border-radius: 16px;
            overflow: hidden;
        }
        
        /* Lead Card */
        .lead-card {
            background: rgba(26, 31, 58, 0.6);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.25rem;
            margin-bottom: 1rem;
            transition: all 0.3s ease;
        }
        
        .lead-card:hover {
            border-color: var(--accent);
            transform: translateX(5px);
            box-shadow: -5px 0 20px var(--accent-glow);
        }
        
        .lead-name {
            font-size: 1.125rem;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 0.5rem;
        }
        
        .lead-info {
            font-size: 0.875rem;
            color: var(--text-secondary);
            margin-bottom: 0.25rem;
        }
        
        /* Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: var(--primary);
        }
        
        ::-webkit-scrollbar-thumb {
            background: var(--accent);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: #8B5CF6;
        }
        </style>
        """, unsafe_allow_html=True)
    
    def render_header(self):
        """Render modern header"""
        st.markdown("""
        <div class="custom-header animate-in">
            <h1>🚀 LeadScraper Pro</h1>
            <p>Professional CRM & Lead Generation Platform</p>
        </div>
        """, unsafe_allow_html=True)
    
    def render_sidebar(self):
        """Render sidebar with navigation"""
        with st.sidebar:
            st.markdown("""
            <div style="text-align: center; padding: 2rem 0;">
                <div style="font-size: 3rem; margin-bottom: 1rem;">🚀</div>
                <h2 style="margin: 0; font-weight: 800; background: linear-gradient(135deg, #6366F1 0%, #8B5CF6 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">LeadScraper</h2>
                <p style="color: #9CA3AF; margin: 0.5rem 0 0 0; font-size: 0.875rem;">PROFESSIONAL CRM</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation
            st.markdown("### 📊 Navigation")
            page = st.radio(
                "Select Page",
                ["Dashboard", "Leads", "Analytics", "Settings"],
                label_visibility="collapsed"
            )
            
            st.markdown("---")
            
            # Quick Stats
            st.markdown("### 📈 Quick Stats")
            
            today_count = self.crm.get_today_count()
            total_leads = self.crm.get_leads()["total"]
            stats = self.crm.get_statistics()
            
            st.markdown(f"""
            <div class="metric-card" style="margin-bottom: 1rem;">
                <div class="metric-label">Today</div>
                <div class="metric-value">{today_count}</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"""
            <div class="metric-card" style="margin-bottom: 1rem;">
                <div class="metric-label">Total Leads</div>
                <div class="metric-value">{total_leads}</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Avg Score</div>
                <div class="metric-value">{stats['overall']['avg_score']:.0f}</div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # System Status
            st.markdown("### 💻 System Status")
            
            serper_key = CONFIG.get("serper_api_key", "")
            openai_key = CONFIG.get("openai_api_key", "")
            
            serper_status = "✅ Connected" if serper_key and serper_key != "YOUR_SERPER_API_KEY" else "❌ Not configured"
            openai_status = "✅ Connected" if openai_key and openai_key != "YOUR_OPENAI_API_KEY" else "⚠️ Not configured"
            
            st.markdown(f"""
            <div class="modern-card" style="font-size: 0.875rem;">
                <div style="margin-bottom: 0.5rem;"><strong>Serper API:</strong> {serper_status}</div>
                <div style="margin-bottom: 0.5rem;"><strong>OpenAI API:</strong> {openai_status}</div>
                <div><strong>Database:</strong> ✅ Connected</div>
            </div>
            """, unsafe_allow_html=True)
            
            return page
    
    def render_dashboard(self):
        """Render main dashboard"""
        stats = self.crm.get_statistics()
        
        # Top Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card animate-in">
                <div class="metric-label">Total Leads</div>
                <div class="metric-value">{stats['overall']['total_leads']}</div>
                <div class="metric-change positive">↗ Active pipeline</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card animate-in" style="animation-delay: 0.1s;">
                <div class="metric-label">Estimated Value</div>
                <div class="metric-value">${stats['overall']['total_value']:,}</div>
                <div class="metric-change positive">↗ Revenue potential</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card animate-in" style="animation-delay: 0.2s;">
                <div class="metric-label">Avg Score</div>
                <div class="metric-value">{stats['overall']['avg_score']:.0f}</div>
                <div class="metric-change">Quality metric</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card animate-in" style="animation-delay: 0.3s;">
                <div class="metric-label">Closed Won</div>
                <div class="metric-value">{stats['overall']['closed_won']}</div>
                <div class="metric-change positive">↗ Conversions</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality Distribution
            quality_data = stats.get("quality_distribution", [])
            if quality_data:
                df_quality = pd.DataFrame(quality_data)
                fig = px.pie(
                    df_quality,
                    values='count',
                    names='tier',
                    title='Lead Quality Distribution',
                    color='tier',
                    color_discrete_map={
                        'Premium': '#F59E0B',
                        'High': '#10B981',
                        'Medium': '#3B82F6',
                        'Low': '#6B7280'
                    },
                    hole=0.4
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F9FAFB',
                    showlegend=True,
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Status Distribution
            status_data = stats.get("status_distribution", [])
            if status_data:
                df_status = pd.DataFrame(status_data[:8])  # Top 8 statuses
                fig = px.bar(
                    df_status,
                    x='count',
                    y='status',
                    orientation='h',
                    title='Lead Status Overview',
                    color='count',
                    color_continuous_scale=[[0, '#6366F1'], [1, '#8B5CF6']]
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F9FAFB',
                    showlegend=False,
                    height=350,
                    yaxis={'categoryorder': 'total ascending'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Daily Trend
        st.markdown("### 📅 Lead Acquisition Trend")
        daily_data = stats.get("daily_leads", [])
        if daily_data:
            df_daily = pd.DataFrame(daily_data)
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily = df_daily.sort_values('date')
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_daily['date'],
                y=df_daily['count'],
                mode='lines+markers',
                name='Leads',
                line=dict(color='#6366F1', width=3),
                marker=dict(size=8, color='#8B5CF6'),
                fill='tozeroy',
                fillcolor='rgba(99, 102, 241, 0.1)'
            ))
            
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#F9FAFB',
                showlegend=False,
                height=300,
                xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
                yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Recent Leads
        st.markdown("### 🆕 Recent Leads")
        leads_data = self.crm.get_leads(page=1, per_page=5)
        
        if leads_data["leads"]:
            for lead in leads_data["leads"]:
                tier = lead.get('quality_tier', 'Unknown')
                tier_class = f"badge-{tier.lower()}" if tier.lower() in ['premium', 'high', 'medium', 'low'] else "badge-low"
                
                st.markdown(f"""
                <div class="lead-card">
                    <div class="lead-name">{lead.get('business_name', 'Unknown')}</div>
                    <div class="lead-info">📍 {lead.get('city', 'Unknown')} • {lead.get('industry', 'Unknown')}</div>
                    <div class="lead-info">📊 Score: {lead.get('lead_score', 0)} • <span class="{tier_class}">{tier}</span></div>
                    <div class="lead-info">📞 {lead.get('phone', 'N/A')} • ✉️ {lead.get('email', 'N/A')}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No leads available. Start scraping to collect leads!")
    
    def render_leads(self):
        """Render leads page"""
        st.markdown("### 👥 Leads Management")
        
        # Filters
        with st.expander("🔍 Filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                search_term = st.text_input("Search", placeholder="Business name, phone, email...")
            with col2:
                status_filter = st.selectbox("Status", ["All"] + CONFIG["lead_management"]["status_options"])
            with col3:
                quality_filter = st.selectbox("Quality", ["All"] + CONFIG["lead_management"]["quality_tiers"])
        
        # Build filters
        filters = {}
        if search_term:
            filters["search"] = search_term
        if status_filter != "All":
            filters["status"] = status_filter
        if quality_filter != "All":
            filters["quality_tier"] = quality_filter
        
        # Get leads
        leads_data = self.crm.get_leads(filters=filters, page=1, per_page=50)
        
        st.metric("Total Leads", leads_data["total"])
        
        if leads_data["leads"]:
            display_data = []
            for lead in leads_data["leads"]:
                display_data.append({
                    "ID": lead.get("id"),
                    "Business": lead.get("business_name", "")[:40],
                    "Phone": lead.get("phone", ""),
                    "City": lead.get("city", ""),
                    "Score": lead.get("lead_score", 0),
                    "Quality": lead.get("quality_tier", "Unknown"),
                    "Status": lead.get("lead_status", "New Lead")
                })
            
            df = pd.DataFrame(display_data)
            st.dataframe(df, use_container_width=True, hide_index=True, height=600)
        else:
            st.info("No leads match the filters")
    
    def render_analytics(self):
        """Render analytics page"""
        st.markdown("### 📊 Analytics & Insights")
        
        stats = self.crm.get_statistics()
        
        # Key Metrics Grid
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            <div class="modern-card">
                <h4 style="color: #6366F1; margin-bottom: 1rem;">📈 Performance</h4>
                <div style="margin-bottom: 0.75rem;">
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Total Leads</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #F9FAFB;">""" + str(stats['overall']['total_leads']) + """</div>
                </div>
                <div style="margin-bottom: 0.75rem;">
                    <div style="color: #9CA3AF; font-size: 0.875rem;">New Leads</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #10B981;">""" + str(stats['overall']['new_leads']) + """</div>
                </div>
                <div>
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Closed Won</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #F59E0B;">""" + str(stats['overall']['closed_won']) + """</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="modern-card">
                <h4 style="color: #10B981; margin-bottom: 1rem;">💰 Revenue</h4>
                <div style="margin-bottom: 0.75rem;">
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Total Value</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #F9FAFB;">$""" + f"{stats['overall']['total_value']:,}" + """</div>
                </div>
                <div style="margin-bottom: 0.75rem;">
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Avg per Lead</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #10B981;">$""" + str(int(stats['overall']['total_value'] / max(stats['overall']['total_leads'], 1))) + """</div>
                </div>
                <div>
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Premium Leads</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #F59E0B;">""" + str(len([l for l in stats.get('quality_distribution', []) if l['tier'] == 'Premium'])) + """</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="modern-card">
                <h4 style="color: #F59E0B; margin-bottom: 1rem;">🎯 Quality</h4>
                <div style="margin-bottom: 0.75rem;">
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Avg Score</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #F9FAFB;">""" + f"{stats['overall']['avg_score']:.0f}" + """</div>
                </div>
                <div style="margin-bottom: 0.75rem;">
                    <div style="color: #9CA3AF; font-size: 0.875rem;">With Ads</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #8B5CF6;">""" + str(stats['overall'].get('leads_with_ads', 0)) + """</div>
                </div>
                <div>
                    <div style="color: #9CA3AF; font-size: 0.875rem;">Directory</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #3B82F6;">""" + str(stats['overall'].get('directory_leads', 0)) + """</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # Additional Analytics
        st.markdown("### 📊 Detailed Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            quality_data = stats.get("quality_distribution", [])
            if quality_data:
                df = pd.DataFrame(quality_data)
                fig = px.bar(
                    df,
                    x='tier',
                    y='count',
                    title='Quality Distribution',
                    color='tier',
                    color_discrete_map={
                        'Premium': '#F59E0B',
                        'High': '#10B981',
                        'Medium': '#3B82F6',
                        'Low': '#6B7280'
                    }
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F9FAFB',
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            status_data = stats.get("status_distribution", [])
            if status_data:
                df = pd.DataFrame(status_data[:6])
                fig = px.pie(
                    df,
                    values='count',
                    names='status',
                    title='Status Breakdown',
                    color_discrete_sequence=px.colors.sequential.Purples_r
                )
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#F9FAFB'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    def render_settings(self):
        """Render settings page"""
        st.markdown("### ⚙️ Settings")
        
        tab1, tab2, tab3 = st.tabs(["🔑 API Keys", "🎯 Targeting", "🏢 Business"])
        
        with tab1:
            st.markdown("#### API Configuration")
            
            col1, col2 = st.columns(2)
            
            with col1:
                current_serper = CONFIG.get("serper_api_key", "")
                new_serper = st.text_input("Serper API Key", value=current_serper, type="password")
                if new_serper != current_serper:
                    CONFIG["serper_api_key"] = new_serper
            
            with col2:
                current_openai = CONFIG.get("openai_api_key", "")
                new_openai = st.text_input("OpenAI API Key", value=current_openai, type="password")
                if new_openai != current_openai:
                    CONFIG["openai_api_key"] = new_openai
            
            if st.button("💾 Save API Keys", type="primary"):
                try:
                    with open(CONFIG_FILE, "w") as f:
                        json.dump(CONFIG, f, indent=2)
                    st.success("✅ API keys saved successfully!")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
        
        with tab2:
            st.markdown("#### Targeting Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                exclude_websites = CONFIG["filters"].get("exclude_without_websites", False)
                new_setting = st.toggle(
                    "Include businesses WITHOUT websites",
                    value=not exclude_websites
                )
                CONFIG["filters"]["exclude_without_websites"] = not new_setting
            
            with col2:
                include_directories = CONFIG["filters"].get("include_directory_listings", True)
                new_directories = st.toggle(
                    "Include directory listings",
                    value=include_directories
                )
                CONFIG["filters"]["include_directory_listings"] = new_directories
            
            if st.button("💾 Save Targeting Settings", type="primary"):
                try:
                    with open(CONFIG_FILE, "w") as f:
                        json.dump(CONFIG, f, indent=2)
                    st.success("✅ Settings saved successfully!")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
        
        with tab3:
            st.markdown("#### Business Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                CONFIG["state"] = st.text_input("State", value=CONFIG.get("state", "PA"))
            
            with col2:
                CONFIG["operating_mode"] = st.selectbox(
                    "Operating Mode",
                    options=["auto", "manual"],
                    index=0 if CONFIG.get("operating_mode", "auto") == "auto" else 1
                )
            
            if st.button("💾 Save Business Settings", type="primary"):
                try:
                    with open(CONFIG_FILE, "w") as f:
                        json.dump(CONFIG, f, indent=2)
                    st.success("✅ Settings saved successfully!")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
    
    def run(self):
        """Run dashboard"""
        if not self.enabled:
            st.error("Dashboard not available")
            return
        
        self.render_header()
        page = self.render_sidebar()
        
        if page == "Dashboard":
            self.render_dashboard()
        elif page == "Leads":
            self.render_leads()
        elif page == "Analytics":
            self.render_analytics()
        elif page == "Settings":
            self.render_settings()

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 LEADSCAPER PRO - PRODUCTION READY")
    print("="*80)
    print("✅ Modern design inspired by MitzMedia")
    print("✅ Fully responsive mobile interface")
    print("✅ Production-ready functionality")
    print("✅ Professional UI/UX")
    print("="*80)
    
    if not STREAMLIT_AVAILABLE:
        print("\n❌ Streamlit not installed")
        print("   Install: pip install streamlit pandas plotly streamlit-autorefresh")
        return
    
    print(f"\n🌐 Dashboard: http://localhost:8501")
    print("="*80)
    
    try:
        dashboard = ModernDashboard()
        dashboard.run()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not REQUESTS_AVAILABLE:
        print("❌ Install: pip install requests beautifulsoup4")
        sys.exit(1)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
