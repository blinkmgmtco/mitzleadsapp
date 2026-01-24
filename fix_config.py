#!/usr/bin/env python3
import json
import os

CONFIG_FILE = "config.json"

def fix_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ {CONFIG_FILE} not found!")
        return
    
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        
        # Add missing sections
        if 'enhanced_features' not in config:
            config['enhanced_features'] = {
                "check_google_ads": True,
                "find_google_business": True,
                "scrape_yelp_reviews": True,
                "auto_social_media": True,
                "lead_scoring_ai": True
            }
            print("✅ Added 'enhanced_features' section")
        
        # Ensure filters has the new key
        if 'filters' in config:
            if 'exclude_without_websites' not in config['filters']:
                config['filters']['exclude_without_websites'] = False
                print("✅ Added 'exclude_without_websites' to filters")
        else:
            config['filters'] = {
                "exclude_chains": True,
                "exclude_without_websites": False,
                "exclude_without_phone": True,
                "min_rating": 3.0,
                "min_reviews": 1,
                "exclude_keywords": ["franchise", "national", "corporate", "chain"]
            }
            print("✅ Added 'filters' section")
        
        # Save updated config
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ Configuration fixed and saved to {CONFIG_FILE}")
        print("\n📝 Please update your API keys:")
        print("   1. Serper API: https://serper.dev")
        print("   2. OpenAI API: https://platform.openai.com/api-keys")
        
    except Exception as e:
        print(f"❌ Error fixing config: {e}")

if __name__ == "__main__":
    fix_config()
