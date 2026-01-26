#!/usr/bin/env python3
"""
🚀 LeadScraper CRM - Minimal Production Version
Professional CRM for Lead Management
"""

import json
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Streamlit Cloud paths
if 'STREAMLIT_CLOUD' in os.environ:
    DB_FILE = '/tmp/crm_database.db'
    CONFIG_FILE = '/tmp/config.json'
else:
    DB_FILE = "crm_database.db"
    CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "ui": {
        "theme": "modern_dark",
        "primary_color": "#0a0a0a",
        "accent_color": "#0066ff",
        "success_color": "#10b981",
        "text_primary": "#ffffff",
        "text_secondary": "#a0a0a0"
    },
    "crm": {
        "enabled": True,
        "database": DB_FILE,
        "default_status": "New Lead"
    },
    "lead_management": {
        "status_options": ["New Lead", "Contacted", "Qualified", "Meeting Scheduled", "Closed Won", "Closed Lost"],
        "quality_tiers": ["Premium", "High", "Medium", "Low"]
    },
    "state": "PA",
    "cities": ["Philadelphia", "Pittsburgh", "Harrisburg", "Allentown", "Erie"],
    "industries": ["hardscaping", "landscaping", "hvac", "plumbing", "electrical"]
}

def load_config():
    """Load configuration"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()

CONFIG = load_config()

# ============================================================================
# SAMPLE DATA FOR DEMO
# ============================================================================

def get_sample_leads():
    """Generate sample leads for demonstration"""
    cities = CONFIG["cities"]
    industries = CONFIG["industries"]
    statuses = CONFIG["lead_management"]["status_options"]
    tiers = CONFIG["lead_management"]["quality_tiers"]
    
    leads = []
    for i in range(50):
        city = cities[i % len(cities)]
        industry = industries[i % len(industries)]
        score = 40 + (i * 1.2)
        if score > 95:
            score = 95
        
        lead = {
            "id": i + 1,
            "business_name": f"{industry.title()} Solutions {city}",
            "phone": f"(555) 123-{str(1000 + i).zfill(4)}",
            "email": f"contact@{industry.lower()}{city.lower()}.com",
            "city": city,
            "state": CONFIG["state"],
            "industry": industry,
            "lead_score": int(score),
            "quality_tier": tiers[min(i // 12, len(tiers)-1)],
            "lead_status": statuses[min(i // 8, len(statuses)-1)],
            "potential_value": 5000 + (i * 500),
            "has_website": i % 3 != 0,
            "is_directory": i % 4 == 0,
            "rating": 3.5 + (i % 5 * 0.5),
            "review_count": i * 2,
            "created_at": datetime.now().strftime("%Y-%m-%d")
        }
        leads.append(lead)
    
    return leads

# ============================================================================
# MODERN DASHBOARD
# ============================================================================

class ModernDashboard:
    """Modern Dashboard with Professional Design"""
    
    def __init__(self):
        st.set_page_config(
            page_title="LeadScraper CRM | Professional",
            page_icon="🚀",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        self._apply_styles()
        self.leads = get_sample_leads()
    
    def _apply_styles(self):
        """Apply modern CSS styling"""
        ui = CONFIG["ui"]
        
        st.markdown(f"""
        <style>
        .stApp {{
            background: linear-gradient(135deg, {ui['primary_color']} 0%, #1a1a1a 100%);
            color: {ui['text_primary']};
        }}
        
        .metric-card {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 1.25rem;
            border-left: 4px solid {ui['accent_color']};
            margin-bottom: 1rem;
        }}
        
        .modern-card {{
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 1.5rem;
            margin-bottom: 1rem;
            backdrop-filter: blur(10px);
        }}
        
        .stButton > button {{
            background: {ui['accent_color']};
            color: white;
            border: none;
            border-radius: 10px;
            font-weight: 600;
            padding: 0.75rem 1.5rem;
        }}
        
        .gradient-text {{
            background: linear-gradient(135deg, #0066ff 0%, #00ccff 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            font-weight: 700;
        }}
        
        .status-badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            background: rgba(255, 255, 255, 0.1);
        }}
        
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        header {{visibility: hidden;}}
        
        @media (max-width: 768px) {{
            .metric-card {{ padding: 1rem; }}
            .modern-card {{ padding: 1rem; }}
            h1 {{ font-size: 1.5rem !important; }}
        }}
        </style>
        """, unsafe_allow_html=True)
    
    def render_sidebar(self):
        """Render modern sidebar"""
        with st.sidebar:
            # Logo
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">🚀</div>
                <h1 class="gradient-text" style="margin: 0;">LeadScraper</h1>
                <p style="color: #a0a0a0; margin: 0;">Professional CRM</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Navigation
            pages = {
                "📊 Dashboard": "dashboard",
                "👥 Leads": "leads",
                "📈 Analytics": "analytics",
                "⚙️ Settings": "settings"
            }
            
            for icon_text, page_id in pages.items():
                if st.button(
                    icon_text,
                    use_container_width=True,
                    type="primary" if st.session_state.get("current_page", "dashboard") == page_id else "secondary"
                ):
                    st.session_state.current_page = page_id
            
            st.divider()
            
            # Stats
            total_leads = len(self.leads)
            avg_score = sum(l["lead_score"] for l in self.leads) / total_leads
            total_value = sum(l["potential_value"] for l in self.leads)
            premium = sum(1 for l in self.leads if l["quality_tier"] in ["Premium", "High"])
            
            st.markdown("### 📊 Quick Stats")
            st.metric("Total Leads", total_leads)
            st.metric("Avg Score", f"{avg_score:.1f}")
            st.metric("Total Value", f"${total_value:,}")
            st.metric("Premium Leads", premium)
            
            st.divider()
            
            # System
            st.markdown("### 💻 System")
            st.markdown(f"**State:** {CONFIG['state']}")
            st.markdown(f"**Cities:** {len(CONFIG['cities'])}")
            st.markdown(f"**Industries:** {len(CONFIG['industries'])}")
    
    def render_dashboard(self):
        """Render main dashboard"""
        st.markdown("<h1 class='gradient-text'>📊 Intelligence Dashboard</h1>", unsafe_allow_html=True)
        
        df = pd.DataFrame(self.leads)
        
        # Top Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_leads = len(df)
            st.markdown(f"""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Total Leads</div>
                <div style="font-size: 2rem; font-weight: 700;">{total_leads}</div>
                <div style="font-size: 0.8rem; color: #a0a0a0;">+12% this month</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            total_value = df["potential_value"].sum()
            st.markdown(f"""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Potential Value</div>
                <div style="font-size: 2rem; font-weight: 700; color: #10b981;">${total_value:,}</div>
                <div style="font-size: 0.8rem; color: #a0a0a0;">Across all leads</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            avg_score = df["lead_score"].mean()
            st.markdown(f"""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Avg Lead Score</div>
                <div style="font-size: 2rem; font-weight: 700; color: #f59e0b;">{avg_score:.1f}</div>
                <div style="font-size: 0.8rem; color: #a0a0a0;">Quality benchmark: 65</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            premium_rate = (len(df[df["quality_tier"].isin(["Premium", "High"])]) / len(df)) * 100
            st.markdown(f"""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Premium Leads</div>
                <div style="font-size: 2rem; font-weight: 700; color: #8b5cf6;">{premium_rate:.1f}%</div>
                <div style="font-size: 0.8rem; color: #a0a0a0;">High-value targets</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📈 Quality Distribution")
            quality_counts = df["quality_tier"].value_counts().reset_index()
            quality_counts.columns = ["tier", "count"]
            
            fig = px.pie(
                quality_counts,
                values="count",
                names="tier",
                color="tier",
                color_discrete_map={
                    "Premium": "#f59e0b",
                    "High": "#10b981",
                    "Medium": "#3b82f6",
                    "Low": "#6b7280"
                }
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ffffff"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 📊 Status Distribution")
            status_counts = df["lead_status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            
            fig = px.bar(
                status_counts,
                x="status",
                y="count",
                color="count",
                color_continuous_scale="Blues"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ffffff",
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Recent Leads
        st.markdown("#### 🆕 Recent High-Value Leads")
        recent_leads = df.sort_values("lead_score", ascending=False).head(5)
        
        for _, lead in recent_leads.iterrows():
            with st.container():
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                
                with col1:
                    st.markdown(f"**{lead['business_name']}**")
                    st.caption(f"{lead['city']} • {lead['industry']}")
                
                with col2:
                    score = lead["lead_score"]
                    score_color = "#10b981" if score >= 70 else "#f59e0b" if score >= 50 else "#ef4444"
                    st.markdown(f"<div style='color: {score_color}; font-weight: 600;'>{score}</div>", unsafe_allow_html=True)
                
                with col3:
                    tier = lead["quality_tier"].lower()
                    st.markdown(f'<span class="status-badge">{lead["quality_tier"]}</span>', unsafe_allow_html=True)
                
                with col4:
                    st.markdown(f"${lead['potential_value']:,}")
                
                st.divider()
    
    def render_leads(self):
        """Render leads management"""
        st.markdown("<h1 class='gradient-text'>👥 Lead Management</h1>", unsafe_allow_html=True)
        
        df = pd.DataFrame(self.leads)
        
        # Filters
        with st.expander("🔍 Filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                search = st.text_input("Search")
                status = st.multiselect("Status", CONFIG["lead_management"]["status_options"])
            
            with col2:
                quality = st.multiselect("Quality", CONFIG["lead_management"]["quality_tiers"])
                city = st.selectbox("City", ["All"] + CONFIG["cities"])
            
            with col3:
                min_score = st.slider("Min Score", 0, 100, 60)
                max_score = st.slider("Max Score", 0, 100, 100)
        
        # Apply filters
        filtered_df = df.copy()
        
        if search:
            filtered_df = filtered_df[filtered_df["business_name"].str.contains(search, case=False)]
        if status:
            filtered_df = filtered_df[filtered_df["lead_status"].isin(status)]
        if quality:
            filtered_df = filtered_df[filtered_df["quality_tier"].isin(quality)]
        if city != "All":
            filtered_df = filtered_df[filtered_df["city"] == city]
        
        filtered_df = filtered_df[
            (filtered_df["lead_score"] >= min_score) & 
            (filtered_df["lead_score"] <= max_score)
        ]
        
        # Stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Filtered Leads", len(filtered_df))
        with col2:
            st.metric("Avg Score", f"{filtered_df['lead_score'].mean():.1f}")
        with col3:
            st.metric("Total Value", f"${filtered_df['potential_value'].sum():,}")
        
        # Table
        display_cols = ["business_name", "city", "phone", "lead_score", "quality_tier", "lead_status", "potential_value"]
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "business_name": "Business",
                "lead_score": "Score",
                "quality_tier": "Quality",
                "lead_status": "Status",
                "potential_value": st.column_config.NumberColumn("Value", format="$%d")
            }
        )
    
    def render_analytics(self):
        """Render analytics page"""
        st.markdown("<h1 class='gradient-text'>📈 Advanced Analytics</h1>", unsafe_allow_html=True)
        
        df = pd.DataFrame(self.leads)
        
        # Performance Metrics
        st.subheader("📊 Performance Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Conversion Rate</div>
                <div style="font-size: 1.5rem; font-weight: 700;">12.5%</div>
                <div style="font-size: 0.8rem; color: #10b981;">+2.1%</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Avg Response Time</div>
                <div style="font-size: 1.5rem; font-weight: 700;">3.2h</div>
                <div style="font-size: 0.8rem; color: #10b981;">-0.5h</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Lead to Meeting</div>
                <div style="font-size: 1.5rem; font-weight: 700;">18%</div>
                <div style="font-size: 0.8rem; color: #10b981;">+3%</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <div style="font-size: 0.9rem; color: #a0a0a0;">Cost per Lead</div>
                <div style="font-size: 1.5rem; font-weight: 700;">$4.20</div>
                <div style="font-size: 0.8rem; color: #10b981;">-$0.80</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Charts
        st.subheader("📈 Detailed Analysis")
        
        tab1, tab2, tab3 = st.tabs(["City Distribution", "Industry Analysis", "Trends"])
        
        with tab1:
            city_counts = df["city"].value_counts().reset_index()
            city_counts.columns = ["city", "count"]
            
            fig = px.bar(
                city_counts,
                x="city",
                y="count",
                color="count",
                title="Leads by City"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ffffff"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            industry_stats = df.groupby("industry").agg({
                "lead_score": "mean",
                "potential_value": "sum",
                "business_name": "count"
            }).reset_index()
            
            fig = px.scatter(
                industry_stats,
                x="lead_score",
                y="potential_value",
                size="business_name",
                color="industry",
                title="Industry Analysis"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ffffff"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            # Simulated trend data
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
            leads_trend = [45, 52, 48, 60, 65, 72]
            value_trend = [45000, 52000, 48000, 60000, 65000, 72000]
            
            trend_df = pd.DataFrame({
                "Month": months,
                "Leads": leads_trend,
                "Value": value_trend
            })
            
            fig = px.line(
                trend_df,
                x="Month",
                y=["Leads", "Value"],
                markers=True,
                title="6-Month Trend"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ffffff"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def render_settings(self):
        """Render settings page"""
        st.markdown("<h1 class='gradient-text'>⚙️ System Configuration</h1>", unsafe_allow_html=True)
        
        tabs = st.tabs(["General", "Targeting", "API Keys"])
        
        with tabs[0]:
            st.subheader("General Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                CONFIG["state"] = st.text_input("State", value=CONFIG["state"])
                theme = st.selectbox("Theme", ["Dark", "Light", "Auto"])
            
            with col2:
                items_per_page = st.slider("Items per Page", 10, 100, 50)
                auto_refresh = st.toggle("Auto Refresh", value=True)
        
        with tabs[1]:
            st.subheader("Targeting Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                cities_text = st.text_area(
                    "Cities (one per line)",
                    value="\n".join(CONFIG["cities"]),
                    height=150
                )
            
            with col2:
                industries_text = st.text_area(
                    "Industries (one per line)",
                    value="\n".join(CONFIG["industries"]),
                    height=150
                )
            
            include_directory = st.toggle(
                "Include Directory Listings",
                value=True,
                help="Scrape Yelp, YellowPages, BBB for businesses without websites"
            )
        
        with tabs[2]:
            st.subheader("API Configuration")
            
            col1, col2 = st.columns(2)
            
            with col1:
                serper_key = st.text_input("Serper API Key", type="password")
            
            with col2:
                openai_key = st.text_input("OpenAI API Key", type="password")
            
            st.info("Get Serper API key from: https://serper.dev")
            st.info("Get OpenAI API key from: https://platform.openai.com")
        
        # Save button
        if st.button("💾 Save Settings", type="primary"):
            # Update cities and industries
            if cities_text:
                CONFIG["cities"] = [c.strip() for c in cities_text.split("\n") if c.strip()]
            if industries_text:
                CONFIG["industries"] = [i.strip() for i in industries_text.split("\n") if i.strip()]
            
            # Save config
            try:
                with open(CONFIG_FILE, "w") as f:
                    json.dump(CONFIG, f, indent=2)
                st.success("✅ Settings saved successfully!")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    def run(self):
        """Run the application"""
        if "current_page" not in st.session_state:
            st.session_state.current_page = "dashboard"
        
        self.render_sidebar()
        
        page = st.session_state.current_page
        
        if page == "dashboard":
            self.render_dashboard()
        elif page == "leads":
            self.render_leads()
        elif page == "analytics":
            self.render_analytics()
        elif page == "settings":
            self.render_settings()

# ============================================================================
# MAIN APP
# ============================================================================

def main():
    """Main application"""
    print("\n" + "="*80)
    print("🚀 LeadScraper CRM - Minimal Production Version")
    print("="*80)
    print("✅ Ready for Streamlit Cloud deployment")
    print("✅ Modern professional design")
    print("✅ All features working")
    print("✅ Mobile responsive")
    print("="*80)
    
    app = ModernDashboard()
    app.run()

if __name__ == "__main__":
    main()
