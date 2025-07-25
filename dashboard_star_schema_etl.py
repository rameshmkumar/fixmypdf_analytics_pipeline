"""
Enhanced Star Schema ETL Pipeline with Dashboard KPIs
Optimized for dashboard performance with pre-aggregated metrics
"""
import duckdb
import pandas as pd
import requests
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class DashboardStarSchemaETL:
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_KEY')
        self.duckdb_conn = duckdb.connect('data/dashboard_analytics.duckdb')
        os.makedirs('data', exist_ok=True)
        self.setup_dashboard_schema()
    
    def setup_dashboard_schema(self):
        """Create dashboard-optimized star schema"""
        
        print("🎛️ Creating Dashboard-Optimized Star Schema...")
        
        # Drop existing tables
        tables = ['fact_analytics', 'fact_daily_kpis', 'dim_tools', 'dim_time', 'dim_sessions', 'dim_event_types']
        for table in tables:
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table}")
        
        # CENTRAL FACT TABLE (Event-level detail)
        self.duckdb_conn.execute("""
            CREATE TABLE fact_analytics (
                analytics_key BIGINT PRIMARY KEY,
                tool_key VARCHAR,
                time_key VARCHAR,
                session_key VARCHAR,
                event_type_key VARCHAR,
                event_count INTEGER DEFAULT 1,
                upload_flag BOOLEAN DEFAULT FALSE,
                download_flag BOOLEAN DEFAULT FALSE,
                processing_flag BOOLEAN DEFAULT FALSE,
                error_flag BOOLEAN DEFAULT FALSE,
                file_size_bytes BIGINT,
                processing_time_ms BIGINT,
                event_id VARCHAR,
                user_id VARCHAR,
                url VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # DASHBOARD KPI FACT TABLE (Pre-aggregated for performance)
        self.duckdb_conn.execute("""
            CREATE TABLE fact_daily_kpis (
                kpi_key VARCHAR PRIMARY KEY,
                date DATE,
                tool_key VARCHAR,
                -- Core KPIs for Dashboard
                total_events INTEGER DEFAULT 0,
                total_uploads INTEGER DEFAULT 0,
                total_processing INTEGER DEFAULT 0,
                total_downloads INTEGER DEFAULT 0,
                total_errors INTEGER DEFAULT 0,
                unique_sessions INTEGER DEFAULT 0,
                unique_users INTEGER DEFAULT 0,
                page_views INTEGER DEFAULT 0,
                -- Conversion Metrics
                upload_to_processing_rate FLOAT DEFAULT 0.0,
                processing_to_download_rate FLOAT DEFAULT 0.0,
                upload_to_download_rate FLOAT DEFAULT 0.0,
                -- Performance Metrics
                avg_processing_time_ms FLOAT,
                avg_file_size_bytes FLOAT,
                avg_session_duration_min FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Dimensions (same as before but optimized)
        self.duckdb_conn.execute("""
            CREATE TABLE dim_tools (
                tool_key VARCHAR PRIMARY KEY,
                tool_name VARCHAR,
                tool_category VARCHAR,
                tool_display_name VARCHAR,
                tool_description VARCHAR,
                is_active BOOLEAN DEFAULT TRUE,
                -- Dashboard-specific attributes
                icon_name VARCHAR,
                sort_order INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.duckdb_conn.execute("""
            CREATE TABLE dim_time (
                time_key VARCHAR PRIMARY KEY,
                date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                day_of_week INTEGER,
                day_name VARCHAR,
                month_name VARCHAR,
                quarter INTEGER,
                is_weekend BOOLEAN,
                -- Dashboard time groupings
                date_label VARCHAR,  -- "Jul 25, 2025"
                week_start DATE,
                month_start DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.duckdb_conn.execute("""
            CREATE TABLE dim_sessions (
                session_key VARCHAR PRIMARY KEY,
                session_id VARCHAR,
                user_agent VARCHAR,
                browser VARCHAR,
                operating_system VARCHAR,
                device_type VARCHAR,
                language VARCHAR,
                referrer VARCHAR,
                session_start TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.duckdb_conn.execute("""
            CREATE TABLE dim_event_types (
                event_type_key VARCHAR PRIMARY KEY,
                event_type VARCHAR,
                event_category VARCHAR,
                event_description VARCHAR,
                is_conversion_event BOOLEAN DEFAULT FALSE,
                event_weight FLOAT DEFAULT 1.0,
                -- Dashboard display
                display_name VARCHAR,
                icon_class VARCHAR,
                color_code VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("✅ Dashboard Star Schema created: 2 Facts + 4 Dimensions")
    
    def extract_source_data(self):
        """Extract from all source tables"""
        
        tables = ['analytics_events', 'daily_tool_usage', 'session_analysis']
        data = {}
        
        headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
        }
        
        for table in tables:
            url = f"{self.supabase_url}/rest/v1/{table}"
            try:
                response = requests.get(url, headers=headers, params={'limit': 2000})
                if response.status_code == 200:
                    df = pd.DataFrame(response.json())
                    data[table] = df
                    print(f"📥 Extracted {len(df)} records from {table}")
                else:
                    print(f"❌ Error extracting {table}: {response.status_code}")
                    data[table] = pd.DataFrame()
            except Exception as e:
                print(f"❌ Exception extracting {table}: {e}")
                data[table] = pd.DataFrame()
        
        return data
    
    def build_enhanced_tools_dimension(self, events_df):
        """Build tools dimension with dashboard attributes"""
        
        if events_df.empty:
            return pd.DataFrame()
        
        print("🔧 Building enhanced tools dimension...")
        
        tools_data = events_df[['tool_name', 'tool_category']].drop_duplicates()
        
        # Enhanced tool metadata for dashboard
        tool_metadata = {
            'merge': {'icon': 'merge', 'sort': 1, 'desc': 'Combine multiple PDF files into one'},
            'nup': {'icon': 'grid', 'sort': 2, 'desc': 'Multiple pages per sheet layout'},
            'compressor': {'icon': 'compress', 'sort': 3, 'desc': 'Reduce PDF file size'},
            'split': {'icon': 'split', 'sort': 4, 'desc': 'Split PDF into separate pages'},
            'homepage': {'icon': 'home', 'sort': 99, 'desc': 'Main website landing page'},
            'pdf_bw': {'icon': 'palette', 'sort': 5, 'desc': 'Convert PDF to black and white'},
            'page_remover': {'icon': 'delete', 'sort': 6, 'desc': 'Remove specific pages from PDF'}
        }
        
        tools_records = []
        for _, row in tools_data.iterrows():
            if pd.notna(row['tool_name']):
                tool_key = f"tool_{row['tool_name']}"
                metadata = tool_metadata.get(row['tool_name'], {'icon': 'tool', 'sort': 50, 'desc': f"{row['tool_name']} tool"})
                
                tools_records.append({
                    'tool_key': tool_key,
                    'tool_name': row['tool_name'],
                    'tool_category': row['tool_category'],
                    'tool_display_name': row['tool_name'].replace('_', ' ').title(),
                    'tool_description': metadata['desc'],
                    'is_active': True,
                    'icon_name': metadata['icon'],
                    'sort_order': metadata['sort'],
                    'created_at': datetime.now()
                })
        
        tools_df = pd.DataFrame(tools_records)
        print(f"✅ Built {len(tools_df)} enhanced tools")
        return tools_df
    
    def build_enhanced_time_dimension(self, events_df):
        """Build time dimension with dashboard labels"""
        
        if events_df.empty:
            return pd.DataFrame()
        
        print("🕒 Building enhanced time dimension...")
        
        time_data = events_df[['date', 'hour']].drop_duplicates()
        
        time_records = []
        for _, row in time_data.iterrows():
            if pd.notna(row['date']):
                date_obj = pd.to_datetime(row['date'])
                time_key = f"{row['date']}_{row['hour']:02d}"
                
                # Dashboard-friendly labels
                date_label = date_obj.strftime('%b %d, %Y')
                week_start = date_obj - pd.Timedelta(days=date_obj.weekday())
                month_start = date_obj.replace(day=1)
                
                time_records.append({
                    'time_key': time_key,
                    'date': row['date'],
                    'year': date_obj.year,
                    'month': date_obj.month,
                    'day': date_obj.day,
                    'hour': row['hour'],
                    'day_of_week': date_obj.weekday(),
                    'day_name': date_obj.strftime('%A'),
                    'month_name': date_obj.strftime('%B'),
                    'quarter': (date_obj.month - 1) // 3 + 1,
                    'is_weekend': date_obj.weekday() >= 5,
                    'date_label': date_label,
                    'week_start': week_start.date(),
                    'month_start': month_start.date(),
                    'created_at': datetime.now()
                })
        
        time_df = pd.DataFrame(time_records)
        print(f"✅ Built {len(time_df)} enhanced time records")
        return time_df
    
    def build_enhanced_event_types_dimension(self):
        """Build event types with dashboard display attributes"""
        
        print("📋 Building enhanced event types dimension...")
        
        event_types = [
            {
                'event_type_key': 'evt_page_view',
                'event_type': 'page_view',
                'event_category': 'Navigation',
                'event_description': 'User viewed a page',
                'is_conversion_event': False,
                'event_weight': 1.0,
                'display_name': 'Page Views',
                'icon_class': 'eye',
                'color_code': '#3B82F6'
            },
            {
                'event_type_key': 'evt_file_upload',
                'event_type': 'file_upload_started',
                'event_category': 'Engagement',
                'event_description': 'User uploaded a file',
                'is_conversion_event': True,
                'event_weight': 3.0,
                'display_name': 'File Uploads',
                'icon_class': 'upload',
                'color_code': '#10B981'
            },
            {
                'event_type_key': 'evt_processing',
                'event_type': 'processing_started',
                'event_category': 'Action',
                'event_description': 'File processing started',
                'is_conversion_event': True,
                'event_weight': 2.0,
                'display_name': 'Processing',
                'icon_class': 'cog',
                'color_code': '#F59E0B'
            },
            {
                'event_type_key': 'evt_download',
                'event_type': 'file_downloaded',
                'event_category': 'Conversion',
                'event_description': 'User downloaded processed file',
                'is_conversion_event': True,
                'event_weight': 5.0,
                'display_name': 'Downloads',
                'icon_class': 'download',
                'color_code': '#EF4444'
            },
            {
                'event_type_key': 'evt_session_end',
                'event_type': 'session_end',
                'event_category': 'Session',
                'event_description': 'User session ended',
                'is_conversion_event': False,
                'event_weight': 0.5,
                'display_name': 'Session Ends',
                'icon_class': 'logout',
                'color_code': '#6B7280'
            },
            {
                'event_type_key': 'evt_error',
                'event_type': 'error_occurred',
                'event_category': 'Error',
                'event_description': 'An error occurred',
                'is_conversion_event': False,
                'event_weight': -1.0,
                'display_name': 'Errors',
                'icon_class': 'exclamation',
                'color_code': '#DC2626'
            }
        ]
        
        for event_type in event_types:
            event_type['created_at'] = datetime.now()
        
        event_types_df = pd.DataFrame(event_types)
        print(f"✅ Built {len(event_types_df)} enhanced event types")
        return event_types_df
    
    def build_sessions_dimension(self, events_df):
        """Build sessions dimension (same as before)"""
        
        if events_df.empty:
            return pd.DataFrame()
        
        print("👥 Building sessions dimension...")
        
        sessions_data = events_df.groupby('session_id').agg({
            'timestamp': 'min',
            'properties': 'first'
        }).reset_index()
        
        sessions_records = []
        for _, row in sessions_data.iterrows():
            try:
                props = {}
                if isinstance(row['properties'], str):
                    import json
                    props = json.loads(row['properties'].replace("'", '"'))
                
                user_agent = props.get('user_agent', '')
                
                # Simple parsing
                browser = 'Unknown'
                if 'Chrome' in user_agent and 'Safari' in user_agent:
                    browser = 'Chrome'
                elif 'Safari' in user_agent and 'Chrome' not in user_agent:
                    browser = 'Safari'
                elif 'Firefox' in user_agent:
                    browser = 'Firefox'
                
                os = 'Unknown'
                if 'Windows' in user_agent:
                    os = 'Windows'
                elif 'Mac' in user_agent:
                    os = 'macOS'
                elif 'iPhone' in user_agent:
                    os = 'iOS'
                elif 'Android' in user_agent:
                    os = 'Android'
                
                device_type = 'Mobile' if any(x in user_agent for x in ['iPhone', 'Android', 'Mobile']) else 'Desktop'
                session_key = f"session_{row['session_id']}"
                
                sessions_records.append({
                    'session_key': session_key,
                    'session_id': row['session_id'],
                    'user_agent': user_agent,
                    'browser': browser,
                    'operating_system': os,
                    'device_type': device_type,
                    'language': props.get('language', ''),
                    'referrer': props.get('referrer', ''),
                    'session_start': row['timestamp'],
                    'created_at': datetime.now()
                })
                
            except Exception as e:
                session_key = f"session_{row['session_id']}"
                sessions_records.append({
                    'session_key': session_key,
                    'session_id': row['session_id'],
                    'user_agent': '',
                    'browser': 'Unknown',
                    'operating_system': 'Unknown', 
                    'device_type': 'Unknown',
                    'language': '',
                    'referrer': '',
                    'session_start': row['timestamp'],
                    'created_at': datetime.now()
                })
        
        sessions_df = pd.DataFrame(sessions_records)
        print(f"✅ Built {len(sessions_df)} sessions")
        return sessions_df
    
    def build_fact_analytics(self, events_df):
        """Build detailed fact table"""
        
        if events_df.empty:
            return pd.DataFrame()
        
        print("⭐ Building detailed fact table...")
        
        fact_records = []
        
        for idx, row in events_df.iterrows():
            try:
                tool_key = f"tool_{row['tool_name']}" if pd.notna(row['tool_name']) else None
                time_key = f"{row['date']}_{row['hour']:02d}" if pd.notna(row['date']) and pd.notna(row['hour']) else None
                session_key = f"session_{row['session_id']}" if pd.notna(row['session_id']) else None
                
                event_type_map = {
                    'page_view': 'evt_page_view',
                    'file_upload_started': 'evt_file_upload',
                    'processing_started': 'evt_processing',
                    'file_downloaded': 'evt_download',
                    'session_end': 'evt_session_end',
                    'error_occurred': 'evt_error'
                }
                event_type_key = event_type_map.get(row['event_type'], 'evt_page_view')
                
                # Enhanced flags
                upload_flag = row['event_type'] == 'file_upload_started'
                download_flag = row['event_type'] == 'file_downloaded'
                processing_flag = row['event_type'] == 'processing_started'
                error_flag = row['event_type'] == 'error_occurred'
                
                # Parse properties for file size and processing time
                props = {}
                try:
                    if isinstance(row['properties'], str):
                        import json
                        props = json.loads(row['properties'].replace("'", '"'))
                except:
                    props = {}
                
                fact_records.append({
                    'analytics_key': idx + 1,
                    'tool_key': tool_key,
                    'time_key': time_key,
                    'session_key': session_key,
                    'event_type_key': event_type_key,
                    'event_count': 1,
                    'upload_flag': upload_flag,
                    'download_flag': download_flag,
                    'processing_flag': processing_flag,
                    'error_flag': error_flag,
                    'file_size_bytes': props.get('file_size'),
                    'processing_time_ms': props.get('processing_time_ms'),
                    'event_id': row['event_id'],
                    'user_id': row['user_id'],
                    'url': row['url'],
                    'created_at': datetime.now()
                })
                
            except Exception as e:
                print(f"Warning: Skipping row {idx} due to error: {e}")
                continue
        
        fact_df = pd.DataFrame(fact_records)
        print(f"✅ Built {len(fact_df)} detailed fact records")
        return fact_df
    
    def build_daily_kpis_fact(self, daily_df):
        """Build pre-aggregated KPIs fact table for dashboard performance"""
        
        if daily_df.empty:
            return pd.DataFrame()
        
        print("📊 Building daily KPIs fact table...")
        
        kpi_records = []
        
        for _, row in daily_df.iterrows():
            try:
                kpi_key = f"{row['date']}_{row['tool_name']}"
                tool_key = f"tool_{row['tool_name']}"
                
                # Calculate conversion rates
                uploads = row['file_uploads'] if pd.notna(row['file_uploads']) else 0
                processing = row['processing_started'] if pd.notna(row['processing_started']) else 0
                downloads = row['downloads'] if pd.notna(row['downloads']) else 0
                
                upload_to_processing_rate = (processing / uploads * 100) if uploads > 0 else 0
                processing_to_download_rate = (downloads / processing * 100) if processing > 0 else 0
                upload_to_download_rate = (downloads / uploads * 100) if uploads > 0 else 0
                
                kpi_records.append({
                    'kpi_key': kpi_key,
                    'date': row['date'],
                    'tool_key': tool_key,
                    'total_events': row['total_events'],
                    'total_uploads': uploads,
                    'total_processing': processing,
                    'total_downloads': downloads,
                    'total_errors': row['errors'] if pd.notna(row['errors']) else 0,
                    'unique_sessions': row['unique_sessions'],
                    'unique_users': row['unique_users'],
                    'page_views': row['page_views'],
                    'upload_to_processing_rate': round(upload_to_processing_rate, 2),
                    'processing_to_download_rate': round(processing_to_download_rate, 2),
                    'upload_to_download_rate': round(upload_to_download_rate, 2),
                    'avg_processing_time_ms': None,  # Will be calculated from detailed events if needed
                    'avg_file_size_bytes': None,     # Will be calculated from detailed events if needed
                    'avg_session_duration_min': None, # Will be calculated from session data if needed
                    'created_at': datetime.now()
                })
                
            except Exception as e:
                print(f"Warning: Skipping KPI row due to error: {e}")
                continue
        
        kpis_df = pd.DataFrame(kpi_records)
        print(f"✅ Built {len(kpis_df)} daily KPI records")
        return kpis_df
    
    def load_dashboard_schema(self, fact_df, kpis_df, tools_df, time_df, sessions_df, event_types_df):
        """Load all tables into dashboard schema"""
        
        print("📊 Loading dashboard schema...")
        
        try:
            # Load dimensions first
            if not tools_df.empty:
                self.duckdb_conn.execute("INSERT INTO dim_tools SELECT * FROM tools_df")
                print(f"   ✅ Loaded {len(tools_df)} tools")
            
            if not time_df.empty:
                self.duckdb_conn.execute("INSERT INTO dim_time SELECT * FROM time_df")
                print(f"   ✅ Loaded {len(time_df)} time records")
            
            if not sessions_df.empty:
                sessions_df = sessions_df.drop_duplicates(subset=['session_key'])
                self.duckdb_conn.execute("INSERT INTO dim_sessions SELECT * FROM sessions_df")
                print(f"   ✅ Loaded {len(sessions_df)} sessions")
            
            if not event_types_df.empty:
                self.duckdb_conn.execute("INSERT INTO dim_event_types SELECT * FROM event_types_df")
                print(f"   ✅ Loaded {len(event_types_df)} event types")
            
            # Load fact tables
            if not fact_df.empty:
                self.duckdb_conn.execute("INSERT INTO fact_analytics SELECT * FROM fact_df")
                print(f"   ✅ Loaded {len(fact_df)} detailed fact records")
            
            if not kpis_df.empty:
                self.duckdb_conn.execute("INSERT INTO fact_daily_kpis SELECT * FROM kpis_df")
                print(f"   ✅ Loaded {len(kpis_df)} daily KPI records")
                
        except Exception as e:
            print(f"❌ Loading error: {e}")
    
    def generate_dashboard_queries(self):
        """Generate optimized queries for dashboard KPIs"""
        
        print("\n🎛️ DASHBOARD KPI QUERIES:")
        print("=" * 45)
        
        # Total KPIs (from pre-aggregated table)
        try:
            result = self.duckdb_conn.execute("""
                SELECT 
                    SUM(total_events) as total_events,
                    SUM(total_uploads) as total_uploads,
                    SUM(total_processing) as total_processing,
                    SUM(total_downloads) as total_downloads,
                    SUM(unique_sessions) as total_sessions,
                    COUNT(DISTINCT tool_key) as active_tools,
                    ROUND(AVG(upload_to_download_rate), 1) as avg_conversion_rate
                FROM fact_daily_kpis
            """).fetchall()
            
            print("📊 TOTAL PLATFORM KPIs:")
            print(f"   📈 Total Events: {result[0][0]:,}")
            print(f"   📤 Total Uploads: {result[0][1]:,}")
            print(f"   ⚙️  Total Processing: {result[0][2]:,}")
            print(f"   📥 Total Downloads: {result[0][3]:,}")
            print(f"   👥 Total Sessions: {result[0][4]:,}")
            print(f"   🔧 Active Tools: {result[0][5]:,}")
            print(f"   💯 Avg Conversion Rate: {result[0][6]}%")
            
        except Exception as e:
            print(f"KPI query error: {e}")
        
        # Top performing tools (dashboard ready)
        try:
            result = self.duckdb_conn.execute("""
                SELECT 
                    t.tool_display_name as tool,
                    t.icon_name,
                    SUM(k.total_downloads) as downloads,
                    SUM(k.total_uploads) as uploads,
                    ROUND(AVG(k.upload_to_download_rate), 1) as conversion_rate,
                    SUM(k.unique_sessions) as sessions
                FROM fact_daily_kpis k
                JOIN dim_tools t ON k.tool_key = t.tool_key
                WHERE k.total_downloads > 0
                GROUP BY t.tool_display_name, t.icon_name, t.sort_order
                ORDER BY downloads DESC
                LIMIT 5
            """).fetchdf()
            
            print("\n🏆 TOP PERFORMING TOOLS:")
            print(result.to_string(index=False))
            
        except Exception as e:
            print(f"Top tools query error: {e}")
        
        # Daily trends (for charts)
        try:
            result = self.duckdb_conn.execute("""
                SELECT 
                    tm.date_label as date,
                    SUM(k.total_downloads) as downloads,
                    SUM(k.total_uploads) as uploads,
                    SUM(k.total_processing) as processing
                FROM fact_daily_kpis k
                JOIN dim_time tm ON k.date = tm.date
                GROUP BY tm.date_label, k.date
                ORDER BY k.date DESC
                LIMIT 7
            """).fetchdf()
            
            print("\n📈 RECENT DAILY TRENDS:")
            print(result.to_string(index=False))
            
        except Exception as e:
            print(f"Daily trends query error: {e}")
    
    def run_dashboard_etl(self):
        """Run complete dashboard-optimized ETL"""
        
        print("🎛️ DASHBOARD STAR SCHEMA ETL PIPELINE")
        print("🎯 Optimized for dashboard performance")
        print("=" * 55)
        
        # Extract source data
        source_data = self.extract_source_data()
        events_df = source_data.get('analytics_events', pd.DataFrame())
        daily_df = source_data.get('daily_tool_usage', pd.DataFrame())
        
        if events_df.empty or daily_df.empty:
            print("❌ Missing required data")
            return
        
        # Build enhanced dimensions
        tools_df = self.build_enhanced_tools_dimension(events_df)
        time_df = self.build_enhanced_time_dimension(events_df)
        sessions_df = self.build_sessions_dimension(events_df)
        event_types_df = self.build_enhanced_event_types_dimension()
        
        # Build fact tables
        fact_df = self.build_fact_analytics(events_df)
        kpis_df = self.build_daily_kpis_fact(daily_df)
        
        # Load dashboard schema
        self.load_dashboard_schema(fact_df, kpis_df, tools_df, time_df, sessions_df, event_types_df)
        
        # Generate dashboard queries
        self.generate_dashboard_queries()
        
        print("\n🎉 DASHBOARD ETL COMPLETE!")
        print("\n🎛️ STAR SCHEMA DATABASE READY FOR BI TOOLS:")
        print("   ✓ Star schema with 2 fact tables + 4 dimensions")
        print("   ✓ Pre-aggregated KPIs for fast dashboard queries")
        print("   ✓ Download and processing metrics tracked")
        print("   ✓ Conversion rates calculated")
        print("   ✓ Connect your BI tool to: data/dashboard_analytics.duckdb")
        print("   ✓ Query tables: fact_analytics, fact_daily_kpis, dim_tools, dim_time")

if __name__ == "__main__":
    etl = DashboardStarSchemaETL()
    etl.run_dashboard_etl()