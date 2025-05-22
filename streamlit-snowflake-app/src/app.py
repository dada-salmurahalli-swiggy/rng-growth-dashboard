import warnings
import os
import sys
import streamlit as st
import pandas as pd
import snowflake.connector # type: ignore
from config import SNOWFLAKE_CONFIG
from datetime import datetime
from datetime import datetime, timedelta
from datetime import datetime, timedelta
import plotly.graph_objects as go
import matplotlib.colors as mcolors

from utils import (
    show_welcome_page,
    fetch_tables,
    fetch_rng_daily_data,
    fetch_table_data,
    fetch_comparison_data,
    style_dataframe,
    fetch_weekly_data,
    sort_category_cohorts,
    ask_ai_agent  # <-- import the AI agent function
)

# Set page config first, before any other Streamlit commands
st.set_page_config(page_title="Snowflake Tables Viewer", layout="wide")

# Suppress warnings
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

# Suppress stdout/stderr from Snowflake connector
class DevNull:
    def write(self, msg): pass
    def flush(self): pass

sys.stdout = DevNull()
sys.stderr = DevNull()

# --- Trigger Streamlit Cloud rebuild: 2025-05-23 ---

@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        # Test connection
        with conn.cursor() as cur:
            cur.execute("SELECT current_version()")
            version = cur.fetchone()[0]
            print(f"Connected to Snowflake version: {version}")
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        st.error("Please check your credentials in .streamlit/secrets.toml")
        raise


def clean_percentage(val):
    """Convert string percentage to float"""
    try:
        # Convert string to float, keeping the same scale (3.11 stays 3.11)
        return float(str(val).strip())
    except:
        return None



def main():
    st.markdown("""
        <style>
        .rng-title {
            font-size: 48px;
            font-family: 'Arial Black', 'Segoe UI', Arial, sans-serif;
            font-weight: bold;
            text-align: left;
            margin-top: 0px;
            margin-bottom: 10px;
            letter-spacing: 2px;
            background: linear-gradient(90deg, #1e293b 10%, #0ea5e9 90%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 2px 2px 8px #0ea5e955;
        }
        </style>
        <div class="rng-title">RnG Dashboard</div>
    """, unsafe_allow_html=True)

    # Initialize session state for navigation
    if 'nav_selection' not in st.session_state:
        st.session_state.nav_selection = None

    # Add this CSS at the top of your main() function or before any Streamlit commands
    st.markdown("""
        <style>
        .sidebar-title {
            font-size: 28px;
            font-weight: bold;
            color: #0ea5e9;
            text-align: center;
            margin-bottom: 20px;
            letter-spacing: 1px;
        }
        .sidebar-section {
            font-size: 18px;
            color: #e5e7eb;
            margin-top: 25px;
            margin-bottom: 10px;
            font-weight: 600;
            letter-spacing: 0.5px;
            border-bottom: 1px solid #0ea5e9;
            padding-bottom: 4px;
        }
        .sidebar-nav-btn {
            width: 100%;
            text-align: left;
            padding: 10px 16px;
            margin-bottom: 8px;
            border-radius: 8px;
            border: none;
            background: #293548;
            color: #e5e7eb;
            font-size: 17px;
            font-weight: 500;
            transition: background 0.2s;
        }
        .sidebar-nav-btn.selected, .sidebar-nav-btn:hover {
            background: linear-gradient(90deg, #0ea5e9 60%, #1e293b 100%);
            color: #fff;
        }
        .sidebar-bedrock-btn {
            width: 100%;
            margin-top: 30px;
            background: linear-gradient(90deg, #0ea5e9 60%, #1e293b 100%);
            color: #fff;
            font-weight: bold;
            font-size: 18px;
            border-radius: 8px;
            border: none;
            padding: 12px 0;
            transition: background 0.2s;
        }
        .metric-header {
            font-size: 22px;
            color: #0ea5e9;
            font-weight: bold;
            margin-top: 20px;
            margin-bottom: 10px;
        }
        .snowflake-conn-badge {
            position: fixed;
            top: 60px;
            right: 30px;
            background: linear-gradient(90deg, #0ea5e9 60%, #1e293b 100%);
            color: white;
            padding: 12px 28px;
            border-radius: 30px;
            font-size: 18px;
            font-weight: bold;
            box-shadow: 0 2px 12px #0ea5e955;
            z-index: 9999;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .snowflake-emoji {
            font-size: 22px;
        }
        </style>
    """, unsafe_allow_html=True)

    # Sidebar content
    st.sidebar.markdown('<div class="sidebar-title">🍽️ Instamart Dashboard</div>', unsafe_allow_html=True)
    st.sidebar.markdown('<div class="sidebar-section">Navigation</div>', unsafe_allow_html=True)

    nav_options = [
        ("🏠 Home", None),
        ("📊 DoD View", "dod"),
        ("🌆 City Level DoD", "city"),
        ("⏰ Hourly DPO", "hourly_dpo"),
        ("📈 RNG Chart", "chart"),
    ]

    for label, nav_key in nav_options:
        btn_class = "sidebar-nav-btn"
        if st.session_state.get("nav_selection") == nav_key:
            btn_class += " selected"
        if st.sidebar.button(label, key=f"nav_{nav_key}", help=label, use_container_width=True):
            st.session_state.nav_selection = nav_key

    # --- Amazon Bedrock Button replaced with AI Agent UI ---
    st.sidebar.markdown('<div class="sidebar-section">Ask AI Agent</div>', unsafe_allow_html=True)
    api_key = st.sidebar.text_input("OpenAI API Key", type="password", key="ai_api_key")
    user_question = st.sidebar.text_area("Ask a question about your tables:", key="ai_question")
    if st.sidebar.button("Ask AI", key="ai_ask_btn"):
        if not api_key or not user_question:
            st.sidebar.warning("Please enter your API key and a question.")
        else:
            with st.sidebar:
                with st.spinner("AI is thinking..."):
                    # Optionally, you can pass table context here
                    answer = ask_ai_agent(user_question, api_key)
                    st.success("AI Response:")
                    st.write(answer)

    # Establish connection to Snowflake
    try:
        conn = get_connection()


        st.markdown("""
            <style>
            .snowflake-conn-badge {
                position: fixed;
                top: 60px;
                right: 30px;
                background: linear-gradient(90deg, #43ea7f 60%, #1ecb5c 100%);
                color: white;
                padding: 12px 28px;
                border-radius: 30px;
                font-size: 18px;
                font-weight: bold;
                box-shadow: 0 2px 12px #1ecb5c33;
                z-index: 9999;
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .snowflake-emoji {
                font-size: 22px;
            }
            </style>
            <div class="snowflake-conn-badge">
                <span class="snowflake-emoji">❄️</span>
                Snowflake Connected
            </div>
        """, unsafe_allow_html=True)
        #st.success("Snowflake connection is successfull")





        if st.session_state.nav_selection == "dod":
            st.markdown("<h2 style='text-align:center; color:#0ea5e9;'>DoD View</h2>", unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            with col1:
                selected_date = st.date_input("Select a date", value=datetime.now().date(), key="dod_selected_date")
            with col2:
                compare_date = st.date_input(
                    "Compare with",
                    value=(selected_date - timedelta(days=7)),
                    max_value=selected_date,
                    key="dod_compare_date"
                )

            with st.spinner('Fetching DoD data...'):
                date_list = [selected_date.strftime('%Y-%m-%d'), compare_date.strftime('%Y-%m-%d')]
                query = f"""
                    SELECT * FROM TEMP.PUBLIC.RNG_DAILY
                    WHERE start_date IN ('{date_list[0]}', '{date_list[1]}')
                """
                cur = conn.cursor()
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
                cur.close()

                if data and columns:
                    df = pd.DataFrame(data, columns=columns)
                    df.columns = df.columns.str.lower()
                    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date

                    # After df["start_date"] = pd.to_datetime(df["start_date"]).dt.date

                    df['tu_vu_pct'] = (df['transacting_users'] / df['visitors'] * 100).round(2)
                    df['vu_pct'] = (df['visitors'] / df['base'] * 100).round(2)
                    df['repeat_rate'] = (df['orders_on_date'] / df['transacting_users']).round(2)

                    # Define metrics to compare
                    metrics = [
                                'base',
                                'transacting_users',
                                'visitors',
                                'orders_on_date',
                                'tu_vu_pct',    # New
                                'vu_pct',       # New
                                'repeat_rate',  # New
                                'menu_sessions',
                                'cart_sessions',
                                'menu_droppers',
                                'cart_droppers'
                            ]

                    # Create tabs for each metric
                    for metric in metrics:
                        metric_display = {
                            'base': '📊 Total Base',
                            'transacting_users': '👥 Transacting Users',
                            'visitors': '👀 Unique Visitors',
                            'orders_on_date': '🛍️ Daily Orders',
                            'tu_vu_pct': '🔗 TU / VU %',
                            'vu_pct': '📈 VU %',
                            'repeat_rate': '🔄 Repeat Rate',
                            'menu_sessions': '📱 Menu Sessions',
                            'cart_sessions': '🛒 Cart Sessions',
                            'menu_droppers': '↩️ Menu Droppers',
                            'cart_droppers': '🔙 Cart Droppers'
                        }.get(metric, metric.replace('_', ' ').title())
                        
                        st.markdown(f"<div class='metric-header'>{metric_display}</div>", unsafe_allow_html=True)

                        if metric == 'vu_pct':
                            df_metric = df[
                                (~df['category'].isin(['NU', 'Unassigned'])) &
                                (df['base'] != 0)
                            ]
                        else:
                            df_metric = df

                        pivot_df = df_metric.pivot_table(
                            values=metric,
                            index=['category', 'cohorts'],
                            columns='start_date',
                            aggfunc='sum'
                        ).round(2)
                        
                        # Sort and prepare the table
                        pivot_df = pivot_df.reset_index()
                        pivot_df = sort_category_cohorts(pivot_df)
                        pivot_df = pivot_df.set_index(['category', 'cohorts'])
                        
                        # Calculate percentage difference
                        dates = sorted(pivot_df.columns)
                        if len(dates) == 2:
                            pivot_df = pivot_df[~((pivot_df[dates[0]] == 0) & (pivot_df[dates[1]] == 0))]

                            # Calculate absolute difference
                            pivot_df['Absolute Diff'] = pivot_df[dates[1]] - pivot_df[dates[0]]

                            # Calculate percentage change
                            pivot_df['% Change'] = ((pivot_df[dates[1]] - pivot_df[dates[0]]) / pivot_df[dates[0]] * 100).round(1)

                            # Reorder columns to show % change and absolute diff first
                            pivot_df = pivot_df[['% Change', 'Absolute Diff', dates[1], dates[0]]]

                            # Rename columns for clarity
                            pivot_df = pivot_df.rename(columns={
                                dates[1]: f"Selected ({dates[1]})",
                                dates[0]: f"Compare ({dates[0]})"
                            })

                            # Style the dataframe
                            def create_light_colormap():
                                colors = ['#ffcdd2', '#ffffff', '#c8e6c9']  # Light red, white, light green
                                return mcolors.LinearSegmentedColormap.from_list('custom', colors)

                            format_dict = {
                                '% Change': '{:+.1f}%',
                                'Absolute Diff': '{:+,.2f}',
                                f"Selected ({dates[1]})": '{:,.2f}',
                                f"Compare ({dates[0]})": '{:,.2f}'
                            }
                            if metric in ['tu_vu_pct', 'vu_pct']:
                                format_dict[f"Selected ({dates[1]})"] = '{:.1f}%'
                                format_dict[f"Compare ({dates[0]})"] = '{:.1f}%'
                            if metric == 'repeat_rate':
                                format_dict[f"Selected ({dates[1]})"] = '{:.2f}'
                                format_dict[f"Compare ({dates[0]})"] = '{:.2f}'

                            styled_df = pivot_df.style.format(format_dict).background_gradient(
                                subset=['% Change'],
                                cmap=create_light_colormap(),
                                vmin=-20,
                                vmax=20
                            )

                            st.dataframe(styled_df, use_container_width=True)
                            
                        st.markdown("<hr style='margin: 20px 0; opacity: 0.3;'>", unsafe_allow_html=True)

                else:
                    st.info("No data found for the selected dates.")



        elif st.session_state.nav_selection == "city":
            st.markdown("<h2 style='text-align:center; color:#0ea5e9;'>City Level DoD</h2>", unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            with col1:
                selected_date = st.date_input("Select a date", value=datetime.now().date(), key="city_selected_date")
            with col2:
                compare_date = st.date_input(
                    "Compare with",
                    value=(selected_date - timedelta(days=7)),
                    max_value=selected_date,
                    key="city_compare_date"
                )

            with st.spinner('Fetching City Level data...'):
                date_list = [selected_date.strftime('%Y-%m-%d'), compare_date.strftime('%Y-%m-%d')]
                query = f"""
                    SELECT * FROM TEMP.PUBLIC.RNG_CITY_DAILY
                    WHERE start_date IN ('{date_list[0]}', '{date_list[1]}')
                """
                cur = conn.cursor()
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
                cur.close()

                if data and columns:
                    df = pd.DataFrame(data, columns=columns)
                    df.columns = df.columns.str.lower()
                    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date

                    df['tu_vu_pct_city'] = (df['transacting_users'] / df['visitors'].replace(0, pd.NA) * 100).round(2)                    
                    df['vu_pct_city'] = (df['visitors'] / df['base'] * 100).round(2)
                    df['repeat_rate_city'] = (df['orders_on_date'] / df['transacting_users']).round(2)

                    # Define metrics to compare
                    metrics = [
                        'base',
                        'transacting_users',
                        'orders_on_date',
                        'visitors',
                        'tu_vu_pct_city',    # New
                        'vu_pct_city',       # New
                        'repeat_rate_city',  # New
                        'menu_sessions',
                        'cart_sessions',
                        'menu_droppers',
                        'cart_droppers'
                    ]

                    # Create sections for each metric
                    for metric in metrics:
                        metric_display = {
                            'base': '📊 Total Base',
                            'transacting_users': '👥 Transacting Users',
                            'orders_on_date': '🛍️ Daily Orders',
                            'visitors': '👀 Unique Visitors',
                            'tu_vu_pct_city': '🔗 TU / VU %',
                            'vu_pct_city': '📈 VU %',
                            'repeat_rate_city': '🔄 Repeat Rate',
                            'menu_sessions': '📱 Menu Sessions',
                            'cart_sessions': '🛒 Cart Sessions',
                            'menu_droppers': '↩️ Menu Droppers',
                            'cart_droppers': '🔙 Cart Droppers'
                        }.get(metric, metric.replace('_', ' ').title())
                        
                        st.markdown(f"<div class='metric-header'>{metric_display}</div>", unsafe_allow_html=True)
                        
                        # Filter for VU %
                        if metric == 'vu_pct_city':
                            df_metric = df[
                                (~df['category'].isin(['NU', 'Unassigned'])) &
                                (df['base'] != 0)
                            ]
                        else:
                            df_metric = df


                        agg_func = 'mean' if metric in ['tu_vu_pct_city', 'vu_pct_city', 'repeat_rate_city'] else 'sum'

                        # Create pivot table by city
                        agg_func = 'mean' if metric in ['tu_vu_pct_city', 'vu_pct_city', 'repeat_rate_city'] else 'sum'
                        pivot_df = df_metric.pivot_table(
                            values=metric,
                            index=['city'],
                            columns='start_date',
                            aggfunc=agg_func
                        ).round(2)

                        # Sort by the latest date's values in descending order
                        latest_date = max(df['start_date'])
                        city_base_sizes = df[df['start_date'] == latest_date].groupby('city')['base'].sum()
                        
                        # Sort and prepare the table
                        pivot_df = pivot_df.reset_index()
                        pivot_df['sort_key'] = pivot_df['city'].map(city_base_sizes)
                        pivot_df = pivot_df.sort_values('sort_key', ascending=False)
                        pivot_df = pivot_df.drop('sort_key', axis=1)
                        pivot_df = pivot_df.set_index(['city'])
                        
                        # Calculate percentage difference
                        dates = sorted(pivot_df.columns)
                        if len(dates) == 2:
                            pivot_df = pivot_df[~((pivot_df[dates[0]] == 0) & (pivot_df[dates[1]] == 0))]
                            
                            # Calculate absolute difference
                            pivot_df['Absolute Diff'] = pivot_df[dates[1]] - pivot_df[dates[0]]
                            
                            # Calculate percentage change
                            pivot_df['% Change'] = ((pivot_df[dates[1]] - pivot_df[dates[0]]) / pivot_df[dates[0]] * 100).round(1)
                            
                            # Reorder columns
                            pivot_df = pivot_df[['% Change', 'Absolute Diff', dates[1], dates[0]]]
                            
                            # Rename columns for clarity
                            pivot_df = pivot_df.rename(columns={
                                dates[1]: f"Selected ({dates[1]})",
                                dates[0]: f"Compare ({dates[0]})"
                            })
                            
                            # Style the dataframe - matching DoD style
                            def create_light_colormap():
                                colors = ['#ffcdd2', '#ffffff', '#c8e6c9']  # Light red, white, light green
                                return mcolors.LinearSegmentedColormap.from_list('custom', colors)
                            

                            format_dict = {
                                '% Change': '{:+.1f}%',
                                'Absolute Diff': '{:+,.0f}',
                                f"Selected ({dates[1]})": '{:,.0f}',
                                f"Compare ({dates[0]})": '{:,.0f}'
                            }
                            if metric in ['tu_vu_pct_city', 'vu_pct_city']:
                                format_dict[f"Selected ({dates[1]})"] = '{:.1f}%'
                                format_dict[f"Compare ({dates[0]})"] = '{:.1f}%'
                            if metric == 'repeat_rate_city':
                                format_dict[f"Selected ({dates[1]})"] = '{:.2f}'
                                format_dict[f"Compare ({dates[0]})"] = '{:.2f}'

                            styled_df = pivot_df.style.format(format_dict).background_gradient(
                                subset=['% Change'],
                                cmap=create_light_colormap(),
                                vmin=-20,
                                vmax=20
                            ).set_properties(**{
                                'font-weight': '500'
                            })
                            
                            st.dataframe(styled_df, use_container_width=True)



                        
                        st.markdown("<hr style='margin: 20px 0; opacity: 0.3;'>", unsafe_allow_html=True)
                else:
                    st.info("No data found for the selected dates.")




        elif st.session_state.nav_selection == "hourly_dpo":

            st.markdown("<h2 style='text-align:center; color:#0ea5e9;'>Hourly DPO Trend</h2>", unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            with col1:
                selected_date = st.date_input("Select a date", value=datetime.now().date(), key="dpo_selected_date")
            with col2:
                compare_date = st.date_input(
                    "Compare with",
                    value=(selected_date - timedelta(days=7)),
                    max_value=selected_date,
                    key="dpo_compare_date"
                )

            with st.spinner('Fetching Hourly DPO data...'):
                date_list = [selected_date.strftime('%Y-%m-%d'), compare_date.strftime('%Y-%m-%d')]
                query = f"""
                    SELECT * FROM temp.public.rng_hourly_dpo
                    WHERE start_date IN ('{date_list[0]}', '{date_list[1]}')
                    ORDER BY start_date, order_hour, category, cohorts
                """
                cur = conn.cursor()
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
                cur.close()

                if data and columns:
                    df = pd.DataFrame(data, columns=columns)
                    df.columns = df.columns.str.lower()
                    df["start_date"] = pd.to_datetime(df["start_date"]).dt.date

                    # --- ADD THIS BLOCK ---
                    if 'pct_disc_orders' in df.columns:
                        df['pct_disc_orders'] = pd.to_numeric(df['pct_disc_orders'], errors='coerce')
                    # --- END ADD ---

                    metrics = [
                        ('dpo_fc', '💳 DPO Free Cash'),
                        ('dpo_coupons', '🎟️ DPO Coupons'),
                        ('dpo_both', '🎯 DPO Combined'),
                        ('pct_disc_orders', '📊 % Discounted Orders')
                    ]

                                    
                    for metric, metric_display in metrics:
                        st.markdown(f"<div class='metric-header'>{metric_display}</div>", unsafe_allow_html=True)

                        # Pivot for selected date
                        df_sel = df[df['start_date'] == selected_date]
                        if not df_sel.empty:
                            pivot_sel = df_sel.pivot_table(
                                values=metric,
                                index=['category', 'cohorts'],
                                columns='order_hour',
                                aggfunc='mean'
                            ).round(4 if metric == 'pct_disc_orders' else 2)
                            pivot_sel = pivot_sel.reset_index()
                            pivot_sel = sort_category_cohorts(pivot_sel)
                            pivot_sel = pivot_sel.set_index(['category', 'cohorts'])
                            st.markdown(f"##### Selected Date: {selected_date}")
                            st.dataframe(
                                pivot_sel.style.format('{:.1f}%' if metric == 'pct_disc_orders' else '{:.2f}'),
                                use_container_width=True
                            )
                        else:
                            st.info(f"No data for selected date: {selected_date}")

                        # Pivot for compare date
                        df_cmp = df[df['start_date'] == compare_date]
                        if not df_cmp.empty:
                            pivot_cmp = df_cmp.pivot_table(
                                values=metric,
                                index=['category', 'cohorts'],
                                columns='order_hour',
                                aggfunc='mean'
                            ).round(4 if metric == 'pct_disc_orders' else 2)
                            pivot_cmp = pivot_cmp.reset_index()
                            pivot_cmp = sort_category_cohorts(pivot_cmp)
                            pivot_cmp = pivot_cmp.set_index(['category', 'cohorts'])
                            st.markdown(f"##### Compare Date: {compare_date}")
                            st.dataframe(
                                pivot_cmp.style.format('{:.1f}%' if metric == 'pct_disc_orders' else '{:.2f}'),
                                use_container_width=True
                            )
                        else:
                            st.info(f"No data for compare date: {compare_date}")

                        # After displaying the compare table for each metric, add:
                        if not df_sel.empty and not df_cmp.empty:
                            # Align indices for proper comparison
                            pivot_sel, pivot_cmp = pivot_sel.align(pivot_cmp, join='outer', axis=0, fill_value=0)
                            if metric == 'pct_disc_orders':
                                # Absolute difference in percentage points
                                pct_change = (pivot_sel - pivot_cmp)
                                pct_change = pct_change.replace([pd.NA, float('inf'), -float('inf')], 0)
                                fmt = '{:+.1f}pp'
                            else:
                                # Percent change for other metrics
                                pct_change = ((pivot_sel - pivot_cmp) / pivot_cmp.replace(0, pd.NA) * 100).round(1)
                                pct_change = pct_change.replace([pd.NA, float('inf'), -float('inf')], 0)
                                fmt = '{:+.1f}%'

                            def style_pct(val):
                                try:
                                    if pd.isna(val):
                                        return ''
                                    elif val > 0:
                                        return 'background-color: #69db7c'
                                    elif val < 0:
                                        return 'background-color: #ff6b6b'
                                    else:
                                        return ''
                                except:
                                    return ''

                            st.markdown("##### % Change (Selected vs Compare)")
                            st.dataframe(
                                pct_change.style.format(fmt).applymap(style_pct),
                                use_container_width=True
                            )
                        st.markdown("<hr style='margin: 20px 0; opacity: 0.3;'>", unsafe_allow_html=True)
                        pass
                else:
                   st.info("No data found for the selected dates.")




        elif st.session_state.nav_selection == "rng":
            # Show RNG Daily table
            with st.spinner('Fetching RNG Daily data...'):
                st.subheader("Contents of RNG Daily Table")
                columns, data = fetch_rng_daily_data(conn)
                
                if data and columns:
                    df = pd.DataFrame(data, columns=columns)
                    df = sort_category_cohorts(df)
                    st.dataframe(df, height=400, use_container_width=True)
                else:
                    st.warning("No data found in the RNG Daily table.")



        elif st.session_state.nav_selection == "chart":
            st.subheader("RNG Daily Comparison")
            col1, col2 = st.columns([3, 1])
            with col1:
                max_date = datetime.now().date()
                selected_date = st.date_input(
                    "Select a date to compare",
                    value=max_date,
                    max_value=max_date
                )
            with col2:
                if selected_date:
                    st.info(f": {selected_date.strftime('%A, %B %d, %Y')}")

            # Category renaming map
            category_map = {
                'DU_30_45_Days': 'STDU',
                'DU_45+_Days': 'LTDU',
                'RU_last30_Days': 'RU',
                'NU': 'NU'
            }

            if selected_date:
                with st.spinner('Fetching comparison data...'):
                    df = fetch_weekly_data(conn, selected_date.strftime('%Y-%m-%d'))
                    if not df.empty:
                        df.columns = df.columns.str.lower()
                        df['start_date'] = pd.to_datetime(df['start_date'])  # Use datetime for x-axis
                        df['category'] = df['category'].replace(category_map)
                        # Ensure current_monday and selected_date are datetime64[ns] for comparison
                        current_monday = pd.to_datetime(selected_date - timedelta(days=selected_date.weekday()))
                        selected_datetime = pd.to_datetime(selected_date)
                        current_week = df[(df['start_date'] >= current_monday) & (df['category'] != 'Unassigned')]
                        prev_week = df[(df['start_date'] < current_monday) & (df['category'] != 'Unassigned')]

                        # Calculate additional metrics
                        df['tu_vu_pct'] = (df['transacting_users'] / df['visitors'].replace(0, pd.NA) * 100).round(2)
                        df['vu_pct'] = (df['visitors'] / df['base'].replace(0, pd.NA) * 100).round(2)
                        df['repeat_rate'] = (df['orders_on_date'] / df['transacting_users'].replace(0, pd.NA)).round(2)

                        metrics = [
                            ('base', '📊 Total Base'),
                            ('transacting_users', '👥 Transacting Users'),
                            ('visitors', '👀 Unique Visitors'),
                            ('orders_on_date', '🛍️ Daily Orders'),
                            ('tu_vu_pct', '🔗 TU / VU %'),
                            ('vu_pct', '📈 VU %'),
                            ('repeat_rate', '🔄 Repeat Rate'),
                            ('menu_sessions', '📱 Menu Sessions'),
                            ('cart_sessions', '🛒 Cart Sessions'),
                            ('menu_droppers', '↩️ Menu Droppers'),
                            ('cart_droppers', '🔙 Cart Droppers')
                        ]

                        for metric, metric_display in metrics:
                            st.markdown(f"<div class='metric-header'>{metric_display}</div>", unsafe_allow_html=True)
                            if metric == 'vu_pct':
                                df_metric = df[(~df['category'].isin(['NU', 'Unassigned'])) & (df['base'] != 0)]
                            else:
                                df_metric = df[df['category'] != 'Unassigned']
                            fig = go.Figure()
                            all_dates = pd.date_range(df_metric['start_date'].min(), df_metric['start_date'].max(), freq='D')
                            # Current week
                            curr_data = df_metric[df_metric['start_date'] >= current_monday]
                            for cat in curr_data['category'].unique():
                                cat_data = curr_data[curr_data['category'] == cat].sort_values('start_date')
                                # Aggregate to ensure no duplicate dates
                                cat_data_agg = cat_data.groupby('start_date', as_index=False)[metric].sum()
                                cat_data_agg = cat_data_agg.set_index('start_date').reindex(all_dates).reset_index()
                                cat_data_agg['category'] = cat
                                fig.add_trace(
                                    go.Scatter(
                                        x=cat_data_agg['index'],
                                        y=cat_data_agg[metric],
                                        name=f"{cat} (Current)",
                                        mode='lines',
                                        line=dict(width=4)
                                    )
                                )
                            # Previous week
                            prev_data = df_metric[df_metric['start_date'] < current_monday]
                            for cat in prev_data['category'].unique():
                                cat_data = prev_data[prev_data['category'] == cat].sort_values('start_date')
                                cat_data_agg = cat_data.groupby('start_date', as_index=False)[metric].sum()
                                cat_data_agg = cat_data_agg.set_index('start_date').reindex(all_dates).reset_index()
                                cat_data_agg['category'] = cat
                                fig.add_trace(
                                    go.Scatter(
                                        x=cat_data_agg['index'],
                                        y=cat_data_agg[metric],
                                        name=f"{cat} (Previous)",
                                        mode='lines',
                                        line=dict(dash='dash', width=3)
                                    )
                                )
                            fig.update_layout(
                                title=f'{metric_display} Trends Comparison',
                                xaxis_title='Date',
                                yaxis_title=metric_display,
                                hovermode='x unified',
                                showlegend=True,
                                height=700,
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    xanchor="right",
                                    x=1,
                                    font=dict(size=14)
                                ),
                                xaxis=dict(
                                    tickformat='%b %d',
                                    tickangle=-30,
                                    tickfont=dict(size=14),
                                    automargin=True
                                )
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            st.markdown("<hr style='margin: 20px 0; opacity: 0.3;'>", unsafe_allow_html=True)
                    else:
                        st.warning("No data available for the selected date range.")

        else:
            show_welcome_page()


    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")

if __name__ == "__main__":
    main()







