import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
import openai

def show_welcome_page():
    st.markdown("""
        <style>
        .super-welcome {
            font-size: 90px;
            font-family: 'Arial Black', sans-serif;
            background: linear-gradient(90deg, #FF5722 10%, #F44336 90%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-align: center;
            font-weight: bold;
            margin-top: 40px;
            margin-bottom: 10px;
            letter-spacing: 2px;
        }
        .dashboard-title {
            font-size: 42px;
            color: #222;
            font-family: 'Segoe UI', 'Arial', sans-serif;
            text-align: center;
            font-weight: 700;
            margin-bottom: 30px;
            letter-spacing: 1px;
            text-shadow: 2px 2px 8px #ffccbc33;
        }
        </style>
    """, unsafe_allow_html=True)
    st.markdown('<div class="super-welcome">Welcome to Swiggy</div>', unsafe_allow_html=True)
    st.markdown('<div class="dashboard-title"></div>', unsafe_allow_html=True)

def fetch_tables(conn):
    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[1] for row in cur.fetchall()]
        cur.close()
        return tables
    except Exception as e:
        st.error(f"Error fetching tables: {e}")
        return []

def fetch_rng_daily_data(conn, date_filter=None, limit=200):
    try:
        cur = conn.cursor()
        if date_filter:
            query = f"""
                SELECT * FROM TEMP.PUBLIC.RNG_DAILY 
                WHERE start_date = '{date_filter}'
                LIMIT {limit}
            """
        else:
            query = f"SELECT * FROM TEMP.PUBLIC.RNG_DAILY LIMIT {limit}"
        
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        return columns, data
    except Exception as e:
        st.error(f"Error fetching RNG daily data: {e}")
        return [], []

def fetch_table_data(conn, table_name, limit=200):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        return columns, data
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return [], []

def fetch_comparison_data(conn):
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        last_week = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        
        cur = conn.cursor()
        query = f"""
            SELECT * FROM TEMP.PUBLIC.RNG_DAILY 
            WHERE start_date IN ('{yesterday}', '{last_week}')
        """
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        return columns, data
    except Exception as e:
        st.error(f"Error fetching comparison data: {e}")
        return [], []


def fetch_weekly_data(conn, selected_date):
    """Fetch data for selected days of current and previous week"""
    try:
        # Convert string to datetime if needed
        if isinstance(selected_date, str):
            selected_date = datetime.strptime(selected_date, '%Y-%m-%d')
            
        # Get current week's Monday
        current_monday = selected_date - timedelta(days=selected_date.weekday())
        # Get days between Monday and selected date
        days_to_include = (selected_date - current_monday).days + 1
        
        # Get previous week's corresponding dates
        prev_week_monday = current_monday - timedelta(days=7)
        prev_week_end = prev_week_monday + timedelta(days=days_to_include - 1)

        query = f"""
            SELECT *
            FROM TEMP.PUBLIC.RNG_DAILY
            WHERE (
                (start_date BETWEEN '{current_monday.strftime('%Y-%m-%d')}' AND '{selected_date.strftime('%Y-%m-%d')}')
                OR 
                (start_date BETWEEN '{prev_week_monday.strftime('%Y-%m-%d')}' AND '{prev_week_end.strftime('%Y-%m-%d')}')
            )
            ORDER BY start_date, category, cohorts
        """
        
        cur = conn.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Error fetching weekly data: {e}")
        return pd.DataFrame()
    
def style_dataframe(df):
    """Apply consistent dark styling to all dataframes"""
    return df.style.set_table_styles([
        {'selector': 'th', 'props': [
            ('background-color', '#000'),      # Pure black header background
            ('color', '#FFD700'),              # Gold header text for high contrast
            ('font-weight', 'bold'),
            ('border', '2px solid #FFD700'),   # Gold border for header
            ('font-size', '17px')
        ]},
        {'selector': 'td', 'props': [
            ('color', '#fff'),                 # White text in cells
            ('background-color', '#222'),      # Dark cell background
            ('border', '1px solid #FFD700'),   # Gold border for cells
            ('padding', '8px'),
            ('font-size', '15px')
        ]},
        {'selector': 'tr:nth-child(even)', 'props': [
            ('background-color', '#181818')    # Slightly lighter for even rows
        ]},
        {'selector': 'tr:hover', 'props': [
            ('background-color', '#333')       # Highlight on hover
        ]},
    ]).set_properties(**{
        'border': '2px solid #FFD700',
        'padding': '8px',
        'text-align': 'right',
        'color': '#fff',
        'background-color': '#222'
    })

def sort_category_cohorts(df):
    custom_order = [
        ("NU", "New Users"),
        ("RU_last30_Days", "RU1-5"),
        ("RU_last30_Days", "RU6-10"),
        ("RU_last30_Days", "RU10+"),
        ("DU_30_45_Days", "RU1-5"),
        ("DU_30_45_Days", "RU6-10"),
        ("DU_30_45_Days", "RU10+"),
        ("DU_45+_Days", "RU1-5"),
        ("DU_45+_Days", "RU6-10"),
        ("DU_45+_Days", "RU10+"),
        ("Unassigned", "Unassigned"),
    ]
    if "category" in df.columns and "cohorts" in df.columns:
        df["__sort_order"] = df.apply(
            lambda row: custom_order.index((row["category"], row["cohorts"]))
            if (row["category"], row["cohorts"]) in custom_order else len(custom_order),
            axis=1
        )
        df = df.sort_values("__sort_order").drop(columns="__sort_order")
        df = df.reset_index(drop=True)
    return df

def ask_ai_agent(question, api_key, context=None):
    """Send a question to OpenAI and return the response. Optionally provide table context."""
    prompt = question
    if context:
        prompt = f"Context: {context}\n\nQuestion: {question}"
    try:
        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"AI Error: {e}"