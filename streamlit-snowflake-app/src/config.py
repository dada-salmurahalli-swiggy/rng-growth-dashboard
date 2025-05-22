import streamlit as st

SNOWFLAKE_CONFIG = {
    "user": st.secrets["snowflake"]["user"],
    "account": st.secrets["snowflake"]["account"],
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
    "authenticator": st.secrets["snowflake"]["authenticator"],  # keep this for SSO
}