"""Operations dashboard: pipeline health, key reconciliation, personal-data controls and store metrics.

It reads the same ops, conformed and metrics tables everything else does, as the dashboard_reader role, which has
no access to raw or restricted data. Run with: streamlit run dashboard/app.py
"""

import streamlit as st
import views

st.set_page_config(page_title="Store ops pipelines", layout="wide")

nav = st.navigation([st.Page(views.pipeline_health, title="Pipeline health", url_path="health", default=True),
                     st.Page(views.reconciliation, title="Key reconciliation", url_path="reconciliation"),
                     st.Page(views.controls, title="Personal data controls", url_path="controls"),
                     st.Page(views.store_metrics, title="Store metrics", url_path="stores")])
nav.run()
