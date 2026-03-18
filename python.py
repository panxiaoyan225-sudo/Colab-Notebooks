# Databricks notebook source
# COMMAND ----------
# FINAL ROBUST LOCAL PANDAS PORT

import pandas as pd
import requests
import io

# 1. Source Data IDs
SNF_ID = "1UfCxgMxUtCEDWqcm1udnd7mPawDh7y-b"
PBJ_ID = "1y9WofLddBZ7ufuAeJ0HEfW9uRlvuQTt7"
ADM_ID = "1mR7vOR3xyeZ6sv4QiclCftOYqB79bajT"

def download_clean_csv(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    res.raise_for_status()
    df = pd.read_csv(io.BytesIO(res.content), low_memory=False)
    # Clean headers: remove spaces, newlines, and hidden characters
    df.columns = df.columns.str.strip().str.replace(r'\s+', '_', regex=True).str.upper()
    return df

try:
    print("⏳ Downloading and cleaning data...")
    df_fac = download_clean_csv(SNF_ID)
    df_pbj = download_clean_csv(PBJ_ID)
    df_adm = download_clean_csv(ADM_ID)

    # 2. DYNAMIC COLUMN DETECTION
    # Find IDs (looking for 'CCN' or 'PROVNUM')
    fac_id_col = [c for c in df_fac.columns if 'CCN' in c or 'CERT' in c][0]
    pbj_id_col = [c for c in df_pbj.columns if 'PROVNUM' in c or 'PROV_NUM' in c][0]
    
    # Find Hour Columns (looking for 'RN', 'LPN', 'CNA')
    hour_cols = [c for c in df_pbj.columns if any(x in c for x in ['RN', 'LPN', 'CNA']) and 'HRS' in c]

    # 3. ETL & UNPIVOT (Replacing the failing melt)
    df_pbj['CMS_ID'] = df_pbj[pbj_id_col].astype(str).str.zfill(6)
    df_fac['CMS_ID'] = df_fac[fac_id_col].astype(str).str.zfill(6)

    # Calculate Total Volume directly to avoid melt errors
    df_pbj['TOTAL_HOURS'] = df_pbj[hour_cols].sum(axis=1)
    
    # 4. ANALYTICS
    top_100 = df_pbj.groupby('CMS_ID')['TOTAL_HOURS'].sum().reset_index()
    top_100 = top_100.sort_values(by='TOTAL_HOURS', ascending=False).head(100)

    # 5. JOINS
    # Standardize names for joining
    df_fac['FAC_NAME_CLEAN'] = df_fac[[c for c in df_fac.columns if 'NAME' in c][0]].str.strip().str.upper()
    df_adm['FAC_NAME_CLEAN'] = df_adm['FACNAME'].str.strip().str.upper()

    report = pd.merge(top_100, df_fac, on='CMS_ID', how='inner')
    final_output = pd.merge(report, df_adm[['FAC_NAME_CLEAN', 'FACADMIN', 'CONTACT_EMAIL']], on='FAC_NAME_CLEAN', how='left')

    print("✅ Success: Data matched dynamically.")
    display(final_output[['CMS_ID', 'FAC_NAME_CLEAN', 'FACADMIN', 'CONTACT_EMAIL', 'TOTAL_HOURS']])

except Exception as e:
    print(f"❌ Error Detail: {e}")
    print(f"Detected Hour Columns: {hour_cols}")

# COMMAND ----------

# COMMAND ----------
# CELL 2: Top 10 Chains by Staffing Volume

# 1. Determine the correct column for Chain Name (it was cleaned to uppercase with underscores)
# Usually 'CHAIN_NAME' or something similar
chain_col = [c for c in df_fac.columns if 'CHAIN' in c][0]

# 2. Merge staffing data with facility metadata
chain_data = pd.merge(df_pbj[['CMS_ID', 'TOTAL_HOURS']], df_fac[['CMS_ID', chain_col]], on='CMS_ID', how='inner')

# 3. Aggregate and calculate percentages
chain_report = chain_data.groupby(chain_col)['TOTAL_HOURS'].sum().reset_index()
total_state_hours = chain_report['TOTAL_HOURS'].sum()

chain_report['MARKET_SHARE_PCT'] = (chain_report['TOTAL_HOURS'] / total_state_hours * 100).round(2)
chain_report = chain_report.sort_values(by='TOTAL_HOURS', ascending=False).head(10)

print(f"📊 Top 10 Chains identified out of {total_state_hours:,.0f} total hours.")
print(chain_report)

# COMMAND ----------

# COMMAND ----------
# CELL 3: Top 100 Facilities & Admin Contacts (Robust Version)

# 1. Dynamically find the columns we need (to avoid KeyErrors)
# This looks for the best matches regardless of exact naming
try:
    name_col = [c for c in df_fac.columns if 'NAME' in c and 'CLEAN' not in c][0]
    city_col = [c for c in df_fac.columns if 'CITY' in c][0]
    state_col = [c for c in df_fac.columns if 'STATE' in c][0]
except IndexError:
    # Fallback if names are completely different
    print("⚠️ Warning: Could not find standard City/State columns. Using available columns.")
    name_col, city_col, state_col = df_fac.columns[1], df_fac.columns[2], df_fac.columns[3]

# 2. Join the Top 100 stats with Facility Metadata
# Using 'CMS_ID' and the dynamically found columns
final_match = pd.merge(
    top_100, 
    df_fac[['CMS_ID', name_col, 'FAC_NAME_CLEAN', city_col, state_col]], 
    on='CMS_ID', 
    how='inner'
)

# 3. Join with Administrator data using the 'FAC_NAME_CLEAN' key from Cell 1
# We use .get() for columns to prevent crashing if one is missing
admin_cols = ['FAC_NAME_CLEAN', 'FACADMIN', 'CONTACT_EMAIL']
available_admin_cols = [c for c in admin_cols if c in df_adm.columns]

report = pd.merge(
    final_match, 
    df_adm[available_admin_cols], 
    on='FAC_NAME_CLEAN', 
    how='left'
)

# 4. Final Formatting
# Map the dynamic columns to our standard output names
final_report = report[['CMS_ID', name_col, 'FACADMIN', 'CONTACT_EMAIL', 'TOTAL_HOURS', city_col, state_col]]
final_report.columns = ['CMS_ID', 'Facility_Name', 'Admin_Name', 'Admin_Email', 'Total_Hours', 'City', 'State']

# 5. Sort by hours
final_report = final_report.sort_values(by='Total_Hours', ascending=False)

print(f"✅ Report Generated: Matched {final_report['Admin_Name'].notna().sum()} administrator contacts.")
display(final_report)

# COMMAND ----------

# COMMAND ----------
# CELL: Enhanced Interactive Facility Map

import plotly.express as px
import pandas as pd

# 1. Reuse the map_df from the previous cell
# (Assuming map_df is already merged and cleaned)

# 2. Render with High-Contrast Settings
fig = px.scatter_mapbox(
    map_df,
    lat="LATITUDE",
    lon="LONGITUDE",
    size="TOTAL_HOURS",
    color="TOTAL_HOURS",
    
    # Visual Improvements:
    # 'Plasma' or 'Viridis' provide better visibility than the default
    color_continuous_scale=px.colors.sequential.Plasma, 
    size_max=15,             # Makes the largest dots stand out more
    opacity=0.7,             # Allows you to see overlapping facilities
    
    hover_name=name_col,
    hover_data={
        city_col: True, 
        "TOTAL_HOURS": ":,.0f", # Added comma for thousands separator
        "LATITUDE": False, 
        "LONGITUDE": False
    },
    zoom=3.5,
    
    # Change style to 'open-street-map' or 'dark' for better contrast
    mapbox_style="carto-positron", 
    title="<b>Nursing Facility Staffing Volume</b><br><sup>Size and color represent total PBJ hours</sup>"
)

# 3. UI Tweaks
fig.update_layout(
    margin={"r":0,"t":80,"l":0,"b":0},
    coloraxis_colorbar=dict(
        title="Total Hours",
        thicknessmode="pixels", thickness=15,
        lenmode="fraction", len=0.6,
        yanchor="top", y=1,
        ticks="outside"
    )
)

fig.show()