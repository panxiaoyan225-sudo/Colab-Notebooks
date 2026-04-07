# Databricks notebook source
# COMMAND ----------
# FINAL ROBUST LOCAL PANDAS PORT - STABLE VERSION

import pandas as pd
import requests
import io
import plotly.express as px

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

def get_col_safely(df, keywords, label):
    """Finds a column containing keywords or provides a descriptive error."""
    matches = [c for c in df.columns if any(k in c for k in keywords)]
    if not matches:
        available = ", ".join(df.columns[:5])
        raise ValueError(f"CRITICAL: Could not find {label} column. Found: {available}...")
    return matches[0]

# Initialize global variables to prevent NameErrors in 'except' block
hour_cols = []
top_100 = pd.DataFrame()

try:
    print("⏳ Downloading and cleaning data...")
    df_fac = download_clean_csv(SNF_ID)
    df_pbj = download_clean_csv(PBJ_ID)
    df_adm = download_clean_csv(ADM_ID)

    # 2. DYNAMIC COLUMN DETECTION (Safer)
    fac_id_col = get_col_safely(df_fac, ['CCN', 'CERT'], "Facility ID")
    pbj_id_col = get_col_safely(df_pbj, ['PROVNUM', 'PROV_NUM'], "PBJ ID")
    
    # Find Hour Columns
    hour_cols = [c for c in df_pbj.columns if any(x in c for x in ['RN', 'LPN', 'CNA']) and 'HRS' in c]
    if not hour_cols:
        raise ValueError("No Staffing Hour columns (RN/LPN/CNA) found in PBJ data.")

    # 3. ETL & UNPIVOT
    df_pbj['CMS_ID'] = df_pbj[pbj_id_col].astype(str).str.zfill(6)
    df_fac['CMS_ID'] = df_fac[fac_id_col].astype(str).str.zfill(6)

    # Calculate Total Volume
    df_pbj['TOTAL_HOURS'] = df_pbj[hour_cols].sum(axis=1)
    
    # 4. ANALYTICS
    top_100 = df_pbj.groupby('CMS_ID')['TOTAL_HOURS'].sum().reset_index()
    top_100 = top_100.sort_values(by='TOTAL_HOURS', ascending=False).head(100)

    # 5. JOINS
    name_search_col = get_col_safely(df_fac, ['NAME'], "Facility Name")
    df_fac['FAC_NAME_CLEAN'] = df_fac[name_search_col].str.strip().str.upper()
    
    # Check for FACNAME in Admin data
    adm_name_col = get_col_safely(df_adm, ['FACNAME', 'FAC_NAME'], "Admin Facility Name")
    df_adm['FAC_NAME_CLEAN'] = df_adm[adm_name_col].str.strip().str.upper()

    report = pd.merge(top_100, df_fac, on='CMS_ID', how='inner')
    
    # Optional Admin Join
    admin_fields = [c for c in ['FAC_NAME_CLEAN', 'FACADMIN', 'CONTACT_EMAIL'] if c in df_adm.columns]
    final_output = pd.merge(report, df_adm[admin_fields], on='FAC_NAME_CLEAN', how='left')

    print("✅ Success: Data matched dynamically.")

except Exception as e:
    print(f"❌ Error Detail: {e}")
    if hour_cols:
        print(f"Detected Hour Columns: {hour_cols}")
    else:
        print("Debugging: Variable 'hour_cols' was never populated.")

# COMMAND ----------
# CELL 2: Top 10 Chains by Staffing Volume

try:
    chain_col = get_col_safely(df_fac, ['CHAIN'], "Chain Name")
    chain_data = pd.merge(df_pbj[['CMS_ID', 'TOTAL_HOURS']], df_fac[['CMS_ID', chain_col]], on='CMS_ID', how='inner')

    chain_report = chain_data.groupby(chain_col)['TOTAL_HOURS'].sum().reset_index()
    total_state_hours = chain_report['TOTAL_HOURS'].sum()

    chain_report['MARKET_SHARE_PCT'] = (chain_report['TOTAL_HOURS'] / total_state_hours * 100).round(2)
    chain_report = chain_report.sort_values(by='TOTAL_HOURS', ascending=False).head(10)

    print(f"📊 Top 10 Chains identified.")
    print(chain_report)
except Exception as e:
    print(f"⚠️ Cell 2 Skip: {e}")

# COMMAND ----------
# CELL 3: Top 100 Facilities & Admin Contacts

try:
    name_col = [c for c in df_fac.columns if 'NAME' in c and 'CLEAN' not in c][0]
    city_col = get_col_safely(df_fac, ['CITY'], "City")
    state_col = get_col_safely(df_fac, ['STATE'], "State")

    final_match = pd.merge(
        top_100, 
        df_fac[['CMS_ID', name_col, 'FAC_NAME_CLEAN', city_col, state_col]], 
        on='CMS_ID', 
        how='inner'
    )

    admin_cols = ['FAC_NAME_CLEAN', 'FACADMIN', 'CONTACT_EMAIL']
    available_admin_cols = [c for c in admin_cols if c in df_adm.columns]

    report = pd.merge(final_match, df_adm[available_admin_cols], on='FAC_NAME_CLEAN', how='left')

    final_report = report[['CMS_ID', name_col, 'FACADMIN', 'CONTACT_EMAIL', 'TOTAL_HOURS', city_col, state_col]]
    final_report.columns = ['CMS_ID', 'Facility_Name', 'Admin_Name', 'Admin_Email', 'Total_Hours', 'City', 'State']
    final_report = final_report.sort_values(by='Total_Hours', ascending=False)

    print(f"✅ Report Generated: Matched {final_report['Admin_Name'].notna().sum()} administrator contacts.")
    print(final_report.head(10))
    
    # Save Outputs
    final_report.to_csv("final_staffing_report.csv", index=False)
except Exception as e:
    print(f"⚠️ Cell 3 Skip: {e}")

# COMMAND ----------
# CELL 4: Interactive Facility Map

try:
    facility_hours = df_pbj.groupby("CMS_ID")["TOTAL_HOURS"].sum().reset_index()
    zip_col = get_col_safely(df_fac, ['ZIP', 'POSTAL'], "Zip")

    map_df = pd.merge(
        facility_hours,
        df_fac[['CMS_ID', name_search_col, city_col, zip_col, 'LATITUDE', 'LONGITUDE']],
        on="CMS_ID",
        how="inner"
    )

    map_df['LATITUDE'] = pd.to_numeric(map_df['LATITUDE'], errors='coerce')
    map_df['LONGITUDE'] = pd.to_numeric(map_df['LONGITUDE'], errors='coerce')
    map_df = map_df.dropna(subset=['LATITUDE', 'LONGITUDE'])

    fig = px.scatter_mapbox(
        map_df,
        lat="LATITUDE",
        lon="LONGITUDE",
        size="TOTAL_HOURS",
        color="TOTAL_HOURS",
        hover_name=name_search_col,
        zoom=3.5,
        mapbox_style="carto-positron",
        title="Nursing Facility Staffing Volume Map"
    )

    fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0})
    fig.write_html("staffing_map.html")
    print("🌍 Map generated as staffing_map.html")
except Exception as e:
    print(f"⚠️ Map Skip: {e}")