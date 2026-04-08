import os
import json
from streamlit_lottie import st_lottie
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import datetime as dt
import logging
import platform
import base64
import json
from io import BytesIO
from fpdf import FPDF
import warnings
import os
import json

def load_lottiefile(filepath: str):
    base_path = os.path.dirname(__file__)
    full_path = os.path.join(base_path, filepath)
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

warnings.filterwarnings("ignore")

try:
    import winsound
except Exception:
    winsound = None

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

try:
    import smtplib
    from email.message import EmailMessage
except Exception:
    smtplib = None
    EmailMessage = None

try:
    import requests
except Exception:
    requests = None


st.set_page_config(
    page_title="Eva Pharma Full Simulator",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

logging.basicConfig(
    filename="factory_sim.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.25rem;
        padding-bottom: 1.25rem;
    }
    div[data-testid="metric-container"] {
        background: #0f172a;
        border: 1px solid #1e293b;
        padding: 12px 14px;
        border-radius: 16px;
    }
    section[data-testid="stSidebar"] {
        background: #0b1220;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Eva Pharma Production Line Simulator")
st.caption("Digital twin dashboard for production analysis, what-if simulation, alerts, reporting, and scenario comparison.")


def safe_beep():
    try:
        if winsound and platform.system().lower().startswith("win"):
            winsound.Beep(1200, 400)
    except Exception:
        pass


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def make_excel_bytes(df: pd.DataFrame, sheet_name: str = "Report") -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buffer.getvalue()


def make_pdf_bytes(title: str, df: pd.DataFrame) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, title, ln=True, align="C")
    pdf.ln(4)

    pdf.set_font("Arial", size=8)
    header = " | ".join([str(c) for c in df.columns])
    pdf.multi_cell(0, 5, header)
    pdf.ln(2)

    for _, row in df.iterrows():
        line = " | ".join([str(row[c]) for c in df.columns])
        pdf.multi_cell(0, 5, line)

    out = pdf.output(dest="S")
    if isinstance(out, bytes):
        return out
    return out.encode("latin1", errors="ignore")


def send_email_alert(smtp_host, smtp_port, username, password, to_addr, subject, body):
    if not all([smtplib, EmailMessage, smtp_host, smtp_port, username, password, to_addr]):
        return False, "Email not configured"
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = username
        msg["To"] = to_addr
        msg.set_content(body)

        with smtplib.SMTP(smtp_host, int(smtp_port), timeout=15) as server:
            server.starttls()
            server.login(username, password)
            server.send_message(msg)
        return True, "Email sent"
    except Exception as e:
        logging.exception("Email send failed")
        return False, str(e)


def send_sms_alert_stub(provider_url, api_key, to_number, message):
    if not requests or not provider_url:
        return False, "SMS not configured"
    try:
        payload = {
            "api_key": api_key,
            "to": to_number,
            "message": message,
        }
        r = requests.post(provider_url, json=payload, timeout=15)
        ok = r.status_code in (200, 201, 202)
        return ok, f"SMS status {r.status_code}"
    except Exception as e:
        logging.exception("SMS send failed")
        return False, str(e)


def synthetic_trend_frame(df: pd.DataFrame, periods: int, period_label: str, value_col: str):
    rows = []
    for _, row in df.iterrows():
        base = float(row[value_col])
        stage = row["Stage"]
        for i in range(1, periods + 1):
            variation = 1 + 0.04 * np.sin((i / max(periods, 1)) * 2 * np.pi)
            rows.append(
                {
                    "Stage": stage,
                    period_label: i,
                    "Units": max(base * variation, 0.1),
                }
            )
    return pd.DataFrame(rows)


def compute_scenario(base_df: pd.DataFrame, reductions: dict, shift_hours: int, days_per_week: int, min_output_threshold: float):
    df = base_df.copy()
    df["ReductionPercent"] = df["Stage"].map(reductions).fillna(0).astype(float)
    df["Simulated"] = df["BaseTime"] * (1 - df["ReductionPercent"] / 100.0)
    df["Simulated"] = df["Simulated"].clip(lower=0.1)

    bottleneck_idx = df["Simulated"].idxmax()
    bottleneck_row = df.loc[bottleneck_idx]

    df["UnitsPerHour"] = 60.0 / df["Simulated"]
    df["ShiftOutput"] = df["UnitsPerHour"] * shift_hours
    df["DailyOutput"] = df["ShiftOutput"]
    df["WeeklyOutput"] = df["DailyOutput"] * days_per_week
    df["MonthlyOutput"] = df["WeeklyOutput"] * 4
    df["SuggestedTime"] = df["Simulated"]

    if pd.notna(bottleneck_idx):
        df.loc[bottleneck_idx, "SuggestedTime"] = max(df.loc[bottleneck_idx, "Simulated"] * 0.8, 0.1)

    df["Headroom"] = df["ShiftOutput"] - min_output_threshold
    df["Alert"] = df["ShiftOutput"].apply(lambda x: "Below threshold" if x < min_output_threshold else "")
    df["RiskLevel"] = df["ShiftOutput"].apply(lambda x: "High" if x < min_output_threshold else "Normal")

    line_output = (60.0 / max(float(bottleneck_row["Simulated"]), 0.1)) * shift_hours
    efficiency = (df["BaseTime"].sum() / (len(df) * float(bottleneck_row["Simulated"]))) * 100 if len(df) else 0.0

    projected_after_improvement = (60.0 / max(float(df["SuggestedTime"].max()), 0.1)) * shift_hours

    quick_time = float(bottleneck_row["Simulated"]) * 0.8
    temp = df["Simulated"].copy()
    temp.loc[df["Stage"] == bottleneck_row["Stage"]] = quick_time
    quick_action_output = (60.0 / max(float(temp.max()), 0.1)) * shift_hours

    logging.info(
        f"Scenario computed | bottleneck={bottleneck_row['Stage']} | line_output={line_output:.2f} | eff={efficiency:.2f}"
    )

    return {
        "Data": df,
        "Bottleneck": bottleneck_row["Stage"],
        "BottleneckTime": float(bottleneck_row["Simulated"]),
        "TotalOutput": float(line_output),
        "LineOutput": float(line_output),
        "Efficiency": float(efficiency),
        "ProjectedAfterImprovement": float(projected_after_improvement),
        "QuickActionStage": bottleneck_row["Stage"],
        "QuickActionOutput": float(quick_action_output),
    }


with st.sidebar:
    try:
        lottie_factory = load_lottiefile("factory.json")
        st_lottie(lottie_factory, height=200, key="factory_anim")
    except Exception as e:
        st.sidebar.error("لم يتم تحميل الأنيميشن")
    st.header("Factory Settings")
    company_name = st.text_input("Factory name", value="Eva Pharma")
    num_stages = st.number_input("Number of stages", min_value=2, max_value=20, value=5)
    shift_hours = st.slider("Shift hours per day", min_value=1, max_value=24, value=8)
    days_per_week = st.slider("Days per week", min_value=1, max_value=7, value=5)
    num_scenarios = st.number_input("Number of scenarios", min_value=1, max_value=6, value=3)
    min_output_threshold = st.number_input("Minimum acceptable stage output per shift", min_value=1.0, value=10.0, step=1.0)
    enable_audio = st.checkbox("Enable audio alerts", value=False)
    enable_logging_ui = st.checkbox("Show logs on screen", value=True)

    st.divider()
    st.subheader("Notifications")
    email_enabled = st.checkbox("Enable email integration", value=False)
    email_host = st.text_input("SMTP host", value="")
    email_port = st.text_input("SMTP port", value="587")
    email_user = st.text_input("SMTP username", value="")
    email_pass = st.text_input("SMTP password", type="password", value="")
    email_to = st.text_input("Notify email to", value="")

    sms_enabled = st.checkbox("Enable SMS integration", value=False)
    sms_api_url = st.text_input("SMS API URL", value="")
    sms_api_key = st.text_input("SMS API key", value="")
    sms_to = st.text_input("SMS destination", value="")

    st.divider()
    auto_refresh = st.checkbox("Auto refresh dashboard", value=False)
    refresh_seconds = st.slider("Refresh interval seconds", 5, 60, 15)

if auto_refresh and st_autorefresh:
    st_autorefresh(interval=refresh_seconds * 1000, key="factory_refresh")

st.subheader("Stage Configuration")
st.caption("Enter baseline processing time for each stage. Scenario sliders can then simulate improvements or delays.")

base_rows = []
for i in range(int(num_stages)):
    c1, c2 = st.columns([2, 2])
    stage_name = c1.text_input(f"Stage {i + 1} name", value=f"Stage {i + 1}", key=f"stage_name_{i}")
    stage_time = c2.number_input(
        f"{stage_name} baseline time (min)",
        min_value=0.1,
        value=5.0,
        step=0.1,
        key=f"stage_time_{i}",
    )
    base_rows.append({"Stage": stage_name.strip(), "BaseTime": float(stage_time)})

base_df = pd.DataFrame(base_rows)

if base_df.empty:
    st.stop()

st.divider()
st.subheader("Baseline Overview")
st.dataframe(base_df, use_container_width=True)

scenario_outputs = []
scenario_reduction_maps = []

for s in range(int(num_scenarios)):
    st.divider()
    st.subheader(f"Scenario {s + 1}")
    with st.expander(f"Adjust Scenario {s + 1}", expanded=(s == 0)):
        reduction_map = {}
        for i, row in base_df.iterrows():
            cols = st.columns([2, 2, 1])
            cols[0].write(row["Stage"])
            reduction = cols[1].slider(
                f"Reduction % - {row['Stage']} - Scenario {s + 1}",
                min_value=0,
                max_value=50,
                value=20 if i == int(base_df["BaseTime"].idxmax()) else 0,
                key=f"reduction_s{s}_stage{i}",
            )
            reduction_map[row["Stage"]] = reduction
            cols[2].write(f"{reduction}%")
        scenario_reduction_maps.append(reduction_map)

    result = compute_scenario(base_df, reduction_map, shift_hours, days_per_week, min_output_threshold)
    scenario_outputs.append({"Scenario": f"Scenario {s + 1}", **result})

tabs = st.tabs([
    "KPIs",
    "Heatmaps",
    "Trends",
    "Comparison",
    "Alerts",
    "What-if",
    "Reports",
    "Master Dashboard",
    "Optimization Center",
    "Target Planning",
    "Correlation",
    "Pareto",
    "Operating Modes",
    "Snapshot",
    "Control Board",
    "Executive Summary",
])

with tabs[0]:
    st.subheader("Scenario KPIs")
    cols = st.columns(min(len(scenario_outputs), 4))
    if not cols:
        cols = st.columns(1)
    for idx, sc in enumerate(scenario_outputs):
        col = cols[idx % len(cols)]
        col.metric(f"{sc['Scenario']} Bottleneck", sc["Bottleneck"])
        col.metric(f"{sc['Scenario']} Line Output", f"{sc['LineOutput']:.1f}")
        col.metric(f"{sc['Scenario']} Efficiency", f"{sc['Efficiency']:.1f}%")
        col.metric(f"{sc['Scenario']} After Improvement", f"{sc['ProjectedAfterImprovement']:.1f}")

    st.divider()
    for sc in scenario_outputs:
        st.markdown(f"### {sc['Scenario']}")
        st.dataframe(
            sc["Data"][["Stage", "BaseTime", "ReductionPercent", "Simulated", "UnitsPerHour", "ShiftOutput", "Headroom", "Alert"]],
            use_container_width=True,
        )

with tabs[1]:
    st.subheader("Heatmaps")
    for sc in scenario_outputs:
        heat_df = sc["Data"][["Stage", "ShiftOutput"]].copy().set_index("Stage")
        fig = px.imshow(
            heat_df.T,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="Viridis",
            title=f"{sc['Scenario']} Stage Output Heatmap",
        )
        st.plotly_chart(fig, use_container_width=True)

        daily = synthetic_trend_frame(sc["Data"], days_per_week, "Day", "ShiftOutput")
        weekly = synthetic_trend_frame(sc["Data"], days_per_week * 4, "WeekDay", "ShiftOutput")
        fig_daily = px.density_heatmap(
            daily,
            x="Day",
            y="Stage",
            z="Units",
            color_continuous_scale="Viridis",
            title=f"{sc['Scenario']} Daily Heatmap",
        )
        fig_weekly = px.density_heatmap(
            weekly,
            x="WeekDay",
            y="Stage",
            z="Units",
            color_continuous_scale="Viridis",
            title=f"{sc['Scenario']} Weekly Heatmap",
        )
        st.plotly_chart(fig_daily, use_container_width=True)
        st.plotly_chart(fig_weekly, use_container_width=True)

with tabs[2]:
    st.subheader("Trends")
    for sc in scenario_outputs:
        hourly = synthetic_trend_frame(sc["Data"], shift_hours, "Hour", "ShiftOutput")
        monthly = synthetic_trend_frame(sc["Data"], days_per_week * 4, "MonthDay", "ShiftOutput")

        fig_hour = px.line(hourly, x="Hour", y="Units", color="Stage", markers=True, title=f"{sc['Scenario']} Hourly Trend")
        fig_month = px.line(monthly, x="MonthDay", y="Units", color="Stage", markers=True, title=f"{sc['Scenario']} Monthly Trend")

        st.plotly_chart(fig_hour, use_container_width=True)
        st.plotly_chart(fig_month, use_container_width=True)

        st.markdown(f"### {sc['Scenario']} Trend Table")
        trend_table = hourly.groupby("Stage", as_index=False)["Units"].mean().rename(columns={"Units": "AvgUnitsPerHour"})
        st.dataframe(trend_table, use_container_width=True)

with tabs[3]:
    st.subheader("Scenario Comparison")
    comp_rows = []
    for sc in scenario_outputs:
        for _, r in sc["Data"].iterrows():
            comp_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "ShiftOutput": r["ShiftOutput"],
                    "WeeklyOutput": r["WeeklyOutput"],
                    "MonthlyOutput": r["MonthlyOutput"],
                    "UnitsPerHour": r["UnitsPerHour"],
                }
            )

    comp_df = pd.DataFrame(comp_rows)

    fig_shift = px.bar(
        comp_df,
        x="Stage",
        y="ShiftOutput",
        color="Scenario",
        barmode="group",
        title="Shift Output Comparison",
    )
    fig_weekly = px.bar(
        comp_df,
        x="Stage",
        y="WeeklyOutput",
        color="Scenario",
        barmode="group",
        title="Weekly Output Comparison",
    )
    fig_monthly = px.bar(
        comp_df,
        x="Stage",
        y="MonthlyOutput",
        color="Scenario",
        barmode="group",
        title="Monthly Output Comparison",
    )

    st.plotly_chart(fig_shift, use_container_width=True)
    st.plotly_chart(fig_weekly, use_container_width=True)
    st.plotly_chart(fig_monthly, use_container_width=True)

    st.dataframe(comp_df.sort_values(["Scenario", "ShiftOutput"], ascending=[True, False]), use_container_width=True)

with tabs[4]:
    st.subheader("Alerts Center")
    alert_rows = []
    for sc in scenario_outputs:
        alert_df = sc["Data"][sc["Data"]["ShiftOutput"] < min_output_threshold].copy()
        for _, r in alert_df.iterrows():
            alert_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "Output": r["ShiftOutput"],
                    "Headroom": r["Headroom"],
                    "Risk": r["RiskLevel"],
                }
            )
            st.error(f"{sc['Scenario']} - {r['Stage']} output is below threshold at {r['ShiftOutput']:.1f}")
            if enable_audio:
                safe_beep()

            subject = f"{company_name} Alert - {sc['Scenario']} - {r['Stage']}"
            body = f"Stage {r['Stage']} in {sc['Scenario']} is below threshold. Output is {r['ShiftOutput']:.1f}."

            if email_enabled:
                ok, msg = send_email_alert(email_host, email_port, email_user, email_pass, email_to, subject, body)
                st.info(f"Email status for {r['Stage']}: {msg}")
            if sms_enabled:
                ok, msg = send_sms_alert_stub(sms_api_url, sms_api_key, sms_to, body)
                st.info(f"SMS status for {r['Stage']}: {msg}")

            logging.warning(f"ALERT | {sc['Scenario']} | {r['Stage']} | Output={r['ShiftOutput']:.2f}")

    if alert_rows:
        st.dataframe(pd.DataFrame(alert_rows), use_container_width=True)
    else:
        st.success("No stages are below the threshold.")

with tabs[5]:
    st.subheader("What-if Lab")
    selected_scenario_name = st.selectbox(
        "Choose scenario to test",
        [sc["Scenario"] for sc in scenario_outputs],
        key="whatif_scenario_selector",
    )
    selected_index = [sc["Scenario"] for sc in scenario_outputs].index(selected_scenario_name)
    selected_scenario = scenario_outputs[selected_index]

    st.markdown("Adjust multiple stages at once.")
    lab_reduction = {}
    for i, row in selected_scenario["Data"].iterrows():
        c1, c2 = st.columns([2, 2])
        c1.write(row["Stage"])
        lab_reduction[row["Stage"]] = c2.slider(
            f"Test reduction % - {row['Stage']}",
            min_value=0,
            max_value=50,
            value=int(row["ReductionPercent"]),
            key=f"lab_{selected_scenario_name}_{i}",
        )

    lab_result = compute_scenario(base_df, lab_reduction, shift_hours, days_per_week, min_output_threshold)
    lab_df = lab_result["Data"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Lab bottleneck", lab_result["Bottleneck"])
    c2.metric("Lab line output", f"{lab_result['LineOutput']:.1f}")
    c3.metric("Lab efficiency", f"{lab_result['Efficiency']:.1f}%")
    c4.metric("Gain vs projected", f"{lab_result['ProjectedAfterImprovement'] - selected_scenario['ProjectedAfterImprovement']:.1f}")

    st.dataframe(
        lab_df[["Stage", "BaseTime", "ReductionPercent", "Simulated", "UnitsPerHour", "ShiftOutput", "Headroom", "Alert"]],
        use_container_width=True,
    )

    fig_lab = px.bar(
        lab_df,
        x="Stage",
        y=["BaseTime", "Simulated"],
        barmode="group",
        title=f"{selected_scenario_name} Base vs What-if Times",
    )
    st.plotly_chart(fig_lab, use_container_width=True)

    st.write(f"If bottleneck reduction is applied first, projected line output becomes {lab_result['QuickActionOutput']:.1f} units per shift.")

with tabs[6]:
    st.subheader("Reports")
    for sc in scenario_outputs:
        excel_bytes = make_excel_bytes(sc["Data"], sheet_name=sc["Scenario"])
        pdf_bytes = make_pdf_bytes(f"{company_name} - {sc['Scenario']} Report", sc["Data"])

        st.download_button(
            label=f"Download {sc['Scenario']} Excel",
            data=excel_bytes,
            file_name=f"{sc['Scenario']}_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"excel_{sc['Scenario']}",
        )
        st.download_button(
            label=f"Download {sc['Scenario']} PDF",
            data=pdf_bytes,
            file_name=f"{sc['Scenario']}_Report.pdf",
            mime="application/pdf",
            key=f"pdf_{sc['Scenario']}",
        )

    master_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        for _, r in d.iterrows():
            master_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "BaseTime": safe_float(r["BaseTime"]),
                    "ReductionPercent": safe_float(r["ReductionPercent"]),
                    "Simulated": safe_float(r["Simulated"]),
                    "UnitsPerHour": safe_float(r["UnitsPerHour"]),
                    "ShiftOutput": safe_float(r["ShiftOutput"]),
                    "DailyOutput": safe_float(r["DailyOutput"]),
                    "WeeklyOutput": safe_float(r["WeeklyOutput"]),
                    "MonthlyOutput": safe_float(r["MonthlyOutput"]),
                    "SuggestedTime": safe_float(r["SuggestedTime"]),
                    "Headroom": safe_float(r["Headroom"]),
                    "Alert": r["Alert"],
                    "RiskLevel": r["RiskLevel"],
                    "Efficiency": sc["Efficiency"],
                    "Bottleneck": sc["Bottleneck"],
                    "ProjectedAfterImprovement": sc["ProjectedAfterImprovement"],
                }
            )

    master_df = pd.DataFrame(master_rows)

    st.download_button(
        "Download Master Excel",
        data=make_excel_bytes(master_df, sheet_name="Master"),
        file_name=f"{company_name}_MasterReport.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="master_excel",
    )
    st.download_button(
        "Download Master PDF",
        data=make_pdf_bytes(f"{company_name} Master Production Report", master_df),
        file_name=f"{company_name}_MasterReport.pdf",
        mime="application/pdf",
        key="master_pdf",
    )

with tabs[7]:
    st.subheader("Master Summary Dashboard")

    master_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        for _, r in d.iterrows():
            master_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "Simulated": safe_float(r["Simulated"]),
                    "UnitsPerHour": safe_float(r["UnitsPerHour"]),
                    "ShiftOutput": safe_float(r["ShiftOutput"]),
                    "DailyOutput": safe_float(r["DailyOutput"]),
                    "WeeklyOutput": safe_float(r["WeeklyOutput"]),
                    "MonthlyOutput": safe_float(r["MonthlyOutput"]),
                    "SuggestedTime": safe_float(r["SuggestedTime"]),
                    "Alert": r["Alert"],
                    "Efficiency": safe_float(sc["Efficiency"]),
                    "Bottleneck": sc["Bottleneck"],
                    "ProjectedAfterImprovement": safe_float(sc["ProjectedAfterImprovement"]),
                }
            )

    master_df = pd.DataFrame(master_rows)

    summary_cols = st.columns(4)
    summary_cols[0].metric("Scenarios", len(scenario_outputs))
    summary_cols[1].metric("Stages", len(base_df))
    summary_cols[2].metric("Rows", len(master_df))
    summary_cols[3].metric("Alerts", int((master_df["Alert"] != "").sum()) if not master_df.empty else 0)

    st.dataframe(master_df.sort_values(["Scenario", "ShiftOutput"], ascending=[True, False]), use_container_width=True)

    if not master_df.empty:
        pivot_heat = master_df.pivot_table(index="Scenario", columns="Stage", values="ShiftOutput", aggfunc="mean").fillna(0)
        fig_master_heat = px.imshow(
            pivot_heat,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="Viridis",
            title="Scenario vs Stage Output Heatmap",
        )
        st.plotly_chart(fig_master_heat, use_container_width=True)

    risk_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        mean_output = d["ShiftOutput"].mean()
        std_output = d["ShiftOutput"].std(ddof=0)
        if pd.isna(std_output) or std_output == 0:
            std_output = 1.0

        d["ZScore"] = (d["ShiftOutput"] - mean_output) / std_output
        for _, r in d.iterrows():
            risk_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "ShiftOutput": safe_float(r["ShiftOutput"]),
                    "ZScore": safe_float(r["ZScore"]),
                    "RiskLevel": r["RiskLevel"],
                    "BelowThreshold": bool(r["ShiftOutput"] < min_output_threshold),
                }
            )

    risk_df = pd.DataFrame(risk_rows)
    if not risk_df.empty:
        st.dataframe(
            risk_df.sort_values(["BelowThreshold", "ZScore", "ShiftOutput"], ascending=[False, True, True]),
            use_container_width=True,
        )

    reco_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        bottleneck_row = d.loc[d["Simulated"].idxmax()]
        bottleneck_time = float(bottleneck_row["Simulated"])

        t10 = bottleneck_time * 0.90
        t15 = bottleneck_time * 0.85
        t20 = bottleneck_time * 0.80

        temp10 = d["Simulated"].copy()
        temp15 = d["Simulated"].copy()
        temp20 = d["Simulated"].copy()

        temp10.loc[d["Stage"] == bottleneck_row["Stage"]] = t10
        temp15.loc[d["Stage"] == bottleneck_row["Stage"]] = t15
        temp20.loc[d["Stage"] == bottleneck_row["Stage"]] = t20

        out10 = (60 / max(temp10.max(), 0.1)) * shift_hours
        out15 = (60 / max(temp15.max(), 0.1)) * shift_hours
        out20 = (60 / max(temp20.max(), 0.1)) * shift_hours

        reco_rows.append(
            {
                "Scenario": sc["Scenario"],
                "BottleneckStage": bottleneck_row["Stage"],
                "CurrentBottleneckTime": bottleneck_time,
                "CurrentProjectedOutput": sc["ProjectedAfterImprovement"],
                "After10PercentReduction": float(out10),
                "After15PercentReduction": float(out15),
                "After20PercentReduction": float(out20),
                "BestQuickAction": f"Reduce {bottleneck_row['Stage']} time first",
            }
        )

    reco_df = pd.DataFrame(reco_rows)
    if not reco_df.empty:
        st.dataframe(reco_df, use_container_width=True)

    benchmark_df = pd.DataFrame(
        [
            {
                "Scenario": sc["Scenario"],
                "TotalOutput": float(sc["TotalOutput"]),
                "Efficiency": float(sc["Efficiency"]),
                "ProjectedAfterImprovement": float(sc["ProjectedAfterImprovement"]),
            }
            for sc in scenario_outputs
        ]
    )

    fig_benchmark = px.bar(
        benchmark_df.melt(id_vars="Scenario", var_name="Metric", value_name="Value"),
        x="Scenario",
        y="Value",
        color="Metric",
        barmode="group",
        title="Scenario Benchmark",
    )
    st.plotly_chart(fig_benchmark, use_container_width=True)

with tabs[8]:
    st.subheader("Advanced Optimization Center")

    sensitivity_rows = []
    roadmap_rows = []
    best_case_rows = []

    for sc in scenario_outputs:
        scenario_name = sc["Scenario"]
        current_df = sc["Data"].copy()

        for _, stage_row in current_df.iterrows():
            stage_name = stage_row["Stage"]
            base_time = float(stage_row["Simulated"])

            for pct in [5, 10, 15, 20, 25]:
                trial_reductions = {row["Stage"]: 0 for _, row in base_df.iterrows()}
                trial_reductions[stage_name] = pct

                trial_result = compute_scenario(
                    base_df=base_df,
                    reductions=trial_reductions,
                    shift_hours=shift_hours,
                    days_per_week=days_per_week,
                    min_output_threshold=min_output_threshold,
                )

                sensitivity_rows.append(
                    {
                        "Scenario": scenario_name,
                        "Stage": stage_name,
                        "ReductionPercent": pct,
                        "BaseTime": base_time,
                        "ProjectedOutput": trial_result["TotalOutput"],
                        "ProjectedEfficiency": trial_result["Efficiency"],
                        "ProjectedImprovement": trial_result["ProjectedAfterImprovement"],
                        "BottleneckAfterTrial": trial_result["Bottleneck"],
                    }
                )

        sorted_reco = current_df.sort_values(["ShiftOutput", "Simulated"], ascending=[True, False]).copy()
        for rank, (_, row) in enumerate(sorted_reco.iterrows(), start=1):
            roadmap_rows.append(
                {
                    "Scenario": scenario_name,
                    "Priority": rank,
                    "Stage": row["Stage"],
                    "CurrentTime": float(row["Simulated"]),
                    "CurrentOutput": float(row["ShiftOutput"]),
                    "Headroom": float(row["Headroom"]),
                    "RiskLevel": row["RiskLevel"],
                    "QuickAction": f"Reduce {row['Stage']} time",
                }
            )

        best_stage = current_df.loc[current_df["Simulated"].idxmax()]
        best_case_rows.append(
            {
                "Scenario": scenario_name,
                "BestStage": best_stage["Stage"],
                "BestStageTime": float(best_stage["Simulated"]),
                "BestStageOutput": float(best_stage["ShiftOutput"]),
                "ScenarioOutput": float(sc["TotalOutput"]),
                "ScenarioEfficiency": float(sc["Efficiency"]),
                "ProjectedAfterImprovement": float(sc["ProjectedAfterImprovement"]),
            }
        )

    sensitivity_df = pd.DataFrame(sensitivity_rows)
    roadmap_df = pd.DataFrame(roadmap_rows)
    best_case_df = pd.DataFrame(best_case_rows)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sensitivity rows", len(sensitivity_df))
    c2.metric("Roadmap rows", len(roadmap_df))
    c3.metric("Best-case rows", len(best_case_df))
    c4.metric("Scenarios analyzed", len(scenario_outputs))

    if not sensitivity_df.empty:
        st.markdown("### Reduction Sensitivity")
        st.dataframe(
            sensitivity_df.sort_values(
                ["Scenario", "Stage", "ReductionPercent"],
                ascending=[True, True, True],
            ),
            use_container_width=True,
        )

        fig_sens = px.line(
            sensitivity_df,
            x="ReductionPercent",
            y="ProjectedOutput",
            color="Stage",
            facet_col="Scenario",
            markers=True,
            title="Output Sensitivity by Stage Reduction",
        )
        st.plotly_chart(fig_sens, use_container_width=True)

        fig_sens2 = px.line(
            sensitivity_df,
            x="ReductionPercent",
            y="ProjectedEfficiency",
            color="Stage",
            facet_col="Scenario",
            markers=True,
            title="Efficiency Sensitivity by Stage Reduction",
        )
        st.plotly_chart(fig_sens2, use_container_width=True)

    if not roadmap_df.empty:
        st.markdown("### Improvement Roadmap")
        st.dataframe(
            roadmap_df.sort_values(["Scenario", "Priority"], ascending=[True, True]),
            use_container_width=True,
        )

        fig_roadmap = px.scatter(
            roadmap_df,
            x="CurrentTime",
            y="CurrentOutput",
            size=roadmap_df["Headroom"].abs(),
            color="RiskLevel",
            facet_col="Scenario",
            hover_data=["Stage", "QuickAction"],
            title="Stage Priority Map",
        )
        st.plotly_chart(fig_roadmap, use_container_width=True)

    if not best_case_df.empty:
        st.markdown("### Best Case Summary")
        st.dataframe(best_case_df, use_container_width=True)

        fig_best = px.bar(
            best_case_df.melt(
                id_vars="Scenario",
                value_vars=["ScenarioOutput", "ProjectedAfterImprovement"],
                var_name="Metric",
                value_name="Value",
            ),
            x="Scenario",
            y="Value",
            color="Metric",
            barmode="group",
            title="Current vs Projected Output",
        )
        st.plotly_chart(fig_best, use_container_width=True)

    st.subheader("Management Recommendations")

    recommendation_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        bottleneck_row = d.loc[d["Simulated"].idxmax()]
        top_risk = d.sort_values(["RiskLevel", "ShiftOutput"], ascending=[False, True]).iloc[0]

        recommendation_rows.append(
            {
                "Scenario": sc["Scenario"],
                "Bottleneck": sc["Bottleneck"],
                "ImmediateAction": f"Reduce {bottleneck_row['Stage']} time first",
                "SecondaryAction": f"Rebalance {top_risk['Stage']} workload",
                "ExpectedGain": float(sc["ProjectedAfterImprovement"] - sc["TotalOutput"]),
                "RiskStage": top_risk["Stage"],
                "RiskOutput": float(top_risk["ShiftOutput"]),
            }
        )

    recommendation_df = pd.DataFrame(recommendation_rows)
    st.dataframe(recommendation_df, use_container_width=True)

    st.subheader("Executive Export")

    if not sensitivity_df.empty:
        exec_bundle = {
            "Master": master_df,
            "Recommendations": recommendation_df,
            "Sensitivity": sensitivity_df,
            "Roadmap": roadmap_df,
            "BestCase": best_case_df,
        }

        for sheet_name, frame in exec_bundle.items():
            st.download_button(
                label=f"Download {sheet_name} Excel",
                data=make_excel_bytes(frame, sheet_name=sheet_name[:31]),
                file_name=f"{company_name}_{sheet_name}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_{sheet_name}_excel",
            )

            st.download_button(
                label=f"Download {sheet_name} PDF",
                data=make_pdf_bytes(f"{company_name} - {sheet_name}", frame),
                file_name=f"{company_name}_{sheet_name}.pdf",
                mime="application/pdf",
                key=f"download_{sheet_name}_pdf",
            )

with tabs[9]:
    st.subheader("Target Planning")

    target_cols = st.columns(3)
    target_output = target_cols[0].number_input(
        "Target shift output",
        min_value=1.0,
        value=float(master_df["ShiftOutput"].mean()) if not master_df.empty else 100.0,
        step=1.0,
    )
    target_efficiency = target_cols[1].number_input("Target efficiency", min_value=1.0, value=80.0, step=1.0)
    focus_scenario = target_cols[2].selectbox(
        "Target scenario",
        [sc["Scenario"] for sc in scenario_outputs],
        key="target_scenario_selector",
    )

    target_index = [sc["Scenario"] for sc in scenario_outputs].index(focus_scenario)
    target_sc = scenario_outputs[target_index]
    target_df = target_sc["Data"].copy()

    required_gain = float(target_output - target_sc["TotalOutput"])
    required_eff_gain = float(target_efficiency - target_sc["Efficiency"])

    st.metric("Required output gain", f"{required_gain:.1f}")
    st.metric("Required efficiency gain", f"{required_eff_gain:.1f}")

    target_table = []
    for _, r in target_df.iterrows():
        stage_goal_10 = max(float(r["Simulated"]) * 0.90, 0.1)
        stage_goal_15 = max(float(r["Simulated"]) * 0.85, 0.1)
        stage_goal_20 = max(float(r["Simulated"]) * 0.80, 0.1)

        out_10 = (60 / max(target_df["Simulated"].where(target_df["Stage"] != r["Stage"], stage_goal_10).max(), 0.1)) * shift_hours
        out_15 = (60 / max(target_df["Simulated"].where(target_df["Stage"] != r["Stage"], stage_goal_15).max(), 0.1)) * shift_hours
        out_20 = (60 / max(target_df["Simulated"].where(target_df["Stage"] != r["Stage"], stage_goal_20).max(), 0.1)) * shift_hours

        target_table.append(
            {
                "Stage": r["Stage"],
                "CurrentOutput": float(r["ShiftOutput"]),
                "If10PercentBetter": float(out_10),
                "If15PercentBetter": float(out_15),
                "If20PercentBetter": float(out_20),
                "Gain10": float(out_10 - target_sc["TotalOutput"]),
                "Gain15": float(out_15 - target_sc["TotalOutput"]),
                "Gain20": float(out_20 - target_sc["TotalOutput"]),
            }
        )

    target_df2 = pd.DataFrame(target_table)
    st.dataframe(target_df2, use_container_width=True)

    fig_target = px.bar(
        target_df2,
        x="Stage",
        y=["CurrentOutput", "If10PercentBetter", "If15PercentBetter", "If20PercentBetter"],
        barmode="group",
        title=f"Target Planning for {focus_scenario}",
    )
    st.plotly_chart(fig_target, use_container_width=True)

with tabs[10]:
    st.subheader("Correlation and Dependency Analysis")

    corr_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        corr = d[["Simulated", "UnitsPerHour", "ShiftOutput", "DailyOutput", "WeeklyOutput", "MonthlyOutput"]].corr()
        corr_rows.append((sc["Scenario"], corr))

    for scenario_name, corr in corr_rows:
        st.markdown(f"### {scenario_name}")
        st.dataframe(corr, use_container_width=True)
        fig_corr = px.imshow(
            corr,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="RdBu",
            zmin=-1,
            zmax=1,
            title=f"{scenario_name} Correlation Matrix",
        )
        st.plotly_chart(fig_corr, use_container_width=True)

with tabs[11]:
    st.subheader("Pareto Priority of Improvements")

    pareto_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        base_gain = float(sc["ProjectedAfterImprovement"] - sc["TotalOutput"])
        for _, r in d.iterrows():
            if r["Stage"] == sc["Bottleneck"]:
                gain = base_gain
            else:
                gain = float((60 / max(r["Simulated"] * 0.95, 0.1)) * shift_hours - r["ShiftOutput"])
            pareto_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "PotentialGain": gain,
                    "CurrentOutput": float(r["ShiftOutput"]),
                }
            )

    pareto_df = pd.DataFrame(pareto_rows)
    pareto_df = pareto_df.sort_values(["Scenario", "PotentialGain"], ascending=[True, False])
    st.dataframe(pareto_df, use_container_width=True)

    fig_pareto = px.bar(
        pareto_df,
        x="Stage",
        y="PotentialGain",
        color="Scenario",
        barmode="group",
        title="Potential Gain by Stage",
    )
    st.plotly_chart(fig_pareto, use_container_width=True)

with tabs[12]:
    st.subheader("Operating Modes")

    mode_col1, mode_col2, mode_col3 = st.columns(3)
    enable_compare_mode = mode_col1.checkbox("Comparison mode", value=True)
    enable_priority_mode = mode_col2.checkbox("Priority mode", value=True)
    enable_planning_mode = mode_col3.checkbox("Planning mode", value=True)

    if enable_compare_mode:
        st.info("Comparison mode is active")
    if enable_priority_mode:
        st.info("Priority mode is active")
    if enable_planning_mode:
        st.info("Planning mode is active")

with tabs[13]:
    st.subheader("Backup and Restore Snapshot")

    snapshot = {
        "company_name": company_name,
        "num_stages": int(num_stages),
        "shift_hours": int(shift_hours),
        "days_per_week": int(days_per_week),
        "min_output_threshold": float(min_output_threshold),
        "stages": base_df.to_dict(orient="records"),
        "scenario_outputs": [
            {
                "Scenario": sc["Scenario"],
                "Bottleneck": sc["Bottleneck"],
                "TotalOutput": sc["TotalOutput"],
                "Efficiency": sc["Efficiency"],
                "ProjectedAfterImprovement": sc["ProjectedAfterImprovement"],
                "Data": sc["Data"].to_dict(orient="records"),
            }
            for sc in scenario_outputs
        ],
    }

    snapshot_json = json.dumps(snapshot, ensure_ascii=False, indent=2)

    st.download_button(
        "Download Snapshot",
        data=snapshot_json,
        file_name=f"{company_name}_snapshot.json",
        mime="application/json",
    )

    st.code(snapshot_json[:6000], language="json")

with tabs[14]:
    st.subheader("Daily Control Board")

    control_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        for _, r in d.iterrows():
            control_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "CurrentTime": float(r["Simulated"]),
                    "SuggestedTime": float(r["SuggestedTime"]),
                    "TimeReduction": float(r["Simulated"] - r["SuggestedTime"]),
                    "CurrentShiftOutput": float(r["ShiftOutput"]),
                    "PotentialShiftOutput": float((60 / max(r["SuggestedTime"], 0.1)) * shift_hours),
                    "Alert": r["Alert"],
                    "RiskLevel": r["RiskLevel"],
                }
            )

    control_df = pd.DataFrame(control_rows)
    st.dataframe(
        control_df.sort_values(["Scenario", "PotentialShiftOutput"], ascending=[True, False]),
        use_container_width=True,
    )

    fig_control = px.scatter(
        control_df,
        x="CurrentTime",
        y="PotentialShiftOutput",
        size="TimeReduction",
        color="RiskLevel",
        facet_col="Scenario",
        hover_data=["Stage", "Alert"],
        title="Daily Control Board",
    )
    st.plotly_chart(fig_control, use_container_width=True)

with tabs[15]:
    st.subheader("Executive Summary")

    final_cols = st.columns(4)
    leaderboard_rows = []
    for sc in scenario_outputs:
        leaderboard_rows.append(
            {
                "Scenario": sc["Scenario"],
                "TotalOutput": float(sc["TotalOutput"]),
                "Efficiency": float(sc["Efficiency"]),
                "ProjectedAfterImprovement": float(sc["ProjectedAfterImprovement"]),
                "Bottleneck": sc["Bottleneck"],
                "Gain": float(sc["ProjectedAfterImprovement"] - sc["TotalOutput"]),
            }
        )

    leaderboard_df = pd.DataFrame(leaderboard_rows).sort_values(
        ["ProjectedAfterImprovement", "Efficiency", "TotalOutput"],
        ascending=[False, False, False],
    )

    if not leaderboard_df.empty:
        final_cols[0].metric("Best scenario output", f"{leaderboard_df.iloc[0]['ProjectedAfterImprovement']:.1f}")
        final_cols[1].metric("Worst scenario output", f"{leaderboard_df.iloc[-1]['ProjectedAfterImprovement']:.1f}")
        final_cols[2].metric("Average efficiency", f"{leaderboard_df['Efficiency'].mean():.1f}%")
        final_cols[3].metric("Critical stages", int((control_df["Alert"] != "").sum()) if not control_df.empty else 0)
    else:
        final_cols[0].metric("Best scenario output", "0")
        final_cols[1].metric("Worst scenario output", "0")
        final_cols[2].metric("Average efficiency", "0%")
        final_cols[3].metric("Critical stages", 0)

    st.dataframe(leaderboard_df, use_container_width=True)

    fig_leader = px.bar(
        leaderboard_df.melt(
            id_vars="Scenario",
            value_vars=["TotalOutput", "ProjectedAfterImprovement"],
            var_name="Metric",
            value_name="Value",
        ),
        x="Scenario",
        y="Value",
        color="Metric",
        barmode="group",
        title="Scenario Output Leaderboard",
    )
    st.plotly_chart(fig_leader, use_container_width=True)

    health_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        mean_out = d["ShiftOutput"].mean()
        std_out = d["ShiftOutput"].std(ddof=0)
        if pd.isna(std_out) or std_out == 0:
            std_out = 1.0

        d["HealthScore"] = 100 - ((d["Simulated"] / d["Simulated"].max()) * 40) - ((min_output_threshold - d["ShiftOutput"]).clip(lower=0) * 2)
        d["HealthScore"] = d["HealthScore"].clip(lower=0, upper=100)

        for _, r in d.iterrows():
            health_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "HealthScore": float(r["HealthScore"]),
                    "ShiftOutput": float(r["ShiftOutput"]),
                    "Simulated": float(r["Simulated"]),
                    "ZScore": float((r["ShiftOutput"] - mean_out) / std_out),
                }
            )

    health_df = pd.DataFrame(health_rows)
    st.dataframe(health_df.sort_values(["Scenario", "HealthScore"], ascending=[True, False]), use_container_width=True)

    fig_health = px.scatter(
        health_df,
        x="Simulated",
        y="HealthScore",
        color="Scenario",
        size="ShiftOutput",
        hover_data=["Stage", "ZScore"],
        title="Stage Health Score Map",
    )
    st.plotly_chart(fig_health, use_container_width=True)

    st.subheader("Master Notifications Center")

    notif_count = 0
    for sc in scenario_outputs:
        for _, row in sc["Data"].iterrows():
            if row["ShiftOutput"] < min_output_threshold:
                notif_count += 1
                st.error(f"{sc['Scenario']} - {row['Stage']} below threshold with output {row['ShiftOutput']:.1f}")
                logging.warning(f"Below threshold | {sc['Scenario']} | {row['Stage']} | {row['ShiftOutput']:.2f}")

    st.caption(f"Total notifications: {notif_count}")

    st.subheader("Stage Health and Leadership Insight")
    stage_control_rows = []
    for sc in scenario_outputs:
        d = sc["Data"].copy()
        d["ImprovementPriority"] = d["Simulated"].rank(ascending=False, method="dense")
        d["Headroom"] = d["ShiftOutput"] - min_output_threshold
        for _, r in d.iterrows():
            stage_control_rows.append(
                {
                    "Scenario": sc["Scenario"],
                    "Stage": r["Stage"],
                    "CurrentTime": float(r["Simulated"]),
                    "SuggestedTime": float(r["SuggestedTime"]),
                    "DeltaTime": float(r["Simulated"] - r["SuggestedTime"]),
                    "CurrentOutput": float(r["ShiftOutput"]),
                    "PotentialGain": float((60 / max(r["SuggestedTime"], 0.1)) * shift_hours - r["ShiftOutput"]),
                    "Headroom": float(r["Headroom"]),
                    "ImprovementPriority": float(r["ImprovementPriority"]),
                    "Alert": r["Alert"],
                }
            )

    stage_control_df = pd.DataFrame(stage_control_rows)
    st.dataframe(stage_control_df.sort_values(["Scenario", "PotentialGain"], ascending=[True, False]), use_container_width=True)

    fig_stage_control = px.scatter(
        stage_control_df,
        x="CurrentTime",
        y="PotentialGain",
        size="Headroom",
        color="Scenario",
        facet_col="Scenario",
        hover_data=["Stage", "Alert"],
        title="Stage Potential Gain Map",
    )
    st.plotly_chart(fig_stage_control, use_container_width=True)

st.divider()
st.subheader("Export Center")

master_rows = []
for sc in scenario_outputs:
    d = sc["Data"].copy()
    for _, r in d.iterrows():
        master_rows.append(
            {
                "Scenario": sc["Scenario"],
                "Stage": r["Stage"],
                "BaseTime": safe_float(r["BaseTime"]),
                "ReductionPercent": safe_float(r["ReductionPercent"]),
                "Simulated": safe_float(r["Simulated"]),
                "UnitsPerHour": safe_float(r["UnitsPerHour"]),
                "ShiftOutput": safe_float(r["ShiftOutput"]),
                "DailyOutput": safe_float(r["DailyOutput"]),
                "WeeklyOutput": safe_float(r["WeeklyOutput"]),
                "MonthlyOutput": safe_float(r["MonthlyOutput"]),
                "SuggestedTime": safe_float(r["SuggestedTime"]),
                "Headroom": safe_float(r["Headroom"]),
                "Alert": r["Alert"],
                "RiskLevel": r["RiskLevel"],
                "Efficiency": sc["Efficiency"],
                "Bottleneck": sc["Bottleneck"],
                "ProjectedAfterImprovement": sc["ProjectedAfterImprovement"],
            }
        )

master_df = pd.DataFrame(master_rows)

st.download_button(
    "Download Master Excel Report",
    data=make_excel_bytes(master_df, sheet_name="Master"),
    file_name=f"{company_name}_MasterReport.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="final_master_excel",
)

st.download_button(
    "Download Master PDF Report",
    data=make_pdf_bytes(f"{company_name} Master Production Report", master_df),
    file_name=f"{company_name}_MasterReport.pdf",
    mime="application/pdf",
    key="final_master_pdf",
)

st.divider()
st.subheader("Operations Log")

log_lines = []
for sc in scenario_outputs:
    for _, row in sc["Data"].iterrows():
        line = (
            f"{dt.datetime.now().isoformat()} | "
            f"{sc['Scenario']} | {row['Stage']} | "
            f"Base={row['BaseTime']:.2f} | Red={row['ReductionPercent']:.0f}% | "
            f"Sim={row['Simulated']:.2f} | UPH={row['UnitsPerHour']:.2f} | "
            f"Shift={row['ShiftOutput']:.2f} | Alert={row['Alert']}"
        )
        log_lines.append(line)

log_df = pd.DataFrame({"Log": log_lines})
st.dataframe(log_df, use_container_width=True)

if enable_logging_ui:
    st.markdown("### Recent Log Entries")
    try:
        with open("factory_sim.log", "r", encoding="utf-8") as f:
            lines = f.readlines()[-100:]
        st.text_area("Log tail", "".join(lines), height=350)
    except Exception:
        st.info("Log file is not available")
st.success("Advanced modules loaded successfully")


def load_lottiefile(filepath: str):
    import os
    import json
    full_path = os.path.join(os.path.dirname(__file__), filepath)
    with open(full_path, "r", encoding="utf-8") as f:
        return json.load(f)