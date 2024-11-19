import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import duckdb
from cleaning_utils import clean_activity_tracker, clean_order_view, clean_trailer_activity, create_excel_file

st.set_page_config(
    page_title="NFI Auburndale - Coca Cola Reporting Dashboard",
    page_icon="🚛",
    layout="wide"
)

st.title("NFI Auburndale - Coca Cola Reporting Dashboard")
st.markdown('_Alpha V. 2.1.0')

tabs = st.tabs(['Data Upload and Cleaned Data', 'Dwell and On Time Dashboard', 'Load Time Compliance'])

with tabs[0]:
    st.header('Data Upload')
    st.write("Please upload the following data files: Activity Tracker, Order View, and Trailer Activity.")

    def upload_file(file_description):
        return st.sidebar.file_uploader(file_description, type=['csv', 'xlsx', 'xls'])

    activity_tracker = upload_file('Upload Activity Tracker')
    order_view = upload_file('Upload Order View')
    trailer_activity = upload_file('Upload Trailer Activity')

    @st.cache_data
    def read_uploaded_file(file, dtype):
        if file.name.endswith('.csv'):
            return pd.read_csv(file, encoding='ISO-8859-1', dtype=dtype)
        elif file.name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(file, dtype=dtype)
        return None

    dtype_activity_tracker = {'Order #': str}
    dtype_order_view = {'Shipment #': str, 'SAP Delivery # (Order#)': str}
    dtype_trailer_activity = {'SHIPMENT_ID': str}

    if activity_tracker and order_view and trailer_activity:
        try:
            activity_report = read_uploaded_file(activity_tracker, dtype=dtype_activity_tracker)
            order_report = read_uploaded_file(order_view, dtype=dtype_order_view)
            trailer_report = read_uploaded_file(trailer_activity, dtype=dtype_trailer_activity)

            load_times = clean_activity_tracker(activity_report)
            order_report = clean_order_view(order_report)
            trailer_report = clean_trailer_activity(trailer_report)

            con = duckdb.connect(":memory:")
            con.register('load_times_df', load_times)
            con.execute('CREATE TABLE load_times AS SELECT * FROM load_times_df')

            con.register('order_report_df', order_report)
            con.register('trailer_report_df', trailer_report)
            con.execute('CREATE TABLE order_report AS SELECT * FROM order_report_df')
            con.execute('CREATE TABLE trailer_report AS SELECT * FROM trailer_report_df')

            merged_df = con.execute("""
                SELECT
                    order_report."Shipment Num",
                    order_report."Order Num",
                    order_report."Appointment DateTime",
                    order_report."Required DateTime",
                    trailer_report."Checkin DateTime",
                    trailer_report."Checkout DateTime",
                    order_report."Carrier" AS "Carrier",
                    order_report."Visit Type",
                    trailer_report."Loaded DateTime",
                    trailer_report."Shift",
                    order_report."Scheduled Date",
                    order_report."Week",
                    order_report."Month"
                FROM order_report
                LEFT JOIN trailer_report
                ON order_report."Shipment Num" = trailer_report."Shipment Num"
            """).fetchdf()

            def compliance(row):
                if pd.notna(row['Checkin DateTime']) and row['Required DateTime'] >= row['Checkin DateTime']:
                    return "On Time"
                else:
                    return "Late"

            merged_df['Compliance'] = merged_df.apply(compliance, axis=1)

            def dwell_time(row):
                loaded_datetime = row['Loaded DateTime']
                checkin_datetime = row['Checkin DateTime']
                appt_datetime = row['Appointment DateTime']
                comp = row['Compliance']

                if pd.notna(loaded_datetime):
                    if comp == 'On Time':
                        dwell_time = round((loaded_datetime - appt_datetime).total_seconds() / 3600, 2)
                    elif comp == 'Late':
                        dwell_time = round((loaded_datetime - checkin_datetime).total_seconds() / 3600, 2)
                    else:
                        dwell_time = None
                else:
                    dwell_time = None

                if dwell_time is not None and dwell_time < 0:
                    dwell_time = np.nan

                return dwell_time

            merged_df['Dwell Time (Hours)'] = merged_df.apply(dwell_time, axis=1)

            column_order = [
                "Shipment Num",
                "Order Num",
                "Appointment DateTime",
                "Required DateTime",
                "Checkin DateTime",
                "Compliance",
                "Dwell Time (Hours)",
                "Checkout DateTime",
                "Carrier",
                "Visit Type",
                "Loaded DateTime",
                "Shift",
                "Scheduled Date",
                "Week",
                "Month"
            ]

            merged_df = merged_df[column_order]
            merged_df = merged_df.dropna(subset='Checkin DateTime')

            st.success("All files uploaded and processed successfully!")
            st.session_state['merged_df'] = merged_df
            st.session_state['load_times'] = load_times

        except Exception as e:
            st.error(f"Error in processing files: {e}")
    else:
        st.warning("Please upload all three required files to proceed.")

    if 'merged_df' in st.session_state:
        merged_df = st.session_state['merged_df']
        load_times = st.session_state['load_times']

        st.header('Cleaned Data')
        st.subheader("Dwell and On Time Compliance Preview")
        st.dataframe(merged_df.head())

        csv_merged = merged_df.to_csv(index=False)
        st.download_button(
            label="Download Merged Data as CSV",
            data=csv_merged,
            file_name='merged_data.csv',
            mime='text/csv'
        )

        st.subheader("Load Times Preview")
        st.dataframe(load_times.head())

        csv_load_times = load_times.to_csv(index=False)
        st.download_button(
            label="Download Load Times Data as CSV",
            data=csv_load_times,
            file_name='load_times_data.csv',
            mime='text/csv'
        )

with tabs[1]:

    if 'merged_df' in st.session_state:
        st.header("Dwell and On Time Compliance Dashboard")

        with st.expander("Daily Breakdown"):

            daily_pivots = {}

            st.write("The visualizations below show the daily breakdown of on-time and late shipments.")

            selected_date = st.date_input("Select Date for Daily Dashboard")

            shift_options = ['All'] + list(merged_df['Shift'].unique())
            selected_shift = st.selectbox("Select Shift for Filtering", options=shift_options)

            if selected_date:
                selected_date_str = selected_date.strftime("%m/%d/%Y")
                st.write(f"Selected Date: {selected_date_str} | Selected Shift: {selected_shift}")

                if selected_shift == 'All':
                    filtered_df = merged_df[merged_df['Scheduled Date'] == selected_date_str]
                else:
                    filtered_df = merged_df[(merged_df['Scheduled Date'] == selected_date_str) & 
                                            (merged_df['Shift'] == selected_shift)]

                if filtered_df.empty:
                    st.warning("No data available for the selected date and shift.")
                else:
                    view_option = st.radio("Select view mode:", ['Pivot Tables', 'Visualizations'], index=1)

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        compliance_pivot = filtered_df.pivot_table(
                            values='Shipment Num',
                            index='Scheduled Date',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                        compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)

                        daily_pivots['Daily On Time Compliance'] = compliance_pivot

                        if view_option == 'Pivot Tables':
                            st.subheader("On Time Compliance by Date")
                            st.write(compliance_pivot)
                        else:
                            categories = ['On Time', 'Late']
                            counts = [
                                compliance_pivot['On Time'].sum() if 'On Time' in compliance_pivot.columns else 0,
                                compliance_pivot['Late'].sum() if 'Late' in compliance_pivot.columns else 0
                            ]
                            colors = ['green', 'red']
                            fig = go.Figure(data=[go.Pie(labels=categories, values=counts, hole=0.4, marker=dict(colors=colors))])
                            fig.update_layout(title_text="On Time vs. Late Compliance")
                            st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        filtered_df['Dwell Time Category'] = pd.cut(
                            filtered_df['Dwell Time (Hours)'],
                            bins=[-np.inf, 0, 1, 2, 3, np.inf],
                            labels=['<2', '2-3', '3-4', '4-5', '5+']
                        )

                        dwell_count_pivot = filtered_df.pivot_table(
                            values='Shipment Num',
                            index='Dwell Time Category',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)

                        daily_pivots['Daily Dwell Time Distribution'] = dwell_count_pivot

                        if view_option == 'Pivot Tables':
                            st.subheader("Daily Count by Dwell Time")
                            st.table(dwell_count_pivot)
                        else:
                            categories = dwell_count_pivot['Dwell Time Category']
                            late_percentages = (
                                round(dwell_count_pivot['Late'] / dwell_count_pivot['Grand Total'] * 100, 2)
                                if 'Late' in dwell_count_pivot.columns else [0] * len(dwell_count_pivot)
                            )
                            on_time_percentages = (
                                round(dwell_count_pivot['On Time'] / dwell_count_pivot['Grand Total'] * 100, 2)
                                if 'On Time' in dwell_count_pivot.columns else [0] * len(dwell_count_pivot)
                            )
                            fig = go.Figure()
                            fig.add_trace(go.Bar(x=categories, y=on_time_percentages, name='On Time', marker_color='green', text=on_time_percentages, textposition='inside'))
                            fig.add_trace(go.Bar(x=categories, y=late_percentages, name='Late', marker_color='red', text=late_percentages, textposition='inside'))
                            fig.update_layout(barmode='stack', title="100% Stacked Bar: Dwell Time Category")
                            st.plotly_chart(fig, use_container_width=True)

                    with col3:
                        dwell_average_pivot = filtered_df.pivot_table(
                            values='Dwell Time (Hours)',
                            index='Visit Type',
                            columns='Compliance',
                            aggfunc='mean'
                        ).reset_index()

                        daily_pivots['Daily Average Dwell Time'] = dwell_average_pivot

                        if view_option == 'Pivot Tables':
                            st.subheader("Average Dwell Time by Visit Type")
                            st.table(dwell_average_pivot)
                        else:
                            fig = go.Figure()
                            fig.add_trace(
                                go.Bar(
                                    x=dwell_average_pivot['Visit Type'],
                                    y=dwell_average_pivot['Late'] if 'Late' in dwell_average_pivot.columns else [0] * len(dwell_average_pivot),
                                    name='Late',
                                    marker=dict(color='rgba(255, 0, 0, 0.7)'),
                                    text=[f'{val:.1f}' for val in dwell_average_pivot['Late']] if 'Late' in dwell_average_pivot.columns else ['0.0%' for _ in range(len(dwell_average_pivot))],
                                    textposition='auto',
                                    textfont=dict(color='white')
                                )
                            )
                            fig.add_trace(
                                go.Bar(
                                    x=dwell_average_pivot['Visit Type'],
                                    y=dwell_average_pivot['On Time'] if 'On Time' in dwell_average_pivot.columns else [0] * len(dwell_average_pivot),
                                    name='On Time',
                                    marker=dict(color='rgba(0, 128, 0, 0.7)'),
                                    text=[f'{val:.1f}' for val in dwell_average_pivot['On Time']] if 'On Time' in dwell_average_pivot.columns else ['0.0%' for _ in range(len(dwell_average_pivot))],
                                    textposition='auto',
                                    textfont=dict(color='white')
                                )
                            )
                            fig.update_layout(barmode='group', title="Average Dwell Time by Visit Type")
                            st.plotly_chart(fig, use_container_width=True)

                    carrier_pivot = filtered_df.pivot_table(
                        values='Shipment Num',
                        index='Carrier',
                        columns='Compliance',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()
                    carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                    carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                    carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)

                    daily_pivots['Daily Carrier Compliance'] = carrier_pivot

                    if view_option == 'Pivot Tables':
                        st.subheader("On Time Compliance by Carrier")
                        st.table(carrier_pivot)
                    else:
                        fig = go.Figure(data=go.Heatmap(
                            z=carrier_pivot['On Time %'].values.reshape(1, -1),
                            x=carrier_pivot['Carrier'],
                            y=['On Time %'],
                            colorscale='RdYlGn',
                            text=carrier_pivot['On Time %'].values.reshape(1, -1),
                            texttemplate="%{text:.2f}%"
                        ))
                        fig.update_layout(title="On Time Compliance Percentage by Carrier", xaxis_tickangle=45)
                        st.plotly_chart(fig, use_container_width=True)
            if daily_pivots:
                st.write("Download All Pivot Tables as Excel")
                excel_file = create_excel_file(daily_pivots)
                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name="dwell_and_compliance_daily_pivots.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        with st.expander("Weekly Breakdown"):

            weekly_pivots = {}

            st.write("The visualizations below show the weekly breakdown of on-time and late shipments.")

            selected_week = st.number_input("Select Week for Weekly Dashboard", min_value=1, max_value=52, step=1)
            selected_shift_weekly = st.selectbox("Select Shift for Filtering", options=shift_options, key="weekly_shift_select")

            if selected_week:
                st.write(f"Selected Week: {selected_week} | Selected Shift: {selected_shift_weekly}")

                if selected_shift_weekly == 'All':
                    weekly_filtered_df = merged_df[merged_df['Week'] == selected_week]
                else:
                    weekly_filtered_df = merged_df[(merged_df['Week'] == selected_week) & 
                                                (merged_df['Shift'] == selected_shift_weekly)]

                if weekly_filtered_df.empty:
                    st.warning("No data available for the selected week and shift.")
                else:
                    view_option_weekly = st.radio("Select view mode for weekly data:", ['Pivot Tables', 'Visualizations'], index=1)

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        trend_data = weekly_filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()

                        weekly_pivots['Weekly On Time Compliance'] = trend_data

                        if view_option_weekly == 'Pivot Tables':
                            st.subheader("On Time Compliance by Date (Weekly)")
                            st.write(trend_data)
                        else:
                            fig = go.Figure()

                            if 'On Time' in trend_data.columns:
                                fig.add_trace(go.Scatter(
                                    x=trend_data['Scheduled Date'], 
                                    y=trend_data['On Time'], 
                                    mode='lines+markers+text',
                                    name='On Time',
                                    line=dict(color='green'),
                                    text=trend_data['On Time'],
                                    textposition='top center',
                                    textfont=dict(color='white')
                                ))

                            if 'Late' in trend_data.columns:
                                fig.add_trace(go.Scatter(
                                    x=trend_data['Scheduled Date'], 
                                    y=trend_data['Late'], 
                                    mode='lines+markers+text',
                                    name='Late',
                                    line=dict(color='red'),
                                    text=trend_data['Late'],
                                    textposition='top center',
                                    textfont=dict(color='white')
                                ))

                            fig.update_layout(title="On Time vs. Late Compliance Trend (Weekly)", xaxis_title="Date", yaxis_title="Shipment Count")
                            st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        weekly_filtered_df['Dwell Time Category'] = pd.cut(
                            weekly_filtered_df['Dwell Time (Hours)'],
                            bins=[-np.inf, 0, 1, 2, 3, np.inf],
                            labels=['<2', '2-3', '3-4', '4-5', '5+']
                        )
                        dwell_count_pivot_weekly = weekly_filtered_df.pivot_table(
                            values='Shipment Num',
                            index='Dwell Time Category',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        dwell_count_pivot_weekly['Grand Total'] = dwell_count_pivot_weekly.select_dtypes(include=[np.number]).sum(axis=1)

                        weekly_pivots['Weekly Dwell Time Distribution'] = dwell_count_pivot_weekly

                        if view_option_weekly == 'Pivot Tables':
                            st.subheader("Weekly Count by Dwell Time")
                            st.table(dwell_count_pivot_weekly)
                        else:
                            categories = dwell_count_pivot_weekly['Dwell Time Category']
                            late_percentages = (
                                round(dwell_count_pivot_weekly['Late'] / dwell_count_pivot_weekly['Grand Total'] * 100, 2)
                                if 'Late' in dwell_count_pivot_weekly.columns else [0] * len(dwell_count_pivot_weekly)
                            )
                            on_time_percentages = (
                                round(dwell_count_pivot_weekly['On Time'] / dwell_count_pivot_weekly['Grand Total'] * 100, 2)
                                if 'On Time' in dwell_count_pivot_weekly.columns else [0] * len(dwell_count_pivot_weekly)
                            )
                            fig = go.Figure()
                            fig.add_trace(go.Bar(x=categories, y=on_time_percentages, name='On Time', marker_color='green', text=on_time_percentages, textposition='inside'))
                            fig.add_trace(go.Bar(x=categories, y=late_percentages, name='Late', marker_color='red', text=late_percentages, textposition='inside'))
                            fig.update_layout(barmode='stack', title="100% Stacked Bar: Dwell Time Category (Weekly)")
                            st.plotly_chart(fig, use_container_width=True)

                    with col3:
                        dwell_average_pivot_weekly = weekly_filtered_df.pivot_table(
                            values='Dwell Time (Hours)',
                            index='Visit Type',
                            columns='Compliance',
                            aggfunc='mean'
                        ).reset_index()

                        weekly_pivots['Weekly Average Dwell Time'] = dwell_average_pivot_weekly

                        if view_option_weekly == 'Pivot Tables':
                            st.subheader("Average Dwell Time by Visit Type (Weekly)")
                            st.table(dwell_average_pivot_weekly)
                        else:
                            fig = go.Figure()
                            fig.add_trace(
                                go.Bar(
                                    x=dwell_average_pivot_weekly['Visit Type'],
                                    y=dwell_average_pivot_weekly['Late'] if 'Late' in dwell_average_pivot_weekly.columns else [0] * len(dwell_average_pivot_weekly),
                                    name='Late',
                                    marker=dict(color='rgba(255, 0, 0, 0.7)'),
                                    text=[f'{val:.1f}' for val in dwell_average_pivot_weekly['Late']] if 'Late' in dwell_average_pivot_weekly.columns else ['0.0%' for _ in range(len(dwell_average_pivot_weekly))],
                                    textposition='auto',
                                    textfont=dict(color='white')
                                )
                            )
                            fig.add_trace(
                                go.Bar(
                                    x=dwell_average_pivot_weekly['Visit Type'],
                                    y=dwell_average_pivot_weekly['On Time'] if 'On Time' in dwell_average_pivot_weekly.columns else [0] * len(dwell_average_pivot_weekly),
                                    name='On Time',
                                    marker=dict(color='rgba(0, 128, 0, 0.7)'),
                                    text=[f'{val:.1f}' for val in dwell_average_pivot_weekly['On Time']] if 'On Time' in dwell_average_pivot_weekly.columns else ['0.0%' for _ in range(len(dwell_average_pivot_weekly))],
                                    textposition='auto',
                                    textfont=dict(color='white')
                                )
                            )
                            fig.update_layout(barmode='group', title="Average Dwell Time by Visit Type (Weekly)")
                            st.plotly_chart(fig, use_container_width=True)

                    carrier_pivot_weekly = weekly_filtered_df.pivot_table(
                        values='Shipment Num',
                        index='Carrier',
                        columns='Compliance',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()
                    carrier_pivot_weekly['Grand Total'] = carrier_pivot_weekly.select_dtypes(include=[np.number]).sum(axis=1)
                    carrier_pivot_weekly['On Time %'] = round((carrier_pivot_weekly.get('On Time', 0) / carrier_pivot_weekly['Grand Total']) * 100, 2)
                    carrier_pivot_weekly = carrier_pivot_weekly.sort_values(by='On Time %', ascending=False)

                    weekly_pivots['Weekly Carrier Compliance'] = carrier_pivot_weekly

                    if view_option_weekly == 'Pivot Tables':
                        st.subheader("On Time Compliance by Carrier (Weekly)")
                        st.table(carrier_pivot_weekly)
                    else:
                        fig = go.Figure(data=go.Heatmap(
                            z=carrier_pivot_weekly['On Time %'].values.reshape(1, -1),
                            x=carrier_pivot_weekly['Carrier'],
                            y=['On Time %'],
                            colorscale='RdYlGn',
                            text=carrier_pivot_weekly['On Time %'].values.reshape(1, -1),
                            texttemplate="%{text:.2f}%"
                        ))
                        fig.update_layout(title="On Time Compliance Percentage by Carrier (Weekly)", xaxis_tickangle=45)
                        st.plotly_chart(fig, use_container_width=True)
            if weekly_pivots:
                st.write("Download All Pivot Tables as Excel")
                excel_file = create_excel_file(weekly_pivots)
                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name="dwell_and_compliance_weekly_pivots.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        with st.expander("Monthly Breakdown"):

            monthly_pivots = {}

            st.write("The visualizations below show the monthly breakdown of on-time and late shipments.")

            selected_month = st.number_input("Select Month for Monthly Dashboard", min_value=1, max_value=12, step=1)
            selected_shift_monthly = st.selectbox("Select Shift for Filtering", options=shift_options, key="monthly_shift_select")

            if selected_month:
                st.write(f"Selected Month: {selected_month} | Selected Shift: {selected_shift_monthly}")

                if selected_shift_monthly == 'All':
                    monthly_filtered_df = merged_df[merged_df['Month'] == selected_month]
                else:
                    monthly_filtered_df = merged_df[(merged_df['Month'] == selected_month) & 
                                                    (merged_df['Shift'] == selected_shift_monthly)]

                if monthly_filtered_df.empty:
                    st.warning("No data available for the selected month and shift.")
                else:
                    view_option_monthly = st.radio("Select view mode for monthly data:", ['Pivot Tables', 'Visualizations'], index=1)

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        trend_data = monthly_filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()

                        monthly_pivots['Monthly On Time Compliance'] = trend_data

                        if view_option_monthly == 'Pivot Tables':
                            st.subheader("On Time Compliance by Date (Monthly)")
                            st.write(trend_data)
                        else:
                            fig = go.Figure()

                            if 'On Time' in trend_data.columns:
                                fig.add_trace(go.Scatter(
                                    x=trend_data['Scheduled Date'], 
                                    y=trend_data['On Time'], 
                                    mode='lines+markers+text',
                                    name='On Time',
                                    line=dict(color='green'),
                                    text=trend_data['On Time'],
                                    textposition='top center',
                                    textfont=dict(color='white')
                                ))

                            if 'Late' in trend_data.columns:
                                fig.add_trace(go.Scatter(
                                    x=trend_data['Scheduled Date'], 
                                    y=trend_data['Late'], 
                                    mode='lines+markers+text',
                                    name='Late',
                                    line=dict(color='red'),
                                    text=trend_data['Late'],
                                    textposition='top center',
                                    textfont=dict(color='white')
                                ))

                            fig.update_layout(title="On Time vs. Late Compliance Trend (Monthly)", xaxis_title="Date", yaxis_title="Shipment Count")
                            st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        monthly_filtered_df['Dwell Time Category'] = pd.cut(
                            monthly_filtered_df['Dwell Time (Hours)'],
                            bins=[-np.inf, 0, 1, 2, 3, np.inf],
                            labels=['<2', '2-3', '3-4', '4-5', '5+']
                        )
                        dwell_count_pivot_monthly = monthly_filtered_df.pivot_table(
                            values='Shipment Num',
                            index='Dwell Time Category',
                            columns='Compliance',
                            aggfunc='count',
                            fill_value=0
                        ).reset_index()
                        dwell_count_pivot_monthly['Grand Total'] = dwell_count_pivot_monthly.select_dtypes(include=[np.number]).sum(axis=1)

                        monthly_pivots['Monthly Dwell Time Distribution'] = dwell_count_pivot_monthly

                        if view_option_monthly == 'Pivot Tables':
                            st.subheader("Monthly Count by Dwell Time")
                            st.table(dwell_count_pivot_monthly)
                        else:
                            categories = dwell_count_pivot_monthly['Dwell Time Category']
                            late_percentages = (
                                round(dwell_count_pivot_monthly['Late'] / dwell_count_pivot_monthly['Grand Total'] * 100, 2)
                                if 'Late' in dwell_count_pivot_monthly.columns else [0] * len(dwell_count_pivot_monthly)
                            )
                            on_time_percentages = (
                                round(dwell_count_pivot_monthly['On Time'] / dwell_count_pivot_monthly['Grand Total'] * 100, 2)
                                if 'On Time' in dwell_count_pivot_monthly.columns else [0] * len(dwell_count_pivot_monthly)
                            )
                            fig = go.Figure()
                            fig.add_trace(go.Bar(x=categories, y=on_time_percentages, name='On Time', marker_color='green', text=on_time_percentages, textposition='inside'))
                            fig.add_trace(go.Bar(x=categories, y=late_percentages, name='Late', marker_color='red', text=late_percentages, textposition='inside'))
                            fig.update_layout(barmode='stack', title="100% Stacked Bar: Dwell Time Category (Monthly)")
                            st.plotly_chart(fig, use_container_width=True)

                    with col3:
                        dwell_average_pivot_monthly = monthly_filtered_df.pivot_table(
                            values='Dwell Time (Hours)',
                            index='Visit Type',
                            columns='Compliance',
                            aggfunc='mean'
                        ).reset_index()

                        monthly_pivots['Monthly Dwell Time Average'] = dwell_average_pivot_monthly

                        if view_option_monthly == 'Pivot Tables':
                            st.subheader("Average Dwell Time by Visit Type (Monthly)")
                            st.table(dwell_average_pivot_monthly)
                        else:
                            fig = go.Figure()
                            fig.add_trace(
                                go.Bar(
                                    x=dwell_average_pivot_monthly['Visit Type'],
                                    y=dwell_average_pivot_monthly['Late'] if 'Late' in dwell_average_pivot_monthly.columns else [0] * len(dwell_average_pivot_monthly),
                                    name='Late',
                                    marker=dict(color='rgba(255, 0, 0, 0.7)'),
                                    text=[f'{val:.1f}' for val in dwell_average_pivot_monthly['Late']] if 'Late' in dwell_average_pivot_monthly.columns else ['0.0%' for _ in range(len(dwell_average_pivot_monthly))],
                                    textposition='auto',
                                    textfont=dict(color='white')
                                )
                            )
                            fig.add_trace(
                                go.Bar(
                                    x=dwell_average_pivot_monthly['Visit Type'],
                                    y=dwell_average_pivot_monthly['On Time'] if 'On Time' in dwell_average_pivot_monthly.columns else [0] * len(dwell_average_pivot_monthly),
                                    name='On Time',
                                    marker=dict(color='rgba(0, 128, 0, 0.7)'),
                                    text=[f'{val:.1f}' for val in dwell_average_pivot_monthly['On Time']] if 'On Time' in dwell_average_pivot_monthly.columns else ['0.0%' for _ in range(len(dwell_average_pivot_monthly))],
                                    textposition='auto',
                                    textfont=dict(color='white')
                                )
                            )
                            fig.update_layout(barmode='group', title="Average Dwell Time by Visit Type (Monthly)")
                            st.plotly_chart(fig, use_container_width=True)

                    carrier_pivot_monthly = monthly_filtered_df.pivot_table(
                        values='Shipment Num',
                        index='Carrier',
                        columns='Compliance',
                        aggfunc='count',
                        fill_value=0
                    ).reset_index()
                    carrier_pivot_monthly['Grand Total'] = carrier_pivot_monthly.select_dtypes(include=[np.number]).sum(axis=1)
                    carrier_pivot_monthly['On Time %'] = round((carrier_pivot_monthly.get('On Time', 0) / carrier_pivot_monthly['Grand Total']) * 100, 2)
                    carrier_pivot_monthly = carrier_pivot_monthly.sort_values(by='On Time %', ascending=False)

                    monthly_pivots['Monthly Carrier Compliance'] = carrier_pivot_monthly

                    if view_option_monthly == 'Pivot Tables':
                        st.subheader("On Time Compliance by Carrier (Monthly)")
                        st.table(carrier_pivot_monthly)
                    else:
                        fig = go.Figure(data=go.Heatmap(
                            z=carrier_pivot_monthly['On Time %'].values.reshape(1, -1),
                            x=carrier_pivot_monthly['Carrier'],
                            y=['On Time %'],
                            colorscale='RdYlGn',
                            text=carrier_pivot_monthly['On Time %'].values.reshape(1, -1),
                            texttemplate="%{text:.2f}%"
                        ))
                        fig.update_layout(title="On Time Compliance Percentage by Carrier (Monthly)", xaxis_tickangle=45)
                        st.plotly_chart(fig, use_container_width=True)

            if monthly_pivots:
                st.write("Download All Pivot Tables as Excel")
                excel_file = create_excel_file(monthly_pivots)
                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name="dwell_and_compliance_monthly_pivots.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        with st.expander("YTD Breakdown"):

            pivot_tables = {}
        
            st.write("The visualizations below show the year-to-date breakdown of on-time and late shipments.")

            selected_shift_ytd = st.selectbox("Select Shift for Filtering", options=shift_options, key="ytd_shift_select")

            if selected_shift_ytd == 'All':
                ytd_filtered_df = merged_df
            else:
                ytd_filtered_df = merged_df[merged_df['Shift'] == selected_shift_ytd]

            trend_data = ytd_filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()
            trend_data['Scheduled Date'] = pd.to_datetime(trend_data['Scheduled Date'])

            trend_data['Month'] = trend_data['Scheduled Date'].dt.to_period('M')

            monthly_avg = trend_data.groupby('Month').mean(numeric_only=True).reset_index()
            monthly_avg['Month'] = monthly_avg['Month'].dt.to_timestamp()

            monthly_avg['On Time Rounded'] = monthly_avg['On Time'].round() if 'On Time' in monthly_avg.columns else 0
            monthly_avg['Late Rounded'] = monthly_avg['Late'].round() if 'Late' in monthly_avg.columns else 0

            pivot_tables['YTD On Time Compliance'] = monthly_avg[['Month', 'On Time', 'Late']]

            view_option_ytd = st.radio("Select view mode for YTD data:", ['Pivot Tables', 'Visualizations'], index=1)

            col1, col2, col3 = st.columns(3)

            with col1:
                if view_option_ytd == 'Pivot Tables':
                    st.subheader("Monthly Average Compliance (YTD)")
                    st.write(monthly_avg[['Month', 'On Time', 'Late']])
                else:
                    fig = go.Figure()

                    if 'On Time' in monthly_avg.columns:
                        fig.add_trace(go.Scatter(
                            x=monthly_avg['Month'], 
                            y=monthly_avg['On Time'], 
                            mode='lines+markers+text',
                            name='On Time',
                            line=dict(color='green'),
                            text=monthly_avg['On Time Rounded'],
                            textposition='top center'
                        ))

                    if 'Late' in monthly_avg.columns:
                        fig.add_trace(go.Scatter(
                            x=monthly_avg['Month'], 
                            y=monthly_avg['Late'], 
                            mode='lines+markers+text',
                            name='Late',
                            line=dict(color='red'),
                            text=monthly_avg['Late Rounded'],
                            textposition='top center'
                        ))

                    fig.update_layout(
                        title='Average Compliance Trend Per Month (YTD)',
                        xaxis_title='Month',
                        yaxis_title='Average Number of Shipments',
                        xaxis=dict(type='category'),
                        template='plotly_white'
                    )

                    st.plotly_chart(fig, use_container_width=True, key="ytd_line_chart")

            with col2:
                ytd_filtered_df['Dwell Time Category'] = pd.cut(
                    ytd_filtered_df['Dwell Time (Hours)'],
                    bins=[-np.inf, 0, 1, 2, 3, np.inf],
                    labels=['<2', '2-3', '3-4', '4-5', '5+']
                )
                dwell_count_pivot_ytd = ytd_filtered_df.pivot_table(
                    values='Shipment Num',
                    index='Dwell Time Category',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                dwell_count_pivot_ytd['Grand Total'] = dwell_count_pivot_ytd.select_dtypes(include=[np.number]).sum(axis=1)

                pivot_tables['YTD Dwell Time Distribution'] = dwell_count_pivot_ytd

                if view_option_ytd == 'Pivot Tables':
                    st.subheader("YTD Count by Dwell Time")
                    st.table(dwell_count_pivot_ytd)
                else:
                    categories = dwell_count_pivot_ytd['Dwell Time Category']
                    late_percentages = (
                        round(dwell_count_pivot_ytd['Late'] / dwell_count_pivot_ytd['Grand Total'] * 100, 2)
                        if 'Late' in dwell_count_pivot_ytd.columns else [0] * len(dwell_count_pivot_ytd)
                    )
                    on_time_percentages = (
                        round(dwell_count_pivot_ytd['On Time'] / dwell_count_pivot_ytd['Grand Total'] * 100, 2)
                        if 'On Time' in dwell_count_pivot_ytd.columns else [0] * len(dwell_count_pivot_ytd)
                    )
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=categories, y=on_time_percentages, name='On Time', marker_color='green', text=on_time_percentages, textposition='inside'))
                    fig.add_trace(go.Bar(x=categories, y=late_percentages, name='Late', marker_color='red', text=late_percentages, textposition='inside'))
                    fig.update_layout(barmode='stack', title="100% Stacked Bar: Dwell Time Category (YTD)")
                    st.plotly_chart(fig, use_container_width=True)

            with col3:
                dwell_average_pivot_ytd = ytd_filtered_df.pivot_table(
                    values='Dwell Time (Hours)',
                    index='Visit Type',
                    columns='Compliance',
                    aggfunc='mean'
                ).reset_index()

                pivot_tables['YTD Average Dwell Time'] = dwell_average_pivot_ytd

                if view_option_ytd == 'Pivot Tables':
                    st.subheader("Average Dwell Time by Visit Type (YTD)")
                    st.table(dwell_average_pivot_ytd)
                else:
                    fig = go.Figure()
                    fig.add_trace(
                        go.Bar(
                            x=dwell_average_pivot_ytd['Visit Type'],
                            y=dwell_average_pivot_ytd['Late'] if 'Late' in dwell_average_pivot_ytd.columns else [0] * len(dwell_average_pivot_ytd),
                            name='Late',
                            marker=dict(color='rgba(255, 0, 0, 0.7)'),
                            text=[f'{val:.1f}' for val in dwell_average_pivot_ytd['Late']] if 'Late' in dwell_average_pivot_ytd.columns else ['0.0%' for _ in range(len(dwell_average_pivot_ytd))],
                            textposition='auto',
                            textfont=dict(color='white')
                        )
                    )
                    fig.add_trace(
                        go.Bar(
                            x=dwell_average_pivot_ytd['Visit Type'],
                            y=dwell_average_pivot_ytd['On Time'] if 'On Time' in dwell_average_pivot_ytd.columns else [0] * len(dwell_average_pivot_ytd),
                            name='On Time',
                            marker=dict(color='rgba(0, 128, 0, 0.7)'),
                            text=[f'{val:.1f}' for val in dwell_average_pivot_ytd['On Time']] if 'On Time' in dwell_average_pivot_ytd.columns else ['0.0%' for _ in range(len(dwell_average_pivot_ytd))],
                            textposition='auto',
                            textfont=dict(color='white')
                        )
                    )
                    fig.update_layout(barmode='group', title="Average Dwell Time by Visit Type (YTD)")
                    st.plotly_chart(fig, use_container_width=True)

            carrier_pivot_ytd = ytd_filtered_df.pivot_table(
                values='Shipment Num',
                index='Carrier',
                columns='Compliance',
                aggfunc='count',
                fill_value=0
            ).reset_index()
            carrier_pivot_ytd['Grand Total'] = carrier_pivot_ytd.select_dtypes(include=[np.number]).sum(axis=1)
            carrier_pivot_ytd['On Time %'] = round((carrier_pivot_ytd.get('On Time', 0) / carrier_pivot_ytd['Grand Total']) * 100, 2)
            carrier_pivot_ytd = carrier_pivot_ytd.sort_values(by='On Time %', ascending=False)

            pivot_tables['YTD Carrier Compliance'] = carrier_pivot_ytd

            if view_option_ytd == 'Pivot Tables':
                st.subheader("On Time Compliance by Carrier (YTD)")
                st.table(carrier_pivot_ytd)
            else:
                fig = go.Figure(data=go.Heatmap(
                    z=carrier_pivot_ytd['On Time %'].values.reshape(1, -1),
                    x=carrier_pivot_ytd['Carrier'],
                    y=['On Time %'],
                    colorscale='RdYlGn',
                    text=carrier_pivot_ytd['On Time %'].values.reshape(1, -1),
                    texttemplate="%{text:.2f}%"
                ))
                fig.update_layout(title="On Time Compliance Percentage by Carrier (YTD)", xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)

            if pivot_tables:
                st.write("Download All Pivot Tables as Excel")
                excel_file = create_excel_file(pivot_tables)
                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name="dwell_and_compliance_YTD_pivots.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

with tabs[2]:
    if 'load_times' in st.session_state:
        st.header("Load Time Compliance Dashboard")

        load_times['Date'] = pd.to_datetime(load_times['Date'], format='%m/%d/%Y', errors='coerce')

        load_times['Compliance'] = load_times['Load Time (minutes)'].apply(lambda x: "Compliant" if x <= 90 else "Non-Compliant")

        date_filter_option = st.selectbox(
            "Select Date Filter",
            options=["YTD", "Date", "Week", "Month"]
        )

        if date_filter_option == "YTD":
            date_filtered_load_times = load_times
        elif date_filter_option == "Date":
            selected_date = st.date_input("Select a Date").strftime('%m/%d/%Y')
            date_filtered_load_times = load_times[load_times['Date'] == pd.to_datetime(selected_date, format='%m/%d/%Y')]
        elif date_filter_option == "Week":
            selected_week = st.number_input("Select Week Number", min_value=1, max_value=53, step=1)
            date_filtered_load_times = load_times[load_times['Week'] == selected_week]
        elif date_filter_option == "Month":
            selected_month = st.number_input("Select Month Number", min_value=1, max_value=12, step=1)
            date_filtered_load_times = load_times[load_times['Month'] == selected_month]

        if date_filtered_load_times.empty:
            st.warning("No data available for the selected date filter. Please adjust your selection.")
        else:
            display_mode = st.radio("Select Display Mode", options=["Visualizations", "Pivot Tables"])

            filter_by = st.selectbox("Filter By", options=["Shift", "User"])

            if filter_by == "Shift":
                shift_filter = st.selectbox("Select Shift", options=["All"] + list(load_times['Shift'].unique()))
            elif filter_by == "User":
                user_filter = st.selectbox("Select User ID", options=["All"] + list(load_times['User Id'].unique()))
            order_type_filter = st.selectbox("Select Order Type for Filtering", options=['All'] + list(load_times['Order Type'].unique()))

            filtered_load_times = date_filtered_load_times.copy()
            if filter_by == "Shift" and shift_filter != "All":
                filtered_load_times = filtered_load_times[filtered_load_times['Shift'] == shift_filter]
            elif filter_by == "User" and user_filter != "All":
                filtered_load_times = filtered_load_times[filtered_load_times['User Id'] == user_filter]

            st.write(f"Displaying data for **{filter_by}**: **{'All' if (filter_by == 'Shift' and shift_filter == 'All') or (filter_by == 'User' and user_filter == 'All') else shift_filter if filter_by == 'Shift' else user_filter}**")

            compliance_rate = round(filtered_load_times['Compliance'].value_counts(normalize=True).get("Compliant", 0) * 100, 2)

            st.subheader(f"Load Time Compliance Rate: {compliance_rate}%")

            col1, col2, col3 = st.columns(3)

            if display_mode == "Visualizations":
                with col1:
                    fig1 = go.Figure(data=[go.Pie(
                        labels=filtered_load_times['Compliance'].value_counts().index,
                        values=filtered_load_times['Compliance'].value_counts().values,
                        hole=0.4,
                        marker=dict(colors=["green" if label == "Compliant" else "red" for label in filtered_load_times['Compliance'].value_counts().index])
                    )])
                    fig1.update_layout(title_text="Load Time Compliance (90 Minutes Target)")
                    st.plotly_chart(fig1, use_container_width=True)

                with col2:
                    max_load_time = filtered_load_times['Load Time (minutes)'].max()
                    bins = np.linspace(0, max_load_time, 30)
                    bin_centers = (bins[:-1] + bins[1:]) / 2

                    colors = ['green' if x <= 90 else 'red' for x in bin_centers]

                    fig2 = go.Figure()

                    for i in range(len(bins) - 1):
                        bin_data = filtered_load_times[
                            (filtered_load_times['Load Time (minutes)'] >= bins[i]) &
                            (filtered_load_times['Load Time (minutes)'] < bins[i+1])
                        ]
                        frequency = len(bin_data)

                        fig2.add_trace(go.Bar(
                            x=[(bins[i] + bins[i+1]) / 2],
                            y=[frequency],
                            marker=dict(
                                color=colors[i],
                                line=dict(color='black', width=0.5)
                            ),
                            text=f"{frequency}",
                            textposition='outside',
                            name=f'{bins[i]:.2f} - {bins[i+1]:.2f}',
                            showlegend=False
                        ))

                    fig2.update_layout(
                        title="Distribution of Load Times (Minutes) with Frequencies",
                        xaxis_title="Load Time (minutes)",
                        yaxis_title="Frequency",
                        bargap=0.1 
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                with col3:
                    avg_load_time_compliance = filtered_load_times.groupby('Compliance')['Load Time (minutes)'].mean().reset_index()
                    fig3 = go.Figure(data=[go.Bar(
                        x=avg_load_time_compliance['Compliance'],
                        y=avg_load_time_compliance['Load Time (minutes)'],
                        text=avg_load_time_compliance['Load Time (minutes)'].round(2),
                        textposition='auto',
                        marker_color=["green" if comp == "Compliant" else "red" for comp in avg_load_time_compliance['Compliance']]
                    )])
                    fig3.update_layout(
                        title="Average Load Time by Compliance Status",
                        xaxis_title="Compliance",
                        yaxis_title="Average Load Time (minutes)"
                    )
                    st.plotly_chart(fig3, use_container_width=True)

                compliance_by_shift_order = date_filtered_load_times.pivot_table(
                    values='Order Num',
                    index='Shift',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()

                compliance_by_shift_order['Total'] = (
                    compliance_by_shift_order['Compliant'] + compliance_by_shift_order['Non-Compliant']
                )
                compliance_by_shift_order['Compliance Rate (%)'] = round(
                    (compliance_by_shift_order['Compliant'] / compliance_by_shift_order['Total']) * 100, 2
                )

                compliance_by_shift_order.sort_values('Compliance Rate (%)', ascending=False, inplace=True)

                heatmap_text = [
                    f"{row['Compliance Rate (%)']}%<br>out of {row['Total']} loads"
                    for _, row in compliance_by_shift_order.iterrows()
                ]

                st.subheader("Compliance Rate Heatmap by Shift with Total Loads")
                compliance_rates = compliance_by_shift_order['Compliance Rate (%)'].values.reshape(1, -1)

                fig5 = go.Figure(data=go.Heatmap(
                    z=compliance_rates,
                    x=compliance_by_shift_order['Shift'],
                    y=['Compliance Rate'],
                    colorscale='RdYlGn',
                    text=compliance_rates.round(2).astype(str) + "%",
                    texttemplate="<b>%{text}</b>",
                    textfont=dict(size=16),
                    colorbar=dict(title="Compliance %")
                ))

                fig5.update_layout(
                    xaxis=dict(
                        title="Shift",
                        tickmode='array',
                        tickvals=compliance_by_shift_order['Shift']
                    ),
                    yaxis=dict(
                        title="",
                        showticklabels=False 
                    )
                )

                st.plotly_chart(fig5, use_container_width=True)

            elif display_mode == "Pivot Tables":
                with col1:
                    compliance_pivot = filtered_load_times.pivot_table(
                        values='Order Num',
                        index='Compliance',
                        aggfunc='count'
                    ).reset_index()
                    compliance_pivot.rename(columns={'Order Num': 'Count'}, inplace=True)
                    total_orders = compliance_pivot['Count'].sum()
                    compliance_pivot['Percentage (%)'] = round((compliance_pivot['Count'] / total_orders) * 100, 2)
                    st.write("Compliance Distribution Pivot Table")
                    st.dataframe(compliance_pivot)

                with col2:
                    # Bin edges for grouping load times
                    max_load_time = filtered_load_times['Load Time (minutes)'].max()
                    bins = np.linspace(0, max_load_time, 30)

                    # Bin labels
                    bin_labels = [f"{int(bins[i])} - {int(bins[i+1])}" for i in range(len(bins)-1)]

                    # Assign bins to the load times
                    filtered_load_times['Load Time Bin (minutes)'] = pd.cut(
                        filtered_load_times['Load Time (minutes)'], 
                        bins=bins, 
                        labels=bin_labels, 
                        include_lowest=True
                    )

                    # Create frequency table
                    frequency_table = filtered_load_times.pivot_table(
                        values='Order Num', 
                        index='Load Time Bin (minutes)', 
                        aggfunc='count', 
                        fill_value=0
                    ).rename(columns={'Order Num': 'Frequency'}).reset_index()

                    # Add total row
                    total_row = pd.DataFrame({
                        'Load Time Bin (minutes)': ['Total'],
                        'Frequency': [frequency_table['Frequency'].sum()]
                    })

                    # Append the total row
                    frequency_table_with_total = pd.concat([frequency_table, total_row], ignore_index=True)

                    # Display the modified table
                    st.write("Load Times Statistics Pivot Table (Frequency Distribution)")
                    st.dataframe(frequency_table_with_total)

                with col3:
                    avg_load_time_pivot = filtered_load_times.groupby('Compliance')['Load Time (minutes)'].mean().reset_index()
                    avg_load_time_pivot.rename(columns={'Load Time (minutes)': 'Average Load Time'}, inplace=True)
                    st.write("Average Load Time by Compliance Pivot Table")
                    st.dataframe(avg_load_time_pivot)

                compliance_by_shift_pivot = date_filtered_load_times.pivot_table(
                    values='Order Num',
                    index='Shift',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                compliance_by_shift_pivot['Total'] = (
                    compliance_by_shift_pivot['Compliant'] + compliance_by_shift_pivot['Non-Compliant']
                )
                compliance_by_shift_pivot['Compliance Rate (%)'] = round(
                    (compliance_by_shift_pivot['Compliant'] / compliance_by_shift_pivot['Total']) * 100, 2
                )
                st.subheader("Compliance by Shift Pivot Table")
                st.dataframe(compliance_by_shift_pivot)

                pivot_tables = {
                    "Compliance Distribution": compliance_pivot,
                    "Load Times Statistics": frequency_table_with_total,
                    "Average Load Time": avg_load_time_pivot,
                    "Compliance by Shift": compliance_by_shift_pivot
                }

                st.write("Download Pivot Tables as Excel")
                excel_file = create_excel_file(pivot_tables)
                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name="pivot_tables.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
           



