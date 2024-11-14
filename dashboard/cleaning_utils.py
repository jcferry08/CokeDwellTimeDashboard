import pandas as pd

def clean_activity_tracker(activity_report):

    activity_report.rename(columns={'ï»¿Create DateTime': 'Create DateTime', 'Order #': 'Order Num'}, inplace=True)

    columns_to_keep = ['Create DateTime', 'Order Num']
    activity_report = activity_report.drop(columns=activity_report.columns.difference(columns_to_keep))

    activity_report['Create DateTime'] = pd.to_datetime(activity_report['Create DateTime'])

    order_groups = activity_report.groupby('Order Num')['Create DateTime']
    load_times = order_groups.agg(lambda x: round((x.max() - x.min()).total_seconds() / 60, 2)).reset_index()
    load_times.columns = ['Order Num', 'Load Time (minutes)']
    
    shift_calendat_path = 'data/raw/Shift Calendar 2024.csv'
    shift_calendar = pd.read_csv(shift_calendat_path)

    shift_calendar['Date'] = pd.to_datetime(shift_calendar['Date'], format='%m/%d/%y')

    def get_shift(create_datetime):
        date = create_datetime.date()
        shift = '1' if 7 <= create_datetime.hour < 19 else '2'

        shift_color = shift_calendar.loc[shift_calendar['Date'] == pd.Timestamp(date), shift].values
        return shift_color[0] if shift_color.size > 0 else None
    
    activity_report['Shift'] = activity_report['Create DateTime'].apply(get_shift)
    activity_report['Order Type'] = activity_report['Order Num'].apply(lambda x: 'Shuttle' if x.startswith('02') else ('Customer Load' if x.startswith('04') else 'Unknown'))
    additional_info = activity_report.groupby('Order Num').agg({'Shift': 'first', 'Order Type': 'first'}).reset_index()
    load_times = load_times.merge(additional_info, on='Order Num', how='left')

    return load_times

def clean_order_view(order_report):

    order_report['Appointment Date'] = pd.to_datetime(order_report['Appointment Date'])
    order_report_sorted = order_report.sort_values(by=['Shipment #', 'Appointment Date'], ascending=[True, False])
    order_report = order_report_sorted.drop_duplicates(subset='Shipment #', keep='first')

    columns_to_keep = ['Shipment #', 'SAP Delivery # (Order#)', 'Appointment Date', 'Carrier', 'Appointment Type']
    order_report = order_report.drop(columns=order_report.columns.difference(columns_to_keep))

    order_report = order_report.dropna()

    order_report.rename(columns={'Shipment #': 'Shipment Num', 'SAP Delivery # (Order#)': 'Order Num', 'Appointment Date': 'Appointment DateTime', 'Appointment Type': 'Visit Type'}, inplace=True)

    def required_time(row):
        if row['Visit Type'] == 'LIVE':
            return row['Appointment DateTime'] + pd.Timedelta(minutes=15)
        else:
            return row['Appointment DateTime'] + pd.Timedelta(hours=24)
        
    order_report['Required DateTime'] = order_report.apply(required_time, axis=1)

    order_report['Scheduled Date'] = order_report['Appointment DateTime'].dt.strftime("%m/%d/%Y")
    order_report['Week'] = order_report['Appointment DateTime'].dt.isocalendar().week
    order_report['Month'] = order_report['Appointment DateTime'].dt.month

    return order_report

def clean_trailer_activity(trailer_report):

    trailer_report = trailer_report[trailer_report['ACTIVITY TYPE '] == 'CLOSED']

    columns_to_keep = ['CHECKIN DATE TIME', 'CHECKOUT DATE TIME', 'Date/Time', 'SHIPMENT_ID', 'Date/Time', ]
    trailer_report = trailer_report.drop(columns=trailer_report.columns.difference(columns_to_keep))

    trailer_report = trailer_report.dropna()

    trailer_report['Date/Time'] = pd.to_datetime(trailer_report['Date/Time'])
    trailer_report = trailer_report.sort_values(by=['SHIPMENT_ID', 'Date/Time'], ascending=[True, False])
    trailer_report = trailer_report.drop_duplicates(subset='SHIPMENT_ID', keep='first')

    trailer_report.rename(columns={'CHECKIN DATE TIME': 'Checkin DateTime', 'CHECKOUT DATE TIME': 'Checkout DateTime', 'SHIPMENT_ID': 'Shipment Num', 'Date/Time': 'Loaded DateTime'}, inplace=True)

    trailer_report['Checkin DateTime'] = pd.to_datetime(trailer_report['Checkin DateTime'])
    trailer_report['Checkout DateTime'] = pd.to_datetime(trailer_report['Checkout DateTime'])

    shift_calendat_path = 'data/raw/Shift Calendar 2024.csv'
    shift_calendar = pd.read_csv(shift_calendat_path)

    shift_calendar['Date'] = pd.to_datetime(shift_calendar['Date'], format='%m/%d/%y')

    def get_shift(create_datetime):
        date = create_datetime.date()
        shift = '1' if 7 <= create_datetime.hour < 19 else '2'

        shift_color = shift_calendar.loc[shift_calendar['Date'] == pd.Timestamp(date), shift].values
        return shift_color[0] if shift_color.size > 0 else None
    
    trailer_report['Shift'] = trailer_report['Checkin DateTime'].apply(get_shift)

    return trailer_report