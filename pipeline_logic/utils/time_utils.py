import pendulum
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Union
import re
import pandas as pd

def floor_timestamp_with_input(timestamp_iso: str, floor_to: str = None, timezone: str = "America/Los_Angeles") -> str:
    """
    Floor a given timestamp to the specified granularity unit, respecting day boundaries.
    
    Args:
        timestamp_iso: Input timestamp in ISO format
        floor_to: Granularity like '1h', '30m', '15m', etc.
        timezone: Timezone for calculations
        
    Returns:
        Floored timestamp in ISO format, not crossing day boundaries
    """
    try:
        print(f"[DEBUG] timestamp_iso: {timestamp_iso}, floor_to: {floor_to}, timezone: {timezone}")
        
        if not floor_to or not floor_to.strip():
            return timestamp_iso
            
        pattern = r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
        match = re.fullmatch(pattern, floor_to.strip())
        
        if not match or not any(int(g or 0) > 0 for g in match.groups()):
            return timestamp_iso
            
        # Parse input timestamp
        ts = pendulum.parse(timestamp_iso).in_timezone(timezone)
        start_of_day = ts.start_of('day')
        
        if int(match.group(1) or 0) > 0:
            # Floor to day - return start of same day
            floored = ts.start_of('day')
        elif int(match.group(2) or 0) > 0:
            # Floor to hour
            floored = ts.start_of('hour')
        elif int(match.group(3) or 0) > 0:
            # Floor to minute  
            floored = ts.start_of('minute')
        elif int(match.group(4) or 0) > 0:
            # Floor to second
            floored = ts.start_of('second')
        else:
            return timestamp_iso
            
        # Ensure we don't go before start of day
        if floored < start_of_day:
            floored = start_of_day
            
        return floored.to_iso8601_string()
        
    except Exception as e:
        print(f"[ERROR] Exception in floor_timestamp_with_input: {e}")
        raise
    finally:
        print("[TRACE] floor_timestamp_with_input executed")


def ceil_timestamp_to_unit(ceil_to: str = None, timezone: str = "America/Los_Angeles") -> str:
    try:
        print(f"[DEBUG] ceil_to: {ceil_to}, timezone: {timezone}")
        if not ceil_to or not ceil_to.strip():
            return pendulum.now(timezone).to_iso8601_string()

        pattern = r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
        match = re.fullmatch(pattern, ceil_to.strip())

        if not match or not any(int(g or 0) > 0 for g in match.groups()):
            return pendulum.now(timezone).to_iso8601_string()

        now = pendulum.now(timezone)

        if int(match.group(1) or 0) > 0:
            # Round up to next day if not already at start of day
            start = now.start_of('day')
            return start.add(days=1).to_iso8601_string() if now > start else start.to_iso8601_string()
        elif int(match.group(2) or 0) > 0:
            # Round up to next hour if not already at start of hour
            start = now.start_of('hour')
            return start.add(hours=1).to_iso8601_string() if now > start else start.to_iso8601_string()
        elif int(match.group(3) or 0) > 0:
            # Round up to next minute if not already at start of minute
            start = now.start_of('minute')
            return start.add(minutes=1).to_iso8601_string() if now > start else start.to_iso8601_string()
        elif int(match.group(4) or 0) > 0:
            # Round up to next second if not already at start of second
            start = now.start_of('second')
            return start.add(seconds=1).to_iso8601_string() if now > start else start.to_iso8601_string()
        else:
            return now.to_iso8601_string()
    except Exception as e:
        print(f"[ERROR] Exception in ceil_timestamp_to_unit: {e}")
        raise
    finally:
        print("[TRACE] ceil_timestamp_to_unit executed")

        
def round_timestamp_nearest(round_to: str = None, timezone: str = "America/Los_Angeles") -> str:
    try:
        print(f"[DEBUG] round_to: {round_to}, timezone: {timezone}")
        if not round_to or not round_to.strip():
            return pendulum.now(timezone).to_iso8601_string()

        pattern = r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
        match = re.fullmatch(pattern, round_to.strip())

        if not match or not any(int(g or 0) > 0 for g in match.groups()):
            return pendulum.now(timezone).to_iso8601_string()

        now = pendulum.now(timezone)

        if int(match.group(1) or 0) > 0:
            start = now.start_of('day')
            next_start = start.add(days=1)
        elif int(match.group(2) or 0) > 0:
            start = now.start_of('hour')
            next_start = start.add(hours=1)
        elif int(match.group(3) or 0) > 0:
            start = now.start_of('minute')
            next_start = start.add(minutes=1)
        elif int(match.group(4) or 0) > 0:
            start = now.start_of('second')
            next_start = start.add(seconds=1)
        else:
            return now.to_iso8601_string()

        midpoint = start.add(seconds=(next_start - start).total_seconds() / 2)
        rounded = start if now < midpoint else next_start
        return rounded.to_iso8601_string()
    except Exception as e:
        print(f"[ERROR] Exception in round_timestamp_nearest: {e}")
        raise
    finally:
        print("[TRACE] round_timestamp_nearest executed")


def get_rounded_past_timestamp(x_time_back: str = None, timezone: str = "America/Los_Angeles") -> str:
    try:
        print(f"[DEBUG] x_time_back: {x_time_back}, timezone: {timezone}")
        if not x_time_back or not x_time_back.strip():
            return pendulum.now(timezone).to_iso8601_string()

        pattern = r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
        match = re.fullmatch(pattern, x_time_back.strip())

        if not match or not any(int(g or 0) > 0 for g in match.groups()):
            return pendulum.now(timezone).to_iso8601_string()

        now = pendulum.now(timezone)
        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)

        if days > 0:
            now = now.start_of('day')
        elif hours > 0:
            now = now.start_of('hour')
        elif minutes > 0:
            now = now.start_of('minute')
        elif seconds > 0:
            now = now.start_of('second')

        past_time = now.subtract(days=days, hours=hours, minutes=minutes, seconds=seconds)
        return past_time.to_iso8601_string()
    except Exception as e:
        print(f"[ERROR] Exception in get_rounded_past_timestamp: {e}")
        raise
    finally:
        print("[TRACE] get_rounded_past_timestamp executed")


def generate_time_windows(start_ts: str, dt: str, end_ts: str = None, timezone: str = "America/Los_Angeles") -> list:
    try:
        print(f"[DEBUG] start_ts: {start_ts}, dt: {dt}, end_ts: {end_ts}, timezone: {timezone}")
        pattern = r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
        match = re.fullmatch(pattern, dt.strip())
        if not match or not any(int(g or 0) > 0 for g in match.groups()):
            raise ValueError("Invalid dt format. Use '1h', '30m', '2h30m', etc.")

        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)

        if days > 0:
            raise ValueError("Window duration cannot span full days. Use h/m/s granularity only.")

        start = pendulum.parse(start_ts, tz=timezone)

        # Use end_ts if provided, otherwise use end_of_day
        if end_ts:
            end_limit = pendulum.parse(end_ts, tz=timezone)
        else:
            end_limit = start.end_of('day')

        windows = []
        current = start

        while True:
            next_time = current.add(hours=hours, minutes=minutes, seconds=seconds)
            if next_time > end_limit:  # Changed back to > since we want to stop before exceeding the limit
                break
            windows.append((current.to_iso8601_string(), next_time.to_iso8601_string()))
            current = next_time

        return windows
    except Exception as e:
        print(f"[ERROR] Exception in generate_time_windows: {e}")
        raise
    finally:
        print("[TRACE] generate_time_windows executed")



def calculate_window_granularity_minutes(start_iso: str, end_iso: str) -> int:
    try:
        print(f"[DEBUG] start_iso: {start_iso}, end_iso: {end_iso}")
        start = pendulum.parse(start_iso)
        end = pendulum.parse(end_iso)
        return int((end - start).total_minutes())
    except Exception as e:
        print(f"[ERROR] Exception in calculate_window_granularity_minutes: {e}")
        raise
    finally:
        print("[TRACE] calculate_window_granularity_minutes executed")

def check_time_window_continuity(windows: Union[List[Tuple[str, str]], Tuple[Tuple[str, str], ...]]) -> List[Tuple[str, str]]:
    try:
        print(f"[DEBUG] windows: {windows}")
        if not windows:
            return []

        windows_sorted = sorted(windows, key=lambda x: pendulum.parse(x[0]))
        missing_intervals = []
        for i in range(len(windows_sorted) - 1):
            current_end = pendulum.parse(windows_sorted[i][1])
            next_start = pendulum.parse(windows_sorted[i + 1][0])
            if current_end != next_start:
                missing_intervals.append((current_end.to_iso8601_string(), next_start.to_iso8601_string()))
        return missing_intervals
    except Exception as e:
        print(f"[ERROR] Exception in check_time_window_continuity: {e}")
        raise
    finally:
        print("[TRACE] check_time_window_continuity executed")

def check_time_window_overlaps(windows: Union[List[Tuple[str, str]], Tuple[Tuple[str, str], ...]]) -> List[Tuple[Tuple[str, str], Tuple[str, str]]]:
    try:
        print(f"[DEBUG] windows: {windows}")
        windows = sorted(windows, key=lambda x: pendulum.parse(x[0]))
        overlaps = []
        for i in range(len(windows) - 1):
            current_start, current_end = map(pendulum.parse, windows[i])
            next_start, next_end = map(pendulum.parse, windows[i + 1])
            if next_start < current_end:
                overlaps.append((windows[i], windows[i + 1]))
        return overlaps
    except Exception as e:
        print(f"[ERROR] Exception in check_time_window_overlaps: {e}")
        raise
    finally:
        print("[TRACE] check_time_window_overlaps executed")


def normalize_timestamp_to_iso8601(ts: Union[datetime, pd.Timestamp, pendulum.DateTime]) -> str:
    try:
        print(f"[DEBUG] ts type: {type(ts)}")
        if isinstance(ts, pendulum.DateTime):
            return ts.to_iso8601_string()
        elif isinstance(ts, pd.Timestamp):
            return ts.isoformat()
        elif isinstance(ts, datetime):
            return ts.isoformat()
        else:
            raise TypeError(f"Unsupported timestamp type: {type(ts)}")
    except Exception as e:
        print(f"[ERROR] Exception in normalize_timestamp_to_iso8601: {e}")
        raise
    finally:
        print("[TRACE] normalize_timestamp_to_iso8601 executed")



def diff_in_minutes(start_ts_iso: str, end_ts_iso: str) -> float:
    try:
        print(f"[DEBUG] start_ts_iso: {start_ts_iso}, end_ts_iso: {end_ts_iso}")
        start = pendulum.parse(start_ts_iso)
        end = pendulum.parse(end_ts_iso)
        return (end - start).in_minutes()
    except Exception as e:
        print(f"[ERROR] Exception in diff_in_minutes: {e}")
        raise
    finally:
        print("[TRACE] diff_in_minutes executed")



def format_timestamp_for_elasticsearch(ts: Union[datetime, pd.Timestamp, pendulum.DateTime]) -> str:
    """Convert timestamp to Elasticsearch format: %Y-%m-%dT%H:%M:%SZ"""
    try:
        if isinstance(ts, pd.Timestamp):
            return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(ts, pendulum.DateTime):
            return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            raise TypeError(f"Unsupported timestamp type: {type(ts)}")
    except Exception as e:
        print(f"[ERROR] Exception in format_timestamp_for_elasticsearch: {e}")
        raise            


# # Example usage
# if __name__ == "__main__":
#     base = {'c1': 1, 'c2': "user", 'c3': 0}
#     ts_fields = ('ts_1', 'ts_2')
#     windows = [
#         ("2025-06-19T10:00:00+05:30", "2025-06-19T11:00:00+05:30"),
#         ("2025-06-19T11:00:00+05:30", "2025-06-19T12:00:00+05:30"),
#         ("2025-06-19T12:00:00+05:30", "2025-06-19T13:00:00+05:30")
#     ]

#     df = construct_window_records(base, ts_fields, windows)
#     print(df)


# # Sample test cases
# if __name__ == "__main__":
#     print("Test: 1d →", get_rounded_past_timestamp("1d", "Asia/Kolkata"))
#     print("Test: 2h →", get_rounded_past_timestamp("2h", "Asia/Kolkata"))
#     print("Test: 15m →", get_rounded_past_timestamp("15m", "Asia/Kolkata"))
#     print("Test: 45s →", get_rounded_past_timestamp("45s", "Asia/Kolkata"))
#     print("Test: 1d2h30m45s →", get_rounded_past_timestamp("1d2h30m45s", "Asia/Kolkata"))
#     print("Test: Empty string →", get_rounded_past_timestamp("", "Asia/Kolkata"))
#     print("Test: Zero duration →", get_rounded_past_timestamp("0d0h0m0s", "Asia/Kolkata"))
#     print("Test: None →", get_rounded_past_timestamp(None, "Asia/Kolkata"))
#     print("--"*20)
#     print("Nearest day:", round_timestamp_nearest("1d", "Asia/Kolkata"))
#     print("Nearest hour:", round_timestamp_nearest("1h", "Asia/Kolkata"))
#     print("Nearest minute:", round_timestamp_nearest("1m", "Asia/Kolkata"))
#     print("Nearest second:", round_timestamp_nearest("1s", "Asia/Kolkata"))
#     print("Empty input (no rounding):", round_timestamp_nearest("", "Asia/Kolkata"))
#     print("-"*20)
#     print("Floored to day:", floor_timestamp_to_unit("1d", "Asia/Kolkata"))
#     print("Floored to hour:", floor_timestamp_to_unit("2h", "Asia/Kolkata"))
#     print("Floored to minute:", floor_timestamp_to_unit("15m", "Asia/Kolkata"))
#     print("Floored to second:", floor_timestamp_to_unit("30s", "Asia/Kolkata"))
#     print("Empty input (no floor):", floor_timestamp_to_unit("", "Asia/Kolkata"))

# Test
# if __name__ == "__main__":
#     result = generate_time_windows("2025-06-19T00:00:00+05:30", "1h")
#     for w in result:
#         print(w)

# Test
# if __name__ == "__main__":
#     test_windows = [
#         ("2025-06-19T10:00:00+05:30", "2025-06-19T11:00:00+05:30"),
#         ("2025-06-19T10:30:00+05:30", "2025-06-19T11:30:00+05:30"),  # Overlaps with above
#         ("2025-06-19T11:30:00+05:30", "2025-06-19T12:30:00+05:30"),  # No overlap
#         ("2025-06-19T12:00:00+05:30", "2025-06-19T13:00:00+05:30"),  # Overlaps with above
#         ("2025-06-19T13:00:00+05:30", "2025-06-19T14:00:00+05:30")   # No overlap
#     ]

#     overlap_result = check_time_window_overlaps(test_windows)
#     for o in overlap_result:
#         print("Overlap:", o)



# # Test inputs
# ts_pendulum = pendulum.datetime(2025, 6, 19, 14, 30, tz="Asia/Kolkata")
# ts_pandas = pd.Timestamp("2025-06-19T14:30:00+05:30")
# ts_datetime = datetime(2025, 6, 19, 14, 30, tzinfo=ZoneInfo("Asia/Kolkata"))

# # Outputs
# print("Pendulum Timestamp:", ts_pendulum, "| Type:", type(ts_pendulum), "| ISO:", normalize_timestamp_to_iso8601(ts_pendulum))
# print("Pandas Timestamp  :", ts_pandas, "| Type:", type(ts_pandas), "| ISO:", normalize_timestamp_to_iso8601(ts_pandas))
# print("Datetime Timestamp:", ts_datetime, "| Type:", type(ts_datetime), "| ISO:", normalize_timestamp_to_iso8601(ts_datetime))


# start = "2025-06-19T13:00:00+05:30"
# end = "2025-06-29T14:15:30+05:30"
# print("Duration (min):", diff_in_minutes(start, end))



