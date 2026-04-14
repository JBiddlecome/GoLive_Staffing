from datetime import datetime

def test_logic(start_str, end_str, break_mins, sec_break_mins, reason):
    item = {
        "employee_start": datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S") if start_str else None,
        "employee_end": datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S") if end_str else None,
        "employee_no_sec_break_reason": reason
    }
    
    total_duration_minutes = 0
    if item["employee_start"] and item["employee_end"]:
        duration = item["employee_end"] - item["employee_start"]
        total_duration_minutes = duration.total_seconds() / 60
    
    break1_mins = break_mins or 0
    break2_mins = sec_break_mins or 0
    
    worked_minutes = total_duration_minutes - break1_mins - break2_mins
    worked_hours = round(worked_minutes / 60, 2)
    
    is_over_10_hours = worked_hours > 10
    is_sec_break_short_or_missing = (break2_mins < 30)
    is_reason_3 = (item["employee_no_sec_break_reason"] == 3)
    
    excluded = is_over_10_hours and is_sec_break_short_or_missing and is_reason_3
    
    print(f"Duration: {total_duration_minutes}m, Worked Mins: {worked_minutes}m, Worked Hours: {worked_hours}")
    print(f"over_10: {is_over_10_hours}, sec_short: {is_sec_break_short_or_missing}, reason_3: {is_reason_3} -> Excluded: {excluded}")

print("Testing exactly 10 hours worked:")
test_logic("2024-01-01 09:00:00", "2024-01-01 19:30:00", 30, 0, 3)

print("\nTesting 10.01 hours worked:")
test_logic("2024-01-01 09:00:00", "2024-01-01 19:31:00", 30, 0, 3)

print("\nTesting 9.98 hours worked:")
test_logic("2024-01-01 09:00:00", "2024-01-01 19:29:00", 30, 0, 3)
