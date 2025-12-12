COLUMN_DOCS = {
    # ===========================
    # IDENTIFICATION
    # ===========================
    "First Name": "Employee first name.",
    "Last Name": "Employee last name.",
    "Client": "Client company where the shift occurred.",
    "Venue Name": "Name of the venue or location for the event.",
    "Industry": "Client industry category.",
    "County of Venue": "County where the venue is located.",

    # ===========================
    # SHIFT INFO
    # ===========================
    "Date": "Date of the shift (MM/DD/YYYY).",
    "Position": "Employee job/position for the shift.",
    "Status": "Whether the employee worked, no-showed, or was cancelled.",

    # ===========================
    # HOURS (EMPLOYEE)
    # ===========================
    "Reg H (e)": "Regular hours worked by the employee.",
    "OT H (e)": "Overtime hours worked by the employee.",
    "DT H (e)": "Double-time hours worked by the employee.",
    "Meal H (e)": "Paid meal hours.",
    "Non-Worked Hours (e)": "Paid but non-worked hours (e.g., sick pay).",

    # Derived rule (important for SQL generation):
    # Hours Worked = Reg H (e) + OT H (e) + DT H (e)

    # ===========================
    # PAY
    # ===========================
    "Pay Rate": "Standard hourly pay rate for the employee.",
    "Std Pay": "Regular pay dollar amount for the shift.",
    "OT P": "Dollar amount of overtime pay.",
    "DT P": "Dollar amount of double-time pay.",
    "Bonus": "Extra pay or shift bonus.",
    "Gross Pay": "Total pay to employee for this shift (all components).",

    # ===========================
    # BILLING (REVENUE)
    # ===========================
    "Bill Rate": "Hourly billing rate charged to the client.",
    "Total Bill": "Total revenue billed to the client for this shift.",

    # ===========================
    # COLUMNS TO IGNORE
    # ===========================
    "IGNORE_COLUMNS": [
        "Day",
        "WC",
        "Markup",
        "Event State",
        "Venue Code",
        "Code",
        "AM/PM",
        "Work State",
        "OT Rate",
        "DT Rate",
        "Tip (e)",
        "Park (e)",
        "Travel (e)",
        "Travel Mileage Pay",
        "Service (e)",
        "Non-Worked Pay (e)",
        "Reimb Pay (e)",
        "Meal Break Reason (e)",
        "2nd Meal Break Reason (e)",
        "P.A.W",
        "Reg H (c)",
        "OT H (c)",
        "DT H (c)",
        "Reg Rate (c)",
        "Non-Worked Hours (c)",
        "Reg B",
        "OT R",
        "DT R",
        "Tip (c)",
        "Park (c)",
        "Travel (c)",
        "Travel Mileage Bill",
        "Service (c)",
        "Meal (c)",
        "Non-Worked Bill (c)",
        "Meal Break Minutes (c)",
        "Meal Break Reason (c)",
        "2nd Meal Break Reason (c)",
        "Reconciled",
        "Position Level",
        "Start Date",
        "Employee Rating",
        "Client Rating",
        "WC rate",
        "MSP rate",
        "Sales Executive",
        "1st Event Date",
        "Client Won Date",
        "Prior Won Date",
        "Notes",
        "Rehire Date",
        "Recruited By",
        "Surcharge Billed",
        "Cancelled In Deadline",
        "Background Query",
        "Employee Notes",
        "Client Notes",
        "Minimum Billed",
        "Cancellation Reason",
        "Background",
    ],

    # ===========================
    # RULES FOR THE AI
    # ===========================
    "RULES": """
    - Employee Name = First Name || ' ' || Last Name
    - Hours Worked = Reg H (e) + OT H (e) + DT H (e)
    - Employee Pay = Gross Pay
    - Revenue = Total Bill
    - Ignore all columns listed in IGNORE_COLUMNS unless explicitly asked.
    - For hourly or revenue questions, always aggregate using SUM().
    - Use the table name `shifts`.
    """,
}
