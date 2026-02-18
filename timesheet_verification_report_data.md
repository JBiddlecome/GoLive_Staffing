# Timesheet Verification Report Data Sources

This document outlines the specific Amazon RDS database tables and fields used to generate the "Timesheet Verification" report.

## Primary Data Sources

The report is primarily generated from the `shift_employee` table, joining with `event`, `client`, `venue`, `employee`, and `timesheet` tables.

### Tables Involved

| Table Name | Description |
| :--- | :--- |
| `shift_employee` | Links employees to specific events/shifts. Contains rates and cancellation status. |
| `event` | Contains event details like date, title, and state. |
| `client` | Client information including markup and billing details. |
| `venue` | Venue information where the event took place. |
| `employee` | Employee personal details and payroll ID. |
| `timesheet` | Actual worked hours, adjustments, tips, parking, and verification status. |
| `shift_position` | Links the shift to a specific position/role. |
| `position` | The name/description of the job role. |
| `wc_code` | Workers Compensation codes (linked via Client). |
| `min_wage_rate` | Employee work state/minimum wage classification. |

## Field Mapping

The following table maps the columns in the exported Excel report to the specific database fields.

| Report Column | Database Table | Database Field | Notes |
| :--- | :--- | :--- | :--- |
| **Day** | `event` | `date` | Formatted as Day Name (e.g., Monday). |
| **Date** | `event` | `date` | Date of the event. |
| **WC** | `wc_code` | `wc_code` | Via `client.wc_id`. Fallback to `employee.state` + "8810". |
| **Client** | `client` | `name` | May include `msp.name` and `division.name`. |
| **Markup** | `client` | `markup` | |
| **Venue** | `venue` | `name` | |
| **Event** | `event` | `title` | |
| **Position** | `position` | `description` | Via `shift_position.position_id`. |
| **Code** | `shift_position` | `code` | |
| **#Emp** | `employee` | `payroll_id` | |
| **First Name** | `employee` | `first_name` | One-time capitalized. |
| **Last Name** | `employee` | `last_name` | One-time capitalized. |
| **Work State** | `min_wage_rate` | `description` | Via `employee.min_wage_id`. |
| **Reg H (c)** | *Calculated* | - | Derived from `timesheet` start/end or `shift` duration. |
| **OT H (c)** | *Calculated* | - | Calculated based on state laws (CA/NV) and hours worked. |
| **DT H (c)** | *Calculated* | - | Calculated based on state laws (Double Time). |
| **Reg Rate (c)** | `shift_employee` | `bill_rate` | Base billing rate. |
| **Non-Worked Hours (c)** | *Calculated* | - | Difference between minimum billable and actual worked. |
| **Cert Cost (e)** | `certification` | `cost` | If applicable (derived logic). |
| **OT R** | *Calculated* | - | Typically `bill_rate * 1.5`. |
| **DT R** | *Calculated* | - | Typically `bill_rate * 2.0`. |
| **Tip (c)** | `timesheet` | `client_tips` | |
| **Park (c)** | `timesheet` | `client_parking` | |
| **Travel (c)** | `timesheet` | `client_travel` | |
| **Service (c)** | `timesheet` | `client_service_charge`| Percentage applied to bill rate. |
| **Meal (c)** | `timesheet` | `client_no_break_penalty`| Penalty hours if applicable. |
| **Non-Worked Bill (c)** | *Calculated* | - | `Non-Worked Hours * Bill Rate`. |
| **Reimb Pay (e)** | `employee_other_work`| `cost` | Reimbursements (if applicable). |
| **Bill Rate** | `shift_employee` | `bill_rate` | |
| **Total Bill** | *Calculated* | - | Sum of all billable components. |
| **Status** | `timesheet` | `client_worked` | Status ID (Worked, Sent Home, Cancelled). |
| **Cancellation Reason**| `shift_employee` | `cancel_reason` | Reason ID. |
| **Verification (c)** | *Derived* | - | Logic based on `timesheet.start_verified`, `timesheet.end_verified`, `event.timeclock`. |
| **Verification (e)** | *Derived* | - | Logic based on `timesheet.start_verified_at`, `timesheet.end_verified_at`. |

## Key Relationships

*   `shift_employee.event_id` -> `event.event_id`
*   `shift_employee.employee_id` -> `employee.employee_id`
*   `shift_employee.shift_employee_id` -> `timesheet.shift_employee_id`
*   `event.client_id` -> `client.client_id`
*   `event.venue_id` -> `venue.venue_id`
*   `shift_employee.shift_position_id` -> `shift_position.shift_position_id` -> `position.position_id`
