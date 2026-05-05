# GoLive Staffing Database — Complete AI Training Reference

> **Purpose**: This document provides everything an AI agent needs to understand the `cstaffing_live` MariaDB database used by GoLive Staffing, a culinary/hospitality temporary staffing agency based in California. The database powers scheduling, timekeeping, payroll, billing, and employee management.

---

## 1. Domain Overview

GoLive Staffing places temporary culinary and hospitality workers (employees) at client venues for shifts. The core workflow is:

1. A **Client** (e.g., a hotel, casino, catering company) has one or more **Venues** (physical locations).
2. An **Event** is created for a specific date at a specific venue (e.g., "Commerce Casino on 2026-03-15").
3. Each event contains one or more **Shifts** (time blocks, e.g., 6:00 AM – 2:00 PM).
4. Each shift has one or more **Shift Positions** (e.g., "need 3 Line Cooks at $22/hr pay, $35/hr bill").
5. Employees are assigned to positions via **Shift Employee** records (each with individual pay/bill rates).
6. When an employee is confirmed, a **Timesheet** is auto-created (1:1 with shift_employee).
7. Timesheets track clock-in/out times, worked status, hours, and financial calculations.

---

## 2. Entity Hierarchy & Join Paths

```
client (top-level customer)
  └─ venue (physical location, FK: venue.client_id → client.client_id)
  └─ event (a booking on a date, FK: event.client_id → client.client_id)

venue
  └─ event (FK: event.venue_id → venue.venue_id)

event (a specific date + venue combination)
  └─ shift (a time block within the event, FK: shift.event_id → event.event_id)

shift (e.g., 6AM-2PM)
  └─ shift_position (a role needed, FK: shift_position.shift_id → shift.shift_id)

shift_position (e.g., "3 Line Cooks needed")
  └─ shift_employee (an assigned worker, FK: shift_employee.shift_position_id → shift_position.shift_position_id)

shift_employee (one employee on one position)
  └─ timesheet (1:1 unique, FK: timesheet.shift_employee_id → shift_employee.shift_employee_id)
  └─ employee (FK: shift_employee.employee_id → employee.employee_id)
```

### Critical Join Rules

- **Soft deletes**: ONLY these tables have a `deleted_at` column: `client`, `venue`, `event`, `shift`, `shift_position`, `shift_employee`, `employee`. For these tables, always filter `WHERE deleted_at IS NULL` for active records. **All other tables do NOT have `deleted_at`** — do not add that filter to them.
- **Active employee on shift**: `shift_employee.cancel_reason = 0 AND shift_employee.deleted_at IS NULL`
- **Confirmed employee**: `shift_employee.confirmed = 1 AND shift_employee.cancel_reason = 0`
- **Timesheet uniqueness**: `timesheet.shift_employee_id` has a UNIQUE constraint — exactly one timesheet per shift_employee.
- **Denormalized FKs on timesheet**: `timesheet.event_id` and `timesheet.employee_id` are denormalized copies (also reachable via shift_employee joins).

### Full Payroll Join Path

```sql
FROM timesheet t
JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
JOIN shift s ON s.shift_id = sp.shift_id
JOIN event e ON e.event_id = t.event_id
JOIN employee emp ON emp.employee_id = t.employee_id
JOIN venue v ON v.venue_id = e.venue_id
JOIN client c ON c.client_id = e.client_id
```

### Other Key Relationships

| From | To | Join |
|---|---|---|
| venue | user (staffing manager) | `venue.staffing_manager_id = user.id` |
| client | user (account manager) | `client.staff_id = user.id` |
| dnr | employee + client | `dnr.employee_id`, `dnr.client_id` (+ optional `venue_id`) |
| exclusive | employee + client | `exclusive.employee_id`, `exclusive.client_id` (+ optional `venue_id`) |
| employee_certification | employee + certification | `employee_certification.employee_id`, `employee_certification.certification_id` |
| venue_certification | venue + certification | `venue_certification.venue_id`, `venue_certification.certification_id` |
| client | msp | `client.msp_id = msp.id` |
| client | wc_code | `client.wc_id = wc_code.wc_id` |
| client | min_wage_rate | `client.min_wage_id = min_wage_rate.min_wage_id` |
| shift_position | position | `shift_position.position_id = position.position_id` |

---

## 3. Enum / Status Code Reference

### 3.1 timesheet.employee_worked / timesheet.client_worked

| Value | Meaning | Financial Impact |
|---|---|---|
| `WORKED` | Employee completed the shift | Normal pay and billing based on actual hours |
| `SENTHOME` | Client sent employee home early | Employee gets minimum pay guarantee (CA/WA/NY). See §6. |
| `CANCELLED` | Shift was cancelled | If within cancellation deadline → minimum pay/bill applies |
| `NOSHOW` | Employee did not show up | Usually $0 pay. Client may or may not be billed. |

**SENTHOME auto-detection**: If `employee_worked = 'WORKED'` AND actual worked hours < 50% of scheduled shift AND `less_hours_reason = 1` (client sent home), the system auto-changes status to `SENTHOME`.

### 3.2 timesheet.use_sheet

| Value | Meaning |
|---|---|
| `CLIENT` | Use client's clock-in/out times for ALL calculations (billing AND pay) |
| `EMPLOYEE` | Use employee's clock-in/out times for ALL calculations |
| `NULL` (empty) | Use BOTH sheets independently: client_seconds for billing, employee_seconds for pay |

### 3.3 timesheet.status

| Value | Meaning |
|---|---|
| `NEW` | Created, no time entries yet |
| `IN_PROGRESS` | Both sides entered times but discrepancy exists |
| `COMPLETED` | Both sides agree (no discrepancy) |

### 3.4 shift_employee.cancel_reason

| Code | Meaning | Category |
|---|---|---|
| 0 | **ACTIVE** (not cancelled) | Active |
| 2 | Employee cancelled < 24 hours notice | Employee-caused |
| 3 | Employee cancelled > 24 hours notice | Employee-caused |
| 4 | Client cancelled shift | Client-caused |
| 5 | Client decreased staff needed | Client-caused |
| 6 | Employee cancelled within policy | Employee-caused |
| 7 | Employee moved to another shift | Employee-caused |
| 8 | Employee not qualified | Employee-caused |
| 9 | Accidental sign-up | Administrative |
| 10 | Other | Administrative |
| 11 | Employee declined | Employee-caused |
| 12 | Declined by admin | Administrative |
| 13 | Shift request not confirmed | Administrative |
| 14 | No call / no show | Employee-caused |
| 15 | Do Not Return (DNR) notice | Administrative |
| 41 | Client had cancelled (historical rebook) | Historical |
| 51 | Client had decreased staff (historical rebook) | Historical |

**Codes 41 and 51**: When an employee is rebooked onto an event where they were previously client-cancelled (4→41) or staff-decreased (5→51), the old record's cancel_reason is updated to preserve history.

### 3.5 employee.status

| Code | Meaning |
|---|---|
| 1 | Active |
| 2 | Candidate (applied, not yet approved) |
| 3 | Hiatus (temporarily unavailable) |
| 5 | Terminated |
| 6 | Resigned |
| 10 | Inactive 60 days |
| 14 | Other |
| 15 | IFR (In For Review) |

### 3.6 client.status

| Code | Meaning |
|---|---|
| 0 | Terminated |
| 1 | Active |
| 3 | Prospect |
| 4 | Candidate Partner |
| 10 | Inactive 60 days |
| 11 | Inactive 180 days |
| 12 | Inactive 365 days |

### 3.7 client.separate_venue (Invoice Separation Mode)

| Code | Meaning |
|---|---|
| 0 | No separation (bill all venues together) |
| 1 | Invoice venues separately (by venue_id) |
| 2 | Venues are separate customers |
| 3 | Invoice venues separately AND by day |
| 4 | Venues are separate customers AND separated by day |
| 5 | Invoice separately by PO number |

### 3.8 client.payment_type

| Code | Meaning |
|---|---|
| 1 | Credit Card |
| 2 | Invoice |

### 3.9 client.no_break_penalty / event.no_break_penalty

| Code | Meaning |
|---|---|
| 1 | Enabled — charge penalty if meal break was missed |
| 2 | Disabled |

### 3.10 employee.flag (Color Tags)

| Code | Color |
|---|---|
| 0 | Orange |
| 1 | Red |
| 2 | Green |
| 3 | Brown |
| 4 | Blue |
| 5 | Purple |
| 6 | Yellow |

### 3.11 shift_employee.confirmed

| Value | Meaning |
|---|---|
| 0 | Pending / Not yet confirmed |
| 1 | Confirmed for shift |

### 3.12 shift_employee.confirm_type

| Code | Meaning |
|---|---|
| 1 | By employee (self-confirmed) |
| 2 | By phone |
| 3 | By email |
| 4 | Other |
| 5 | By text |

### 3.13 event.timeclock

| Value | Meaning |
|---|---|
| `DISABLED` | No digital timeclock |
| `CLIENT` | Client enters times |
| `EMPLOYEE` | Employee enters times |
| `CLIENT_EMPLOYEE` | Both enter times (creates discrepancy check) |
| `EMPLOYEE_CODE` | Employee uses QR/PIN code |
| `KRONOS` | External system — no manual edit allowed |
| `ATLAS` | External system — no manual edit allowed |
| `NOWSTA` | External system — no manual edit allowed |

### 3.14 timesheet.start_verified / end_verified

| Value | Meaning |
|---|---|
| `VERIFIED` | Clock-in/out verified on time |
| `AUTO_VERIFIED` | System auto-verified |
| `MANUALLY_VERIFIED` | Admin manually verified |
| `WAS_VERIFIED` | Was verified but employee clocked in later than tolerance |
| `NON_VERIFIED` | Not yet verified |

### 3.15 client.industry

Values: `HOTEL`, `CLUB`, `ENTERTAINMENT_STUDIO`, `STADIUM`, `CONVENTION`, `CORPORATE_DINING`, `CATERING_COMPANY`, `SCHOOL`, `HEALTHCARE`, `SENIOR_ASSISTED_LIVING`, `REHABILITATION`, `RESTAURANT`, `OTHER`, `CANDIDATE_REFERRAL_PARTNER`, `CASINO`, `PRIVATE_EVENT`, `PRODUCTION`

### 3.16 shift_employee.overtime_paid_by

| Value | Meaning |
|---|---|
| `CLIENT` | Overtime cost billed to client |
| `AGENCY` | Overtime absorbed by agency (GoLive) |

### 3.17 shift_position.was_published

| Code | Meaning |
|---|---|
| 0 | Never published to employee portal |
| 1 | Published (initial state) |
| 2 | Published and done |

---

## 4. Key Table Field Reference

### 4.1 `timesheet` — The Financial Heart of the System

Every financial calculation flows through this table. One row = one employee's record for one shift placement.

| Field | Type | Description |
|---|---|---|
| `timesheet_id` | PK int | Primary key |
| `shift_employee_id` | FK int (UNIQUE) | Links to placement — **one timesheet per placement** |
| `event_id` | FK int | Denormalized FK to event (also reachable via shift_employee) |
| `employee_id` | FK int | Denormalized FK to employee |
| `employee_start` | datetime | Employee's clock-in time |
| `employee_end` | datetime | Employee's clock-out time |
| `employee_break_start` | datetime | Employee's first meal break start |
| `employee_break_end` | datetime | Employee's first meal break end |
| `employee_sec_break_start` | datetime | Employee's second meal break start |
| `employee_sec_break_end` | datetime | Employee's second meal break end |
| `client_start` | datetime | Client's recorded clock-in time |
| `client_end` | datetime | Client's recorded clock-out time |
| `client_break_start/end` | datetime | Client's first meal break times |
| `client_sec_break_start/end` | datetime | Client's second meal break times |
| `employee_seconds` | int | **Computed**: total worked seconds from employee's clock entries |
| `client_seconds` | int | **Computed**: total worked seconds from client's clock entries |
| `reg_seconds` | int | Computed regular-time seconds (set by overtime analyzer) |
| `ot_seconds` | int | Computed overtime seconds |
| `dt_seconds` | int | Computed double-time seconds |
| `use_sheet` | enum | `CLIENT`, `EMPLOYEE`, or NULL — whose times are authoritative (see §3.2) |
| `employee_worked` | enum | `WORKED`, `SENTHOME`, `CANCELLED`, `NOSHOW` (see §3.1) |
| `client_worked` | enum | Same values as employee_worked |
| `employee_min_pay` | bool int | 1 = guaranteed minimum pay applies (CA/WA/NY only) |
| `client_min_bill` | bool int | 1 = client owes minimum billing (CA/WA/NY only) |
| `employee_no_pay` | bool int | 1 = employee receives $0 (e.g., NOSHOW outside deadline) |
| `client_no_bill` | bool int | 1 = client billed $0 |
| `employee_tips` | decimal | Tips paid to employee |
| `client_tips` | decimal | Tips billed to client |
| `employee_parking` | decimal | Parking reimbursement to employee |
| `client_parking` | decimal | Parking billed to client |
| `employee_travel` | decimal | Travel pay to employee (from event.travel_pay) |
| `client_travel` | decimal | Travel charge to client (from event.travel_charge) |
| `employee_service_charge` | decimal | Employee service charge percentage |
| `client_service_charge` | decimal | Client service charge percentage |
| `employee_no_break_penalty` | decimal | Hours of meal break penalty pay (1 or 2 hours) |
| `client_no_break_penalty` | decimal | Hours of meal break penalty billed |
| `employee_rating` | int | 1-5 star rating given BY client TO employee |
| `client_rating` | int | 1-5 star rating given BY employee TO client |
| `employee_submit_date` | datetime | When employee submitted their timesheet |
| `client_submit_date` | datetime | When client submitted their timesheet |
| `start_verified` | enum | Verification status (see §3.14) |
| `end_verified` | enum | Verification status |
| `start_verified_at` | datetime | GPS/QR clock-in verification timestamp |
| `end_verified_at` | datetime | GPS/QR clock-out verification timestamp |
| `employee_had_meal` | bool int | 1 if employee took first meal break |
| `client_had_meal` | bool int | 1 if client confirms meal break taken |
| `employee_had_sec_meal` | bool int | 1 if second meal break taken |
| `status` | enum | `NEW`, `IN_PROGRESS`, `COMPLETED` |
| `less_hours_reason` | int | 1 = client sent home early, 2 = employee left early |
| `employee_travel_distance` | int | Travel distance in miles (for mileage reimbursement) |
| `client_po` | string | Purchase order number for invoicing |
| `employee_adjustment` | string | Manual pay adjustment amount |
| `client_adjustment` | string | Manual billing adjustment amount |

### 4.2 `shift_employee` — The Placement Record

| Field | Type | Description |
|---|---|---|
| `shift_employee_id` | PK int | Primary key |
| `shift_position_id` | FK int | Which position slot this employee fills |
| `event_id` | FK int | Denormalized FK to event |
| `employee_id` | FK int | Which employee is assigned |
| `rate` | decimal | **Employee PAY rate ($/hr)** — ALWAYS use this for payroll, NOT shift_position.rate |
| `bill_rate` | decimal | **Client BILL rate ($/hr)** for this specific employee |
| `emergency_rate` | bool int | 1 if emergency/surcharge rate applies |
| `emergency_rate_amount` | decimal | Extra surcharge dollar amount |
| `confirmed` | bool int | 1 = confirmed for shift; 0 = pending |
| `confirmed_at` | datetime | When confirmation occurred |
| `confirm_type` | int | How confirmed (see §3.12) |
| `confirmed_by` | FK int | User who confirmed (FK to user.id) |
| `cancel_reason` | int | Cancellation code (see §3.4); **0 = active** |
| `cancelled_at` | datetime | When cancellation occurred |
| `cancelled_in_deadline` | bool int | 1 if cancellation was within client's cancellation window |
| `employee_remove_date` | datetime | When manually removed |
| `remove_by` | FK int | User who removed |
| `hiatus` | bool int | 1 if employee was on hiatus when placed |
| `hiatus_reason` | FK int | FK to status_reason.id |
| `shift_type` | int | 1=double shift, 2=7-day shift, 3=>40hrs/week |
| `overtime` | JSON | `{overtimes: {CLIENT:{...}, EMPLOYEE:{...}}, paidBy: 'CLIENT'\|'AGENCY'}` |
| `overtime_paid_by` | enum | `CLIENT` or `AGENCY` |
| `daily_report_notes` | text | Admin notes on daily report |
| `note_to_employee` | text | Message sent to employee about this placement |
| `request_by` | FK int | User who requested the placement |
| `approved_by` | FK int | User who approved |
| `deleted_at` | datetime | Soft delete timestamp |

> **CRITICAL**: `shift_employee.rate` and `shift_employee.bill_rate` are the AUTHORITATIVE rates for payroll and billing. `shift_position.rate` is the DEFAULT template rate. When a shift_position's rate changes, all active shift_employees on it are updated, but individual employee rates can differ.

### 4.3 `shift_position` — Position Slots on a Shift

| Field | Type | Description |
|---|---|---|
| `shift_position_id` | PK int | Primary key |
| `shift_id` | FK int | Which shift this position belongs to |
| `position_id` | FK int | FK to `position` table (e.g., "Line Cook") |
| `count` | int | **Number of employees needed** for this position |
| `filled` | int | Computed: currently confirmed count |
| `was_filled` | int | Counter: how many times this position has been fully filled |
| `rate` | decimal | Default pay rate (template — actual rate is on shift_employee) |
| `bill_rate` | decimal | Default bill rate (template) |
| `base_rate` | decimal | Original rate before any modifications |
| `base_bill_rate` | decimal | Original bill rate before modifications |
| `bonus` | decimal | Per-shift bonus amount |
| `holiday_rate` | bool int | 1 if holiday rate applies |
| `surcharge` | bool int | 1 if emergency surcharge applies |
| `surcharge_value` | decimal | Surcharge dollar amount |
| `miles_apply` | bool int | 1 if travel mileage reimbursement applies |
| `was_published` | int | Publication status (see §3.17) |
| `backup` | bool int | 1 if this is a standby/backup position |
| `gender` | string | Gender preference for position |
| `additional_title` | string | Additional description appended to position name |
| `code` | string | Position code identifier |
| `deleted_at` | datetime | Soft delete |

### 4.4 `shift` — Time Blocks

| Field | Type | Description |
|---|---|---|
| `shift_id` | PK int | Primary key |
| `event_id` | FK int | Which event this shift belongs to |
| `start` | datetime | Scheduled shift start time |
| `end` | datetime | Scheduled shift end time |
| `old_start` | datetime | **Previous start time if rescheduled** — used for min-pay calculations |
| `old_end` | datetime | **Previous end time if rescheduled** — used for min-pay calculations |
| `purchase_order` | string | PO number (inherits from event if not set) |
| `deleted_at` | datetime | Soft delete |

> **CRITICAL**: When a shift is rescheduled, `old_start`/`old_end` store the ORIGINAL times. Minimum pay is always calculated against the original scheduled duration, not the new times.

### 4.5 `event` — A Booking for a Date + Venue

| Field | Type | Description |
|---|---|---|
| `event_id` | PK int | Primary key |
| `client_id` | FK int | Which client owns this event |
| `venue_id` | FK int | Which venue location |
| `date` | DATE | The calendar date (DATE type, not datetime) |
| `state` | string | **US state where event occurs — governs ALL labor law calculations** |
| `title` | string | Event title (NULL/empty → fall back to venue.name) |
| `travel_charge` | decimal | Per-employee travel surcharge billed to client |
| `travel_pay` | decimal | Per-employee travel pay to employee |
| `no_break_penalty` | int | Inherited from client.no_break_penalty at event creation |
| `timeclock` | enum | Clock method (see §3.13) |
| `timeclock_tolerance` | int | Minutes of grace for verified clock-ins |
| `timeclock_prestart_interval` | int | Minutes before shift employees can clock in |
| `purchase_order` | string | Event-level PO number |
| `stat_shifts_positions_count` | int | Cached: total position slots needed |
| `stat_shifts_count` | int | Cached: number of shift_positions |
| `stat_shifts_filled_count` | int | Cached: fully filled positions |
| `stat_employees_count` | int | Cached: total active placements |
| `stat_employees_confirmed_count` | int | Cached: confirmed placements |
| `stat_filled` | bool | All positions filled |
| `stat_filled_confirmed` | bool | All positions filled AND confirmed |
| `deleted_at` | datetime | Soft delete |

> **CRITICAL**: `event.state` is the authoritative field for all labor law decisions (meal breaks, minimum pay, overtime rules). It overrides `venue.state`.

### 4.6 `client` — Customer Companies

| Field | Type | Description |
|---|---|---|
| `client_id` | PK int | Primary key |
| `name` | string | Display name |
| `invoice_name` | string | Name on invoices (falls back to `name` if empty) |
| `status` | int | See §3.6 |
| `industry` | enum | See §3.15 |
| `staff_id` | FK int | Assigned account manager (FK to user.id) |
| `payment_type` | int | 1=Credit Card, 2=Invoice |
| `cancellation_deadline` | int | Hours before shift start — billing minimum applies if cancelled within |
| `cancellation_deadline_pay` | int | Hours before shift start — pay minimum applies if cancelled within |
| `surcharge_deadline` | int | Hours before shift — emergency surcharge applies |
| `discount` | decimal | Discount percentage (0-100) |
| `discount_vaild_date` | date | **Note: intentional typo in column name** — discount expiration date |
| `invoices_offset` | int | Shift invoice date by N days (positive=future, negative=past) |
| `markup` | JSON | Billing markup rates |
| `min_wage_id` | FK int | FK to min_wage_rate |
| `wc_id` | FK int | FK to wc_code (workers comp) |
| `msp_id` | FK int | FK to msp (Managed Service Provider) |
| `division_id` | FK int | FK to division |
| `separate_venue` | int | Invoice separation mode (see §3.7) |
| `no_break_penalty` | int | See §3.9 — inherited by new events |
| `auto_confirm` | bool int | 1 = employees auto-confirmed when assigned |
| `background` | int | 1=None required, 2=Specified only, 3=All accepted |
| `deleted_at` | datetime | Soft delete |

### 4.7 `employee` — Workers

| Field | Type | Description |
|---|---|---|
| `employee_id` | PK int | Primary key |
| `first_name` | string | Always use `CONCAT(first_name, ' ', last_name)` for display |
| `last_name` | string | |
| `email` | string | Login/contact email |
| `phone` | string | Primary phone |
| `status` | int | See §3.5 |
| `flag` | int | Color tag (see §3.10) |
| `transportation` | int | 1=Car, 2=Motorcycle, 3=Public Transit, 4=Other |
| `background` | int | 1=Clean, 2=Specified (needs review) |
| `background_query_pending` | bool int | 1 if background check in progress |
| `start_date2` | date | Rehire/restart date |
| `deleted_at` | datetime | Soft delete |

> **Privacy blacklist**: `sex`, `dob`, `ssn` columns exist but are EXCLUDED from AI analytics queries.

### 4.8 `venue` — Physical Locations

| Field | Type | Description |
|---|---|---|
| `venue_id` | PK int | Primary key |
| `client_id` | FK int | Parent client |
| `name` | string | Venue display name |
| `invoice_name` | string | Name on invoices (falls back to `name`) |
| `staffing_manager_id` | FK int | FK to user.id — GoLive employee managing this venue |
| `travel_charge` | decimal | Default travel charge (overridden by event.travel_charge if set) |
| `travel_pay` | decimal | Default travel pay to employee |
| `service_charge` | decimal | Flat service charge amount |
| `state` | string | Physical state (fallback when event.state is null) |
| `status` | int | 0=Inactive, 1=Active |
| `min_wage_id` | FK int | Venue-level min wage (falls back to client if null) |
| `deleted_at` | datetime | Soft delete |

### 4.9 Supporting Tables

| Table | Purpose | Key Fields |
|---|---|---|
| `position` | Job title catalog | `position_id`, `description`, `disable_timeclock_code` |
| `certification` | Required cert types | `certification_id`, `name`, `expiry_duration`, `state` |
| `employee_certification` | Employee's held certs | `employee_id`, `certification_id`, `expiry_date`, `deleted_at` |
| `venue_certification` | Venue-required certs | `venue_id`, `certification_id` |
| `dnr` | Do Not Return list | `employee_id`, `client_id`, `venue_id` (NULL=client-wide), `reason`, `notes` |
| `exclusive` | Preferred employee list | `employee_id`, `client_id`, `venue_id` (NULL=client-wide), `type` (1=preferred, 2=priority) |
| `msp` | Managed Service Provider | `id`, `name`, `rate` (percentage of billing) |
| `wc_code` | Workers comp codes | `wc_id`, `rate` (percentage) |
| `min_wage_rate` | Minimum wage tiers | `min_wage_id`, linked to `min_wage_rate_amount` for actual values |
| `division` | Client divisions | `id`, `name` |
| `status_reason` | Coded reason lookup | `id`, `reason` — used for hiatus reasons etc. |
| `additional_shift_pay` | Extra per-shift pay rules | `rate`, `start_date`, `end_date` — date-range-based extra pay |
| `employee_other_work` | Non-shift work costs | `date`, `rate`, `cost`, `work_hours`, `non_work_hours` |
| `blocked_email` | Suppressed emails | `email` — emails that won't receive system notifications |
| `user` | System users (admins) | `id`, `username`, `email`, `first_name`, `last_name`, `group` |

---

## 5. Scheduled vs. Actual Hours

### 5.1 Scheduled Shift Duration

```
scheduled_hours = TIMESTAMPDIFF(SECOND, shift.start, shift.end) / 3600.0
```

If the shift was rescheduled (old_start/old_end exist), use the ORIGINAL times for minimum pay calculations:

```
original_scheduled_hours = TIMESTAMPDIFF(SECOND,
    COALESCE(shift.old_start, shift.start),
    COALESCE(shift.old_end, shift.end)
) / 3600.0
```

### 5.2 Actual Worked Hours (from timesheet)

The `use_sheet` field determines which clock entries are authoritative:

| use_sheet | Billing hours source | Pay hours source |
|---|---|---|
| `CLIENT` | `client_seconds / 3600` | `client_seconds / 3600` |
| `EMPLOYEE` | `employee_seconds / 3600` | `employee_seconds / 3600` |
| NULL (empty) | `client_seconds / 3600` | `employee_seconds / 3600` |

When `use_sheet` is NULL, the system uses BOTH sheets independently — client times for billing, employee times for pay.

### 5.3 Seconds Calculation

`employee_seconds` and `client_seconds` are automatically computed before every save by the `BaseTimesheetAnalyzer`. They represent total worked seconds = (end - start) minus any break durations. All time entries are **rounded to the nearest minute** (≥30 seconds rounds up).

---

## 6. Meal Break Policy (State-Based)

Meal break deductions are applied to the **scheduled shift duration** to calculate "work hours" — the basis for minimum pay.

### California (CA) and Washington (WA):
- Shift > 5 hours → deduct **0.5 hours** (first 30-minute break)
- Shift > 10 hours → deduct an additional **0.5 hours** (second break) = 1.0 total

### All Other States (NY, NV, etc.):
- **No automatic meal break deduction** from scheduled hours

### SQL Formula (for CA/WA):
```sql
CASE
  WHEN event.state IN ('CA', 'WA') THEN
    scheduled_hours
    - CASE
        WHEN scheduled_hours > 10 THEN 1.0
        WHEN scheduled_hours > 5  THEN 0.5
        ELSE 0.0
      END
  ELSE scheduled_hours
END AS work_hours
```

### Meal Break Penalty
If `event.no_break_penalty = 1` (inherited from client) and an employee did NOT take their required meal break, a penalty is charged:
- `timesheet.employee_no_break_penalty` = 1 or 2 (hours of penalty pay at the employee's rate)
- `timesheet.client_no_break_penalty` = same, billed at the client's rate

---

## 7. Minimum Pay & Minimum Billing Rules

Minimum pay/billing is the most complex business logic in the system. It governs what happens when employees don't work a full shift.

### 7.1 When Minimum Pay Applies

Minimum pay ONLY applies in these US states: **California (CA), Washington (WA), New York (NY)**

The system checks `event.state` (not venue.state) to determine applicability.

When triggered, `timesheet.employee_min_pay = 1` and/or `timesheet.client_min_bill = 1` are set.

### 7.2 SENTHOME — Employee Sent Home Early

**Trigger**: Employee shows up, starts working, but the client sends them home early (before 50% of scheduled hours).

**Auto-detection rule**: If `employee_worked = 'WORKED'` AND actual worked seconds < `shift.getMinBillingHours() × 3600` AND `less_hours_reason = 1` → system automatically changes status to `SENTHOME` and sets `employee_min_pay = 1`.

**Minimum pay formula** (the `getMinBillingHours()` method):

```
Step 1: Get original scheduled hours (using old_start/old_end if rescheduled)
Step 2: Calculate work_hours by deducting meal breaks (CA/WA only)
Step 3: min_pay_hours = MIN(work_hours × 0.5, 4.0)
```

**Example**: 8-hour shift in CA → work_hours = 8.0 - 0.5 = 7.5 → min_pay = MIN(7.5 × 0.5, 4.0) = MIN(3.75, 4.0) = **3.75 hours**

**Example**: 10-hour shift in CA → work_hours = 10.0 - 1.0 = 9.0 → min_pay = MIN(9.0 × 0.5, 4.0) = MIN(4.5, 4.0) = **4.0 hours** (capped)

**Example**: 6-hour shift in NY → work_hours = 6.0 (no meal deduction) → min_pay = MIN(6.0 × 0.5, 4.0) = MIN(3.0, 4.0) = **3.0 hours**

**Pay calculation for SENTHOME**:
```
If actual_worked_hours >= min_pay_hours:
    pay = actual_worked_hours × rate  (employee worked more than minimum)
Else:
    pay = min_pay_hours × rate
    non_worked_pay = (min_pay_hours - actual_worked_hours) × rate
```

### 7.3 CANCELLED — Shift Cancelled Within Deadline

**Trigger**: Client cancels a shift (or decreases staff) within the `cancellation_deadline_pay` window.

**Cancellation deadline check**:
```
in_deadline = shift.start <= NOW() + client.cancellation_deadline_pay hours
```

If `in_deadline = true`:
- `timesheet.employee_worked = 'CANCELLED'`
- `timesheet.client_worked = 'CANCELLED'`
- `timesheet.employee_min_pay = 1` (in CA/WA/NY)
- `timesheet.client_min_bill = 1` (in CA/WA/NY)
- `shift_employee.cancel_reason = 4` (client cancelled) or `5` (decreased staff)

**Pay for CANCELLED shift**: Same `getMinBillingHours()` formula as SENTHOME, but actual worked hours are 0, so:
```
pay = min_pay_hours × rate
non_worked_pay = min_pay_hours × rate  (all hours are non-worked)
```

### 7.4 Client Billing Minimum

Client billing uses a **different floor** than employee pay:
- Shift ≥ 4 hours → billing minimum = **4.0 hours**
- Shift < 4 hours → billing minimum = **2.0 hours**

This only applies when `timesheet.client_min_bill = 1`.

### 7.5 NOSHOW — Employee Did Not Appear

- `timesheet.employee_worked = 'NOSHOW'`
- If within cancellation deadline: `timesheet.employee_no_pay = 1` (no pay)
- If outside deadline: employee may still owe nothing, but client may still be billed

### 7.6 Late Hours Adjustment

If an employee clocks in late (actual start > scheduled start), the late time reduces the minimum pay/bill floor:

```
late_hours = MAX(actual_start - scheduled_start, 0) / 3600
adjusted_min = MAX(min_pay_hours - late_hours, 2.0)
```

The floor never drops below 2.0 hours.

---

## 8. Billing & Pay Calculation Formulas

### 8.1 Rate Sources

| What | Source Field | Notes |
|---|---|---|
| Employee pay rate | `shift_employee.rate` | ALWAYS use this, never shift_position.rate |
| Client bill rate | `shift_employee.bill_rate` | ALWAYS use this |
| OT pay rate | `shift_employee.rate × 1.5` | Standard multiplier |
| DT pay rate | `shift_employee.rate × 2.0` | Standard multiplier |
| OT bill rate | `shift_employee.bill_rate × 1.5` | Standard multiplier |
| DT bill rate | `shift_employee.bill_rate × 2.0` | Standard multiplier |

### 8.2 Overtime / Double-Time Thresholds

**California (CA)** — daily overtime:
- Hours > 12 → double-time on excess (DT = hours - 12, OT = 4, REG = 8)
- Hours > 8 → overtime on excess (OT = hours - 8, REG = 8)
- Hours ≤ 8 → all regular

**Nevada (NV)** — daily overtime:
- Hours > 8 → overtime on excess (OT = hours - 8, REG = 8)
- Hours ≤ 8 → all regular
- No double-time

**All other states**: All hours are regular (no daily OT/DT thresholds).

> **Note**: Weekly overtime (>40 hours/week) exists but is calculated separately by the `OvertimeHandler` and stored in `shift_employee.overtime` as a JSON blob. The payroll export reports use per-day thresholds.

### 8.3 Total Bill Formula

```
total_bill = reg_bill + ot_bill + dt_bill + non_worked_bill
           + service_charge + meal_break_penalty
           + tips + parking + travel

Where:
  reg_bill       = regular_hours × bill_rate
  ot_bill        = ot_hours × (bill_rate × 1.5)
  dt_bill        = dt_hours × (bill_rate × 2.0)
  non_worked_bill = non_worked_hours × bill_rate
  service_charge = (reg_bill + ot_bill + dt_bill) × (client_service_charge / 100) + venue.service_charge
  meal_penalty   = bill_rate × client_no_break_penalty (if > 0)
  tips           = timesheet.client_tips
  parking        = timesheet.client_parking
  travel         = timesheet.client_travel
```

### 8.4 Total Pay Formula

```
total_pay = reg_pay + ot_pay + dt_pay + non_worked_pay
          + service_charge + meal_break_penalty
          + tips + parking + travel
          + additional_shift_pay + bonus

Where:
  reg_pay        = regular_hours × pay_rate
  ot_pay         = ot_hours × (pay_rate × 1.5)
  dt_pay         = dt_hours × (pay_rate × 2.0)
  non_worked_pay = non_worked_hours × pay_rate
  service_charge = (reg_pay + ot_pay + dt_pay) × (employee_service_charge / 100)
  meal_penalty   = pay_rate × employee_no_break_penalty (if > 0)
  tips           = timesheet.employee_tips
  parking        = timesheet.employee_parking
  travel         = timesheet.employee_travel
  additional_shift_pay = from additional_shift_pay table (date-range matched)
  bonus          = shift_position.bonus
```

### 8.5 Extras Only Apply When Worked

Bonus, additional_shift_pay, service charges, tips, parking, and travel are ONLY added when BOTH `client_worked` AND `employee_worked` are `WORKED` or `SENTHOME`. If either is `CANCELLED` or `NOSHOW`, these extras are zeroed out.

### 8.6 No-Pay and No-Bill Overrides

If `timesheet.employee_no_pay = 1`: ALL pay components = $0
If `timesheet.client_no_bill = 1`: ALL billing components = $0

### 8.7 Profit Calculation

```
profit = total_bill - gross_pay - msp_fee - wc_fee - payroll_tax - other_work

Where:
  msp_fee      = total_bill × msp.rate (from client → msp table)
  wc_fee       = total_bill × wc_code.rate (from client → wc_code table)
  payroll_tax  = gross_pay × 0.10 (10% flat estimate)
  other_work   = SUM from employee_other_work table for the period
```

---

## 9. Cancellation Deadline Logic

Each client has two separate deadline windows:

| Field | Purpose |
|---|---|
| `client.cancellation_deadline` | Hours before shift start — if cancelled within, **client is billed minimum** |
| `client.cancellation_deadline_pay` | Hours before shift start — if cancelled within, **employee gets minimum pay** |

**Example**: Client has `cancellation_deadline = 24` and `cancellation_deadline_pay = 48`.
- Shift starts at 8:00 AM Monday
- If cancelled Sunday 8:00 AM (24h before) → client billed minimum AND employee paid minimum
- If cancelled Saturday 8:00 AM (48h before) → employee paid minimum, but client NOT billed minimum
- If cancelled Friday → neither minimum applies

### Surcharge Deadline
`client.surcharge_deadline` — hours before shift start when emergency surcharge applies. If an employee is booked within this window, `shift_employee.emergency_rate = 1` and `emergency_rate_amount` is set.

---

## 10. Shift Lifecycle — Complete State Machine

### 10.1 Normal Flow (Employee Works Full Shift)

```
1. Event created → date + venue + client
2. Shift created → start/end times
3. ShiftPosition created → position type + count + rates
4. ShiftEmployee created → employee assigned, rate/bill_rate copied from position
5. Employee confirmed → shift_employee.confirmed = 1
6. Timesheet auto-created → status = 'NEW'
7. Employee clocks in → employee_start set, start_verified_at set
8. Employee clocks out → employee_end set, end_verified_at set
9. Client enters times (if applicable) → client_start/end set
10. employee_seconds/client_seconds computed
11. employee_worked = 'WORKED', client_worked = 'WORKED'
12. Payroll processed using actual hours × rates
```

### 10.2 Sent Home Early (SENTHOME)

```
1-6. Same as normal flow
7. Employee clocks in and starts working
8. Client sends employee home after partial shift
9. Employee clocks out (early)
10. System detects: worked < 50% of getMinBillingHours() AND less_hours_reason = 1
11. Auto-sets: employee_worked = 'SENTHOME'
12. In CA/WA/NY: employee_min_pay = 1
13. Pay = MAX(actual_hours, getMinBillingHours()) × rate
14. Non-worked hours = getMinBillingHours() - actual_hours (if positive)
```

### 10.3 Shift Cancelled by Client (CANCELLED)

```
1-6. Same as normal flow
7. Client cancels the shift
8. System checks: is shift.start within cancellation_deadline_pay?
9. If YES (within deadline):
   - shift_employee.cancel_reason = 4 (client cancelled)
   - timesheet.employee_worked = 'CANCELLED'
   - timesheet.client_worked = 'CANCELLED'
   - In CA/WA/NY: employee_min_pay = 1, client_min_bill = 1
   - Pay = getMinBillingHours() × rate (all non-worked)
10. If NO (outside deadline):
    - shift_employee.cancel_reason = 10 (other)
    - No minimum pay/bill
```

### 10.4 Employee No-Show (NOSHOW)

```
1-6. Same as normal flow
7. Employee does not appear
8. Admin or client marks: employee_worked = 'NOSHOW'
9. shift_employee.cancel_reason = 14
10. employee_no_pay = 1 (typically)
11. Pay = $0
```

### 10.5 Shift Rescheduled

```
1-6. Same as normal flow
7. Admin changes shift times
8. old_start = original start, old_end = original end
9. new start/end saved to shift.start/shift.end
10. All confirmed employees put back to "request" (confirmed = 0)
11. If within cancellation deadline:
    - timesheet.employee_worked = 'CANCELLED'
    - timesheet.client_worked = 'CANCELLED'
    - employee_min_pay = 1, client_min_bill = 1
    - Minimum calculated using OLD times (old_start/old_end)
12. Employees must re-confirm for the new times
```

---

## 11. Common SQL Query Patterns

### Active confirmed employees on future events
```sql
SELECT emp.employee_id, CONCAT(emp.first_name, ' ', emp.last_name) AS name
FROM shift_employee se
JOIN shift_position sp ON sp.shift_position_id = se.shift_position_id
JOIN shift s ON s.shift_id = sp.shift_id
JOIN event e ON e.event_id = s.event_id
JOIN employee emp ON emp.employee_id = se.employee_id
WHERE se.cancel_reason = 0
  AND se.deleted_at IS NULL
  AND se.confirmed = 1
  AND s.deleted_at IS NULL
  AND e.deleted_at IS NULL
  AND e.date >= CURDATE()
```

### Full payroll query with hours
```sql
SELECT
  e.date, c.name AS client_name, v.name AS venue_name,
  CONCAT(emp.first_name, ' ', emp.last_name) AS employee_name,
  se.rate AS pay_rate, se.bill_rate,
  t.employee_worked, t.use_sheet,
  t.employee_seconds / 3600.0 AS employee_hours,
  t.client_seconds / 3600.0 AS client_hours,
  CASE
    WHEN t.use_sheet = 'EMPLOYEE' THEN t.employee_seconds / 3600.0
    WHEN t.use_sheet = 'CLIENT' THEN t.client_seconds / 3600.0
    ELSE t.client_seconds / 3600.0
  END AS billing_hours
FROM timesheet t
JOIN shift_employee se ON se.shift_employee_id = t.shift_employee_id
JOIN event e ON e.event_id = t.event_id
JOIN employee emp ON emp.employee_id = t.employee_id
JOIN venue v ON v.venue_id = e.venue_id
JOIN client c ON c.client_id = e.client_id
WHERE e.deleted_at IS NULL
  AND e.date BETWEEN '2026-01-01' AND '2026-03-31'
```

### Fill rate per client
```sql
SELECT c.name,
  SUM(sp.count) AS positions_needed,
  COUNT(CASE WHEN se.confirmed = 1 AND se.cancel_reason = 0 THEN 1 END) AS filled,
  ROUND(COUNT(CASE WHEN se.confirmed = 1 AND se.cancel_reason = 0 THEN 1 END)
        / NULLIF(SUM(sp.count), 0) * 100, 1) AS fill_pct
FROM client c
JOIN event e ON c.client_id = e.client_id
JOIN shift s ON e.event_id = s.event_id
JOIN shift_position sp ON s.shift_id = sp.shift_id
LEFT JOIN shift_employee se ON sp.shift_position_id = se.shift_position_id AND se.deleted_at IS NULL
WHERE c.deleted_at IS NULL AND e.deleted_at IS NULL AND s.deleted_at IS NULL AND sp.deleted_at IS NULL
GROUP BY c.client_id
```

### Minimum pay timesheets
```sql
SELECT * FROM timesheet t
JOIN event e ON t.event_id = e.event_id
WHERE t.employee_min_pay = 1
  AND t.employee_worked IN ('SENTHOME', 'CANCELLED')
  AND e.state IN ('CA', 'WA', 'NY')
```

---

## 12. Data Integrity Notes

1. **Time rounding**: All clock entries are rounded to nearest minute (≥30s rounds up). If start == end after rounding, both are set to NULL.
2. **Timesheet auto-creation**: A timesheet is created when `shift_employee.confirmed` changes to 1. Travel pay/charge values are copied from the event (or venue as fallback).
3. **Rate cascading**: When `shift_position.rate` or `bill_rate` changes, all active (not cancelled, not deleted) `shift_employee` records on that position are updated to match.
4. **Event date changes**: If `event.date` is changed after timesheets have time entries, the system throws an error. Dates cannot be changed on events with filled timesheets.
5. **Client reactivation**: Creating a future event for an inactive client (status 10/11/12) auto-reactivates the client to status 1.
6. **Cancel reason history (41/51)**: When rebooking an employee on an event where they had a client-caused cancellation, the old record's cancel_reason changes from 4→41 or 5→51.
7. **Column typo**: `client.discount_vaild_date` is intentionally misspelled in the database. Do not "fix" this.

---

*End of GoLive Database Reference*
