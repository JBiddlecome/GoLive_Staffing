# Database Schema for Codex

The following is the DDL for my MariaDB instance 'practice-db-small'.
Please use these table and column names for the Python export tool.

'''sql
-- cstaffing.auth_rule definition

CREATE TABLE `auth_rule` (
  `name` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `data` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `created_at` int(11) DEFAULT NULL,
  `updated_at` int(11) DEFAULT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing.calendar definition

CREATE TABLE `calendar` (
  `calendar_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `start` datetime DEFAULT NULL,
  `end` datetime DEFAULT NULL,
  PRIMARY KEY (`calendar_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client definition

CREATE TABLE `client` (
  `client_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `staff_id` bigint(20) DEFAULT NULL COMMENT 'This is the main client information.  Each client can be assigned to a staff member.\n',
  `other_id` varchar(45) DEFAULT NULL,
  `name` varchar(45) NOT NULL,
  `address1` varchar(100) NOT NULL,
  `address2` varchar(100) DEFAULT NULL,
  `address3` varchar(100) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `state` varchar(45) DEFAULT NULL,
  `zip` varchar(45) DEFAULT NULL,
  `phone` varchar(24) DEFAULT NULL,
  `fax` varchar(24) DEFAULT NULL,
  `contact` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `background_check` varchar(45) DEFAULT NULL,
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `status` tinyint(4) DEFAULT 1,
  `min_wage_id` int(11) DEFAULT NULL,
  `min_billing` int(4) DEFAULT 0,
  `workers_comp` varchar(50) DEFAULT NULL,
  `payment_type` tinyint(4) DEFAULT NULL,
  `cc_expiry_month` varchar(2) DEFAULT NULL,
  `cc_expiry_year` varchar(4) DEFAULT NULL,
  `net_terms` tinyint(4) DEFAULT 10,
  `pay_notes` text DEFAULT NULL,
  `wc_id` int(100) DEFAULT NULL,
  `invoiced` tinyint(2) DEFAULT 0,
  `proceed` tinyint(2) DEFAULT 0,
  `clean_background` tinyint(4) DEFAULT NULL,
  `felony` varchar(50) DEFAULT NULL,
  `misdemeanor` varchar(50) DEFAULT NULL,
  `visible_tattoos_allow` int(10) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`client_id`),
  KEY `min_wage_id` (`min_wage_id`)
) ENGINE=InnoDB AUTO_INCREMENT=279 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client_document definition

CREATE TABLE `client_document` (
  `client_doc_id` bigint(50) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(50) NOT NULL,
  `user_id` bigint(50) NOT NULL,
  `filename` varchar(150) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`client_doc_id`),
  KEY `client_id` (`client_id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client_grooming definition

CREATE TABLE `client_grooming` (
  `client_groomin_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(50) DEFAULT NULL,
  `position_id` bigint(50) DEFAULT NULL,
  `grooming_tools` text DEFAULT NULL,
  PRIMARY KEY (`client_groomin_id`),
  KEY `client_id` (`client_id`,`position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client_position definition

CREATE TABLE `client_position` (
  `client_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) DEFAULT NULL,
  `group_id` int(20) DEFAULT NULL,
  `position_id` bigint(20) DEFAULT NULL,
  `standard_rate` tinyint(2) NOT NULL DEFAULT 0,
  `tip_rate` decimal(5,2) DEFAULT NULL,
  `pay_rate` decimal(5,2) DEFAULT NULL,
  `bill_rate` decimal(5,2) DEFAULT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `uniform_types` varchar(255) DEFAULT NULL,
  `position_requirement` text DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`client_position_id`),
  KEY `client_id` (`client_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client_tools definition

CREATE TABLE `client_tools` (
  `client_tools_id` int(255) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(50) DEFAULT NULL,
  `position_id` bigint(50) DEFAULT NULL,
  `tools` text DEFAULT NULL,
  PRIMARY KEY (`client_tools_id`),
  KEY `client_id` (`client_id`),
  KEY `position_id` (`position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client_uniform definition

CREATE TABLE `client_uniform` (
  `client_uniform_id` int(255) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(50) DEFAULT NULL,
  `position_id` bigint(50) DEFAULT NULL,
  `uniform` text DEFAULT NULL,
  PRIMARY KEY (`client_uniform_id`),
  KEY `client_id` (`client_id`),
  KEY `position_id` (`position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=62 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.company_info definition

CREATE TABLE `company_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `office_hours` text DEFAULT NULL,
  `user_guide_app` text DEFAULT NULL,
  `scheduling_event` text DEFAULT NULL,
  `call_out` text DEFAULT NULL,
  `attandance` text DEFAULT NULL,
  `backup_shift` text DEFAULT NULL,
  `meal_period` text DEFAULT NULL,
  `emergency_phone` text DEFAULT NULL,
  `uniform` text DEFAULT NULL,
  `reporting_hours` text DEFAULT NULL,
  `payroll` text DEFAULT NULL,
  `permanent_placement` text DEFAULT NULL,
  `food_handler` text DEFAULT NULL,
  `at_will_employment` text DEFAULT NULL,
  `final_note` text DEFAULT NULL,
  `pay_day` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci ROW_FORMAT=DYNAMIC;


-- cstaffing.employee definition

CREATE TABLE `employee` (
  `employee_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `dob` date DEFAULT NULL,
  `email` varchar(45) NOT NULL,
  `max_hours` float DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `payroll_id` varchar(32) DEFAULT NULL,
  `address1` varchar(100) NOT NULL,
  `address2` varchar(100) DEFAULT NULL,
  `city` varchar(100) NOT NULL,
  `state` varchar(100) NOT NULL,
  `zip` varchar(25) NOT NULL,
  `language` varchar(100) DEFAULT NULL,
  `mobile` varchar(25) DEFAULT NULL,
  `home` varchar(25) DEFAULT NULL,
  `work` varchar(25) DEFAULT NULL,
  `status` smallint(6) DEFAULT NULL,
  `hr_status` varchar(20) DEFAULT NULL,
  `sex` varchar(15) DEFAULT NULL,
  `std_pay_rate` decimal(5,2) DEFAULT NULL,
  `photo` varchar(100) DEFAULT NULL,
  `send_emails` tinyint(4) NOT NULL DEFAULT 1,
  `randy_notes` text DEFAULT NULL,
  `flag` tinyint(2) DEFAULT NULL,
  `flag_note` text DEFAULT NULL,
  `experience` text DEFAULT NULL,
  `appearance` varchar(255) DEFAULT NULL,
  `wotc` varchar(10) DEFAULT NULL,
  `interview_notes` varchar(255) DEFAULT NULL,
  `fhc` varchar(150) DEFAULT NULL,
  `fhc_expiry` date DEFAULT NULL,
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `clean_background` tinyint(4) DEFAULT NULL,
  `background_issues` varchar(50) DEFAULT '0',
  `misdemeanor` varchar(50) DEFAULT NULL,
  `restrict_to_exclusive` tinyint(4) NOT NULL DEFAULT 0,
  `e_verify` varchar(45) DEFAULT NULL,
  `i9_completed` tinyint(4) DEFAULT 0,
  `created_on` datetime DEFAULT NULL,
  `updated_on` datetime DEFAULT NULL,
  `w4` varchar(45) DEFAULT NULL,
  `ssn` varchar(20) DEFAULT NULL,
  `orientation_schedule_date` datetime DEFAULT NULL,
  `orientation_complete_date` date DEFAULT NULL,
  `skill_cook` varchar(2) DEFAULT NULL,
  `skill_server` varchar(2) DEFAULT NULL,
  `skill_bartender` varchar(2) DEFAULT NULL,
  `pp_mailing_list` tinyint(4) unsigned DEFAULT 0 COMMENT 'Permanent Paycheck Mailing List',
  `background_check` text DEFAULT NULL,
  `uniforms_complete` tinyint(4) DEFAULT NULL,
  `uniforms_missing` text DEFAULT NULL,
  `howheard` tinyint(4) DEFAULT NULL,
  `expected_rate` varchar(10) DEFAULT NULL,
  `smartphone` tinyint(4) DEFAULT NULL,
  `transportation` int(11) DEFAULT NULL,
  `trans_other` varchar(50) DEFAULT NULL,
  `referred_date` date DEFAULT NULL,
  `referred_by` bigint(20) DEFAULT NULL,
  `interview_date` date DEFAULT NULL,
  `interviewer` bigint(20) DEFAULT NULL,
  `apply_position` bigint(20) DEFAULT NULL,
  `application_file` varchar(150) DEFAULT NULL,
  `worked_before` tinyint(4) DEFAULT NULL,
  `work_eligibility` tinyint(4) DEFAULT NULL,
  `applied_before` tinyint(4) DEFAULT NULL,
  `applied_date` date DEFAULT NULL,
  `currently_employed` tinyint(4) DEFAULT NULL,
  `contact_employer` tinyint(4) DEFAULT NULL,
  `provide_docs` tinyint(4) DEFAULT NULL,
  `hs_name` varchar(100) DEFAULT NULL,
  `hs_study` varchar(100) DEFAULT NULL,
  `hs_graduated` tinyint(4) DEFAULT NULL,
  `hs_degree` varchar(100) DEFAULT NULL,
  `college_name` varchar(100) DEFAULT NULL,
  `college_study` varchar(100) DEFAULT NULL,
  `college_years` varchar(45) DEFAULT NULL,
  `college_graduated` tinyint(4) DEFAULT NULL,
  `college_degree` varchar(100) DEFAULT NULL,
  `other_name` varchar(100) DEFAULT NULL,
  `other_study` varchar(100) DEFAULT NULL,
  `other_years` varchar(45) DEFAULT NULL,
  `other_graduated` tinyint(4) DEFAULT NULL,
  `other_degree` varchar(100) DEFAULT NULL,
  `tips_certified` tinyint(4) DEFAULT NULL,
  `servsafe_certified` tinyint(4) DEFAULT NULL,
  `ca_fh_certificate` tinyint(4) DEFAULT NULL,
  `capable` tinyint(4) DEFAULT NULL,
  `accomodations` text DEFAULT NULL,
  `dob_month` varchar(2) DEFAULT NULL,
  `dob_day` varchar(2) DEFAULT NULL,
  `confidential` varchar(100) DEFAULT NULL,
  `garnishment` int(10) DEFAULT NULL,
  `visible_tattoos` int(10) NOT NULL DEFAULT 0,
  PRIMARY KEY (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15423 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_application definition

CREATE TABLE `employee_application` (
  `employee_application_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `interview_date` date DEFAULT NULL,
  `interviewer` bigint(20) DEFAULT NULL,
  `position_id` bigint(20) DEFAULT NULL,
  `referred_by` bigint(20) DEFAULT NULL,
  `referred_date` date DEFAULT NULL,
  `application_file` varchar(150) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`employee_application_id`),
  KEY `employee_id` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_available definition

CREATE TABLE `employee_available` (
  `employee_available_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `available` tinyint(2) NOT NULL DEFAULT 0,
  `date` date NOT NULL,
  PRIMARY KEY (`employee_available_id`),
  KEY `fk_employee_available_employee_idx` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=162 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_document definition

CREATE TABLE `employee_document` (
  `employee_document_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `filename` varchar(150) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`employee_document_id`),
  KEY `fk_document_employee_idx` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=180 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_government definition

CREATE TABLE `employee_government` (
  `employee_document_id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `filename` varchar(150) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `document_type` varchar(100) DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`employee_document_id`),
  KEY `employee_id` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_profile_updates definition

CREATE TABLE `employee_profile_updates` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `attribute_name` varchar(200) DEFAULT NULL,
  `old_value` varchar(200) DEFAULT NULL,
  `new_value` varchar(200) DEFAULT NULL,
  `updated_by` bigint(20) DEFAULT NULL,
  `updated_on` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `employee_id` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=16137 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_status_reason definition

CREATE TABLE `employee_status_reason` (
  `reason_id` int(100) NOT NULL AUTO_INCREMENT,
  `reason_type` int(10) DEFAULT NULL,
  `reason` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`reason_id`)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.event_edits definition

CREATE TABLE `event_edits` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(50) DEFAULT NULL,
  `new_data` blob DEFAULT NULL,
  `updated_by` bigint(50) DEFAULT NULL,
  `updated_on` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `event_id` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2698 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci COMMENT='This table will sotre event edit data.';


-- cstaffing.government_form definition

CREATE TABLE `government_form` (
  `form_id` int(100) NOT NULL AUTO_INCREMENT,
  `form_type` varchar(100) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`form_id`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.group_rate definition

CREATE TABLE `group_rate` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` int(20) DEFAULT NULL,
  `client_id` bigint(20) DEFAULT NULL,
  `bill_rate` decimal(5,2) DEFAULT NULL,
  `pay_rate` decimal(5,2) DEFAULT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `group_id` (`group_id`,`client_id`)
) ENGINE=InnoDB AUTO_INCREMENT=496 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.`language` definition

CREATE TABLE `language` (
  `language_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `language` varchar(20) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`language_id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.mail_queue definition

CREATE TABLE `mail_queue` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `subject` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `attempts` int(11) DEFAULT NULL,
  `last_attempt_time` datetime DEFAULT NULL,
  `sent_time` datetime DEFAULT NULL,
  `time_to_send` datetime NOT NULL,
  `swift_message` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `IX_time_to_send` (`time_to_send`),
  KEY `IX_sent_time` (`sent_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing.migration definition

CREATE TABLE `migration` (
  `version` varchar(180) NOT NULL,
  `apply_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.min_wage_rate definition

CREATE TABLE `min_wage_rate` (
  `min_wage_id` int(11) NOT NULL AUTO_INCREMENT,
  `description` varchar(100) NOT NULL,
  `rate` decimal(5,2) DEFAULT 10.00 COMMENT 'This table is responsible for holding the minimum wage rates and is assignable to each location/department.',
  `default` tinyint(4) DEFAULT 0,
  PRIMARY KEY (`min_wage_id`)
) ENGINE=InnoDB AUTO_INCREMENT=54 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.payroll_note_type definition

CREATE TABLE `payroll_note_type` (
  `payroll_note_type_id` int(150) NOT NULL AUTO_INCREMENT,
  `type` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`payroll_note_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.position_group definition

CREATE TABLE `position_group` (
  `group_id` int(20) NOT NULL AUTO_INCREMENT,
  `group_name` varchar(10) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=35 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.position_move definition

CREATE TABLE `position_move` (
  `position_move_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) DEFAULT NULL,
  `group_id` int(20) DEFAULT NULL,
  `position_id` bigint(20) DEFAULT NULL,
  `old_group_id` int(20) DEFAULT NULL,
  `delete_status` tinyint(2) NOT NULL DEFAULT 0,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`position_move_id`),
  KEY `client_id` (`client_id`,`group_id`,`position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.position_sub_type definition

CREATE TABLE `position_sub_type` (
  `sub_type_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) DEFAULT NULL,
  `title` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`sub_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.preference definition

CREATE TABLE `preference` (
  `preferences_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_submit_email` varchar(100) DEFAULT NULL,
  `employee_change_email` varchar(100) DEFAULT NULL,
  `employee_status_email` varchar(25) DEFAULT NULL,
  `client_notification_email` varchar(50) DEFAULT NULL,
  `payroll_filter_date_range` varchar(50) DEFAULT NULL,
  `timesheet_notification_email` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`preferences_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.publish definition

CREATE TABLE `publish` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(50) DEFAULT NULL,
  `user_id` bigint(50) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `event_id` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1270 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.publish_employee definition

CREATE TABLE `publish_employee` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `event_id` int(11) DEFAULT NULL,
  `employee_id` int(11) DEFAULT NULL,
  `shift_position_id` int(11) DEFAULT NULL,
  `created_on` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `employee_id` (`employee_id`),
  KEY `shift_position_id` (`shift_position_id`),
  KEY `event_id` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=268663 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.`session` definition

CREATE TABLE `session` (
  `id` char(40) NOT NULL,
  `expire` int(11) DEFAULT NULL,
  `data` longblob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.shift_change_invite definition

CREATE TABLE `shift_change_invite` (
  `shift_change_invite_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'This table defines shift change invitations.',
  `shift_employee_id` bigint(20) NOT NULL,
  `shift_id` bigint(20) NOT NULL COMMENT 'This is the ID of the shift the employee is being invited to.',
  `fragment` varchar(45) NOT NULL,
  `accepted` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`shift_change_invite_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.sms definition

CREATE TABLE `sms` (
  `sms_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `number` varchar(16) NOT NULL,
  `message` varchar(256) DEFAULT NULL,
  `messageid` varchar(32) DEFAULT NULL,
  `sent` datetime NOT NULL,
  `status` varchar(16) DEFAULT NULL,
  `inbound` datetime DEFAULT NULL,
  `inbound_message` varchar(256) DEFAULT NULL,
  `notes` text DEFAULT NULL,
  PRIMARY KEY (`sms_id`),
  KEY `fk_employee_key_idx` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.staff definition

CREATE TABLE `staff` (
  `staff_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `status` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`staff_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.task definition

CREATE TABLE `task` (
  `task_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `complete` tinyint(4) DEFAULT 0,
  `completed_on` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.templates definition

CREATE TABLE `templates` (
  `template_id` smallint(6) NOT NULL AUTO_INCREMENT,
  `shift_invite_subject` text DEFAULT NULL,
  `shift_invite` text DEFAULT NULL,
  `shift_confirmation_subject` text DEFAULT NULL,
  `shift_confirmation` text DEFAULT NULL,
  `shift_change_subject` text DEFAULT NULL,
  `shift_change` text DEFAULT NULL,
  `shift_removed_subject` text DEFAULT NULL,
  `shift_removed` text DEFAULT NULL,
  PRIMARY KEY (`template_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.timesheet_sent definition

CREATE TABLE `timesheet_sent` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(50) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `status` tinyint(2) NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `event_id` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=791 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.uniform definition

CREATE TABLE `uniform` (
  `uniform_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `title` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`uniform_id`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.`user` definition

CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `auth_key` varchar(32) DEFAULT NULL,
  `password_hash` varchar(255) DEFAULT NULL,
  `password_reset_token` varchar(255) DEFAULT NULL,
  `email` varchar(255) NOT NULL,
  `associated_id` bigint(20) DEFAULT NULL,
  `role` varchar(24) DEFAULT NULL,
  `status` smallint(6) NOT NULL DEFAULT 10,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  `first_name` varchar(100) DEFAULT NULL,
  `last_name` varchar(100) DEFAULT NULL,
  `timezone` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `email` (`email`),
  UNIQUE KEY `password_reset_token` (`password_reset_token`)
) ENGINE=InnoDB AUTO_INCREMENT=1881 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing.user_device definition

CREATE TABLE `user_device` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `employee_id` int(20) NOT NULL,
  `device_token` varchar(500) DEFAULT NULL,
  `role` varchar(100) DEFAULT NULL,
  `platform` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `employee_id` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=435 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.venue_contact definition

CREATE TABLE `venue_contact` (
  `venue_contact_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) NOT NULL,
  `name` varchar(100) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `email` varchar(45) DEFAULT NULL,
  `phone` varchar(20) DEFAULT NULL,
  `mobile` varchar(20) DEFAULT NULL,
  `timesheet` tinyint(4) DEFAULT NULL,
  `invoicing` tinyint(4) DEFAULT NULL,
  `approval` tinyint(4) DEFAULT NULL,
  `sort_order` int(10) unsigned DEFAULT NULL,
  KEY `venue_id` (`venue_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.wc_code definition

CREATE TABLE `wc_code` (
  `wc_id` int(100) NOT NULL AUTO_INCREMENT,
  `wc_code` varchar(100) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`wc_id`)
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.auth_item definition

CREATE TABLE `auth_item` (
  `name` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `type` int(11) NOT NULL,
  `description` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `rule_name` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `data` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `created_at` int(11) DEFAULT NULL,
  `updated_at` int(11) DEFAULT NULL,
  PRIMARY KEY (`name`),
  KEY `rule_name` (`rule_name`),
  KEY `idx-auth_item-type` (`type`),
  CONSTRAINT `auth_item_ibfk_1` FOREIGN KEY (`rule_name`) REFERENCES `auth_rule` (`name`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing.auth_item_child definition

CREATE TABLE `auth_item_child` (
  `parent` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `child` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  PRIMARY KEY (`parent`,`child`),
  KEY `child` (`child`),
  CONSTRAINT `auth_item_child_ibfk_1` FOREIGN KEY (`parent`) REFERENCES `auth_item` (`name`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `auth_item_child_ibfk_2` FOREIGN KEY (`child`) REFERENCES `auth_item` (`name`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing.client_contact definition

CREATE TABLE `client_contact` (
  `client_contact_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `name` varchar(100) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `email` varchar(45) DEFAULT NULL,
  `phone` varchar(20) DEFAULT NULL,
  `mobile` varchar(20) DEFAULT NULL,
  `office` varchar(20) DEFAULT NULL,
  `ext` varchar(20) DEFAULT NULL,
  `timesheet` tinyint(4) DEFAULT NULL,
  `invoicing` tinyint(4) DEFAULT NULL,
  `sort_order` int(10) unsigned DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`client_contact_id`),
  KEY `fk_client_contact_client_idx` (`client_id`),
  CONSTRAINT `fk_client_contact_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`)
) ENGINE=InnoDB AUTO_INCREMENT=576 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.client_note definition

CREATE TABLE `client_note` (
  `client_note_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `note` text DEFAULT NULL,
  `date` timestamp NULL DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`client_note_id`),
  KEY `fk_account_key_idx` (`client_id`),
  CONSTRAINT `fk_client_note_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=48 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_experience definition

CREATE TABLE `employee_experience` (
  `ee_id` bigint(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `employer` varchar(100) DEFAULT NULL,
  `address` varchar(100) DEFAULT NULL,
  `phone` varchar(45) DEFAULT NULL,
  `title` varchar(45) DEFAULT NULL,
  `supervisor` varchar(100) DEFAULT NULL,
  `whyleft` varchar(100) DEFAULT NULL,
  `start` date DEFAULT NULL,
  `end` date DEFAULT NULL,
  `start_rate` varchar(15) DEFAULT NULL,
  `end_rate` varchar(15) DEFAULT NULL,
  `work_performed` text DEFAULT NULL,
  PRIMARY KEY (`ee_id`),
  KEY `fk_ee_employee_idx` (`employee_id`),
  CONSTRAINT `fk_ee_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_note definition

CREATE TABLE `employee_note` (
  `employee_note_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `note` text DEFAULT NULL,
  `datetime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `user_id` int(11) NOT NULL,
  `type` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`employee_note_id`),
  KEY `fk_note_to_employee_idx` (`employee_id`),
  KEY `fk_note_to_author_idx` (`user_id`),
  CONSTRAINT `fk_note_to_author` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_note_to_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=1120 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.`position` definition

CREATE TABLE `position` (
  `position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` int(20) DEFAULT NULL,
  `description` varchar(100) NOT NULL,
  `rate` decimal(5,2) NOT NULL COMMENT 'This is the table responsible for the ',
  `uniform` text DEFAULT NULL,
  `tools` text DEFAULT NULL,
  `grooming_tools` text DEFAULT NULL,
  PRIMARY KEY (`position_id`),
  KEY `group_id` (`group_id`),
  CONSTRAINT `fk_group_id` FOREIGN KEY (`group_id`) REFERENCES `position_group` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=199 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.position_material definition

CREATE TABLE `position_material` (
  `position_material_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `description` varchar(100) NOT NULL,
  `required` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`position_material_id`),
  KEY `fk_material_position_idx` (`position_id`),
  CONSTRAINT `fk_material_position` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.uniform_type definition

CREATE TABLE `uniform_type` (
  `uniform_type_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `uniform_id` bigint(20) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `description` text DEFAULT NULL,
  PRIMARY KEY (`uniform_type_id`),
  KEY `fk_uniform_id_key_idx` (`uniform_id`),
  CONSTRAINT `fk_uniform_id_key` FOREIGN KEY (`uniform_id`) REFERENCES `uniform` (`uniform_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=189 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.user_stat definition

CREATE TABLE `user_stat` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `last_logged_in` date DEFAULT NULL,
  `updated_availability` date DEFAULT NULL,
  PRIMARY KEY (`user_id`),
  CONSTRAINT `user_stat_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1881 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.venue definition

CREATE TABLE `venue` (
  `venue_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `name` varchar(100) NOT NULL,
  `financed` tinyint(4) DEFAULT 0,
  `send_timesheet` tinyint(4) NOT NULL DEFAULT 1,
  `photo` text DEFAULT NULL,
  `address1` varchar(100) NOT NULL,
  `address2` varchar(100) DEFAULT NULL,
  `phone` varchar(20) DEFAULT NULL,
  `state` varchar(45) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `zip` varchar(45) DEFAULT NULL,
  `language` varchar(100) DEFAULT NULL,
  `contact` varchar(150) DEFAULT NULL,
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `notes` text DEFAULT NULL,
  `venue_details` text DEFAULT NULL,
  `description` text DEFAULT NULL,
  `parking` text DEFAULT NULL,
  `parking_reimbursement` text DEFAULT NULL,
  `free_parking` tinyint(2) NOT NULL DEFAULT 0,
  `check_in` text DEFAULT NULL,
  `background_requirements` text DEFAULT NULL,
  `uniform_requirements` text DEFAULT NULL,
  `directions` text DEFAULT NULL,
  `staffing_manager_id` int(11) DEFAULT NULL,
  `sales_rep_id` int(11) DEFAULT NULL,
  `parkings` int(10) DEFAULT NULL,
  `parking_note` text DEFAULT NULL,
  `parking_charge` decimal(5,2) DEFAULT NULL,
  `travel_charge` decimal(5,2) DEFAULT 0.00,
  `service_charge` decimal(5,2) DEFAULT 0.00,
  `factored` tinyint(2) DEFAULT NULL,
  PRIMARY KEY (`venue_id`),
  KEY `fk_account_client_idx` (`client_id`),
  CONSTRAINT `fk_venue_to_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`)
) ENGINE=InnoDB AUTO_INCREMENT=489 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.venue_picture definition

CREATE TABLE `venue_picture` (
  `venue_picture_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `picture` varchar(100) NOT NULL,
  PRIMARY KEY (`venue_picture_id`),
  KEY `fk_venue_picture_to_venue_idx` (`venue_id`),
  CONSTRAINT `fk_venue_picture_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.venue_position definition

CREATE TABLE `venue_position` (
  `venue_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `client_id` bigint(20) NOT NULL,
  `group_id` int(11) DEFAULT NULL,
  `position_id` bigint(20) NOT NULL,
  `sub_type_id` bigint(20) DEFAULT NULL,
  `gender` varchar(20) DEFAULT NULL,
  `rate` decimal(5,2) NOT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `bill_rate` decimal(5,2) DEFAULT NULL,
  `exclusive` tinyint(4) NOT NULL DEFAULT 0,
  `description` text DEFAULT NULL,
  `use_standard_rate` tinyint(4) DEFAULT 0,
  `uniform_types` varchar(255) DEFAULT NULL,
  `uniform` text DEFAULT NULL,
  `tools` text DEFAULT NULL,
  `grooming_tools` text DEFAULT NULL,
  PRIMARY KEY (`venue_position_id`),
  KEY `fk_position_dept_idx` (`venue_id`),
  KEY `fk_vposition_to_position` (`position_id`),
  CONSTRAINT `fk_vposition_to_position` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`),
  CONSTRAINT `fk_vpositoin_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=912 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.auth_assignment definition

CREATE TABLE `auth_assignment` (
  `item_name` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `user_id` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `created_at` int(11) DEFAULT NULL,
  PRIMARY KEY (`item_name`,`user_id`),
  CONSTRAINT `auth_assignment_ibfk_1` FOREIGN KEY (`item_name`) REFERENCES `auth_item` (`name`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing.dnr definition

CREATE TABLE `dnr` (
  `dnr_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) NOT NULL,
  `date` timestamp NOT NULL DEFAULT current_timestamp(),
  `reason` varchar(200) DEFAULT NULL,
  `other_reason` varchar(100) DEFAULT NULL,
  `notes` text DEFAULT NULL COMMENT 'This table contains the list of employees who are banned from working for certain clients usually due to poor performance or other negative factors.\n',
  `created_by` bigint(50) DEFAULT NULL,
  PRIMARY KEY (`dnr_id`),
  KEY `fk_dnr_employee_idx` (`employee_id`),
  KEY `fk_dnr_venue_idx` (`venue_id`),
  CONSTRAINT `fk_dnr_to_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_dnr_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=821 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.employee_position definition

CREATE TABLE `employee_position` (
  `employee_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `position_id` bigint(20) NOT NULL,
  `sub_type_id` bigint(20) DEFAULT -1,
  `level` tinyint(4) DEFAULT NULL COMMENT 'This field allows the staff to rate the employees on their positions.',
  `rate` decimal(5,2) DEFAULT NULL,
  PRIMARY KEY (`employee_position_id`),
  KEY `fk_position_position_idx` (`position_id`),
  KEY `fk_employee_employee_idx` (`employee_id`),
  CONSTRAINT `fk_position_position` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=9051 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.event definition

CREATE TABLE `event` (
  `event_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `has_address` tinyint(2) DEFAULT 0,
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `special_address` text DEFAULT NULL,
  `date` date NOT NULL,
  `no_employees` tinyint(4) NOT NULL DEFAULT 1,
  `emergency` tinyint(4) DEFAULT 0,
  `purchase_order` varchar(45) DEFAULT NULL,
  `travel_charge` decimal(5,2) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `venue_details` text DEFAULT NULL,
  `admin_notes` text DEFAULT NULL,
  `directions` text DEFAULT NULL,
  `parking` text DEFAULT NULL,
  `parking_note` varchar(200) DEFAULT NULL,
  `parking_reimbursement` text DEFAULT NULL,
  `check_in` text DEFAULT NULL,
  `background_requirements` text DEFAULT NULL,
  `document1` varchar(150) DEFAULT NULL,
  `document2` varchar(150) DEFAULT NULL,
  `document3` varchar(150) DEFAULT NULL,
  `description1` varchar(150) DEFAULT NULL,
  `description2` varchar(150) DEFAULT NULL,
  `description3` varchar(150) DEFAULT NULL,
  `invoice_label` varchar(100) DEFAULT NULL,
  `verbal_timesheet` tinyint(4) DEFAULT 0,
  `created_by` int(11) DEFAULT -1,
  PRIMARY KEY (`event_id`),
  KEY `fk_event_venue_idx` (`venue_id`),
  KEY `fk_event_client_idx` (`client_id`),
  CONSTRAINT `fk_event_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_event_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=2114 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.exclusive definition

CREATE TABLE `exclusive` (
  `exclusive_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) NOT NULL,
  `employee_id` bigint(20) NOT NULL,
  `reason` varchar(100) DEFAULT NULL,
  `notes` text DEFAULT NULL,
  `created_by` bigint(20) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`exclusive_id`),
  KEY `fk_exclusive_department_idx` (`venue_id`),
  KEY `fk_exclusive_employee_idx` (`employee_id`),
  KEY `fk_exclusive_to_client` (`client_id`),
  CONSTRAINT `fk_exclusive_to_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`),
  CONSTRAINT `fk_exclusive_to_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_exclusive_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1519 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.shift definition

CREATE TABLE `shift` (
  `shift_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) NOT NULL,
  `start_time` time NOT NULL,
  `end_time` time NOT NULL,
  PRIMARY KEY (`shift_id`),
  KEY `fk_shift_event_idx` (`event_id`),
  CONSTRAINT `fk_shift_event_key` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=4332 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.shift_position definition

CREATE TABLE `shift_position` (
  `shift_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shift_id` bigint(20) NOT NULL,
  `position_id` bigint(20) NOT NULL,
  `additional_title` varchar(100) DEFAULT NULL,
  `sub_type_id` bigint(20) DEFAULT -1,
  `gender` tinyint(2) DEFAULT NULL,
  `rate` decimal(5,2) NOT NULL DEFAULT 0.00,
  `bill_rate` decimal(5,2) DEFAULT NULL,
  `count` tinyint(4) NOT NULL DEFAULT 1,
  `backup` tinyint(4) DEFAULT 0,
  `holiday_rate` tinyint(2) NOT NULL DEFAULT 0,
  `surcharge` tinyint(2) NOT NULL DEFAULT 0,
  `surcharge_value` decimal(5,2) DEFAULT NULL,
  `grooming_tools` text DEFAULT NULL,
  `uniform` text DEFAULT NULL,
  `tools` text DEFAULT NULL,
  `position_description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`shift_position_id`),
  KEY `fk_shift_position_shift_idx` (`shift_id`),
  KEY `fk_shift_to_positoin` (`position_id`),
  CONSTRAINT `fk_shift_positoin_to_shift` FOREIGN KEY (`shift_id`) REFERENCES `shift` (`shift_id`),
  CONSTRAINT `fk_shift_to_positoin` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4420 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.timesheet definition

CREATE TABLE `timesheet` (
  `timesheet_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `event_id` bigint(20) NOT NULL,
  `shift_employee_id` bigint(20) NOT NULL,
  `employee_start_time` time DEFAULT NULL,
  `employee_meal_break` time DEFAULT NULL,
  `employee_end_time` time DEFAULT NULL,
  `employee_break_start` time DEFAULT NULL,
  `employee_break_end` time DEFAULT NULL,
  `employee_notes` text DEFAULT NULL,
  `employee_rating` int(10) DEFAULT NULL,
  `reimbursement_file` varchar(250) DEFAULT NULL,
  `employee_no_pay` tinyint(2) DEFAULT NULL,
  `employee_min_pay` tinyint(2) DEFAULT NULL,
  `employee_tips` decimal(5,2) DEFAULT NULL,
  `employee_parking` decimal(5,2) DEFAULT NULL,
  `employee_travel` decimal(5,2) DEFAULT NULL,
  `employee_service_charge` decimal(5,2) DEFAULT NULL,
  `employee_adjustment` varchar(10) DEFAULT NULL,
  `employee_adj_notes` text DEFAULT NULL,
  `employee_meal` varchar(10) DEFAULT NULL,
  `employee_timesheet_upload` varchar(100) DEFAULT NULL,
  `client_start_time` time DEFAULT NULL,
  `client_meal_break` time DEFAULT NULL,
  `client_end_time` time DEFAULT NULL,
  `client_break_start` time DEFAULT NULL,
  `client_break_end` time DEFAULT NULL,
  `client_rating` float DEFAULT NULL,
  `client_dnr` tinyint(4) DEFAULT 0,
  `client_no_bill` tinyint(2) DEFAULT NULL,
  `client_no_ot` tinyint(2) DEFAULT NULL,
  `client_sig` tinyint(2) DEFAULT NULL,
  `client_po` varchar(100) DEFAULT NULL COMMENT 'purchase order. It will come from event , if blank then can add in timesheet ',
  `client_tips` decimal(5,2) DEFAULT NULL,
  `client_parking` decimal(5,2) DEFAULT NULL,
  `client_travel` decimal(5,2) DEFAULT NULL,
  `client_service_charge` decimal(5,2) DEFAULT NULL,
  `add_preferred` tinyint(4) DEFAULT 0,
  `noshow` tinyint(4) DEFAULT 0,
  `workedless` tinyint(4) DEFAULT 0,
  `whyless` text DEFAULT NULL,
  `client_notes` text DEFAULT NULL,
  `discrepancy` varchar(2) DEFAULT NULL,
  `nominbill` tinyint(4) DEFAULT 0,
  `mealpenalty` varchar(10) DEFAULT NULL COMMENT 'This column is so that we can add in additional time that will be charged to the client.\n',
  `adjustment` varchar(10) DEFAULT NULL COMMENT 'This column is used for miscellaneous adjustments.  Example is if the employee gets injured and we pay employee for full day while only billing client for partial.\n',
  `adjust_notes` text DEFAULT NULL,
  `emergency_rate` tinyint(4) DEFAULT 0,
  `hr_adjustment` varchar(10) DEFAULT NULL,
  `hr_notes` text DEFAULT NULL,
  `wage_replacement` decimal(5,2) DEFAULT NULL,
  `missed_event` int(11) NOT NULL DEFAULT 0,
  `meal_reason` int(10) DEFAULT NULL,
  `meal_other_field` varchar(150) DEFAULT NULL COMMENT 'This is filed is used when other option chosen from meal reason.',
  `less_hour` int(10) DEFAULT NULL,
  `less_hour_field` varchar(150) DEFAULT NULL COMMENT 'This is filed is used when other option chosen from less hours.',
  PRIMARY KEY (`timesheet_id`),
  KEY `fk_timesheet_employee_idx` (`employee_id`),
  KEY `fk_timesheet_event_idx` (`event_id`),
  KEY `fk_timesheet_employe_shift_idx` (`shift_employee_id`),
  CONSTRAINT `fk_timesheet_to_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4057 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.timesheet_upload definition

CREATE TABLE `timesheet_upload` (
  `timesheet_upload_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) NOT NULL,
  `datetime` timestamp NOT NULL DEFAULT current_timestamp(),
  `filename` varchar(100) NOT NULL,
  `notes` text DEFAULT NULL,
  PRIMARY KEY (`timesheet_upload_id`),
  KEY `fk_ts_event_idx` (`event_id`),
  CONSTRAINT `fk_ts_upload_to_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=108 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.shift_employee definition

CREATE TABLE `shift_employee` (
  `shift_employee_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shift_position_id` bigint(20) NOT NULL,
  `event_id` bigint(20) DEFAULT NULL,
  `employee_id` bigint(20) NOT NULL,
  `confirmed` tinyint(4) NOT NULL DEFAULT 0,
  `bill_rate` decimal(5,2) DEFAULT 0.00,
  `rate` decimal(5,2) DEFAULT 0.00,
  `cancel_reason` tinyint(4) DEFAULT 0,
  `employee_cancel_reason` text DEFAULT NULL,
  `emergency_rate` tinyint(4) NOT NULL DEFAULT 0,
  `emergency_rate_amount` decimal(5,2) DEFAULT 0.00,
  `cancel_notes` text DEFAULT NULL,
  `confirm_type` tinyint(4) DEFAULT NULL,
  `confirm_notes` text DEFAULT NULL,
  `confirmed_by` int(11) DEFAULT NULL,
  `request_by` bigint(50) DEFAULT NULL,
  `approved_by` bigint(50) DEFAULT NULL,
  `read_notification` int(11) NOT NULL DEFAULT 0,
  `daily_report_notes` text DEFAULT NULL,
  `overtime_reason` text DEFAULT NULL,
  PRIMARY KEY (`shift_employee_id`),
  KEY `fk_shift_employee_shift_idx` (`shift_position_id`),
  KEY `fk_shift_employee_employee_idx` (`employee_id`),
  KEY `fk_shift_employee_to_event` (`event_id`),
  CONSTRAINT `fk_shift_employee_key` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_shift_employee_to_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`),
  CONSTRAINT `fk_shift_employee_to_position` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5308 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing.payroll_note definition

CREATE TABLE `payroll_note` (
  `payroll_note_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shift_employee_id` bigint(20) DEFAULT NULL,
  `payroll_note_type_id` int(150) DEFAULT NULL,
  `note` varchar(255) NOT NULL,
  `on_sheet` tinyint(2) DEFAULT 0,
  `user_id` bigint(255) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`payroll_note_id`),
  KEY `shift_employee_id` (`shift_employee_id`),
  CONSTRAINT `fk_shift_employee` FOREIGN KEY (`shift_employee_id`) REFERENCES `shift_employee` (`shift_employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
