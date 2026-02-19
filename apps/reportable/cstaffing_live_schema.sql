-- cstaffing_live.auth_rule definition

CREATE TABLE `auth_rule` (
  `name` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `data` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `created_at` int(11) DEFAULT NULL,
  `updated_at` int(11) DEFAULT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing_live.calendar definition

CREATE TABLE `calendar` (
  `calendar_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `start` datetime DEFAULT NULL,
  `end` datetime DEFAULT NULL,
  PRIMARY KEY (`calendar_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.company_info definition

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
  `privacy` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci ROW_FORMAT=DYNAMIC;


-- cstaffing_live.debug_publishing definition

CREATE TABLE `debug_publishing` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` int(11) NOT NULL,
  `employee` varchar(255) NOT NULL,
  `publishing_id` int(11) NOT NULL,
  `shift_position_id` int(11) NOT NULL,
  `reason` text NOT NULL,
  `created_at` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=598387 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.del_group_rate definition

CREATE TABLE `del_group_rate` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` int(20) DEFAULT NULL,
  `client_id` bigint(20) DEFAULT NULL,
  `bill_rate` decimal(5,2) DEFAULT NULL,
  `pay_rate` decimal(5,2) DEFAULT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3742 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.del_position_move definition

CREATE TABLE `del_position_move` (
  `position_move_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) DEFAULT NULL,
  `group_id` int(20) DEFAULT NULL,
  `position_id` bigint(20) DEFAULT NULL,
  `old_group_id` int(20) DEFAULT NULL,
  `delete_status` tinyint(2) NOT NULL DEFAULT 0,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`position_move_id`),
  KEY `client_id` (`client_id`,`group_id`,`position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=201 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_application definition

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


-- cstaffing_live.employee_available definition

CREATE TABLE `employee_available` (
  `employee_available_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `available` tinyint(2) NOT NULL DEFAULT 0,
  `date` date NOT NULL,
  PRIMARY KEY (`employee_available_id`),
  KEY `fk_employee_available_employee_idx` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=387 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_document definition

CREATE TABLE `employee_document` (
  `employee_document_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `filename` varchar(150) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`employee_document_id`),
  KEY `fk_document_employee_idx` (`employee_id`)
) ENGINE=InnoDB AUTO_INCREMENT=159 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_profile_updates definition

CREATE TABLE `employee_profile_updates` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `attribute_name` varchar(200) DEFAULT NULL,
  `old_value` text DEFAULT NULL,
  `new_value` text DEFAULT NULL,
  `updated_by` bigint(20) DEFAULT NULL,
  `updated_on` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=99180 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.event_edits definition

CREATE TABLE `event_edits` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(50) DEFAULT NULL,
  `new_data` blob DEFAULT NULL,
  `updated_by` bigint(50) DEFAULT NULL,
  `updated_on` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `event_id` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=181667 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci COMMENT='This table will sotre event edit data.';


-- cstaffing_live.government_form definition

CREATE TABLE `government_form` (
  `form_id` int(100) NOT NULL AUTO_INCREMENT,
  `form_type` varchar(100) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`form_id`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.hiatus_employee definition

CREATE TABLE `hiatus_employee` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) DEFAULT NULL,
  `reason_id` int(200) DEFAULT NULL,
  `added_by` bigint(20) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=933 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.`language` definition

CREATE TABLE `language` (
  `language_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `language` varchar(20) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `preferred` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`language_id`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.mail_queue definition

CREATE TABLE `mail_queue` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `subject` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `attempts` int(11) DEFAULT NULL,
  `last_attempt_time` datetime DEFAULT NULL,
  `sent_time` datetime DEFAULT NULL,
  `time_to_send` datetime NOT NULL,
  `swift_message` longtext DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `IX_time_to_send` (`time_to_send`),
  KEY `IX_sent_time` (`sent_time`)
) ENGINE=InnoDB AUTO_INCREMENT=11858657 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing_live.migration definition

CREATE TABLE `migration` (
  `version` varchar(180) NOT NULL,
  `apply_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.min_wage_rate definition

CREATE TABLE `min_wage_rate` (
  `min_wage_id` int(11) NOT NULL AUTO_INCREMENT,
  `description` varchar(100) NOT NULL,
  `default` tinyint(4) DEFAULT 0,
  PRIMARY KEY (`min_wage_id`)
) ENGINE=InnoDB AUTO_INCREMENT=123 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.oauth_clients definition

CREATE TABLE `oauth_clients` (
  `client_id` varchar(32) NOT NULL,
  `client_secret` varchar(32) DEFAULT NULL,
  `redirect_uri` varchar(1000) NOT NULL,
  `grant_types` varchar(100) NOT NULL,
  `scope` varchar(2000) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.oauth_jwt definition

CREATE TABLE `oauth_jwt` (
  `client_id` varchar(32) NOT NULL,
  `subject` varchar(80) DEFAULT NULL,
  `public_key` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.oauth_public_keys definition

CREATE TABLE `oauth_public_keys` (
  `client_id` varchar(255) NOT NULL,
  `public_key` varchar(2000) DEFAULT NULL,
  `private_key` varchar(2000) DEFAULT NULL,
  `encryption_algorithm` varchar(100) DEFAULT 'RS256'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.oauth_scopes definition

CREATE TABLE `oauth_scopes` (
  `scope` varchar(2000) NOT NULL,
  `is_default` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.oauth_users definition

CREATE TABLE `oauth_users` (
  `username` varchar(255) NOT NULL,
  `password` varchar(2000) DEFAULT NULL,
  `first_name` varchar(255) DEFAULT NULL,
  `last_name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.onboard_forms definition

CREATE TABLE `onboard_forms` (
  `onboard_form_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `form_type` tinyint(4) DEFAULT NULL,
  `signature` varchar(150) DEFAULT NULL,
  `sign_type` tinyint(2) DEFAULT NULL COMMENT '0 = type sign, 1=draw sign',
  `first_name` varchar(100) DEFAULT NULL,
  `last_name` varchar(100) DEFAULT NULL,
  `address1` varchar(255) DEFAULT NULL,
  `address2` varchar(255) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL,
  `zip` varchar(20) DEFAULT NULL,
  `ssn` varchar(20) DEFAULT NULL COMMENT 'social security number',
  `marital_status` tinyint(2) DEFAULT NULL,
  `last_name_differ` tinyint(2) DEFAULT NULL,
  `middle_name` varchar(200) DEFAULT NULL,
  `other_last_name` varchar(200) DEFAULT NULL,
  `dob` date DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `telephone` varchar(50) DEFAULT NULL,
  `attest_form` tinyint(4) DEFAULT NULL,
  `uscis_number` varchar(20) DEFAULT NULL COMMENT 'i9 form',
  `auth_work_expire_date` date DEFAULT NULL COMMENT 'i9 form',
  `auth_form_type` tinyint(2) DEFAULT NULL COMMENT 'i9 form',
  `foreign_passport_country` varchar(50) DEFAULT NULL COMMENT 'i9 form',
  `preparer_check` tinyint(2) DEFAULT NULL COMMENT 'for i9 form',
  `preparer_first_name` varchar(100) DEFAULT NULL COMMENT 'for i9 form',
  `preparer_last_name` varchar(100) DEFAULT NULL COMMENT 'for i9 form',
  `preparer_address` varchar(255) DEFAULT NULL COMMENT 'for i9 form',
  `preparer_city` varchar(50) DEFAULT NULL COMMENT 'for i9 form',
  `preparer_state` varchar(20) DEFAULT NULL COMMENT 'for i9 form',
  `preparer_zip` varchar(20) DEFAULT NULL COMMENT 'for i9 form',
  `ip_address` varchar(100) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `a_form` varchar(100) DEFAULT NULL COMMENT 'for I9 form',
  `a_issuing_authority` varchar(100) DEFAULT NULL COMMENT 'for I9 form',
  `a_document_number` varchar(50) DEFAULT NULL COMMENT 'for I9 form',
  `a_expriy_date` date DEFAULT NULL COMMENT 'for I9 form',
  `b_form` varchar(100) DEFAULT NULL COMMENT 'for I9 form',
  `b_issuing_authority` varchar(100) DEFAULT NULL COMMENT 'for I9 form',
  `b_document_number` varchar(50) DEFAULT NULL COMMENT 'for I9 form',
  `b_expriy_date` date DEFAULT NULL COMMENT 'for I9 form',
  `c_form` varchar(100) DEFAULT NULL COMMENT 'for I9 form',
  `c_issuing_authority` varchar(100) DEFAULT NULL COMMENT 'for I9 form',
  `c_document_number` varchar(50) DEFAULT NULL COMMENT 'for I9 form',
  `c_expriy_date` date DEFAULT NULL COMMENT 'for I9 form',
  `country` varchar(200) DEFAULT NULL COMMENT 'for 8850 form',
  `swa_check` tinyint(2) DEFAULT NULL COMMENT 'for 8850 form',
  `snap_check` tinyint(2) DEFAULT NULL COMMENT 'for 8850 form',
  `unemployed_check` tinyint(2) DEFAULT NULL COMMENT 'for 8850 form',
  `disability_armed_check` tinyint(2) DEFAULT NULL COMMENT 'for 8850 form',
  `disability_unemployed_check` tinyint(2) DEFAULT NULL COMMENT 'for 8850 form',
  `family_check` tinyint(2) DEFAULT NULL COMMENT 'for 8850 form',
  `crime_check` tinyint(2) DEFAULT NULL COMMENT 'background disclosure form',
  `crime_detail` varchar(255) DEFAULT NULL COMMENT 'background disclosure form',
  `resident_check` tinyint(2) DEFAULT NULL COMMENT 'background disclosure form',
  `jobs_check` tinyint(2) DEFAULT NULL COMMENT 'for w4 Form',
  `first_income` decimal(8,2) DEFAULT NULL COMMENT 'for w4 Form',
  `second_income` decimal(8,2) DEFAULT NULL COMMENT 'for w4 Form',
  `total_income` decimal(8,2) DEFAULT NULL COMMENT 'for w4 Form',
  `other_income` decimal(8,2) DEFAULT NULL COMMENT 'for w4 Form',
  `deduction` decimal(8,2) DEFAULT NULL COMMENT 'for w4 Form',
  `extra_withholding` decimal(8,2) DEFAULT NULL COMMENT 'for w4 Form',
  `allowances` varchar(20) DEFAULT NULL COMMENT 'for edd form.',
  `exempt` varchar(200) DEFAULT NULL COMMENT 'for edd form.',
  `job_title` varchar(200) DEFAULT NULL COMMENT 'for eeo1 form',
  `gender` varchar(20) DEFAULT NULL COMMENT 'for eeo1 form',
  `race` tinyint(2) DEFAULT NULL COMMENT 'for eeo1 form',
  `foodbuy_option_one` tinyint(2) DEFAULT NULL COMMENT 'for foodbuy drug form',
  `foodbuy_option_two` tinyint(2) DEFAULT NULL COMMENT 'for foodbuy drug form',
  PRIMARY KEY (`onboard_form_id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=21197 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='To store onboard forms data';


-- cstaffing_live.parking definition

CREATE TABLE `parking` (
  `parking_id` int(200) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`parking_id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.payroll_note_type definition

CREATE TABLE `payroll_note_type` (
  `payroll_note_type_id` int(150) NOT NULL AUTO_INCREMENT,
  `type` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`payroll_note_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.position_sub_type definition

CREATE TABLE `position_sub_type` (
  `sub_type_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) DEFAULT NULL,
  `title` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`sub_type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.preference definition

CREATE TABLE `preference` (
  `preferences_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_submit_email` varchar(100) DEFAULT NULL,
  `employee_change_email` varchar(100) DEFAULT NULL,
  `employee_status_email` varchar(100) DEFAULT NULL,
  `client_notification_email` varchar(50) DEFAULT NULL,
  `payroll_filter_date_range` varchar(50) DEFAULT NULL,
  `timesheet_notification_email` varchar(100) DEFAULT NULL,
  `susp_notification_email` varchar(50) DEFAULT NULL,
  `dnr_notification_email` varchar(50) DEFAULT NULL,
  `hour_notification_email` varchar(200) DEFAULT NULL,
  `fh_expire_notification` varchar(200) DEFAULT NULL,
  `cc_expire_notification` varchar(200) DEFAULT NULL,
  `resign_notification_email` varchar(200) DEFAULT NULL,
  `inactive_thirty_notification` varchar(150) DEFAULT NULL,
  `inactive_sixsty_notification` varchar(150) DEFAULT NULL,
  `onboard_docs_email` varchar(50) DEFAULT NULL COMMENT 'To CC onboard pdf form of employee',
  `employee_photo_changed_email` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`preferences_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.publish definition

CREATE TABLE `publish` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(50) DEFAULT NULL,
  `shifts` varchar(200) DEFAULT NULL,
  `rules` varchar(150) DEFAULT NULL,
  `user_id` bigint(50) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `event_id` (`event_id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=49035 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.publish_employee2 definition

CREATE TABLE `publish_employee2` (
  `id` int(11) NOT NULL DEFAULT 0,
  `event_id` int(11) DEFAULT NULL,
  `employee_id` int(11) DEFAULT NULL,
  `shift_position_id` int(11) DEFAULT NULL,
  `created_on` timestamp NULL DEFAULT NULL,
  `publishing_id` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.queue definition

CREATE TABLE `queue` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `channel` varchar(255) NOT NULL,
  `job` longblob NOT NULL,
  `pushed_at` int(11) NOT NULL,
  `ttr` int(11) NOT NULL,
  `delay` int(11) NOT NULL DEFAULT 0,
  `priority` int(11) unsigned NOT NULL DEFAULT 1024,
  `reserved_at` int(11) DEFAULT NULL,
  `attempt` int(11) DEFAULT NULL,
  `done_at` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `channel` (`channel`),
  KEY `reserved_at` (`reserved_at`),
  KEY `priority` (`priority`)
) ENGINE=InnoDB AUTO_INCREMENT=12967738 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.`session` definition

CREATE TABLE `session` (
  `id` char(40) NOT NULL,
  `expire` int(11) DEFAULT NULL,
  `data` longblob DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_change_invite definition

CREATE TABLE `shift_change_invite` (
  `shift_change_invite_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'This table defines shift change invitations.',
  `shift_employee_id` bigint(20) NOT NULL,
  `shift_id` bigint(20) NOT NULL COMMENT 'This is the ID of the shift the employee is being invited to.',
  `fragment` varchar(45) NOT NULL,
  `accepted` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`shift_change_invite_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.sms definition

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
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.staff definition

CREATE TABLE `staff` (
  `staff_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `status` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`staff_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.task definition

CREATE TABLE `task` (
  `task_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `complete` tinyint(4) DEFAULT 0,
  `completed_on` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.templates definition

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


-- cstaffing_live.temporary_file definition

CREATE TABLE `temporary_file` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(255) NOT NULL,
  `related_id` int(11) DEFAULT NULL,
  `file_name` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `file_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=118636 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.timesheet_open definition

CREATE TABLE `timesheet_open` (
  `timesheet_open_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  `open` tinyint(2) DEFAULT NULL,
  `updated` tinyint(2) DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `updated_at` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`timesheet_open_id`),
  KEY `event_id` (`event_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15246 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.timesheet_sent definition

CREATE TABLE `timesheet_sent` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(50) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `status` tinyint(2) NOT NULL DEFAULT 1,
  `custom` tinyint(1) DEFAULT NULL,
  `roster` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=98421 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.user_device definition

CREATE TABLE `user_device` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `employee_id` int(20) NOT NULL,
  `device_token` varchar(500) DEFAULT NULL,
  `role` varchar(100) DEFAULT NULL,
  `platform` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10898 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.user_stat definition

CREATE TABLE `user_stat` (
  `user_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.wc_code definition

CREATE TABLE `wc_code` (
  `wc_id` int(100) NOT NULL AUTO_INCREMENT,
  `wc_code` varchar(100) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `rate` decimal(8,4) DEFAULT NULL,
  `state` varchar(255) NOT NULL,
  PRIMARY KEY (`wc_id`)
) ENGINE=InnoDB AUTO_INCREMENT=97 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.auth_item definition

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


-- cstaffing_live.auth_item_child definition

CREATE TABLE `auth_item_child` (
  `parent` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `child` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  PRIMARY KEY (`parent`,`child`),
  KEY `child` (`child`),
  CONSTRAINT `auth_item_child_ibfk_1` FOREIGN KEY (`parent`) REFERENCES `auth_item` (`name`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `auth_item_child_ibfk_2` FOREIGN KEY (`child`) REFERENCES `auth_item` (`name`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing_live.del_group_rate_amount definition

CREATE TABLE `del_group_rate_amount` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_rate_id` bigint(20) NOT NULL,
  `pay_rate` decimal(7,2) DEFAULT NULL,
  `bill_rate` decimal(7,2) DEFAULT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_grraam_cu` (`created_by`),
  KEY `fk_grraam_grra` (`group_rate_id`),
  CONSTRAINT `fk_grraam_grra` FOREIGN KEY (`group_rate_id`) REFERENCES `del_group_rate` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5667 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.oauth_access_tokens definition

CREATE TABLE `oauth_access_tokens` (
  `access_token` varchar(40) NOT NULL,
  `client_id` varchar(32) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `expires` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `scope` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`access_token`),
  KEY `client_id` (`client_id`),
  CONSTRAINT `oauth_access_tokens_ibfk_1` FOREIGN KEY (`client_id`) REFERENCES `oauth_clients` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.oauth_authorization_codes definition

CREATE TABLE `oauth_authorization_codes` (
  `authorization_code` varchar(40) NOT NULL,
  `client_id` varchar(32) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `redirect_uri` varchar(1000) NOT NULL,
  `expires` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `scope` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`authorization_code`),
  KEY `client_id` (`client_id`),
  CONSTRAINT `oauth_authorization_codes_ibfk_1` FOREIGN KEY (`client_id`) REFERENCES `oauth_clients` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.oauth_refresh_tokens definition

CREATE TABLE `oauth_refresh_tokens` (
  `refresh_token` varchar(40) NOT NULL,
  `client_id` varchar(32) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `expires` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `scope` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`refresh_token`),
  KEY `client_id` (`client_id`),
  CONSTRAINT `oauth_refresh_tokens_ibfk_1` FOREIGN KEY (`client_id`) REFERENCES `oauth_clients` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;


-- cstaffing_live.sick_wage_pay definition

CREATE TABLE `sick_wage_pay` (
  `sick_wage_pay_id` bigint(50) NOT NULL AUTO_INCREMENT,
  `wc_id` int(100) NOT NULL,
  `date_range` varchar(50) DEFAULT NULL COMMENT 'used as week of month',
  `type` tinyint(2) DEFAULT NULL,
  `hours` decimal(5,2) DEFAULT NULL,
  `pay_rate` decimal(5,2) DEFAULT NULL,
  PRIMARY KEY (`sick_wage_pay_id`),
  KEY `wc_id` (`wc_id`),
  CONSTRAINT `fk_sick_wage_wc` FOREIGN KEY (`wc_id`) REFERENCES `wc_code` (`wc_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.`user` definition

CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sf_id` varchar(100) DEFAULT NULL COMMENT 'Salesforce Object ID',
  `username` varchar(255) NOT NULL,
  `auth_key` varchar(32) DEFAULT NULL,
  `password_hash` varchar(255) DEFAULT NULL,
  `password_reset_token` varchar(255) DEFAULT NULL,
  `email` varchar(255) NOT NULL,
  `phone` varchar(255) DEFAULT NULL,
  `associated_id` bigint(20) DEFAULT NULL,
  `role` varchar(24) DEFAULT NULL,
  `status` smallint(6) NOT NULL DEFAULT 10,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  `first_name` varchar(100) DEFAULT NULL,
  `last_name` varchar(100) DEFAULT NULL,
  `timezone` varchar(45) DEFAULT NULL,
  `verify_token` text DEFAULT NULL COMMENT 'column used for onboard signup',
  `email_verified` tinyint(2) DEFAULT 0 COMMENT 'column used for onboard signup',
  `password_hash2` varchar(255) DEFAULT NULL,
  `group` varchar(50) NOT NULL,
  `last_logged_in` date DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `settings` text DEFAULT NULL,
  `test` tinyint(1) NOT NULL DEFAULT 0,
  `created_by` int(11) DEFAULT NULL,
  `created_from` varchar(255) DEFAULT NULL,
  `terms_accepted` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `email` (`email`),
  UNIQUE KEY `password_reset_token` (`password_reset_token`),
  KEY `associated_id` (`associated_id`),
  KEY `fk_u_du` (`deleted_by`),
  KEY `fk_u_cu` (`created_by`),
  CONSTRAINT `fk_u_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_u_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=34892 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing_live.activation definition

CREATE TABLE `activation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `token` varchar(100) NOT NULL,
  `type` varchar(100) NOT NULL,
  `related_id` int(11) NOT NULL,
  `data` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_activation_cu` (`created_by`),
  CONSTRAINT `fk_activation_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=63514 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.additional_shift_pay definition

CREATE TABLE `additional_shift_pay` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `rate` decimal(5,2) NOT NULL,
  `start_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_asp_cu` (`created_by`),
  CONSTRAINT `fk_asp_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.attestation_question definition

CREATE TABLE `attestation_question` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `question` text NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `first_shift` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `fk_attestation_question_u` (`created_by`),
  CONSTRAINT `fk_attestation_question_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=88 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.auth_assignment definition

CREATE TABLE `auth_assignment` (
  `item_name` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `user_id` varchar(64) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `created_at` int(11) DEFAULT NULL,
  PRIMARY KEY (`item_name`,`user_id`),
  CONSTRAINT `auth_assignment_ibfk_1` FOREIGN KEY (`item_name`) REFERENCES `auth_item` (`name`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;


-- cstaffing_live.background definition

CREATE TABLE `background` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `severity` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_background` (`name`,`severity`),
  KEY `fk_bg_cu` (`created_by`),
  KEY `fk_bg_du` (`deleted_by`),
  CONSTRAINT `fk_bg_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_bg_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=45 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.billing_type definition

CREATE TABLE `billing_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_bt_cu` (`created_by`),
  CONSTRAINT `fk_bt_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.blocked_email definition

CREATE TABLE `blocked_email` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `email` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `created_from` varchar(255) DEFAULT 'USER_SETTINGS',
  PRIMARY KEY (`id`),
  KEY `fk_be_cu` (`created_by`),
  CONSTRAINT `fk_be_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=14140 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.company_info_item definition

CREATE TABLE `company_info_item` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `status` varchar(255) NOT NULL,
  `title` varchar(255) NOT NULL,
  `body` text NOT NULL,
  `created_by` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `weight` int(11) NOT NULL DEFAULT 0,
  `interface` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_cii_u` (`created_by`),
  CONSTRAINT `fk_cii_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=58 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.del_position_group definition

CREATE TABLE `del_position_group` (
  `group_id` int(20) NOT NULL AUTO_INCREMENT,
  `group_name` varchar(255) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`group_id`),
  KEY `fk_pogr_du` (`deleted_by`),
  CONSTRAINT `fk_pogr_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=57 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.division definition

CREATE TABLE `division` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_division_cu` (`created_by`),
  CONSTRAINT `fk_division_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.document_type definition

CREATE TABLE `document_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `reportable` tinyint(3) NOT NULL DEFAULT 0,
  `label` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `type` varchar(255) NOT NULL,
  `visible_when` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_dt_cu` (`created_by`),
  CONSTRAINT `fk_dt_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=35 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_government definition

CREATE TABLE `employee_government` (
  `employee_document_id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `filename` varchar(150) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  `document_type_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`employee_document_id`),
  KEY `fk_eg_dt` (`document_type_id`),
  CONSTRAINT `fk_eg_dt` FOREIGN KEY (`document_type_id`) REFERENCES `document_type` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2330 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.grooming_tool definition

CREATE TABLE `grooming_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_grooming_tool_u` (`created_by`),
  KEY `fk_grooming_tool_du` (`deleted_by`),
  CONSTRAINT `fk_grooming_tool_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_grooming_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=647 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.health_screening definition

CREATE TABLE `health_screening` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_hs_u` (`created_by`),
  CONSTRAINT `fk_hs_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.history_entry definition

CREATE TABLE `history_entry` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `related` varchar(255) NOT NULL,
  `related_id` int(11) NOT NULL,
  `model` varchar(255) NOT NULL,
  `model_id` int(11) DEFAULT NULL,
  `changes` mediumtext DEFAULT NULL,
  `notes` varchar(255) DEFAULT NULL,
  `keys` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_history_entry_u` (`created_by`),
  CONSTRAINT `fk_history_entry_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2423781 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.history_entry_notification definition

CREATE TABLE `history_entry_notification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `history_entry_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `history_entry_ids` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_history_entry_noti_h` (`history_entry_id`),
  KEY `fk_history_entry_noti_u` (`created_by`),
  CONSTRAINT `fk_history_entry_noti_h` FOREIGN KEY (`history_entry_id`) REFERENCES `history_entry` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_history_entry_noti_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=22048 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.holiday definition

CREATE TABLE `holiday` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `date` date NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_hol_cu` (`created_by`),
  CONSTRAINT `fk_hol_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.job definition

CREATE TABLE `job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `data` text DEFAULT NULL,
  `queue_id` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `ended_at` timestamp NULL DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `params` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_job_u` (`created_by`),
  CONSTRAINT `fk_job_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=19065 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.late_fee_policy definition

CREATE TABLE `late_fee_policy` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_lfp_cu` (`created_by`),
  KEY `fk_lfp_du` (`deleted_by`),
  CONSTRAINT `fk_lfp_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_lfp_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.min_wage_rate_amount definition

CREATE TABLE `min_wage_rate_amount` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `min_wage_id` int(11) NOT NULL,
  `rate` decimal(5,2) NOT NULL,
  `start_date` datetime DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `note` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_mwra_u` (`created_by`),
  KEY `fk_mwra_mwr` (`min_wage_id`),
  CONSTRAINT `fk_mwra_mwr` FOREIGN KEY (`min_wage_id`) REFERENCES `min_wage_rate` (`min_wage_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_mwra_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=403 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.msp definition

CREATE TABLE `msp` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `rate` decimal(8,4) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `penalty_formula` varchar(255) NOT NULL DEFAULT 'BILL_RATE',
  `penalty_formula_multiplier` decimal(8,4) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_msp_cu` (`created_by`),
  CONSTRAINT `fk_msp_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.net_terms_entry definition

CREATE TABLE `net_terms_entry` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `net_terms` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_nte_cu` (`created_by`),
  CONSTRAINT `fk_nte_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.notification definition

CREATE TABLE `notification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `type` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `body` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `data` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `severity` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `attachments` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `published_at` timestamp NULL DEFAULT NULL,
  `processed_at` timestamp NULL DEFAULT NULL,
  `push_notification` tinyint(1) NOT NULL DEFAULT 0,
  `popup` tinyint(1) DEFAULT 0,
  `email` tinyint(1) DEFAULT 0,
  `global` tinyint(1) NOT NULL DEFAULT 0,
  `archived` tinyint(1) DEFAULT NULL,
  `processing_at` timestamp NULL DEFAULT NULL,
  `history` tinyint(3) NOT NULL DEFAULT 0,
  `long_body` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_noti_cru` (`created_by`),
  KEY `i_notification` (`global`),
  KEY `ix_notification_published_at` (`published_at`),
  KEY `ix_notification_processed_at` (`processed_at`),
  KEY `ix_notification_push_notification` (`push_notification`),
  KEY `ix_notification_email` (`email`),
  KEY `ix_notification_type` (`type`),
  KEY `ix_notification_archived` (`archived`),
  CONSTRAINT `fk_noti_cru` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=40463809 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


-- cstaffing_live.notification_addressee definition

CREATE TABLE `notification_addressee` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `notification_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_notification_addressee` (`notification_id`,`user_id`),
  KEY `fk_notiaddr_user` (`user_id`),
  CONSTRAINT `fk_notiaddr_noti` FOREIGN KEY (`notification_id`) REFERENCES `notification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_notiaddr_user` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=43086000 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.notification_seen definition

CREATE TABLE `notification_seen` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `notification_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_notification_seen` (`notification_id`,`user_id`),
  KEY `fk_nose_u` (`user_id`),
  CONSTRAINT `fk_nose_n` FOREIGN KEY (`notification_id`) REFERENCES `notification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_nose_u` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1501525 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.notification_seen_until definition

CREATE TABLE `notification_seen_until` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `until` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_notification_seen_until` (`user_id`),
  CONSTRAINT `fk_noseu_u` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3683 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.notification_subscription definition

CREATE TABLE `notification_subscription` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `status` varchar(255) NOT NULL,
  `user_id` int(11) NOT NULL,
  `device_token` varchar(255) NOT NULL,
  `device_uuid` varchar(255) NOT NULL,
  `device_name` varchar(255) NOT NULL,
  `device_platform` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_notification_subscription_token` (`device_token`),
  KEY `fk_nosu_u` (`user_id`),
  CONSTRAINT `fk_nosu_u` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=45689 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.other_work_type definition

CREATE TABLE `other_work_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `description` text DEFAULT NULL,
  `work_hours` decimal(5,2) NOT NULL DEFAULT 0.00,
  `rate` varchar(255) NOT NULL,
  `custom_rate` decimal(5,2) DEFAULT NULL,
  `cost` decimal(5,2) NOT NULL,
  `cost_description` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `non_work_hours` decimal(5,2) NOT NULL DEFAULT 0.00,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_owt_u` (`created_by`),
  KEY `fk_owt_du` (`deleted_by`),
  CONSTRAINT `fk_owt_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_owt_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.`position` definition

CREATE TABLE `position` (
  `position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` int(20) DEFAULT NULL,
  `description` varchar(100) NOT NULL,
  `rate` decimal(5,2) NOT NULL COMMENT 'This is the table responsible for the ',
  `uniform` text DEFAULT NULL,
  `tools` text DEFAULT NULL,
  `grooming_tools` text DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `visible_when` text DEFAULT NULL,
  `disable_timeclock_code` tinyint(3) NOT NULL DEFAULT 0,
  PRIMARY KEY (`position_id`),
  KEY `group_id` (`group_id`),
  KEY `fk_pos_du` (`deleted_by`),
  CONSTRAINT `fk_pos_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=323 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.position_grooming_tool definition

CREATE TABLE `position_grooming_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `grooming_tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_pos_gro` (`position_id`,`grooming_tool_id`),
  KEY `fk_position_grooming_tool_ti` (`grooming_tool_id`),
  KEY `fk_position_grooming_tool_u` (`created_by`),
  CONSTRAINT `fk_position_grooming_tool_pi` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_grooming_tool_ti` FOREIGN KEY (`grooming_tool_id`) REFERENCES `grooming_tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_grooming_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1371 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.position_material definition

CREATE TABLE `position_material` (
  `position_material_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `description` varchar(100) NOT NULL,
  `required` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`position_material_id`),
  KEY `fk_material_position_idx` (`position_id`),
  CONSTRAINT `fk_material_position` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.registration_position definition

CREATE TABLE `registration_position` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `description` (`description`),
  KEY `fk_rp_cu` (`created_by`),
  CONSTRAINT `fk_rp_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.registration_position_position definition

CREATE TABLE `registration_position_position` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `registration_position_id` int(11) NOT NULL,
  `position_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_rpp_rp` (`registration_position_id`),
  KEY `fk_rpp_po` (`position_id`),
  CONSTRAINT `fk_rpp_po` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_rpp_rp` FOREIGN KEY (`registration_position_id`) REFERENCES `registration_position` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.state definition

CREATE TABLE `state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `user_created` int(11) DEFAULT NULL,
  `date_updated` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_state_userc` (`user_created`),
  CONSTRAINT `fk_state_userc` FOREIGN KEY (`user_created`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=61 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.status_reason definition

CREATE TABLE `status_reason` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(255) NOT NULL,
  `reason` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `parent_id` int(11) DEFAULT NULL,
  `visible_when` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_sr_cu` (`created_by`),
  KEY `fk_sr_parent` (`parent_id`),
  CONSTRAINT `fk_sr_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_sr_parent` FOREIGN KEY (`parent_id`) REFERENCES `status_reason` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=257 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.system_notification_address definition

CREATE TABLE `system_notification_address` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `notification` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `options` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_sna_cu` (`created_by`),
  KEY `uq_sys_no_add` (`notification`),
  KEY `uq_system_notification_address` (`notification`,`email`),
  CONSTRAINT `fk_sna_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=193 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.timesheet_close definition

CREATE TABLE `timesheet_close` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `from` date NOT NULL,
  `to` date NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_tc_cu` (`created_by`),
  CONSTRAINT `fk_tc_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=196 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.tool definition

CREATE TABLE `tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_tool_u` (`created_by`),
  KEY `fk_tool_du` (`deleted_by`),
  CONSTRAINT `fk_tool_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=70 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.touch definition

CREATE TABLE `touch` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `object` varchar(255) NOT NULL,
  `object_id` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_touch_u` (`user_id`),
  CONSTRAINT `fk_touch_u` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=254113 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.travel_rate definition

CREATE TABLE `travel_rate` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pay_rate` decimal(5,2) DEFAULT NULL,
  `bill_rate` decimal(5,2) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_tr_cu` (`created_by`),
  CONSTRAINT `fk_tr_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.tutorial definition

CREATE TABLE `tutorial` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `status` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `title` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `content3` mediumtext CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `interface` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `categories` text CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `tags` text CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `popup` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `skippable` tinyint(1) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `content` mediumtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_tu_cu` (`created_by`),
  CONSTRAINT `fk_tu_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=82 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


-- cstaffing_live.tutorial_seen definition

CREATE TABLE `tutorial_seen` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tutorial_id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `fk_tuse_tu` (`tutorial_id`),
  KEY `fk_tuse_u` (`user_id`),
  CONSTRAINT `fk_tuse_tu` FOREIGN KEY (`tutorial_id`) REFERENCES `tutorial` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_tuse_u` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=21217 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.uniform definition

CREATE TABLE `uniform` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `images` text DEFAULT NULL,
  `questions` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_uniform_u` (`created_by`),
  KEY `fk_uniform_du` (`deleted_by`),
  CONSTRAINT `fk_uniform_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_uniform_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=269 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.certification definition

CREATE TABLE `certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `document_required` tinyint(1) NOT NULL DEFAULT 0,
  `number_required` tinyint(1) NOT NULL DEFAULT 0,
  `approval_required` tinyint(1) NOT NULL DEFAULT 0,
  `can_expire` tinyint(1) NOT NULL DEFAULT 0,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `required` varchar(255) NOT NULL DEFAULT 'AS_SPECIFIED',
  `has_effective_date` tinyint(1) NOT NULL DEFAULT 0,
  `has_max_allowed_months` tinyint(1) NOT NULL DEFAULT 0,
  `minimum_days_of_hire` int(11) DEFAULT NULL,
  `instructions` text DEFAULT NULL,
  `effective_date` date DEFAULT NULL,
  `reportable` tinyint(3) NOT NULL DEFAULT 0,
  `issued_at_required` tinyint(1) NOT NULL DEFAULT 0,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `visible_when` text DEFAULT NULL,
  `set_it_on_profile` tinyint(1) NOT NULL DEFAULT 1,
  `other_work_type_id` int(11) DEFAULT NULL,
  `state` varchar(255) DEFAULT NULL,
  `minimum_days_of_1st_shift` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_certification_u` (`created_by`),
  KEY `fk_cert_du` (`deleted_by`),
  KEY `fk_certification_owt` (`other_work_type_id`),
  CONSTRAINT `fk_cert_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_certification_owt` FOREIGN KEY (`other_work_type_id`) REFERENCES `other_work_type` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=73 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client definition

CREATE TABLE `client` (
  `client_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `staff_id` bigint(20) DEFAULT NULL COMMENT 'This is the main client information.  Each client can be assigned to a staff member.\n',
  `other_id` varchar(45) DEFAULT NULL,
  `sf_id` varchar(100) DEFAULT NULL COMMENT 'Salesforce Object ID',
  `sf_user_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Salesforce Object USER ID',
  `name` varchar(255) NOT NULL,
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
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `status` tinyint(4) DEFAULT 1,
  `min_wage_id` int(11) DEFAULT NULL,
  `workers_comp` varchar(50) DEFAULT NULL,
  `payment_type` tinyint(4) DEFAULT NULL,
  `cc_expiry_month` varchar(2) DEFAULT NULL,
  `cc_expiry_year` varchar(4) DEFAULT NULL,
  `pay_notes` text DEFAULT NULL,
  `wc_id` int(100) DEFAULT NULL,
  `invoiced` tinyint(2) DEFAULT 0,
  `proceed` tinyint(2) DEFAULT 0,
  `visible_tattoos_allow` int(10) DEFAULT NULL,
  `separate_venue` tinyint(2) NOT NULL DEFAULT 0,
  `discount` decimal(8,2) DEFAULT NULL,
  `discount_vaild_date` date DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `c19_vaccine` tinyint(1) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `invoice_name` varchar(255) DEFAULT NULL,
  `payroll_hours_reminder_enabled` tinyint(1) NOT NULL DEFAULT 1,
  `industry` varchar(255) DEFAULT NULL,
  `industry_other` varchar(255) DEFAULT NULL,
  `msa_expiration` date DEFAULT NULL,
  `created_by` int(11) DEFAULT NULL,
  `msp_id` int(11) DEFAULT NULL,
  `invoice_no` int(11) DEFAULT NULL,
  `division_id` int(11) DEFAULT NULL,
  `cancellation_deadline` int(11) DEFAULT 24,
  `invoices_offset` int(11) NOT NULL DEFAULT 0,
  `won_date` date DEFAULT NULL,
  `surcharge_deadline` int(11) DEFAULT 24,
  `sales_executive_id` int(11) DEFAULT NULL,
  `auto_confirm` tinyint(3) NOT NULL DEFAULT 0,
  `cancellation_deadline_pay` int(11) NOT NULL DEFAULT 24,
  `background` tinyint(3) DEFAULT NULL,
  `background_query_pending` tinyint(3) NOT NULL DEFAULT 0,
  `net_terms_entry_id` int(11) DEFAULT NULL,
  `bundle` varchar(255) NOT NULL DEFAULT 'BASIC',
  `no_break_penalty` tinyint(3) NOT NULL DEFAULT 1,
  `credit_card_expiration` varchar(255) DEFAULT NULL,
  `exposure_limit` varchar(255) DEFAULT NULL,
  `credit_card_authorization_date` varchar(255) DEFAULT NULL,
  `markup` varchar(255) DEFAULT NULL,
  `billing_type_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`client_id`),
  KEY `min_wage_id` (`min_wage_id`),
  KEY `fk_emp_du` (`deleted_by`),
  KEY `fk_client_cu` (`created_by`),
  KEY `fk_cl_msp` (`msp_id`),
  KEY `fk_cl_division` (`division_id`),
  KEY `fk_cl_se` (`sales_executive_id`),
  KEY `fk_cli_nte` (`net_terms_entry_id`),
  KEY `fk_client_bt` (`billing_type_id`),
  CONSTRAINT `fk_cl_division` FOREIGN KEY (`division_id`) REFERENCES `division` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cl_msp` FOREIGN KEY (`msp_id`) REFERENCES `msp` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cl_se` FOREIGN KEY (`sales_executive_id`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cli_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cli_nte` FOREIGN KEY (`net_terms_entry_id`) REFERENCES `net_terms_entry` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_client_bt` FOREIGN KEY (`billing_type_id`) REFERENCES `billing_type` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_client_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_emp_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1820 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_background definition

CREATE TABLE `client_background` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `background_id` int(11) NOT NULL,
  `client_id` bigint(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_client_background` (`background_id`,`client_id`),
  KEY `fk_cbg_client` (`client_id`),
  KEY `fk_cbg_cu` (`created_by`),
  CONSTRAINT `fk_cbg_bg` FOREIGN KEY (`background_id`) REFERENCES `background` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cbg_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cbg_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5383 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_certification definition

CREATE TABLE `client_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_client_certification` (`client_id`,`certification_id`),
  KEY `fk_client_certification_ti` (`certification_id`),
  KEY `fk_client_certification_u` (`created_by`),
  CONSTRAINT `fk_client_certification_pi` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_contact definition

CREATE TABLE `client_contact` (
  `client_contact_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `sf_id` varchar(100) DEFAULT NULL,
  `name` varchar(100) NOT NULL,
  `title` varchar(100) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  `phone` varchar(20) DEFAULT NULL,
  `mobile` varchar(20) DEFAULT NULL,
  `office` varchar(20) DEFAULT NULL,
  `ext` varchar(20) DEFAULT NULL,
  `timesheet` tinyint(4) DEFAULT NULL,
  `invoicing` tinyint(4) DEFAULT NULL,
  `sort_order` int(10) unsigned DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `preferred_method` varchar(255) DEFAULT NULL,
  `preferred` tinyint(3) NOT NULL DEFAULT 0,
  `accounts_receivable` tinyint(1) NOT NULL DEFAULT 0,
  `created_by` int(11) DEFAULT NULL,
  `phone_extension` varchar(255) DEFAULT NULL,
  `mobile_extension` varchar(255) DEFAULT NULL,
  `office_extension` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`client_contact_id`),
  KEY `fk_client_contact_client_idx` (`client_id`),
  KEY `fk_client_contact_cu` (`created_by`),
  CONSTRAINT `fk_client_contact_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`),
  CONSTRAINT `fk_client_contact_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5895 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_document definition

CREATE TABLE `client_document` (
  `client_doc_id` bigint(50) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(50) NOT NULL,
  `user_id` bigint(50) NOT NULL,
  `filename` varchar(150) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `document_type_id` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`client_doc_id`),
  KEY `client_id` (`client_id`),
  KEY `user_id` (`user_id`),
  KEY `fk_cd_dt` (`document_type_id`),
  KEY `fk_cldo_du` (`deleted_by`),
  CONSTRAINT `fk_cd_dt` FOREIGN KEY (`document_type_id`) REFERENCES `document_type` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cldo_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2948 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_holiday definition

CREATE TABLE `client_holiday` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `holiday_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_ch_cl` (`client_id`),
  KEY `fk_ch_hol` (`holiday_id`),
  KEY `fk_ch_cu` (`created_by`),
  CONSTRAINT `fk_ch_cl` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ch_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ch_hol` FOREIGN KEY (`holiday_id`) REFERENCES `holiday` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_late_fee definition

CREATE TABLE `client_late_fee` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `late_fee_policy_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_clf_client` (`client_id`),
  KEY `fk_clf_lfp` (`late_fee_policy_id`),
  KEY `fk_clf_cu` (`created_by`),
  CONSTRAINT `fk_clf_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_clf_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_clf_lfp` FOREIGN KEY (`late_fee_policy_id`) REFERENCES `late_fee_policy` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_multiplier definition

CREATE TABLE `client_multiplier` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `ot` decimal(5,2) DEFAULT NULL,
  `dt` decimal(5,2) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_cm_c` (`client_id`),
  KEY `fk_cm_cu` (`created_by`),
  CONSTRAINT `fk_cm_c` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cm_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_note definition

CREATE TABLE `client_note` (
  `client_note_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `note` text DEFAULT NULL,
  `date` timestamp NULL DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`client_note_id`),
  KEY `fk_account_key_idx` (`client_id`),
  CONSTRAINT `fk_client_note_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=539 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_position definition

CREATE TABLE `client_position` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) DEFAULT NULL,
  `del_group_id` int(20) DEFAULT NULL,
  `position_id` bigint(20) DEFAULT NULL,
  `del_standard_rate` tinyint(2) NOT NULL DEFAULT 0,
  `del_tip_rate` decimal(5,2) DEFAULT NULL,
  `del_pay_rate` decimal(5,2) DEFAULT NULL,
  `del_bill_rate` decimal(5,2) DEFAULT NULL,
  `del_surcharge` decimal(5,2) DEFAULT NULL,
  `del_description` text DEFAULT NULL,
  `del_uniform_types` varchar(255) DEFAULT NULL,
  `del_position_requirement` text DEFAULT NULL,
  `del_date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_client_position` (`client_id`,`position_id`),
  KEY `client_id` (`client_id`),
  KEY `position_id` (`position_id`),
  KEY `fk_cp_cu` (`created_by`),
  CONSTRAINT `fk_cp_c` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_cp_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cp_p` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=256613 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_position_amount definition

CREATE TABLE `client_position_amount` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_position_id` bigint(20) NOT NULL,
  `pay_rate` decimal(5,2) NOT NULL,
  `bill_rate` decimal(5,2) NOT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_cpa_ci` (`client_position_id`),
  KEY `fk_cpa_cu` (`created_by`),
  CONSTRAINT `fk_cpa_ci` FOREIGN KEY (`client_position_id`) REFERENCES `client_position` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_cpa_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=335189 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_position_certification definition

CREATE TABLE `client_position_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_position_id` bigint(20) NOT NULL,
  `del_client_id` bigint(20) DEFAULT NULL,
  `del_position_id` bigint(20) DEFAULT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_client_position_certification_c` (`del_client_id`),
  KEY `fk_client_position_certification_pi` (`del_position_id`),
  KEY `fk_client_position_certification_ti` (`certification_id`),
  KEY `fk_client_position_certification_u` (`created_by`),
  KEY `fk_cpc_cpi` (`client_position_id`),
  CONSTRAINT `fk_client_position_certification_c` FOREIGN KEY (`del_client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_certification_pi` FOREIGN KEY (`del_position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cpc_cpi` FOREIGN KEY (`client_position_id`) REFERENCES `client_position` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=293 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_position_grooming_tool definition

CREATE TABLE `client_position_grooming_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_position_id` bigint(20) NOT NULL,
  `del_client_id` bigint(20) DEFAULT NULL,
  `del_position_id` bigint(20) DEFAULT NULL,
  `grooming_tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_cli_pos_gro` (`del_client_id`,`del_position_id`,`grooming_tool_id`),
  KEY `fk_client_position_grooming_tool_pi` (`del_position_id`),
  KEY `fk_client_position_grooming_tool_ti` (`grooming_tool_id`),
  KEY `fk_client_position_grooming_tool_u` (`created_by`),
  KEY `fk_cpgt_cpi` (`client_position_id`),
  CONSTRAINT `fk_client_position_grooming_tool_c` FOREIGN KEY (`del_client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_grooming_tool_pi` FOREIGN KEY (`del_position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_grooming_tool_ti` FOREIGN KEY (`grooming_tool_id`) REFERENCES `grooming_tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_grooming_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cpgt_cpi` FOREIGN KEY (`client_position_id`) REFERENCES `client_position` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2309 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_position_tool definition

CREATE TABLE `client_position_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_position_id` bigint(20) NOT NULL,
  `del_client_id` bigint(20) DEFAULT NULL,
  `del_position_id` bigint(20) DEFAULT NULL,
  `tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_cli_pos_too` (`del_client_id`,`del_position_id`,`tool_id`),
  KEY `fk_client_position_tool_pi` (`del_position_id`),
  KEY `fk_client_position_tool_ti` (`tool_id`),
  KEY `fk_client_position_tool_u` (`created_by`),
  KEY `fk_cpt_cpi` (`client_position_id`),
  CONSTRAINT `fk_client_position_tool_c` FOREIGN KEY (`del_client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_tool_pi` FOREIGN KEY (`del_position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_tool_ti` FOREIGN KEY (`tool_id`) REFERENCES `tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cpt_cpi` FOREIGN KEY (`client_position_id`) REFERENCES `client_position` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1437 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_position_uniform definition

CREATE TABLE `client_position_uniform` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_position_id` bigint(20) NOT NULL,
  `del_client_id` bigint(20) DEFAULT NULL,
  `del_position_id` bigint(20) DEFAULT NULL,
  `uniform_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_cli_pos_uni` (`del_client_id`,`del_position_id`,`uniform_id`),
  KEY `fk_client_position_uniform_pi` (`del_position_id`),
  KEY `fk_client_position_uniform_ti` (`uniform_id`),
  KEY `fk_client_position_uniform_u` (`created_by`),
  KEY `fk_cpu_cpi` (`client_position_id`),
  CONSTRAINT `fk_client_position_uniform_c` FOREIGN KEY (`del_client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_uniform_pi` FOREIGN KEY (`del_position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_uniform_ti` FOREIGN KEY (`uniform_id`) REFERENCES `uniform` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_client_position_uniform_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cpu_cpi` FOREIGN KEY (`client_position_id`) REFERENCES `client_position` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2598 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.county definition

CREATE TABLE `county` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `state_id` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `user_created` int(11) DEFAULT NULL,
  `date_updated` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_county_userc` (`user_created`),
  KEY `fk_county_state` (`state_id`),
  CONSTRAINT `fk_county_state` FOREIGN KEY (`state_id`) REFERENCES `state` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_county_userc` FOREIGN KEY (`user_created`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3217 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.default_employee_position definition

CREATE TABLE `default_employee_position` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `eligible` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `fk_dep_cu` (`created_by`),
  KEY `fk_dep_po` (`position_id`),
  CONSTRAINT `fk_dep_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_dep_po` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee definition

CREATE TABLE `employee` (
  `employee_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `first_name` varchar(45) NOT NULL,
  `last_name` varchar(45) NOT NULL,
  `dob` date DEFAULT NULL,
  `email` varchar(255) NOT NULL,
  `start_date` date DEFAULT NULL,
  `payroll_id` varchar(32) DEFAULT NULL,
  `address1` varchar(100) DEFAULT NULL,
  `address2` varchar(100) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `state` varchar(100) DEFAULT NULL,
  `zip` varchar(25) DEFAULT NULL,
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
  `flag` tinyint(2) DEFAULT NULL,
  `interview_notes` text DEFAULT NULL,
  `fhc` varchar(150) DEFAULT NULL,
  `fhc_expiry` date DEFAULT NULL,
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `restrict_to_exclusive` tinyint(4) NOT NULL DEFAULT 0,
  `i9_completed` tinyint(4) DEFAULT 0,
  `created_on` datetime DEFAULT NULL,
  `updated_on` datetime DEFAULT NULL,
  `ssn` varchar(255) DEFAULT NULL,
  `orientation_schedule_date` datetime DEFAULT NULL,
  `orientation_complete_date` date DEFAULT NULL,
  `pp_mailing_list` tinyint(4) unsigned DEFAULT 0 COMMENT 'Permanent Paycheck Mailing List',
  `uniforms_complete` tinyint(4) DEFAULT NULL,
  `uniforms_missing` text DEFAULT NULL,
  `howheard` tinyint(4) DEFAULT NULL,
  `expected_rate` varchar(10) DEFAULT NULL,
  `smartphone` tinyint(4) DEFAULT NULL,
  `transportation` int(11) DEFAULT NULL,
  `trans_other` varchar(50) DEFAULT NULL,
  `referred_date` date DEFAULT NULL,
  `referred_by` varchar(255) DEFAULT NULL,
  `interview_date` date DEFAULT NULL,
  `interviewer` int(11) DEFAULT NULL,
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
  `resume` varchar(200) DEFAULT NULL,
  `other_status` int(50) DEFAULT NULL,
  `background_date` date DEFAULT NULL,
  `county_id` int(11) DEFAULT NULL,
  `c19_vaccine` tinyint(1) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `start_date2` date DEFAULT NULL,
  `recruited_by` int(11) DEFAULT NULL,
  `terminated_at` datetime DEFAULT NULL,
  `activated_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `background` tinyint(3) DEFAULT NULL,
  `background_query` int(11) NOT NULL DEFAULT 0,
  `created_from` varchar(255) DEFAULT NULL,
  `work_area_address1` varchar(255) DEFAULT NULL,
  `work_area_address2` varchar(255) DEFAULT NULL,
  `work_area_city` varchar(255) DEFAULT NULL,
  `work_area_state` varchar(255) DEFAULT NULL,
  `work_area_zip` varchar(255) DEFAULT NULL,
  `work_area_latitude` varchar(20) DEFAULT NULL,
  `work_area_longitude` varchar(20) DEFAULT NULL,
  `work_area_unit` varchar(255) DEFAULT NULL,
  `work_area_distance` float DEFAULT NULL,
  `work_area_review` tinyint(1) NOT NULL DEFAULT 0,
  `min_wage_id` int(11) DEFAULT NULL,
  `region` varchar(255) NOT NULL DEFAULT 'NORTHERN_CALIFORNIA',
  `status_reason` varchar(255) DEFAULT NULL,
  `concierge_date` date DEFAULT NULL,
  PRIMARY KEY (`employee_id`),
  UNIQUE KEY `uix_employee_email` (`email`),
  UNIQUE KEY `uix_employee_ssn` (`ssn`),
  KEY `fk_employee_county` (`county_id`),
  KEY `fk_employee_recu` (`recruited_by`),
  KEY `fk_employee_refe` (`referred_by`),
  KEY `fk_employee_intu` (`interviewer`),
  KEY `fk_employee_mwr` (`min_wage_id`),
  CONSTRAINT `fk_employee_county` FOREIGN KEY (`county_id`) REFERENCES `county` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_intu` FOREIGN KEY (`interviewer`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_mwr` FOREIGN KEY (`min_wage_id`) REFERENCES `min_wage_rate` (`min_wage_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_recu` FOREIGN KEY (`recruited_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=44207 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_background definition

CREATE TABLE `employee_background` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `background_id` int(11) NOT NULL,
  `employee_id` bigint(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_employee_background` (`background_id`,`employee_id`),
  KEY `fk_ebg_emp` (`employee_id`),
  KEY `fk_ebg_cu` (`created_by`),
  CONSTRAINT `fk_ebg_bg` FOREIGN KEY (`background_id`) REFERENCES `background` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ebg_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ebg_emp` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3380 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_certification definition

CREATE TABLE `employee_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `certification_id` int(11) NOT NULL,
  `file` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `number` varchar(255) DEFAULT NULL,
  `expires_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `approved_at` timestamp NULL DEFAULT NULL,
  `approved_by` int(11) DEFAULT NULL,
  `issued_at` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_employee_certification` (`employee_id`,`certification_id`),
  KEY `fk_employee_certification_ti` (`certification_id`),
  KEY `fk_employee_certification_u` (`created_by`),
  KEY `fk_employee_certification_au` (`approved_by`),
  CONSTRAINT `fk_employee_certification_au` FOREIGN KEY (`approved_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_certification_pi` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=70809 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_clock_code definition

CREATE TABLE `employee_clock_code` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `msp_id` int(11) NOT NULL,
  `employee_id` bigint(20) NOT NULL,
  `code` varchar(100) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_ecc_m` (`msp_id`),
  KEY `fk_ecc_e` (`employee_id`),
  KEY `fk_ecc_u` (`created_by`),
  CONSTRAINT `fk_ecc_e` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ecc_m` FOREIGN KEY (`msp_id`) REFERENCES `msp` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ecc_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=470 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_experience definition

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


-- cstaffing_live.employee_grooming_tool definition

CREATE TABLE `employee_grooming_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `grooming_tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_emp_pos_gro` (`employee_id`,`grooming_tool_id`),
  KEY `fk_employee_grooming_tool_ti` (`grooming_tool_id`),
  KEY `fk_employee_grooming_tool_u` (`created_by`),
  CONSTRAINT `fk_employee_grooming_tool_pi` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_grooming_tool_ti` FOREIGN KEY (`grooming_tool_id`) REFERENCES `grooming_tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_grooming_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=170524 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_health_screening definition

CREATE TABLE `employee_health_screening` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `health_screening_id` int(11) NOT NULL,
  `date` date NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `approved_at` timestamp NULL DEFAULT NULL,
  `approved_by` int(11) DEFAULT NULL,
  `file` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_ehs_e` (`employee_id`),
  KEY `fk_ehs_hsi` (`health_screening_id`),
  KEY `fk_ehs_u` (`created_by`),
  KEY `fk_ehs_au` (`approved_by`),
  CONSTRAINT `fk_ehs_au` FOREIGN KEY (`approved_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ehs_e` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ehs_hsi` FOREIGN KEY (`health_screening_id`) REFERENCES `health_screening` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ehs_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2835 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_language definition

CREATE TABLE `employee_language` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `language_id` bigint(20) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `user_created` int(11) DEFAULT NULL,
  `date_updated` timestamp NULL DEFAULT NULL,
  `preferred` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix` (`employee_id`,`language_id`),
  KEY `fk_employee_language_language` (`language_id`),
  KEY `fk_employee_language_userc` (`user_created`),
  CONSTRAINT `fk_employee_language_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_language_language` FOREIGN KEY (`language_id`) REFERENCES `language` (`language_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_language_userc` FOREIGN KEY (`user_created`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=20728 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_other_work definition

CREATE TABLE `employee_other_work` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `other_work_type_id` int(11) NOT NULL,
  `work_hours` decimal(5,2) NOT NULL DEFAULT 0.00,
  `cost` decimal(5,2) NOT NULL,
  `notes` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `rate` decimal(12,2) NOT NULL,
  `non_work_hours` decimal(5,2) NOT NULL DEFAULT 0.00,
  `date` date NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_eow_u` (`created_by`),
  KEY `fk_eow_owt` (`other_work_type_id`),
  KEY `fk_eow_emp` (`employee_id`),
  CONSTRAINT `fk_eow_emp` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_eow_owt` FOREIGN KEY (`other_work_type_id`) REFERENCES `other_work_type` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_eow_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=11460 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_position definition

CREATE TABLE `employee_position` (
  `employee_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `position_id` bigint(20) NOT NULL,
  `sub_type_id` bigint(20) DEFAULT -1,
  `level` tinyint(4) DEFAULT NULL COMMENT 'This field allows the staff to rate the employees on their positions.',
  `rate` decimal(5,2) DEFAULT NULL,
  `status` tinyint(3) NOT NULL DEFAULT 1,
  `eligible` tinyint(3) NOT NULL DEFAULT 0,
  PRIMARY KEY (`employee_position_id`),
  UNIQUE KEY `uq_employee_position_1` (`employee_id`,`position_id`),
  KEY `fk_position_position_idx` (`position_id`),
  KEY `fk_employee_employee_idx` (`employee_id`),
  KEY `ix_employee_position_status` (`status`),
  CONSTRAINT `fk_employee_position_position` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=520297 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_tool definition

CREATE TABLE `employee_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_emp_pos_too` (`employee_id`,`tool_id`),
  KEY `fk_employee_tool_ti` (`tool_id`),
  KEY `fk_employee_tool_u` (`created_by`),
  CONSTRAINT `fk_employee_tool_pi` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_tool_ti` FOREIGN KEY (`tool_id`) REFERENCES `tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=174789 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_uniform definition

CREATE TABLE `employee_uniform` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `uniform_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_emp_pos_uni` (`employee_id`,`uniform_id`),
  KEY `fk_employee_uniform_ti` (`uniform_id`),
  KEY `fk_employee_uniform_u` (`created_by`),
  CONSTRAINT `fk_employee_uniform_pi` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_uniform_ti` FOREIGN KEY (`uniform_id`) REFERENCES `uniform` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_employee_uniform_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=334350 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.position_certification definition

CREATE TABLE `position_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_position_certification_pi` (`position_id`),
  KEY `fk_position_certification_ti` (`certification_id`),
  KEY `fk_position_certification_u` (`created_by`),
  CONSTRAINT `fk_position_certification_pi` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.position_tool definition

CREATE TABLE `position_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_pos_too` (`position_id`,`tool_id`),
  KEY `fk_position_tool_ti` (`tool_id`),
  KEY `fk_position_tool_u` (`created_by`),
  CONSTRAINT `fk_position_tool_pi` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_tool_ti` FOREIGN KEY (`tool_id`) REFERENCES `tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1071 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.position_uniform definition

CREATE TABLE `position_uniform` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `position_id` bigint(20) NOT NULL,
  `uniform_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_pos_uni` (`position_id`,`uniform_id`),
  KEY `fk_position_uniform_ti` (`uniform_id`),
  KEY `fk_position_uniform_u` (`created_by`),
  CONSTRAINT `fk_position_uniform_pi` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_uniform_ti` FOREIGN KEY (`uniform_id`) REFERENCES `uniform` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_position_uniform_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1997 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.push_notifications definition

CREATE TABLE `push_notifications` (
  `id` bigint(50) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(50) NOT NULL,
  `data` blob DEFAULT NULL,
  `date_created` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `employee_id` (`employee_id`),
  CONSTRAINT `fk_employee_push_notification` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=966487 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_employee_certification definition

CREATE TABLE `shift_employee_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_shift_employee_certification_pi` (`employee_id`),
  KEY `fk_shift_employee_certification_ti` (`certification_id`),
  KEY `fk_shift_employee_certification_u` (`created_by`),
  CONSTRAINT `fk_shift_employee_certification_pi` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_employee_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_employee_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.sick_wage_employee definition

CREATE TABLE `sick_wage_employee` (
  `sick_wage_employee_id` bigint(50) NOT NULL AUTO_INCREMENT,
  `sick_wage_pay_id` bigint(50) NOT NULL,
  `employee_id` bigint(50) NOT NULL,
  PRIMARY KEY (`sick_wage_employee_id`),
  KEY `sick_wage_pay_id` (`sick_wage_pay_id`),
  KEY `employee_id` (`employee_id`),
  CONSTRAINT `fk_sick_wage_pay` FOREIGN KEY (`sick_wage_pay_id`) REFERENCES `sick_wage_pay` (`sick_wage_pay_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_sick_wage_pay_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=1785 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.user_party definition

CREATE TABLE `user_party` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `_party_id` bigint(20) DEFAULT NULL,
  `employee_id` bigint(20) DEFAULT NULL,
  `client_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_user_party_employee` (`user_id`,`employee_id`),
  UNIQUE KEY `uix_user_party_client` (`user_id`,`client_id`),
  KEY `fk_up_e` (`employee_id`),
  KEY `fk_up_c` (`client_id`),
  CONSTRAINT `fk_up_c` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_up_e` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON UPDATE CASCADE,
  CONSTRAINT `user_party_user` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=32734 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue definition

CREATE TABLE `venue` (
  `venue_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `sf_id` varchar(100) DEFAULT NULL COMMENT 'Salesforce Object ID',
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
  `latitude` varchar(20) DEFAULT NULL,
  `longitude` varchar(20) DEFAULT NULL,
  `admin_notes` text DEFAULT NULL,
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
  `travel_pay` decimal(5,2) DEFAULT NULL,
  `service_charge` decimal(5,2) DEFAULT 0.00,
  `factored` tinyint(2) DEFAULT NULL,
  `venue_code` varchar(50) DEFAULT NULL,
  `county_id` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `invoice_name` varchar(255) DEFAULT NULL,
  `status` tinyint(3) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `timeclock` varchar(255) NOT NULL DEFAULT 'DISABLED',
  `timeclock_code_holder` varchar(255) DEFAULT NULL,
  `timeclock_tolerance` tinyint(3) DEFAULT NULL,
  `timeclock_prestart_interval` tinyint(3) DEFAULT NULL,
  `timeclock_limit` int(11) NOT NULL DEFAULT 0,
  `min_wage_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`venue_id`),
  KEY `fk_account_client_idx` (`client_id`),
  KEY `fk_venue_county` (`county_id`),
  KEY `fk_ven_du` (`deleted_by`),
  KEY `staffing_manager_id` (`staffing_manager_id`),
  KEY `fk_venue_cu` (`created_by`),
  KEY `fk_venue_mwr` (`min_wage_id`),
  CONSTRAINT `fk_ven_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_county` FOREIGN KEY (`county_id`) REFERENCES `county` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_mwr` FOREIGN KEY (`min_wage_id`) REFERENCES `min_wage_rate` (`min_wage_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_to_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2171 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_attestation_question definition

CREATE TABLE `venue_attestation_question` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `attestation_question_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_venue_attestation_question_c` (`client_id`),
  KEY `fk_venue_attestation_question_v` (`venue_id`),
  KEY `fk_venue_attestation_question_aq` (`attestation_question_id`),
  KEY `fk_venue_attestation_question_u` (`created_by`),
  CONSTRAINT `fk_venue_attestation_question_aq` FOREIGN KEY (`attestation_question_id`) REFERENCES `attestation_question` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_attestation_question_c` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_attestation_question_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_attestation_question_v` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=652 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_certification definition

CREATE TABLE `venue_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `effective_date` date DEFAULT NULL,
  `max_allowed_months` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_venue_certification` (`venue_id`,`certification_id`),
  KEY `fk_venue_certification_ti` (`certification_id`),
  KEY `fk_venue_certification_u` (`created_by`),
  KEY `fk_venue_certification_cl` (`client_id`),
  CONSTRAINT `fk_venue_certification_cl` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_certification_pi` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1971 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_contact definition

CREATE TABLE `venue_contact` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `client_contact_id` bigint(20) NOT NULL,
  `created_by` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `preferred` tinyint(3) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_venue_contact` (`venue_id`,`client_contact_id`),
  KEY `fk_vc_cc` (`client_contact_id`),
  CONSTRAINT `fk_vc_cc` FOREIGN KEY (`client_contact_id`) REFERENCES `client_contact` (`client_contact_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_vc_v` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5448 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_document definition

CREATE TABLE `venue_document` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `filename` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `document_type_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_vdoc_ve` (`venue_id`),
  KEY `fk_vdoc_du` (`deleted_by`),
  KEY `fk_vdoc_cu` (`created_by`),
  KEY `fk_vd_dt` (`document_type_id`),
  CONSTRAINT `fk_vd_dt` FOREIGN KEY (`document_type_id`) REFERENCES `document_type` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_vdoc_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_vdoc_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_vdoc_ve` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=454 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_language definition

CREATE TABLE `venue_language` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `language_id` bigint(20) NOT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  `user_created` int(11) DEFAULT NULL,
  `date_updated` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix` (`venue_id`,`language_id`),
  KEY `fk_venue_language_language` (`language_id`),
  KEY `fk_venue_language_userc` (`user_created`),
  CONSTRAINT `fk_venue_language_language` FOREIGN KEY (`language_id`) REFERENCES `language` (`language_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_language_userc` FOREIGN KEY (`user_created`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_language_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3940 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_picture definition

CREATE TABLE `venue_picture` (
  `venue_picture_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `picture` varchar(100) NOT NULL,
  PRIMARY KEY (`venue_picture_id`),
  KEY `fk_venue_picture_to_venue_idx` (`venue_id`),
  CONSTRAINT `fk_venue_picture_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=105 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_position definition

CREATE TABLE `venue_position` (
  `venue_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `venue_id` bigint(20) NOT NULL,
  `del_client_id` bigint(20) NOT NULL,
  `del_group_id` int(11) DEFAULT NULL,
  `position_id` bigint(20) NOT NULL,
  `del_sub_type_id` bigint(20) DEFAULT NULL,
  `del_gender` varchar(20) DEFAULT NULL,
  `del_rate` decimal(5,2) DEFAULT NULL,
  `del_surcharge` decimal(5,2) DEFAULT NULL,
  `del_bill_rate` decimal(5,2) DEFAULT NULL,
  `del_exclusive` tinyint(4) NOT NULL DEFAULT 0,
  `description` text DEFAULT NULL,
  `del_use_standard_rate` tinyint(4) DEFAULT 0,
  `del_uniform_types` varchar(255) DEFAULT NULL,
  `del_uniform` text DEFAULT NULL,
  `del_tools` text DEFAULT NULL,
  `del_grooming_tools` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`venue_position_id`),
  UNIQUE KEY `uq_venue_position` (`venue_id`,`position_id`),
  KEY `fk_position_dept_idx` (`venue_id`),
  KEY `fk_vposition_to_position` (`position_id`),
  KEY `fk_vepos_cu` (`created_by`),
  CONSTRAINT `fk_vepos_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_vposition_to_position` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`),
  CONSTRAINT `fk_vpositoin_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9838 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_position_amount definition

CREATE TABLE `venue_position_amount` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_position_id` bigint(20) NOT NULL,
  `pay_rate` decimal(7,2) DEFAULT NULL,
  `bill_rate` decimal(7,2) DEFAULT NULL,
  `surcharge` decimal(5,2) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_vepoam_cu` (`created_by`),
  KEY `fk_vepoam_vepo` (`venue_position_id`),
  CONSTRAINT `fk_vepoam_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_vepoam_vepo` FOREIGN KEY (`venue_position_id`) REFERENCES `venue_position` (`venue_position_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=12212 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_position_certification definition

CREATE TABLE `venue_position_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_position_id` bigint(20) NOT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `t503` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `fk_venue_position_certification_pi` (`venue_position_id`),
  KEY `fk_venue_position_certification_ti` (`certification_id`),
  KEY `fk_venue_position_certification_u` (`created_by`),
  CONSTRAINT `fk_venue_position_certification_pi` FOREIGN KEY (`venue_position_id`) REFERENCES `venue_position` (`venue_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1241 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_position_grooming_tool definition

CREATE TABLE `venue_position_grooming_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_position_id` bigint(20) NOT NULL,
  `grooming_tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ven_pos_gro` (`venue_position_id`,`grooming_tool_id`),
  KEY `fk_venue_position_grooming_tool_ti` (`grooming_tool_id`),
  KEY `fk_venue_position_grooming_tool_u` (`created_by`),
  CONSTRAINT `fk_venue_position_grooming_tool_pi` FOREIGN KEY (`venue_position_id`) REFERENCES `venue_position` (`venue_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_grooming_tool_ti` FOREIGN KEY (`grooming_tool_id`) REFERENCES `grooming_tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_grooming_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=27509 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_position_tool definition

CREATE TABLE `venue_position_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_position_id` bigint(20) NOT NULL,
  `tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ven_pos_too` (`venue_position_id`,`tool_id`),
  KEY `fk_venue_position_tool_ti` (`tool_id`),
  KEY `fk_venue_position_tool_u` (`created_by`),
  CONSTRAINT `fk_venue_position_tool_pi` FOREIGN KEY (`venue_position_id`) REFERENCES `venue_position` (`venue_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_tool_ti` FOREIGN KEY (`tool_id`) REFERENCES `tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=19921 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.venue_position_uniform definition

CREATE TABLE `venue_position_uniform` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `venue_position_id` bigint(20) NOT NULL,
  `uniform_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_ven_pos_uni` (`venue_position_id`,`uniform_id`),
  KEY `fk_venue_position_uniform_ti` (`uniform_id`),
  KEY `fk_venue_position_uniform_u` (`created_by`),
  CONSTRAINT `fk_venue_position_uniform_pi` FOREIGN KEY (`venue_position_id`) REFERENCES `venue_position` (`venue_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_uniform_ti` FOREIGN KEY (`uniform_id`) REFERENCES `uniform` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_venue_position_uniform_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=36973 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_health_screening definition

CREATE TABLE `client_health_screening` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `health_screening_id` int(11) NOT NULL,
  `effective_date` date DEFAULT NULL,
  `max_allowed_months` int(11) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_chs_hsi` (`health_screening_id`),
  KEY `fk_chs_ci` (`client_id`),
  KEY `fk_chs_vi` (`venue_id`),
  KEY `fk_chs_u` (`created_by`),
  CONSTRAINT `fk_chs_ci` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_chs_hsi` FOREIGN KEY (`health_screening_id`) REFERENCES `health_screening` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_chs_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_chs_vi` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=341 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.client_purchase_order definition

CREATE TABLE `client_purchase_order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `value` varchar(255) NOT NULL,
  `start_date` date DEFAULT NULL,
  `end_date` date DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_cl_po_cl` (`client_id`),
  KEY `fk_cl_po_ve` (`venue_id`),
  KEY `fk_cl_po_cu` (`created_by`),
  CONSTRAINT `fk_cl_po_cl` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cl_po_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_cl_po_ve` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.dnr definition

CREATE TABLE `dnr` (
  `dnr_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `date` timestamp NOT NULL DEFAULT current_timestamp(),
  `reason_id` int(100) DEFAULT NULL,
  `other_reason` varchar(100) DEFAULT NULL,
  `notes` text DEFAULT NULL COMMENT 'This table contains the list of employees who are banned from working for certain clients usually due to poor performance or other negative factors.\n',
  `created_by` bigint(50) DEFAULT NULL,
  `client_id` bigint(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `notification_sent` tinyint(3) NOT NULL DEFAULT 0,
  PRIMARY KEY (`dnr_id`),
  KEY `fk_dnr_employee_idx` (`employee_id`),
  KEY `fk_dnr_venue_idx` (`venue_id`),
  KEY `fk_dnr_to_reason` (`reason_id`),
  KEY `fk_dnr_to_client` (`client_id`),
  CONSTRAINT `fk_dnr_to_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_dnr_to_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_dnr_to_reason` FOREIGN KEY (`reason_id`) REFERENCES `status_reason` (`id`) ON DELETE SET NULL,
  CONSTRAINT `fk_dnr_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=11359 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.event definition

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
  `travel_charge` decimal(5,2) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `venue_details` text DEFAULT NULL,
  `admin_notes` text DEFAULT NULL,
  `directions` text DEFAULT NULL,
  `parking` text DEFAULT NULL,
  `parking_note` text DEFAULT NULL,
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
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `documents` text DEFAULT NULL,
  `timesheet_received` tinyint(1) NOT NULL DEFAULT 0,
  `travel_pay` decimal(5,2) DEFAULT NULL,
  `staff_count_visible` tinyint(1) NOT NULL DEFAULT 1,
  `county_id` int(11) DEFAULT NULL,
  `address1` varchar(255) NOT NULL,
  `address2` varchar(255) DEFAULT NULL,
  `state` varchar(45) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `zip` varchar(45) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `timeclock` varchar(255) NOT NULL DEFAULT 'DISABLED',
  `timeclock_code_holder` varchar(255) DEFAULT NULL,
  `clock_in_code` varchar(10) DEFAULT NULL,
  `clock_out_code` varchar(10) DEFAULT NULL,
  `timeclock_tolerance` tinyint(3) DEFAULT NULL,
  `timeclock_prestart_interval` tinyint(3) DEFAULT NULL,
  `timeclock_limit` int(11) NOT NULL DEFAULT 0,
  `purchase_order` varchar(255) DEFAULT NULL,
  `no_break_penalty` tinyint(3) NOT NULL DEFAULT 1,
  `stat_shifts_positions_count` int(11) NOT NULL DEFAULT 0,
  `stat_shifts_filled_confirmed_count` int(11) DEFAULT NULL,
  `stat_shifts_filled_count` int(11) DEFAULT NULL,
  `stat_filled_confirmed` tinyint(1) DEFAULT NULL,
  `stat_filled` tinyint(1) DEFAULT NULL,
  `stat_employees_confirmed_count` int(11) DEFAULT NULL,
  `stat_employees_count` int(11) DEFAULT NULL,
  `stat_waiting_for_employee_count` int(11) DEFAULT NULL,
  `stat_waiting_for_admin_count` int(11) DEFAULT NULL,
  `stat_shifts_published_count` int(11) DEFAULT NULL,
  `stat_shifts_publish_scheduled_count` int(11) DEFAULT NULL,
  `stat_shifts_count` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`event_id`),
  KEY `fk_event_venue_idx` (`venue_id`),
  KEY `fk_event_client_idx` (`client_id`),
  KEY `fk_ev_co` (`county_id`),
  KEY `date` (`date`),
  KEY `fk_ev_du` (`deleted_by`),
  CONSTRAINT `fk_ev_co` FOREIGN KEY (`county_id`) REFERENCES `county` (`id`),
  CONSTRAINT `fk_ev_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_event_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_event_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=166122 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.event_document definition

CREATE TABLE `event_document` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) NOT NULL,
  `venue_document_id` int(11) DEFAULT NULL,
  `filename` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_edoc_vdoc` (`venue_document_id`),
  KEY `fk_edoc_ev` (`event_id`),
  KEY `fk_edoc_cu` (`created_by`),
  CONSTRAINT `fk_edoc_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_edoc_ev` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_edoc_vdoc` FOREIGN KEY (`venue_document_id`) REFERENCES `venue_document` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=38424 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.exclusive definition

CREATE TABLE `exclusive` (
  `exclusive_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `venue_id` bigint(20) DEFAULT NULL,
  `employee_id` bigint(20) NOT NULL,
  `reason` varchar(100) DEFAULT NULL,
  `notes` text DEFAULT NULL,
  `created_by` bigint(20) DEFAULT NULL,
  `date_created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`exclusive_id`),
  KEY `fk_exclusive_department_idx` (`venue_id`),
  KEY `fk_exclusive_employee_idx` (`employee_id`),
  KEY `fk_exclusive_to_client` (`client_id`),
  CONSTRAINT `fk_exclusive_to_client` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE,
  CONSTRAINT `fk_exclusive_to_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`),
  CONSTRAINT `fk_exclusive_to_venue` FOREIGN KEY (`venue_id`) REFERENCES `venue` (`venue_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10715 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.publishing definition

CREATE TABLE `publishing` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `event_id` bigint(20) NOT NULL,
  `gender` varchar(255) DEFAULT NULL,
  `level` tinyint(3) DEFAULT NULL,
  `begin` timestamp NULL DEFAULT NULL,
  `processed` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `to` varchar(255) DEFAULT NULL,
  `notify_resigned` tinyint(1) NOT NULL DEFAULT 0,
  `employee_statuses` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_pu_c` (`client_id`),
  KEY `fk_pu_e` (`event_id`),
  KEY `fk_pu_u` (`created_by`),
  KEY `fk_pu_du` (`deleted_by`),
  CONSTRAINT `fk_pu_c` FOREIGN KEY (`client_id`) REFERENCES `client` (`client_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_e` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=126288 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.publishing_language definition

CREATE TABLE `publishing_language` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `publishing_id` bigint(20) NOT NULL,
  `language_id` bigint(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_pu_la_p` (`publishing_id`),
  KEY `fk_pu_la_l` (`language_id`),
  KEY `fk_pu_la_u` (`created_by`),
  CONSTRAINT `fk_pu_la_l` FOREIGN KEY (`language_id`) REFERENCES `language` (`language_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_la_p` FOREIGN KEY (`publishing_id`) REFERENCES `publishing` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_la_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift definition

CREATE TABLE `shift` (
  `shift_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) NOT NULL,
  `start` datetime NOT NULL,
  `end` datetime NOT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `clock_in_code` varchar(10) DEFAULT NULL,
  `clock_out_code` varchar(10) DEFAULT NULL,
  `old_start` datetime DEFAULT NULL,
  `old_end` datetime DEFAULT NULL,
  `purchase_order` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`shift_id`),
  KEY `fk_shift_event_idx` (`event_id`),
  KEY `fk_sh_du` (`deleted_by`),
  CONSTRAINT `fk_sh_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_event_key` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=325945 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_position definition

CREATE TABLE `shift_position` (
  `shift_position_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shift_id` bigint(20) NOT NULL,
  `position_id` bigint(20) NOT NULL,
  `additional_title` varchar(100) DEFAULT NULL,
  `sub_type_id` bigint(20) DEFAULT -1,
  `gender` tinyint(2) DEFAULT NULL,
  `rate` decimal(7,2) NOT NULL DEFAULT 0.00,
  `base_rate` decimal(7,2) DEFAULT NULL,
  `base_bill_rate` decimal(7,2) DEFAULT NULL,
  `bill_rate` decimal(7,2) DEFAULT NULL,
  `count` int(10) NOT NULL DEFAULT 1,
  `backup` tinyint(4) DEFAULT 0,
  `holiday_rate` tinyint(2) NOT NULL DEFAULT 0,
  `surcharge` tinyint(2) NOT NULL DEFAULT 0,
  `surcharge_value` decimal(5,2) DEFAULT NULL,
  `grooming_tools` text DEFAULT NULL,
  `uniform` text DEFAULT NULL,
  `tools` text DEFAULT NULL,
  `position_description` text DEFAULT NULL,
  `was_filled` int(11) NOT NULL DEFAULT 0,
  `filled` tinyint(1) NOT NULL DEFAULT 0,
  `was_published` tinyint(1) NOT NULL DEFAULT 0,
  `code` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `deleted_by` int(11) DEFAULT NULL,
  `bonus` decimal(5,2) NOT NULL DEFAULT 0.00,
  `clock_in_code` varchar(10) DEFAULT NULL,
  `clock_out_code` varchar(10) DEFAULT NULL,
  `miles_apply` tinyint(3) NOT NULL DEFAULT 0,
  PRIMARY KEY (`shift_position_id`),
  KEY `fk_shift_position_shift_idx` (`shift_id`),
  KEY `fk_shift_to_positoin` (`position_id`),
  KEY `fk_sh_pos_cu` (`created_by`),
  KEY `fk_sp_du` (`deleted_by`),
  CONSTRAINT `fk_sh_pos_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_positoin` FOREIGN KEY (`position_id`) REFERENCES `position` (`position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_positoin_shift` FOREIGN KEY (`shift_id`) REFERENCES `shift` (`shift_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_sp_du` FOREIGN KEY (`deleted_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=326275 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_position_certification definition

CREATE TABLE `shift_position_certification` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `shift_position_id` bigint(20) NOT NULL,
  `certification_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_shift_position_certification_pi` (`shift_position_id`),
  KEY `fk_shift_position_certification_ti` (`certification_id`),
  KEY `fk_shift_position_certification_u` (`created_by`),
  CONSTRAINT `fk_shift_position_certification_pi` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_certification_ti` FOREIGN KEY (`certification_id`) REFERENCES `certification` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_certification_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=30351 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_position_grooming_tool definition

CREATE TABLE `shift_position_grooming_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `shift_position_id` bigint(20) NOT NULL,
  `grooming_tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_shi_pos_gro` (`shift_position_id`,`grooming_tool_id`),
  KEY `fk_shift_position_grooming_tool_ti` (`grooming_tool_id`),
  KEY `fk_shift_position_grooming_tool_u` (`created_by`),
  CONSTRAINT `fk_shift_position_grooming_tool_pi` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_grooming_tool_ti` FOREIGN KEY (`grooming_tool_id`) REFERENCES `grooming_tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_grooming_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=990001 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_position_tool definition

CREATE TABLE `shift_position_tool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `shift_position_id` bigint(20) NOT NULL,
  `tool_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_shi_pos_too` (`shift_position_id`,`tool_id`),
  KEY `fk_shift_position_tool_ti` (`tool_id`),
  KEY `fk_shift_position_tool_u` (`created_by`),
  CONSTRAINT `fk_shift_position_tool_pi` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_tool_ti` FOREIGN KEY (`tool_id`) REFERENCES `tool` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_tool_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=741009 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_position_uniform definition

CREATE TABLE `shift_position_uniform` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `shift_position_id` bigint(20) NOT NULL,
  `uniform_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_shi_pos_uni` (`shift_position_id`,`uniform_id`),
  KEY `fk_shift_position_uniform_ti` (`uniform_id`),
  KEY `fk_shift_position_uniform_u` (`created_by`),
  CONSTRAINT `fk_shift_position_uniform_pi` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_uniform_ti` FOREIGN KEY (`uniform_id`) REFERENCES `uniform` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_position_uniform_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=1332215 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.timesheet_upload definition

CREATE TABLE `timesheet_upload` (
  `timesheet_upload_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) NOT NULL,
  `datetime` timestamp NOT NULL DEFAULT current_timestamp(),
  `filename` varchar(100) NOT NULL,
  `notes` text DEFAULT NULL,
  PRIMARY KEY (`timesheet_upload_id`),
  KEY `fk_ts_event_idx` (`event_id`),
  CONSTRAINT `fk_ts_upload_to_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=193 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.event_cancellation definition

CREATE TABLE `event_cancellation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(255) NOT NULL,
  `minimum_bill` tinyint(3) NOT NULL DEFAULT 0,
  `event_id` bigint(20) NOT NULL,
  `shift_id` bigint(20) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  `minimum_pay` tinyint(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uix_event_cancellation` (`event_id`,`shift_id`),
  KEY `fk_ec_sh` (`shift_id`),
  KEY `fk_ec_cu` (`created_by`),
  CONSTRAINT `fk_ec_cu` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ec_ev` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_ec_sh` FOREIGN KEY (`shift_id`) REFERENCES `shift` (`shift_id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=129 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.publish_employee definition

CREATE TABLE `publish_employee` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `event_id` bigint(20) NOT NULL,
  `employee_id` bigint(20) NOT NULL,
  `shift_position_id` bigint(20) NOT NULL,
  `created_on` timestamp NULL DEFAULT NULL,
  `publishing_id` bigint(20) DEFAULT NULL,
  `notify` tinyint(3) DEFAULT NULL,
  `notified_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_publish_employee_1` (`event_id`,`employee_id`,`shift_position_id`),
  UNIQUE KEY `uq_publish_employee_2` (`employee_id`,`shift_position_id`),
  KEY `employee_id` (`employee_id`),
  KEY `shift_position_id` (`shift_position_id`),
  KEY `event_id` (`event_id`),
  KEY `fk_pe_p` (`publishing_id`),
  KEY `ix_publish_employee_notify` (`notify`),
  CONSTRAINT `fk_pe_p` FOREIGN KEY (`publishing_id`) REFERENCES `publishing` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_publish_employee_employee_id` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE,
  CONSTRAINT `fk_publish_employee_event_id` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`) ON DELETE CASCADE,
  CONSTRAINT `fk_publish_employee_shift_position_id` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=163909875 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.publishing_shift definition

CREATE TABLE `publishing_shift` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `publishing_id` bigint(20) NOT NULL,
  `shift_id` bigint(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_pu_sh_p` (`publishing_id`),
  KEY `fk_pu_sh_s` (`shift_id`),
  KEY `fk_pu_sh_u` (`created_by`),
  CONSTRAINT `fk_pu_sh_p` FOREIGN KEY (`publishing_id`) REFERENCES `publishing` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_sh_s` FOREIGN KEY (`shift_id`) REFERENCES `shift` (`shift_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_pu_sh_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=178220 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_employee definition

CREATE TABLE `shift_employee` (
  `shift_employee_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shift_position_id` bigint(20) NOT NULL,
  `event_id` bigint(20) DEFAULT NULL,
  `employee_id` bigint(20) NOT NULL,
  `confirmed` tinyint(4) NOT NULL DEFAULT 0,
  `bill_rate` decimal(7,2) DEFAULT 0.00,
  `rate` decimal(7,2) DEFAULT 0.00,
  `cancel_reason` tinyint(4) NOT NULL DEFAULT 0,
  `employee_remove_date` datetime DEFAULT NULL,
  `employee_cancel_reason` text DEFAULT NULL,
  `emergency_rate` tinyint(4) NOT NULL DEFAULT 0,
  `emergency_rate_amount` decimal(5,2) DEFAULT 0.00,
  `cancel_notes` text DEFAULT NULL,
  `confirm_type` tinyint(4) DEFAULT NULL,
  `confirm_notes` text DEFAULT NULL,
  `confirmed_by` bigint(50) DEFAULT NULL,
  `request_by` int(11) DEFAULT NULL,
  `approved_by` bigint(50) DEFAULT NULL,
  `remove_by` bigint(20) DEFAULT NULL,
  `read_notification` int(11) NOT NULL DEFAULT 0,
  `daily_report_notes` text DEFAULT NULL,
  `overtime_type` varchar(255) DEFAULT NULL,
  `overtime_paid_by` text DEFAULT NULL,
  `overtime_reason` text DEFAULT NULL,
  `overtime` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`overtime`)),
  `hiatus` tinyint(2) DEFAULT 0,
  `hiatus_reason` int(50) DEFAULT NULL,
  `shift_type` tinyint(2) DEFAULT 0,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `tech_restore_211222` tinyint(1) NOT NULL DEFAULT 0,
  `attest_uniforms` text DEFAULT NULL,
  `confirmed_at` datetime DEFAULT NULL,
  `cancelled_in_deadline` tinyint(3) NOT NULL DEFAULT 0,
  `clock_in_code` varchar(100) DEFAULT NULL,
  `clock_out_code` varchar(100) DEFAULT NULL,
  `cancelled_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `created_time` timestamp NOT NULL DEFAULT current_timestamp(),
  `note_to_employee` text DEFAULT NULL,
  PRIMARY KEY (`shift_employee_id`),
  KEY `fk_shift_employee_shift_idx` (`shift_position_id`),
  KEY `fk_shift_employee_employee_idx` (`employee_id`),
  KEY `fk_shift_employee_to_event` (`event_id`),
  KEY `confirmed_by` (`confirmed_by`,`request_by`,`approved_by`),
  KEY `remove_by` (`remove_by`),
  KEY `fk_se_rb` (`request_by`),
  CONSTRAINT `fk_se_rb` FOREIGN KEY (`request_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_employee_key` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_shift_employee_to_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`),
  CONSTRAINT `fk_shift_employee_to_position` FOREIGN KEY (`shift_position_id`) REFERENCES `shift_position` (`shift_position_id`)
) ENGINE=InnoDB AUTO_INCREMENT=617861 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.shift_employee_document definition

CREATE TABLE `shift_employee_document` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `shift_employee_id` bigint(20) NOT NULL,
  `filename` varchar(255) NOT NULL,
  `description` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `created_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_shift_employee_document_se` (`shift_employee_id`),
  KEY `fk_shift_employee_document_u` (`created_by`),
  CONSTRAINT `fk_shift_employee_document_se` FOREIGN KEY (`shift_employee_id`) REFERENCES `shift_employee` (`shift_employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_shift_employee_document_u` FOREIGN KEY (`created_by`) REFERENCES `user` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.timesheet definition

CREATE TABLE `timesheet` (
  `timesheet_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `event_id` bigint(20) NOT NULL,
  `shift_employee_id` bigint(20) NOT NULL,
  `employee_start` timestamp NULL DEFAULT NULL,
  `employee_end` timestamp NULL DEFAULT NULL,
  `employee_break_start` timestamp NULL DEFAULT NULL,
  `employee_break_end` timestamp NULL DEFAULT NULL,
  `employee_sec_break_start` timestamp NULL DEFAULT NULL,
  `employee_sec_break_end` timestamp NULL DEFAULT NULL,
  `employee_notes` text DEFAULT NULL,
  `employee_rating` int(10) DEFAULT NULL,
  `reimbursement_file` varchar(250) DEFAULT NULL,
  `employee_no_pay` tinyint(2) DEFAULT NULL,
  `employee_min_pay` tinyint(1) DEFAULT NULL,
  `employee_tips` decimal(5,2) DEFAULT NULL,
  `employee_parking` decimal(5,2) DEFAULT NULL,
  `employee_travel` decimal(5,2) DEFAULT NULL,
  `employee_service_charge` decimal(5,2) DEFAULT NULL,
  `employee_adjustment` int(11) DEFAULT NULL,
  `employee_adjustment_notes` text DEFAULT NULL,
  `employee_no_break_penalty` float DEFAULT NULL,
  `employee_timesheet_upload` varchar(100) DEFAULT NULL,
  `client_start` timestamp NULL DEFAULT NULL,
  `client_end` timestamp NULL DEFAULT NULL,
  `client_break_start` timestamp NULL DEFAULT NULL,
  `client_break_end` timestamp NULL DEFAULT NULL,
  `client_sec_break_start` timestamp NULL DEFAULT NULL,
  `client_sec_break_end` timestamp NULL DEFAULT NULL,
  `client_rating` float DEFAULT NULL,
  `client_no_bill` tinyint(2) DEFAULT NULL,
  `client_po` varchar(100) DEFAULT NULL COMMENT 'purchase order. It will come from event , if blank then can add in timesheet ',
  `client_tips` decimal(5,2) DEFAULT NULL,
  `client_parking` decimal(5,2) DEFAULT NULL,
  `client_travel` decimal(5,2) DEFAULT NULL,
  `client_service_charge` decimal(5,2) DEFAULT NULL,
  `client_notes` text DEFAULT NULL,
  `client_min_bill` tinyint(1) DEFAULT NULL,
  `client_no_break_penalty` float DEFAULT NULL,
  `client_adjustment` int(11) DEFAULT NULL,
  `client_adjustment_notes` text DEFAULT NULL,
  `employee_no_break_reason` tinyint(1) DEFAULT NULL,
  `employee_less_hours_reason` int(10) DEFAULT NULL,
  `employee_worked` varchar(255) NOT NULL DEFAULT 'WORKED',
  `client_worked` varchar(255) NOT NULL DEFAULT 'WORKED',
  `client_less_hours_reason` int(11) DEFAULT NULL,
  `status` varchar(255) NOT NULL DEFAULT 'NEW',
  `employee_no_sec_break_reason` tinyint(1) DEFAULT NULL,
  `client_no_break_reason` tinyint(1) DEFAULT NULL,
  `client_no_sec_break_reason` tinyint(1) DEFAULT NULL,
  `employee_submit_date` timestamp NULL DEFAULT NULL,
  `client_submit_date` timestamp NULL DEFAULT NULL,
  `client_seconds` int(11) DEFAULT NULL,
  `employee_seconds` int(11) DEFAULT NULL,
  `use_sheet` varchar(255) DEFAULT NULL,
  `start_verified` varchar(255) DEFAULT NULL,
  `end_verified` varchar(255) DEFAULT NULL,
  `start_verified_at` timestamp NULL DEFAULT NULL,
  `end_verified_at` timestamp NULL DEFAULT NULL,
  `client_had_meal` int(11) NOT NULL DEFAULT 1,
  `employee_had_meal` int(11) NOT NULL DEFAULT 1,
  `client_had_sec_meal` int(11) NOT NULL DEFAULT 1,
  `employee_had_sec_meal` int(11) NOT NULL DEFAULT 1,
  `employee_travel_distance` int(11) DEFAULT NULL,
  PRIMARY KEY (`timesheet_id`),
  UNIQUE KEY `uq_timesheet_shift_employee_id` (`shift_employee_id`),
  KEY `fk_timesheet_employee_idx` (`employee_id`),
  KEY `fk_timesheet_event_idx` (`event_id`),
  KEY `fk_timesheet_employe_shift_idx` (`shift_employee_id`),
  CONSTRAINT `fk_timesheet_to_event` FOREIGN KEY (`event_id`) REFERENCES `event` (`event_id`),
  CONSTRAINT `fk_ts_emp` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_ts_shift_emp` FOREIGN KEY (`shift_employee_id`) REFERENCES `shift_employee` (`shift_employee_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=462124 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;


-- cstaffing_live.employee_note definition

CREATE TABLE `employee_note` (
  `employee_note_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `employee_id` bigint(20) NOT NULL,
  `note` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `datetime` timestamp NULL DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `shift_employee_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`employee_note_id`),
  KEY `fk_note_to_employee_idx` (`employee_id`),
  KEY `fk_note_to_author_idx` (`user_id`),
  KEY `fk_en_sei` (`shift_employee_id`),
  CONSTRAINT `fk_en_sei` FOREIGN KEY (`shift_employee_id`) REFERENCES `shift_employee` (`shift_employee_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_note_to_author` FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_note_to_employee` FOREIGN KEY (`employee_id`) REFERENCES `employee` (`employee_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=198930 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


-- cstaffing_live.payroll_note definition

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
) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;