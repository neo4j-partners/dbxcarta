[
  {
    "shape": "single_table_filter",
    "question": "Which shoes are black and have at least 10 units in stock?",
    "sql": "SELECT `sid`, `name`, `brand`, `colour`, `stock`, `price` FROM `schemapile_lakehouse`.`sp_169810_init`.`shoe` WHERE `colour` = 'Black' AND `stock` >= 10"
  },
  {
    "shape": "single_table_filter",
    "question": "Which shoes are available in size 8 or size 10?",
    "sql": "SELECT `sid`, `name`, `brand`, `size`, `price`, `stock` FROM `schemapile_lakehouse`.`sp_169810_init`.`shoe` WHERE `size` IN (8, 10)"
  },
  {
    "shape": "aggregation",
    "question": "What is the average shoe price by brand, ordered from highest to lowest average price?",
    "sql": "SELECT `brand`, AVG(`price`) AS avg_price, COUNT(`sid`) AS shoe_count FROM `schemapile_lakehouse`.`sp_169810_init`.`shoe` GROUP BY `brand` ORDER BY avg_price DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which users are active?",
    "sql": "SELECT `id_usuario`, `ds_nome`, `ds_login` FROM `schemapile_lakehouse`.`sp_020891_v01__impl_usuario`.`tb_usuario` WHERE `fl_ativo` = TRUE"
  },
  {
    "shape": "two_table_join",
    "question": "Show each user together with the group they belong to.",
    "sql": "SELECT u.`id_usuario`, u.`ds_nome`, u.`ds_login`, g.`ds_nome` AS group_name FROM `schemapile_lakehouse`.`sp_020891_v01__impl_usuario`.`tb_usuario` u JOIN `schemapile_lakehouse`.`sp_020891_v01__impl_usuario`.`tb_usuario_x_grupo` ug ON u.`id_usuario` = ug.`id_usuario` JOIN `schemapile_lakehouse`.`sp_020891_v01__impl_usuario`.`tb_grupo` g ON ug.`id_grupo` = g.`id_grupo`"
  },
  {
    "shape": "aggregation",
    "question": "How many permissions are assigned to each group?",
    "sql": "SELECT g.`id_grupo`, g.`ds_nome` AS group_name, COUNT(pg.`id_permissao`) AS permission_count FROM `schemapile_lakehouse`.`sp_020891_v01__impl_usuario`.`tb_grupo` g LEFT JOIN `schemapile_lakehouse`.`sp_020891_v01__impl_usuario`.`tb_permissao_x_grupo` pg ON g.`id_grupo` = pg.`id_grupo` GROUP BY g.`id_grupo`, g.`ds_nome` ORDER BY permission_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "List all subjects with more than 60 assigned hours.",
    "sql": "SELECT `id`, `name`, `hours_number` FROM `schemapile_lakehouse`.`sp_033815_schema`.`subjects` WHERE `hours_number` > 60"
  },
  {
    "shape": "two_table_join",
    "question": "Show each student's name with the name of their group.",
    "sql": "SELECT s.`id`, s.`name` AS student_name, g.`name` AS group_name FROM `schemapile_lakehouse`.`sp_033815_schema`.`students` s JOIN `schemapile_lakehouse`.`sp_033815_schema`.`groups_table` g ON s.`group_id` = g.`id`"
  },
  {
    "shape": "aggregation",
    "question": "What is the average grade for each subject?",
    "sql": "SELECT sub.`id`, sub.`name` AS subject_name, AVG(p.`grade`) AS avg_grade FROM `schemapile_lakehouse`.`sp_033815_schema`.`subjects` sub JOIN `schemapile_lakehouse`.`sp_033815_schema`.`progress` p ON sub.`id` = p.`subject_id` GROUP BY sub.`id`, sub.`name` ORDER BY avg_grade DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which tweets have more than 10 likes?",
    "sql": "SELECT `tweetid`, `username`, `tweet_content`, `likes`, `timestamp_t` FROM `schemapile_lakehouse`.`sp_266628_tables`.`tweet` WHERE `likes` > 10"
  },
  {
    "shape": "two_table_join",
    "question": "List each tweet with the first and last name of the user who posted it.",
    "sql": "SELECT t.`tweetid`, t.`tweet_content`, u.`firstname`, u.`lastname` FROM `schemapile_lakehouse`.`sp_266628_tables`.`tweet` t JOIN `schemapile_lakehouse`.`sp_266628_tables`.`users` u ON t.`username` = u.`username`"
  },
  {
    "shape": "aggregation",
    "question": "How many tweets has each user posted?",
    "sql": "SELECT u.`username`, u.`firstname`, u.`lastname`, COUNT(t.`tweetid`) AS tweet_count FROM `schemapile_lakehouse`.`sp_266628_tables`.`users` u LEFT JOIN `schemapile_lakehouse`.`sp_266628_tables`.`tweet` t ON u.`username` = t.`username` GROUP BY u.`username`, u.`firstname`, u.`lastname` ORDER BY tweet_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which articles have a title on record?",
    "sql": "SELECT `articlesid`, `advertiserid`, `categoryid`, `title` FROM `schemapile_lakehouse`.`sp_289361_schema`.`articles` WHERE `title` IS NOT NULL AND `title` <> ''"
  },
  {
    "shape": "two_table_join",
    "question": "List each article with the file paths for its related materials.",
    "sql": "SELECT a.`articlesid`, a.`title`, m.`materialid`, m.`filepath` FROM `schemapile_lakehouse`.`sp_289361_schema`.`articles` a JOIN `schemapile_lakehouse`.`sp_289361_schema`.`material` m ON a.`articlesid` = m.`articlesid`"
  },
  {
    "shape": "aggregation",
    "question": "How many materials are attached to each article?",
    "sql": "SELECT a.`articlesid`, a.`title`, COUNT(m.`materialid`) AS material_count FROM `schemapile_lakehouse`.`sp_289361_schema`.`articles` a LEFT JOIN `schemapile_lakehouse`.`sp_289361_schema`.`material` m ON a.`articlesid` = m.`articlesid` GROUP BY a.`articlesid`, a.`title` ORDER BY material_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which projects are not marked as deleted?",
    "sql": "SELECT `id`, `name`, `owner_id`, `forked_from`, `deleted` FROM `schemapile_lakehouse`.`sp_310630_schema`.`projects` WHERE `deleted` = FALSE"
  },
  {
    "shape": "two_table_join",
    "question": "Show each project with the login of its owner.",
    "sql": "SELECT p.`id`, p.`name` AS project_name, u.`login` AS owner_login, u.`country_code` FROM `schemapile_lakehouse`.`sp_310630_schema`.`projects` p JOIN `schemapile_lakehouse`.`sp_310630_schema`.`users` u ON p.`owner_id` = u.`id`"
  },
  {
    "shape": "aggregation",
    "question": "How many result projects are there for each programming language?",
    "sql": "SELECT `language`, COUNT(*) AS project_count FROM `schemapile_lakehouse`.`sp_310630_schema`.`result_projects` GROUP BY `language` ORDER BY project_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which classes start before 9 AM?",
    "sql": "SELECT `class_id`, `subject_code`, `room_id`, `semester_id`, `start_time`, `end_time`, `instructor_id` FROM `schemapile_lakehouse`.`sp_352545_capstone`.`class` WHERE `start_time` < '09:00:00'"
  },
  {
    "shape": "two_table_join",
    "question": "Show each student with the name of their course.",
    "sql": "SELECT s.`student_id`, s.`year_level`, c.`course_name` FROM `schemapile_lakehouse`.`sp_352545_capstone`.`student` s JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`course` c ON s.`course_id` = c.`course_id`"
  },
  {
    "shape": "aggregation",
    "question": "How many classes are scheduled for each subject?",
    "sql": "SELECT sub.`subject_code`, sub.`subject_title`, COUNT(c.`class_id`) AS class_count FROM `schemapile_lakehouse`.`sp_352545_capstone`.`subject` sub LEFT JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`class` c ON sub.`subject_code` = c.`subject_code` GROUP BY sub.`subject_code`, sub.`subject_title` ORDER BY class_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which doctors have a specialty listed?",
    "sql": "SELECT `doctor_id`, `doctor_name`, `doctor_speciailty`, `doc_location` FROM `schemapile_lakehouse`.`sp_365427_schema`.`doctor` WHERE `doctor_speciailty` IS NOT NULL AND `doctor_speciailty` <> ''"
  },
  {
    "shape": "two_table_join",
    "question": "List appointments with the doctor's name for each appointment.",
    "sql": "SELECT a.`appoi_id`, a.`day`, a.`time_from`, a.`time_to`, d.`doctor_name` FROM `schemapile_lakehouse`.`sp_365427_schema`.`appointments` a JOIN `schemapile_lakehouse`.`sp_365427_schema`.`doctor` d ON a.`doc_id` = d.`doctor_id`"
  },
  {
    "shape": "aggregation",
    "question": "How many appointments does each doctor have?",
    "sql": "SELECT d.`doctor_id`, d.`doctor_name`, COUNT(a.`appoi_id`) AS appointment_count FROM `schemapile_lakehouse`.`sp_365427_schema`.`doctor` d LEFT JOIN `schemapile_lakehouse`.`sp_365427_schema`.`appointments` a ON d.`doctor_id` = a.`doc_id` GROUP BY d.`doctor_id`, d.`doctor_name` ORDER BY appointment_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which spaceships have a light speed rate greater than 50?",
    "sql": "SELECT `id`, `name`, `manufacturer`, `light_speed_rate` FROM `schemapile_lakehouse`.`sp_413175_01_tabledesign_exam21oct2018`.`spaceships` WHERE `light_speed_rate` > 50"
  },
  {
    "shape": "two_table_join",
    "question": "Show each journey with the name of its destination spaceport.",
    "sql": "SELECT j.`id`, j.`journey_start`, j.`journey_end`, j.`purpose`, sp.`name` AS destination_spaceport FROM `schemapile_lakehouse`.`sp_413175_01_tabledesign_exam21oct2018`.`journeys` j JOIN `schemapile_lakehouse`.`sp_413175_01_tabledesign_exam21oct2018`.`spaceports` sp ON j.`destination_spaceport_id` = sp.`id`"
  },
  {
    "shape": "aggregation",
    "question": "How many travel cards are assigned to each journey?",
    "sql": "SELECT j.`id`, j.`purpose`, COUNT(tc.`id`) AS travel_card_count FROM `schemapile_lakehouse`.`sp_413175_01_tabledesign_exam21oct2018`.`journeys` j LEFT JOIN `schemapile_lakehouse`.`sp_413175_01_tabledesign_exam21oct2018`.`travel_cards` tc ON j.`id` = tc.`journey_id` GROUP BY j.`id`, j.`purpose` ORDER BY travel_card_count DESC"
  },
  {
    "shape": "single_table_filter",
    "question": "Which items cost more than 50 USD?",
    "sql": "SELECT `id`, `name`, `price_usd`, `category_id` FROM `schemapile_lakehouse`.`sp_435873_db`.`items` WHERE `price_usd` > 50"
  },
  {
    "shape": "two_table_join",
    "question": "List each order with the email address of the user who placed it.",
    "sql": "SELECT o.`id` AS order_id, o.`status`, o.`created_at`, u.`email` AS user_email FROM `schemapile_lakehouse`.`sp_435873_db`.`orders` o JOIN `schemapile_lakehouse`.`sp_435873_db`.`users` u ON o.`user_id` = u.`id`"
  },
  {
    "shape": "aggregation",
    "question": "What is the average item price in each category?",
    "sql": "SELECT c.`id` AS category_id, c.`name` AS category_name, AVG(i.`price_usd`) AS avg_price FROM `schemapile_lakehouse`.`sp_435873_db`.`categories` c JOIN `schemapile_lakehouse`.`sp_435873_db`.`items` i ON c.`id` = i.`category_id` GROUP BY c.`id`, c.`name` ORDER BY avg_price DESC"
  },
  {
    "shape": "three_table_join",
    "question": "Which members are assigned to each project, and what role does each member have on that project?",
    "sql": "SELECT p.`id` AS project_id, p.`name` AS project_name, m.`id` AS member_id, m.`name` AS member_name, pm.`role` FROM `schemapile_lakehouse`.`sp_002489_project`.`projects` p JOIN `schemapile_lakehouse`.`sp_002489_project`.`project_members` pm ON p.`id` = pm.`project` JOIN `schemapile_lakehouse`.`sp_002489_project`.`members` m ON pm.`member` = m.`id`"
  },
  {
    "shape": "advanced",
    "question": "Show each task with its assigned member and every artifact attached to the task.",
    "sql": "SELECT t.`id` AS task_id, t.`task`, m.`name` AS member_name, a.`id` AS artifact_id, a.`artifact`, a.`filename` FROM `schemapile_lakehouse`.`sp_002489_project`.`tasks` t JOIN `schemapile_lakehouse`.`sp_002489_project`.`members` m ON t.`member` = m.`id` JOIN `schemapile_lakehouse`.`sp_002489_project`.`task_artifacts` ta ON t.`id` = ta.`task` JOIN `schemapile_lakehouse`.`sp_002489_project`.`artifacts` a ON ta.`artifact` = a.`id`"
  },
  {
    "shape": "three_table_join",
    "question": "List each authenticated user with their username and granted authorities.",
    "sql": "SELECT u.`id`, u.`uid`, up.`user_name`, u.`active`, a.`authority` FROM `schemapile_lakehouse`.`sp_008582_auth_services_schema_mysql`.`user_table` u JOIN `schemapile_lakehouse`.`sp_008582_auth_services_schema_mysql`.`username_password_table` up ON u.`uid` = up.`uid` JOIN `schemapile_lakehouse`.`sp_008582_auth_services_schema_mysql`.`authority_table` a ON u.`uid` = a.`uid`"
  },
  {
    "shape": "three_table_join",
    "question": "Which active API keys belong to which client ids and user ids?",
    "sql": "SELECT ak.`id` AS api_key_id, ak.`api_key_desc`, ak.`active`, c.`client_id`, c.`uid`, u.`id` AS user_id FROM `schemapile_lakehouse`.`sp_008582_auth_services_schema_mysql`.`api_key_table` ak JOIN `schemapile_lakehouse`.`sp_008582_auth_services_schema_mysql`.`client_id_table` c ON ak.`client_id` = c.`client_id` JOIN `schemapile_lakehouse`.`sp_008582_auth_services_schema_mysql`.`user_table` u ON c.`uid` = u.`uid` WHERE ak.`active` = TRUE"
  },
  {
    "shape": "advanced",
    "question": "Show each unified user with the roles assigned through their account.",
    "sql": "SELECT u.`id` AS user_id, u.`email`, u.`name` AS user_name, a.`id` AS account_id, r.`id` AS role_id, r.`name` AS role_name FROM `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`users` u JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`accounts` a ON u.`account_id` = a.`id` JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`account_roles` ar ON a.`id` = ar.`account_id` JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`roles` r ON ar.`role_id` = r.`id`"
  },
  {
    "shape": "advanced",
    "question": "For each role, list the permissions it grants, the resource those permissions target, and the resource system.",
    "sql": "SELECT r.`id` AS role_id, r.`name` AS role_name, p.`id` AS permission_id, p.`actions`, res.`name` AS resource_name, s.`name` AS system_name FROM `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`roles` r JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`role_permissions` rp ON r.`id` = rp.`role_id` JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`permissions` p ON rp.`permission_id` = p.`id` JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`resources` res ON p.`resource_id` = res.`id` LEFT JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`systems` s ON res.`system_id` = s.`id`"
  },
  {
    "shape": "aggregation",
    "question": "How many unified users are attached to each organization through account memberships?",
    "sql": "SELECT o.`id` AS organization_id, o.`name` AS organization_name, COUNT(DISTINCT u.`id`) AS user_count FROM `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`organizations` o JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`account_organizations` ao ON o.`id` = ao.`organization_id` JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`accounts` a ON ao.`account_id` = a.`id` JOIN `schemapile_lakehouse`.`sp_178181_v4_026__create_unified_user`.`users` u ON a.`id` = u.`account_id` GROUP BY o.`id`, o.`name` ORDER BY user_count DESC"
  },
  {
    "shape": "advanced",
    "question": "Resolve each tab_no_action row against all four referenced parent tables.",
    "sql": "SELECT n.`tab_a_id`, a.`tab_a_id` AS resolved_a_id, n.`tab_b_id`, b.`tab_b_id` AS resolved_b_id, n.`tab_c_id`, c.`tab_c_id` AS resolved_c_id, n.`tab_d_id`, d.`tab_d_id` AS resolved_d_id FROM `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_no_action` n LEFT JOIN `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_a` a ON n.`tab_a_id` = a.`tab_a_id` LEFT JOIN `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_b` b ON n.`tab_b_id` = b.`tab_b_id` LEFT JOIN `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_c` c ON n.`tab_c_id` = c.`tab_c_id` LEFT JOIN `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_d` d ON n.`tab_d_id` = d.`tab_d_id`"
  },
  {
    "shape": "three_table_join",
    "question": "Which composite wrong-FK rows resolve to ids in both wrong-FK parent tables?",
    "sql": "SELECT wco.`id1`, wfc.`id` AS column_parent_id, wco.`id2`, wfn.`id` AS name_parent_id FROM `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_wrong_fk_column_order` wco JOIN `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_wrong_fk_column` wfc ON wco.`id1` = wfc.`id` JOIN `schemapile_lakehouse`.`sp_300813_erzeuge_zielzustand`.`tab_wrong_fk_name` wfn ON wco.`id2` = wfn.`id`"
  },
  {
    "shape": "advanced",
    "question": "Show each scheduled class with its subject, room type, semester, and academic year.",
    "sql": "SELECT c.`class_id`, s.`subject_title`, r.`room_id`, rt.`room_type`, sem.`semester`, ay.`academic_year`, c.`start_time`, c.`end_time` FROM `schemapile_lakehouse`.`sp_352545_capstone`.`class` c JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`subject` s ON c.`subject_code` = s.`subject_code` JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`room` r ON c.`room_id` = r.`room_id` LEFT JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`room_type` rt ON r.`room_type` = rt.`room_type_id` JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`semester` sem ON c.`semester_id` = sem.`semester_id` JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`academic_year` ay ON sem.`academic_year_id` = ay.`academic_year_id`"
  },
  {
    "shape": "advanced",
    "question": "List attendance records with the student's course, class subject, and attendance status definition.",
    "sql": "SELECT a.`attendance_id`, a.`attendance_datetime`, st.`student_id`, co.`course_name`, su.`subject_title`, ast.`definition` AS attendance_definition FROM `schemapile_lakehouse`.`sp_352545_capstone`.`attendance` a JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`student` st ON a.`student_id` = st.`student_id` JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`course` co ON st.`course_id` = co.`course_id` JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`class` c ON a.`class_id` = c.`class_id` JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`subject` su ON c.`subject_code` = su.`subject_code` LEFT JOIN `schemapile_lakehouse`.`sp_352545_capstone`.`attendance_status` ast ON a.`attendance_status` = ast.`attendance_status`"
  },
  {
    "shape": "advanced",
    "question": "Show each medical consultation with the patient, doctor, hospital, and any hospitalization record.",
    "sql": "SELECT c.`idconsulta`, c.`data`, p.`nome` AS paciente_nome, m.`nome` AS medico_nome, h.`nome` AS hospital_nome, i.`idinternacao`, i.`entrada`, i.`saida` FROM `schemapile_lakehouse`.`sp_359146_formasnormais`.`consulta` c JOIN `schemapile_lakehouse`.`sp_359146_formasnormais`.`paciente` p ON c.`id_paciente` = p.`idpaciente` JOIN `schemapile_lakehouse`.`sp_359146_formasnormais`.`medico` m ON c.`id_medico` = m.`idmedico` JOIN `schemapile_lakehouse`.`sp_359146_formasnormais`.`hospital` h ON c.`id_hospital` = h.`idhospital` LEFT JOIN `schemapile_lakehouse`.`sp_359146_formasnormais`.`internacao` i ON c.`idconsulta` = i.`id_consulta`"
  },
  {
    "shape": "advanced",
    "question": "Show each employee with their department, title, and salary.",
    "sql": "SELECT e.`emp_no`, e.`first_name`, e.`last_name`, d.`dept_name`, t.`title`, s.`salary` FROM `schemapile_lakehouse`.`sp_422245_tableschemata`.`employees` e JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`dept_emp` de ON e.`emp_no` = de.`emp_no` JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`departments` d ON de.`dept_no` = d.`dept_no` JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`titles` t ON e.`emp_title_id` = t.`title_id` JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`salaries` s ON e.`emp_no` = s.`emp_no`"
  },
  {
    "shape": "aggregation",
    "question": "What is the average salary for each department and employee title?",
    "sql": "SELECT d.`dept_name`, t.`title`, AVG(s.`salary`) AS avg_salary, COUNT(DISTINCT e.`emp_no`) AS employee_count FROM `schemapile_lakehouse`.`sp_422245_tableschemata`.`employees` e JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`dept_emp` de ON e.`emp_no` = de.`emp_no` JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`departments` d ON de.`dept_no` = d.`dept_no` JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`titles` t ON e.`emp_title_id` = t.`title_id` JOIN `schemapile_lakehouse`.`sp_422245_tableschemata`.`salaries` s ON e.`emp_no` = s.`emp_no` GROUP BY d.`dept_name`, t.`title` ORDER BY avg_salary DESC"
  },
  {
    "shape": "aggregation",
    "question": "What is the total item value per user and order status?",
    "sql": "SELECT u.`id` AS user_id, u.`email`, o.`status`, SUM(CAST(i.`price_usd` AS DOUBLE)) AS total_item_value, COUNT(i.`id`) AS item_count FROM `schemapile_lakehouse`.`sp_435873_db`.`users` u JOIN `schemapile_lakehouse`.`sp_435873_db`.`orders` o ON u.`id` = o.`user_id` JOIN `schemapile_lakehouse`.`sp_435873_db`.`orders_items` oi ON o.`id` = oi.`order_id` JOIN `schemapile_lakehouse`.`sp_435873_db`.`items` i ON oi.`item_id` = i.`id` GROUP BY u.`id`, u.`email`, o.`status` ORDER BY total_item_value DESC"
  },
  {
    "shape": "advanced",
    "question": "Show each internet sale with the customer, territory, order date, and ship date labels.",
    "sql": "SELECT f.`ordersalesnumber`, c.`customerkey`, c.`firstname`, c.`lastname`, t.`salesterritorycountry`, od.`fulldatealternatekey` AS order_date, sd.`fulldatealternatekey` AS ship_date, f.`salesamount` FROM `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_factinternetsales` f JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimcustomer` c ON f.`customerkey` = c.`customerkey` JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimsalesterritory` t ON f.`salesterritorykey` = t.`salesterritorykey` JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimdate` od ON f.`orderdatekey` = od.`datekey` LEFT JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimdate` sd ON f.`shipdatekey` = sd.`datekey`"
  },
  {
    "shape": "aggregation",
    "question": "What are total internet sales by fiscal year, territory group, and customer education?",
    "sql": "SELECT d.`fiscalyear`, t.`salesterritorygroup`, c.`englisheducation`, SUM(CAST(f.`salesamount` AS DOUBLE)) AS total_sales, SUM(CAST(f.`orderquantity` AS DOUBLE)) AS total_quantity FROM `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_factinternetsales` f JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimdate` d ON f.`orderdatekey` = d.`datekey` JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimsalesterritory` t ON f.`salesterritorykey` = t.`salesterritorykey` JOIN `schemapile_lakehouse`.`sp_492424_create_adventuresoverflow`.`sales_dimcustomer` c ON f.`customerkey` = c.`customerkey` GROUP BY d.`fiscalyear`, t.`salesterritorygroup`, c.`englisheducation` ORDER BY d.`fiscalyear`, total_sales DESC"
  },
  {
    "shape": "advanced",
    "question": "Show each uploaded resource with its class, section, and book context.",
    "sql": "SELECT b.`id` AS book_id, b.`name` AS book_name, s.`id` AS section_id, s.`number` AS section_number, c.`id` AS class_id, c.`number` AS class_number, r.`id` AS resource_id, r.`name` AS resource_name, r.`path` FROM `schemapile_lakehouse`.`sp_535132_zhuanglang`.`book` b JOIN `schemapile_lakehouse`.`sp_535132_zhuanglang`.`section` s ON s.`belong` = b.`id` JOIN `schemapile_lakehouse`.`sp_535132_zhuanglang`.`class` c ON c.`belong` = s.`id` JOIN `schemapile_lakehouse`.`sp_535132_zhuanglang`.`resources` r ON r.`belong` = c.`id`"
  },
  {
    "shape": "advanced",
    "question": "Show each takeout user order with its base order, user, store, and included line items.",
    "sql": "SELECT uo.`order_id`, uo.`state`, uo.`reciever`, buyer.`username` AS buyer_username, st.`store_id`, st.`address` AS store_address, oi.`dish_id`, oi.`amount`, oi.`attribute` FROM `schemapile_lakehouse`.`sp_594700_takeout`.`user_orders` uo JOIN `schemapile_lakehouse`.`sp_594700_takeout`.`orders` o ON uo.`order_id` = o.`order_id` JOIN `schemapile_lakehouse`.`sp_594700_takeout`.`user` buyer ON o.`username` = buyer.`username` LEFT JOIN `schemapile_lakehouse`.`sp_594700_takeout`.`store` st ON buyer.`store_id` = st.`store_id` LEFT JOIN `schemapile_lakehouse`.`sp_594700_takeout`.`order_include` oi ON uo.`order_id` = oi.`order_id`"
  },
  {
    "shape": "aggregation",
    "question": "For each match and team, what score was recorded and how many players belong to that team?",
    "sql": "SELECT p.`id_partido`, p.`fecha`, jor.`id_jornada`, e.`id_equipo`, e.`nombre` AS equipo_nombre, m.`puntuacion`, COUNT(j.`id_jugador`) AS player_count FROM `schemapile_lakehouse`.`sp_596038_ddl_proyecto`.`partido` p JOIN `schemapile_lakehouse`.`sp_596038_ddl_proyecto`.`jornada` jor ON p.`id_jornada` = jor.`id_jornada` JOIN `schemapile_lakehouse`.`sp_596038_ddl_proyecto`.`marcador` m ON p.`id_partido` = m.`id_partido` JOIN `schemapile_lakehouse`.`sp_596038_ddl_proyecto`.`equipo` e ON m.`id_equipo` = e.`id_equipo` LEFT JOIN `schemapile_lakehouse`.`sp_596038_ddl_proyecto`.`jugador` j ON e.`id_equipo` = j.`id_equipo` GROUP BY p.`id_partido`, p.`fecha`, jor.`id_jornada`, e.`id_equipo`, e.`nombre`, m.`puntuacion`"
  }
]
