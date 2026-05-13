# Question-Generation Context (schemapile_lakehouse)

Generated from the live Neo4j graph for catalog `schemapile_lakehouse` across
20 schemas. The three sections below fill the placeholder
slots in `docs/proposal/more-questions.md`:

- The **Schema dump** section replaces `<<<PASTE THE OUTPUT OF fetch_schema_dump HERE>>>`.
- The **Foreign-key list** section replaces the `<<<PASTE THE FK LIST HERE ...>>>` block.
- The **Sample values** section replaces the `<<<PASTE THE SAMPLE-VALUE LIST HERE ...>>>` block.

This file is reproducible; regenerate with:

```
uv run --directory examples/schemapile python scripts/dump_question_context.py \
    > docs/proposal/questions-schema.md
```

FK confidence floor used for this dump: `>= 0.80`.

---

## Schema dump

```
Table: schemapile_lakehouse.sp_002489_project.activities
  id (STRING)
  iteration (STRING)
  current_task (STRING)

Table: schemapile_lakehouse.sp_002489_project.artifacts
  id (STRING)
  artifact (STRING)
  filename (STRING)

Table: schemapile_lakehouse.sp_002489_project.iterations
  id (STRING)
  project (STRING)
  iteration (STRING)
  number (STRING)
  start_date (STRING)
  end_date (STRING)
  current_activity (STRING)

Table: schemapile_lakehouse.sp_002489_project.members
  id (STRING)
  name (STRING)

Table: schemapile_lakehouse.sp_002489_project.project_members
  project (STRING)
  member (STRING)
  role (STRING)

Table: schemapile_lakehouse.sp_002489_project.projects
  id (STRING)
  name (STRING)
  description (STRING)
  process (STRING)
  current_iteration (STRING)
  start_date (STRING)
  end_date (STRING)
  version (STRING)

Table: schemapile_lakehouse.sp_002489_project.task_artifacts
  task (STRING)
  artifact (STRING)

Table: schemapile_lakehouse.sp_002489_project.tasks
  id (STRING)
  task (STRING)
  member (STRING)

Table: schemapile_lakehouse.sp_008582_auth_services_schema_mysql.api_key_table
  id (STRING)
  client_id (STRING)
  api_key (STRING)
  api_key_desc (STRING)
  active (STRING)

Table: schemapile_lakehouse.sp_008582_auth_services_schema_mysql.authority_table
  id (STRING)
  uid (STRING)
  authority (STRING)

Table: schemapile_lakehouse.sp_008582_auth_services_schema_mysql.client_id_table
  id (STRING)
  uid (STRING)
  client_id (STRING)

Table: schemapile_lakehouse.sp_008582_auth_services_schema_mysql.role_table
  id (STRING)
  role_name (STRING)

Table: schemapile_lakehouse.sp_008582_auth_services_schema_mysql.user_table
  id (STRING)
  uid (STRING)
  active (STRING)

Table: schemapile_lakehouse.sp_008582_auth_services_schema_mysql.username_password_table
  id (STRING)
  user_name (STRING)
  password (STRING)
  uid (STRING)

Table: schemapile_lakehouse.sp_020891_v01__impl_usuario.tb_grupo
  id_grupo (STRING)
  ds_nome (STRING)
  ds_descricao (STRING)

Table: schemapile_lakehouse.sp_020891_v01__impl_usuario.tb_permissao
  id_permissao (STRING)
  ds_permissao (STRING)
  ds_descricao (STRING)

Table: schemapile_lakehouse.sp_020891_v01__impl_usuario.tb_permissao_x_grupo
  id_permissao (STRING)
  id_grupo (STRING)

Table: schemapile_lakehouse.sp_020891_v01__impl_usuario.tb_usuario
  id_usuario (STRING)
  ds_nome (STRING)
  ds_login (STRING)
  ds_senha (STRING)
  fl_ativo (STRING)

Table: schemapile_lakehouse.sp_020891_v01__impl_usuario.tb_usuario_x_grupo
  id_usuario (STRING)
  id_grupo (STRING)

Table: schemapile_lakehouse.sp_033815_schema.groups_table
  id (STRING)
  name (STRING)
  students_number (STRING)

Table: schemapile_lakehouse.sp_033815_schema.lesson_types
  id (STRING)
  name (STRING)

Table: schemapile_lakehouse.sp_033815_schema.lessons
  uuid (STRING)
  subject_id (STRING)
  teacher_id (STRING)
  lesson_type (STRING)

Table: schemapile_lakehouse.sp_033815_schema.progress
  uuid (STRING)
  student_id (STRING)
  subject_id (STRING)
  grade (STRING)

Table: schemapile_lakehouse.sp_033815_schema.students
  id (STRING)
  name (STRING)
  course (STRING)
  phone_number (STRING)
  email (STRING)
  group_id (STRING)

Table: schemapile_lakehouse.sp_033815_schema.subjects
  id (STRING)
  name (STRING)
  hours_number (STRING)

Table: schemapile_lakehouse.sp_033815_schema.teacher_positions
  id (STRING)
  name (STRING)

Table: schemapile_lakehouse.sp_033815_schema.teachers
  id (STRING)
  name (STRING)
  phone_number (STRING)
  position (STRING)

Table: schemapile_lakehouse.sp_169810_init.addressbook
  uid (STRING)
  address (STRING)
  isdefault (STRING)

Table: schemapile_lakehouse.sp_169810_init.customer
  uid (STRING)
  username (STRING)
  password (STRING)

Table: schemapile_lakehouse.sp_169810_init.paymentmethod
  uid (STRING)
  cardnumber (STRING)
  type (STRING)
  isdefault (STRING)

Table: schemapile_lakehouse.sp_169810_init.review
  uid (STRING)
  sid (STRING)
  rating (STRING)
  comment (STRING)

Table: schemapile_lakehouse.sp_169810_init.shoe
  sid (STRING)
  stock (STRING)
  price (STRING)
  size (STRING)
  colour (STRING)
  image_url (STRING)
  name (STRING)
  description (STRING)
  brand (STRING)
  style (STRING)

Table: schemapile_lakehouse.sp_169810_init.transaction
  tid (STRING)
  uid (STRING)
  sid (STRING)
  datetime (STRING)
  quantity (STRING)
  address (STRING)
  cardnumber (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.account_access_methods
  id (STRING)
  account_id (STRING)
  name (STRING)
  hashed_password (STRING)
  algorithm (STRING)
  created_at (STRING)
  updated_at (STRING)
  deleted_at (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.account_organizations
  id (STRING)
  account_id (STRING)
  organization_id (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.account_roles
  id (STRING)
  account_id (STRING)
  role_id (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.accounts
  id (STRING)
  ratchet (STRING)
  created_at (STRING)
  updated_at (STRING)
  deleted_at (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.organizations
  id (STRING)
  name (STRING)
  kind (STRING)
  parent_id (STRING)
  scope_id (STRING)
  created_at (STRING)
  updated_at (STRING)
  deleted_at (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.permissions
  id (STRING)
  scope_id (STRING)
  resource_id (STRING)
  actions (STRING)
  frn (STRING)
  created_at (STRING)
  updated_at (STRING)
  deleted_at (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.resources
  id (STRING)
  name (STRING)
  system_id (STRING)
  description (STRING)
  actions (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.role_permissions
  id (STRING)
  role_id (STRING)
  permission_id (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.roles
  id (STRING)
  name (STRING)
  scope_id (STRING)
  created_at (STRING)
  updated_at (STRING)
  deleted_at (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.scope_domains
  id (STRING)
  scope_id (STRING)
  domain (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.scopes
  id (STRING)
  source (STRING)
  parent_id (STRING)
  parent_path (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.systems
  id (STRING)
  name (STRING)
  description (STRING)

Table: schemapile_lakehouse.sp_178181_v4_026__create_unified_user.users
  id (STRING)
  account_id (STRING)
  email (STRING)
  is_disabled (STRING)
  disabled_by (STRING)
  is_blacklisted (STRING)
  blacklisted_by (STRING)
  name (STRING)
  phone_number (STRING)
  created_at (STRING)
  updated_at (STRING)
  deleted_at (STRING)

Table: schemapile_lakehouse.sp_266628_tables.block
  username (STRING)
  blocked_user (STRING)

Table: schemapile_lakehouse.sp_266628_tables.follow
  follower (STRING)
  following (STRING)

Table: schemapile_lakehouse.sp_266628_tables.hashtag
  hashtag (STRING)
  tweetid (STRING)

Table: schemapile_lakehouse.sp_266628_tables.likes
  username (STRING)
  tweetid (STRING)
  timestamp_l (STRING)

Table: schemapile_lakehouse.sp_266628_tables.login_record
  username (STRING)
  timestamp_t (STRING)

Table: schemapile_lakehouse.sp_266628_tables.message
  mess_id (STRING)
  type (STRING)
  s_id (STRING)
  r_id (STRING)
  content (STRING)
  ref_id (STRING)
  timestamp_t (STRING)

Table: schemapile_lakehouse.sp_266628_tables.tweet
  tweetid (STRING)
  type (STRING)
  username (STRING)
  tweet_content (STRING)
  ref_id (STRING)
  timestamp_t (STRING)
  likes (STRING)

Table: schemapile_lakehouse.sp_266628_tables.users
  username (STRING)
  firstname (STRING)
  lastname (STRING)
  birthdate (STRING)
  registery_date (STRING)
  bio (STRING)
  followers (STRING)
  following (STRING)
  password (STRING)

Table: schemapile_lakehouse.sp_289361_schema.advertiser
  advertiserid (STRING)

Table: schemapile_lakehouse.sp_289361_schema.articles
  articlesid (STRING)
  advertiserid (STRING)
  categoryid (STRING)
  title (STRING)
  description (STRING)
  markdown (STRING)
  imagearticle (STRING)

Table: schemapile_lakehouse.sp_289361_schema.category
  name (STRING)
  description (STRING)

Table: schemapile_lakehouse.sp_289361_schema.material
  materialid (STRING)
  articlesid (STRING)
  filepath (STRING)
  virustotalurl (STRING)

Table: schemapile_lakehouse.sp_289361_schema.reader
  readerid (STRING)

Table: schemapile_lakehouse.sp_289361_schema.users
  username (STRING)
  email (STRING)
  name (STRING)
  surname (STRING)
  biografy (STRING)
  imageprofile (STRING)
  password (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_2_column_pk
  id1 (STRING)
  id2 (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_a
  tab_a_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_b
  tab_b_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_c
  tab_c_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_d
  tab_d_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_del_cas
  tab_a_id (STRING)
  tab_b_id (STRING)
  tab_c_id (STRING)
  tab_d_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_index_test
  tab_index_test_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_mixed_name_1
  id (STRING)
  id_1 (STRING)
  id_2 (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_mixed_name_2
  id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_no_action
  tab_a_id (STRING)
  tab_b_id (STRING)
  tab_c_id (STRING)
  tab_d_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_set_null
  tab_a_id (STRING)
  tab_b_id (STRING)
  tab_c_id (STRING)
  tab_d_id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_wrong_fk_column
  id (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_wrong_fk_column_order
  id1 (STRING)
  id2 (STRING)

Table: schemapile_lakehouse.sp_300813_erzeuge_zielzustand.tab_wrong_fk_name
  id (STRING)

Table: schemapile_lakehouse.sp_310630_schema.projects
  id (STRING)
  name (STRING)
  owner_id (STRING)
  forked_from (STRING)
  deleted (STRING)

Table: schemapile_lakehouse.sp_310630_schema.result_files
  login (STRING)
  name (STRING)
  link_file (STRING)
  path_file (STRING)
  path_copy_file (STRING)
  n_authors (STRING)
  n_revs (STRING)
  age_months (STRING)
  duplicate (STRING)
  is_xml (STRING)

Table: schemapile_lakehouse.sp_310630_schema.result_projects
  login (STRING)
  name (STRING)
  language (STRING)
  created_at (STRING)
  last_commit_at (STRING)
  n_commits (STRING)
  location_country (STRING)

Table: schemapile_lakehouse.sp_310630_schema.result_xml_original
  path_file (STRING)
  path_copy_file (STRING)
  has_copy (STRING)
  n_xml_elements (STRING)
  valid (STRING)
  valid_after_repairing (STRING)
  constraints_list (STRING)
  constraints_list_after_repairing (STRING)
  bpmndiagram (STRING)

Table: schemapile_lakehouse.sp_310630_schema.to_query_projects
  id (STRING)
  login (STRING)
  name (STRING)
  status (STRING)

Table: schemapile_lakehouse.sp_310630_schema.users
  id (STRING)
  login (STRING)
  country_code (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.academic_year
  academic_year_id (STRING)
  academic_year (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.attendance
  attendance_id (STRING)
  class_id (STRING)
  student_id (STRING)
  attendance_datetime (STRING)
  attendance_status (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.attendance_status
  attendance_status (STRING)
  definition (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.class
  class_id (STRING)
  subject_code (STRING)
  room_id (STRING)
  semester_id (STRING)
  start_time (STRING)
  end_time (STRING)
  days (STRING)
  instructor_id (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.class_calendar
  class_calendar_id (STRING)
  class_id (STRING)
  event_date (STRING)
  event (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.class_member
  student_id (STRING)
  class_id (STRING)
  status (STRING)
  seat_id (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.consultation
  consultation_id (STRING)
  instructor_id (STRING)
  class_id (STRING)
  datetime_scheduled (STRING)
  start_time (STRING)
  end_time (STRING)
  issue_topic (STRING)
  remarks (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.consultee
  consultation_id (STRING)
  student_id (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.course
  course_id (STRING)
  course_name (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.instructor
  instructor_id (STRING)
  position (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.room
  room_id (STRING)
  room_type (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.room_type
  room_type_id (STRING)
  room_type (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.seat_plan
  seat_id (STRING)
  row (STRING)
  column (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.semester
  semester_id (STRING)
  semester (STRING)
  academic_year_id (STRING)
  start_date (STRING)
  end_date (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.student
  student_id (STRING)
  course_id (STRING)
  year_level (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.subject
  subject_code (STRING)
  subject_title (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.user
  user_id (STRING)
  role_id (STRING)
  password (STRING)
  status (STRING)
  fname (STRING)
  mname (STRING)
  lname (STRING)
  birthdate (STRING)
  contact_no (STRING)

Table: schemapile_lakehouse.sp_352545_capstone.user_role
  role_id (STRING)
  role (STRING)

Table: schemapile_lakehouse.sp_359146_formasnormais.consulta
  idconsulta (STRING)
  id_paciente (STRING)
  id_medico (STRING)
  id_hospital (STRING)
  data (STRING)
  diagnostivo (STRING)

Table: schemapile_lakehouse.sp_359146_formasnormais.hospital
  idhospital (STRING)
  nome (STRING)
  bairro (STRING)
  cidade (STRING)
  estado (STRING)

Table: schemapile_lakehouse.sp_359146_formasnormais.internacao
  idinternacao (STRING)
  entrada (STRING)
  quarto (STRING)
  saida (STRING)
  observacoes (STRING)
  id_consulta (STRING)

Table: schemapile_lakehouse.sp_359146_formasnormais.medico
  idmedico (STRING)
  nome (STRING)
  sexo (STRING)
  especialidade (STRING)
  funcionario (STRING)

Table: schemapile_lakehouse.sp_359146_formasnormais.paciente
  idpaciente (STRING)
  nome (STRING)
  sexo (STRING)
  email (STRING)
  nascimento (STRING)

Table: schemapile_lakehouse.sp_365427_schema.appointments
  appoi_id (STRING)
  day (STRING)
  time_from (STRING)
  time_to (STRING)
  pat_id (STRING)
  doc_id (STRING)

Table: schemapile_lakehouse.sp_365427_schema.contact
  contant_id (STRING)
  phone_number (STRING)
  e_mail (STRING)
  pat_id (STRING)
  doc_id (STRING)

Table: schemapile_lakehouse.sp_365427_schema.doctor
  doctor_id (STRING)
  doctor_name (STRING)
  doctor_speciailty (STRING)
  doc_location (STRING)

Table: schemapile_lakehouse.sp_365427_schema.patient
  patient_id (STRING)
  patient_first_name (STRING)
  patient_last_name (STRING)
  gender (STRING)
  date_of_birth (STRING)
  patient_image (STRING)
  patient_password (STRING)

Table: schemapile_lakehouse.sp_413175_01_tabledesign_exam21oct2018.colonists
  id (STRING)
  first_name (STRING)
  last_name (STRING)
  ucn (STRING)
  birth_date (STRING)

Table: schemapile_lakehouse.sp_413175_01_tabledesign_exam21oct2018.journeys
  id (STRING)
  journey_start (STRING)
  journey_end (STRING)
  purpose (STRING)
  destination_spaceport_id (STRING)
  spaceship_id (STRING)

Table: schemapile_lakehouse.sp_413175_01_tabledesign_exam21oct2018.planets
  id (STRING)
  name (STRING)

Table: schemapile_lakehouse.sp_413175_01_tabledesign_exam21oct2018.spaceports
  id (STRING)
  name (STRING)
  planet_id (STRING)

Table: schemapile_lakehouse.sp_413175_01_tabledesign_exam21oct2018.spaceships
  id (STRING)
  name (STRING)
  manufacturer (STRING)
  light_speed_rate (STRING)

Table: schemapile_lakehouse.sp_413175_01_tabledesign_exam21oct2018.travel_cards
  id (STRING)
  card_number (STRING)
  job_during_journey (STRING)
  colonist_id (STRING)
  journey_id (STRING)

Table: schemapile_lakehouse.sp_422245_tableschemata.departments
  dept_no (STRING)
  dept_name (STRING)

Table: schemapile_lakehouse.sp_422245_tableschemata.dept_emp
  emp_no (STRING)
  dept_no (STRING)

Table: schemapile_lakehouse.sp_422245_tableschemata.dept_manager
  dept_no (STRING)
  emp_no (STRING)

Table: schemapile_lakehouse.sp_422245_tableschemata.employees
  emp_no (STRING)
  emp_title_id (STRING)
  birth_date (STRING)
  first_name (STRING)
  last_name (STRING)
  sex (STRING)
  hire_date (STRING)

Table: schemapile_lakehouse.sp_422245_tableschemata.salaries
  emp_no (STRING)
  salary (STRING)

Table: schemapile_lakehouse.sp_422245_tableschemata.titles
  title_id (STRING)
  title (STRING)

Table: schemapile_lakehouse.sp_435873_db.categories
  id (STRING)
  name (STRING)

Table: schemapile_lakehouse.sp_435873_db.items
  id (STRING)
  name (STRING)
  price_usd (STRING)
  description (STRING)
  category_id (STRING)

Table: schemapile_lakehouse.sp_435873_db.orders
  id (STRING)
  status (STRING)
  created_at (STRING)
  delivered_at (STRING)
  cacelled_at (STRING)
  opened_at (STRING)
  accepted_at (STRING)
  user_id (STRING)

Table: schemapile_lakehouse.sp_435873_db.orders_items
  order_id (STRING)
  item_id (STRING)

Table: schemapile_lakehouse.sp_435873_db.spots
  id (STRING)
  label (STRING)
  is_free (STRING)

Table: schemapile_lakehouse.sp_435873_db.spots_users
  spot_id (STRING)
  user_id (STRING)

Table: schemapile_lakehouse.sp_435873_db.users
  id (STRING)
  email (STRING)
  password (STRING)
  name (STRING)
  role (STRING)

Table: schemapile_lakehouse.sp_492424_create_adventuresoverflow.sales_dimcustomer
  customerkey (STRING)
  geographykey (STRING)
  firstname (STRING)
  lastname (STRING)
  birthdate (STRING)
  maritalstatus (STRING)
  gender (STRING)
  emailaddress (STRING)
  yearlyincome (STRING)
  englisheducation (STRING)
  datefirstpurchase (STRING)
  salesterritorykey (STRING)
  city (STRING)

Table: schemapile_lakehouse.sp_492424_create_adventuresoverflow.sales_dimdate
  datekey (STRING)
  fulldatealternatekey (STRING)
  englishdaynameofweek (STRING)
  englishmonthname (STRING)
  fiscalquarter (STRING)
  fiscalyear (STRING)
  fiscalsemester (STRING)

Table: schemapile_lakehouse.sp_492424_create_adventuresoverflow.sales_dimdateholiday
  dateholidaykey (STRING)
  datekey (STRING)
  territorycode (STRING)
  holidayflag (STRING)
  holidayname (STRING)

Table: schemapile_lakehouse.sp_492424_create_adventuresoverflow.sales_dimsalesterritory
  salesterritorykey (STRING)
  salesterritoryregion (STRING)
  salesterritorycountry (STRING)
  salesterritorygroup (STRING)

Table: schemapile_lakehouse.sp_492424_create_adventuresoverflow.sales_factinternetsales
  ordersalesnumber (STRING)
  productkey (STRING)
  orderdatekey (STRING)
  duedatekey (STRING)
  shipdatekey (STRING)
  customerkey (STRING)
  salesterritorykey (STRING)
  orderquantity (STRING)
  unitprice (STRING)
  discountamount (STRING)
  salesamount (STRING)
  orderdate (STRING)
  duedate (STRING)
  shipdate (STRING)

Table: schemapile_lakehouse.sp_492424_create_adventuresoverflow.sales_factposts
  postkey (STRING)
  answercount (STRING)
  body (STRING)
  closeddate (STRING)
  commentcount (STRING)
  creationdate (STRING)
  favoritecount (STRING)
  lastactivitydate (STRING)
  lasteditdate (STRING)
  score (STRING)
  title (STRING)
  viewcount (STRING)
  closeddatekey (STRING)
  creationdatekey (STRING)
  lastactivitydatekey (STRING)
  lasteditdatekey (STRING)

Table: schemapile_lakehouse.sp_535132_zhuanglang.book
  id (STRING)
  name (STRING)
  upload (STRING)
  discipline (STRING)

Table: schemapile_lakehouse.sp_535132_zhuanglang.class
  id (STRING)
  number (STRING)
  upload (STRING)
  note (STRING)
  belong (STRING)

Table: schemapile_lakehouse.sp_535132_zhuanglang.resources
  id (STRING)
  md5 (STRING)
  name (STRING)
  path (STRING)
  upload (STRING)
  videoorslide (STRING)
  bilibili (STRING)
  note (STRING)
  belong (STRING)

Table: schemapile_lakehouse.sp_535132_zhuanglang.section
  id (STRING)
  number (STRING)
  upload (STRING)
  note (STRING)
  belong (STRING)

Table: schemapile_lakehouse.sp_535132_zhuanglang.teacher
  username (STRING)
  pwd (STRING)
  salt (STRING)
  nkuorzhuanglang (STRING)
  name (STRING)
  person_no (STRING)
  mail (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.location
  uuid (STRING)
  order_id (STRING)
  x (STRING)
  y (STRING)
  time (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.order_include
  dish_id (STRING)
  order_id (STRING)
  amount (STRING)
  attribute (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.orders
  order_id (STRING)
  username (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.score
  use_username (STRING)
  username (STRING)
  score (STRING)
  comment (STRING)
  img (STRING)
  type (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.score_dish
  username (STRING)
  dish_id (STRING)
  grade (STRING)
  comment (STRING)
  img (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.store
  store_id (STRING)
  username (STRING)
  address (STRING)
  s_phone (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.user
  username (STRING)
  store_id (STRING)
  use_username (STRING)
  role (STRING)
  phone (STRING)

Table: schemapile_lakehouse.sp_594700_takeout.user_orders
  username (STRING)
  order_id (STRING)
  state (STRING)
  reciever (STRING)
  recieve_address (STRING)
  r_phone (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.equipo
  id_equipo (STRING)
  nombre (STRING)
  fecha_creacion (STRING)
  comentario (STRING)
  lugar (STRING)
  id_persona (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.jornada
  id_jornada (STRING)
  fecha_inicio (STRING)
  fecha_fin (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.jugador
  id_jugador (STRING)
  dni (STRING)
  nombre (STRING)
  apellido1 (STRING)
  apellido2 (STRING)
  nickname (STRING)
  sueldo (STRING)
  fecha_alta (STRING)
  comentario (STRING)
  id_equipo (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.marcador
  id_marcador (STRING)
  puntuacion (STRING)
  visitante (STRING)
  id_partido (STRING)
  id_equipo (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.partido
  id_partido (STRING)
  fecha (STRING)
  id_jornada (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.perfil
  id_perfil (STRING)
  nombre (STRING)

Table: schemapile_lakehouse.sp_596038_ddl_proyecto.persona
  id_persona (STRING)
  nombre (STRING)
  apellido1 (STRING)
  apellido2 (STRING)
  fecha_alta (STRING)
  usuario (STRING)
  contrasenna (STRING)
  email (STRING)
  id_perfil (STRING)
```

---

## Foreign-key list

Format: `schema.table.column -> schema.table.column  # source=..., confidence=...`.
Trailing comments are informational; strip them before pasting into the
question-generation prompt if you prefer the simpler form.

```
sp_002489_project.activities.current_task -> sp_002489_project.tasks.id  # source=semantic, confidence=0.89
sp_002489_project.activities.id -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.87
sp_002489_project.activities.id -> sp_002489_project.iterations.id  # source=semantic, confidence=0.85
sp_002489_project.activities.id -> sp_002489_project.members.id  # source=semantic, confidence=0.87
sp_002489_project.activities.id -> sp_002489_project.projects.id  # source=semantic, confidence=0.90
sp_002489_project.activities.id -> sp_002489_project.tasks.id  # source=semantic, confidence=0.90
sp_002489_project.activities.iteration -> sp_002489_project.iterations.id  # source=semantic, confidence=0.90
sp_002489_project.activities.iteration -> sp_002489_project.projects.id  # source=semantic, confidence=0.85
sp_002489_project.activities.iteration -> sp_002489_project.tasks.id  # source=semantic, confidence=0.86
sp_002489_project.artifacts.id -> sp_002489_project.iterations.id  # source=semantic, confidence=0.85
sp_002489_project.artifacts.id -> sp_002489_project.members.id  # source=semantic, confidence=0.86
sp_002489_project.artifacts.id -> sp_002489_project.projects.id  # source=semantic, confidence=0.90
sp_002489_project.artifacts.id -> sp_002489_project.tasks.id  # source=semantic, confidence=0.87
sp_002489_project.iterations.current_activity -> sp_002489_project.activities.id  # source=semantic, confidence=0.87
sp_002489_project.iterations.id -> sp_002489_project.members.id  # source=semantic, confidence=0.87
sp_002489_project.iterations.id -> sp_002489_project.projects.id  # source=semantic, confidence=0.90
sp_002489_project.iterations.id -> sp_002489_project.tasks.id  # source=semantic, confidence=0.89
sp_002489_project.iterations.number -> sp_002489_project.projects.id  # source=semantic, confidence=0.86
sp_002489_project.iterations.project -> sp_002489_project.activities.id  # source=semantic, confidence=0.85
sp_002489_project.iterations.project -> sp_002489_project.members.id  # source=semantic, confidence=0.86
sp_002489_project.iterations.project -> sp_002489_project.projects.id  # source=semantic, confidence=0.90
sp_002489_project.iterations.project -> sp_002489_project.tasks.id  # source=semantic, confidence=0.86
sp_002489_project.members.id -> sp_002489_project.projects.id  # source=semantic, confidence=0.90
sp_002489_project.members.id -> sp_002489_project.tasks.id  # source=semantic, confidence=0.89
sp_002489_project.members.name -> sp_002489_project.projects.id  # source=semantic, confidence=0.87
sp_002489_project.project_members.member -> sp_002489_project.members.id  # source=semantic, confidence=0.90
sp_002489_project.project_members.member -> sp_002489_project.projects.id  # source=semantic, confidence=0.88
sp_002489_project.project_members.project -> sp_002489_project.members.id  # source=semantic, confidence=0.90
sp_002489_project.project_members.project -> sp_002489_project.projects.id  # source=semantic, confidence=0.90
sp_002489_project.project_members.role -> sp_002489_project.members.id  # source=semantic, confidence=0.90
sp_002489_project.project_members.role -> sp_002489_project.projects.id  # source=semantic, confidence=0.85
sp_002489_project.projects.current_iteration -> sp_002489_project.iterations.id  # source=semantic, confidence=0.90
sp_002489_project.projects.current_iteration -> sp_002489_project.tasks.id  # source=semantic, confidence=0.86
sp_002489_project.projects.description -> sp_002489_project.activities.id  # source=semantic, confidence=0.88
sp_002489_project.projects.description -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.85
sp_002489_project.projects.description -> sp_002489_project.members.id  # source=semantic, confidence=0.88
sp_002489_project.projects.end_date -> sp_002489_project.activities.id  # source=semantic, confidence=0.85
sp_002489_project.projects.end_date -> sp_002489_project.tasks.id  # source=semantic, confidence=0.85
sp_002489_project.projects.id -> sp_002489_project.tasks.id  # source=semantic, confidence=0.90
sp_002489_project.projects.name -> sp_002489_project.activities.id  # source=semantic, confidence=0.89
sp_002489_project.projects.name -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.88
sp_002489_project.projects.name -> sp_002489_project.iterations.id  # source=semantic, confidence=0.87
sp_002489_project.projects.name -> sp_002489_project.members.id  # source=semantic, confidence=0.89
sp_002489_project.projects.name -> sp_002489_project.tasks.id  # source=semantic, confidence=0.89
sp_002489_project.projects.process -> sp_002489_project.activities.id  # source=semantic, confidence=0.88
sp_002489_project.projects.process -> sp_002489_project.tasks.id  # source=semantic, confidence=0.86
sp_002489_project.projects.start_date -> sp_002489_project.activities.id  # source=semantic, confidence=0.86
sp_002489_project.projects.start_date -> sp_002489_project.members.id  # source=semantic, confidence=0.85
sp_002489_project.projects.start_date -> sp_002489_project.tasks.id  # source=semantic, confidence=0.87
sp_002489_project.projects.version -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.86
sp_002489_project.projects.version -> sp_002489_project.members.id  # source=semantic, confidence=0.85
sp_002489_project.task_artifacts.artifact -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.90
sp_002489_project.task_artifacts.artifact -> sp_002489_project.tasks.id  # source=semantic, confidence=0.87
sp_002489_project.task_artifacts.task -> sp_002489_project.activities.id  # source=semantic, confidence=0.86
sp_002489_project.task_artifacts.task -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.90
sp_002489_project.task_artifacts.task -> sp_002489_project.projects.id  # source=semantic, confidence=0.86
sp_002489_project.task_artifacts.task -> sp_002489_project.tasks.id  # source=semantic, confidence=0.90
sp_002489_project.tasks.member -> sp_002489_project.activities.id  # source=semantic, confidence=0.86
sp_002489_project.tasks.member -> sp_002489_project.members.id  # source=semantic, confidence=0.90
sp_002489_project.tasks.member -> sp_002489_project.projects.id  # source=semantic, confidence=0.86
sp_002489_project.tasks.task -> sp_002489_project.activities.id  # source=semantic, confidence=0.89
sp_002489_project.tasks.task -> sp_002489_project.artifacts.id  # source=semantic, confidence=0.85
sp_002489_project.tasks.task -> sp_002489_project.members.id  # source=semantic, confidence=0.87
sp_002489_project.tasks.task -> sp_002489_project.projects.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.api_key_table.active -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.api_key_table.active -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.api_key_table.active -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.api_key_table.active -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.active -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.api_key_table.api_key -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key_desc -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key_desc -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key_desc -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.api_key_table.api_key_desc -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.api_key_desc -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.client_id -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.api_key_table.client_id -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.client_id -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.87
sp_008582_auth_services_schema_mysql.api_key_table.client_id -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.client_id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.id -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.id -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.id -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.id -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.api_key_table.id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.authority -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.authority_table.authority -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.86
sp_008582_auth_services_schema_mysql.authority_table.authority -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.authority -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.authority_table.authority -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.authority_table.id -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.id -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.id -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.uid -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.uid -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.authority_table.uid -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.uid -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.authority_table.uid -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.client_id -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.client_id -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.client_id_table.client_id -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.client_id_table.client_id -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.client_id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.id -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.id -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.uid -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.uid -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.uid -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.uid -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.client_id_table.uid -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.role_table.id -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.role_table.id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.role_table.role_name -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.role_table.role_name -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.role_table.role_name -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.86
sp_008582_auth_services_schema_mysql.role_table.role_name -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.role_table.role_name -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.user_table.active -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.user_table.active -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.user_table.active -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.86
sp_008582_auth_services_schema_mysql.user_table.active -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.user_table.active -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.user_table.id -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.user_table.uid -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.user_table.uid -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.user_table.uid -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.user_table.uid -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.user_table.uid -> sp_008582_auth_services_schema_mysql.username_password_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.password -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.password -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.username_password_table.password -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.username_password_table.password -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.88
sp_008582_auth_services_schema_mysql.username_password_table.password -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.uid -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.uid -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.username_password_table.uid -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.uid -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.uid -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.user_name -> sp_008582_auth_services_schema_mysql.api_key_table.id  # source=semantic, confidence=0.90
sp_008582_auth_services_schema_mysql.username_password_table.user_name -> sp_008582_auth_services_schema_mysql.authority_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.username_password_table.user_name -> sp_008582_auth_services_schema_mysql.client_id_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.username_password_table.user_name -> sp_008582_auth_services_schema_mysql.role_table.id  # source=semantic, confidence=0.89
sp_008582_auth_services_schema_mysql.username_password_table.user_name -> sp_008582_auth_services_schema_mysql.user_table.id  # source=semantic, confidence=0.90
sp_033815_schema.groups_table.students_number -> sp_033815_schema.students.id  # source=semantic, confidence=0.90
sp_033815_schema.lesson_types.id -> sp_033815_schema.students.id  # source=semantic, confidence=0.85
sp_033815_schema.lesson_types.id -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.88
sp_033815_schema.lesson_types.id -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.89
sp_033815_schema.lesson_types.name -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.87
sp_033815_schema.lessons.lesson_type -> sp_033815_schema.lesson_types.id  # source=semantic, confidence=0.90
sp_033815_schema.lessons.lesson_type -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.86
sp_033815_schema.lessons.subject_id -> sp_033815_schema.lesson_types.id  # source=semantic, confidence=0.90
sp_033815_schema.lessons.subject_id -> sp_033815_schema.subjects.id  # source=semantic, confidence=0.90
sp_033815_schema.lessons.teacher_id -> sp_033815_schema.lesson_types.id  # source=semantic, confidence=0.90
sp_033815_schema.lessons.teacher_id -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.90
sp_033815_schema.lessons.teacher_id -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.90
sp_033815_schema.lessons.uuid -> sp_033815_schema.lesson_types.id  # source=semantic, confidence=0.88
sp_033815_schema.progress.student_id -> sp_033815_schema.students.id  # source=semantic, confidence=0.90
sp_033815_schema.progress.subject_id -> sp_033815_schema.subjects.id  # source=semantic, confidence=0.89
sp_033815_schema.students.course -> sp_033815_schema.lesson_types.id  # source=semantic, confidence=0.86
sp_033815_schema.students.group_id -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.85
sp_033815_schema.students.group_id -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.85
sp_033815_schema.students.id -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.87
sp_033815_schema.students.id -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.88
sp_033815_schema.students.name -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.87
sp_033815_schema.teacher_positions.id -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.90
sp_033815_schema.teacher_positions.name -> sp_033815_schema.teachers.id  # source=semantic, confidence=0.90
sp_033815_schema.teachers.name -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.90
sp_033815_schema.teachers.phone_number -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.90
sp_033815_schema.teachers.position -> sp_033815_schema.teacher_positions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_access_methods.account_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.algorithm -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_access_methods.created_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.deleted_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.hashed_password -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.name -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.name -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.name -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_access_methods.name -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.name -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_access_methods.name -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_access_methods.updated_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.updated_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_access_methods.updated_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_access_methods.updated_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.account_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_organizations.organization_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_roles.account_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.account_roles.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.account_roles.role_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.created_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.accounts.deleted_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.accounts.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.accounts.ratchet -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.accounts.updated_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.created_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.organizations.deleted_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.kind -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.kind -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.kind -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.kind -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.kind -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.organizations.name -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.parent_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.organizations.scope_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.scope_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.scope_id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.scope_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.scope_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.organizations.scope_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.organizations.updated_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.organizations.updated_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.updated_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.organizations.updated_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.actions -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.actions -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.actions -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.actions -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.actions -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.actions -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.created_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.permissions.deleted_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.deleted_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.deleted_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.frn -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.resource_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.permissions.scope_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.permissions.updated_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.resources.description -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.description -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.resources.id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.resources.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.resources.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.resources.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.resources.name -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.name -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.name -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.name -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.resources.name -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.resources.name -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.resources.system_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.role_permissions.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.role_permissions.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.role_permissions.permission_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.role_permissions.role_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.roles.created_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.created_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.roles.created_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.created_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.roles.deleted_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.deleted_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.roles.deleted_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.roles.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.roles.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.roles.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.roles.name -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.roles.scope_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.scope_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.roles.scope_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.scope_id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.roles.scope_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.scope_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.roles.updated_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.updated_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.roles.updated_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.roles.updated_at -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scope_domains.domain -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scope_domains.domain -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scope_domains.id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scope_domains.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.scope_domains.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.scope_domains.scope_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.scope_domains.scope_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.scope_domains.scope_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scope_domains.scope_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scope_domains.scope_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scope_domains.scope_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.scopes.id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.scopes.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scopes.parent_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scopes.parent_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.scopes.parent_id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scopes.parent_id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scopes.parent_path -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scopes.source -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.scopes.source -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.scopes.source -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.scopes.source -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.systems.description -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.systems.description -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.systems.description -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.systems.description -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.systems.description -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.systems.description -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.systems.id -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.systems.name -> sp_178181_v4_026__create_unified_user.users.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.account_id -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.created_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.deleted_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.deleted_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.disabled_by -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.email -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.is_disabled -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.account_roles.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.89
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.scope_domains.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.88
sp_178181_v4_026__create_unified_user.users.name -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.90
sp_178181_v4_026__create_unified_user.users.phone_number -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.phone_number -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.phone_number -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.phone_number -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.phone_number -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.account_access_methods.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.account_organizations.id  # source=semantic, confidence=0.85
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.accounts.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.organizations.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.permissions.id  # source=semantic, confidence=0.87
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.resources.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.role_permissions.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.roles.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.scopes.id  # source=semantic, confidence=0.86
sp_178181_v4_026__create_unified_user.users.updated_at -> sp_178181_v4_026__create_unified_user.systems.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.86
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id1 -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.86
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_2_column_pk.id2 -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_a.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_a.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_a.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_a.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_a.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_a.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_b.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_b.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_b.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_b.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_b.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_c.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_c.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_c.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_c.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_d.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_d.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_d.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_del_cas.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.87
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_1 -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_1 -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_1 -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_1 -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_1 -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.86
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_1 -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_2 -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_2 -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_2 -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_2 -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_2 -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.87
sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id_2 -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_no_action.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_no_action.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_no_action.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_no_action.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_set_null.tab_a_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.87
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_set_null.tab_b_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.87
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_set_null.tab_c_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.87
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_a.tab_a_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_b.tab_b_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_c.tab_c_id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_d.tab_d_id  # source=inferred_metadata, confidence=0.83
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_index_test.tab_index_test_id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_1.id  # source=semantic, confidence=0.89
sp_300813_erzeuge_zielzustand.tab_set_null.tab_d_id -> sp_300813_erzeuge_zielzustand.tab_mixed_name_2.id  # source=semantic, confidence=0.88
sp_300813_erzeuge_zielzustand.tab_wrong_fk_column.id -> sp_300813_erzeuge_zielzustand.tab_wrong_fk_name.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_wrong_fk_column_order.id1 -> sp_300813_erzeuge_zielzustand.tab_wrong_fk_column.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_wrong_fk_column_order.id1 -> sp_300813_erzeuge_zielzustand.tab_wrong_fk_name.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_wrong_fk_column_order.id2 -> sp_300813_erzeuge_zielzustand.tab_wrong_fk_column.id  # source=semantic, confidence=0.90
sp_300813_erzeuge_zielzustand.tab_wrong_fk_column_order.id2 -> sp_300813_erzeuge_zielzustand.tab_wrong_fk_name.id  # source=semantic, confidence=0.90
sp_310630_schema.projects.deleted -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.90
sp_310630_schema.projects.id -> sp_310630_schema.to_query_projects.id  # source=inferred_metadata, confidence=0.83
sp_310630_schema.projects.id -> sp_310630_schema.users.id  # source=inferred_metadata, confidence=0.83
sp_310630_schema.projects.name -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.90
sp_310630_schema.projects.owner_id -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.89
sp_310630_schema.result_projects.created_at -> sp_310630_schema.projects.id  # source=semantic, confidence=0.90
sp_310630_schema.result_projects.created_at -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.90
sp_310630_schema.result_projects.language -> sp_310630_schema.projects.id  # source=semantic, confidence=0.88
sp_310630_schema.result_projects.language -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.85
sp_310630_schema.result_projects.last_commit_at -> sp_310630_schema.projects.id  # source=semantic, confidence=0.86
sp_310630_schema.result_projects.location_country -> sp_310630_schema.projects.id  # source=semantic, confidence=0.89
sp_310630_schema.result_projects.location_country -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.87
sp_310630_schema.result_projects.login -> sp_310630_schema.projects.id  # source=semantic, confidence=0.90
sp_310630_schema.result_projects.login -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.89
sp_310630_schema.result_projects.n_commits -> sp_310630_schema.projects.id  # source=semantic, confidence=0.85
sp_310630_schema.result_projects.name -> sp_310630_schema.projects.id  # source=semantic, confidence=0.90
sp_310630_schema.result_projects.name -> sp_310630_schema.to_query_projects.id  # source=semantic, confidence=0.90
sp_310630_schema.to_query_projects.id -> sp_310630_schema.users.id  # source=inferred_metadata, confidence=0.83
sp_310630_schema.to_query_projects.login -> sp_310630_schema.projects.id  # source=semantic, confidence=0.90
sp_310630_schema.to_query_projects.name -> sp_310630_schema.projects.id  # source=semantic, confidence=0.90
sp_310630_schema.to_query_projects.status -> sp_310630_schema.projects.id  # source=semantic, confidence=0.90
sp_352545_capstone.academic_year.academic_year -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.academic_year.academic_year_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.class.class_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.87
sp_352545_capstone.class.class_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.89
sp_352545_capstone.class.instructor_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.88
sp_352545_capstone.class.instructor_id -> sp_352545_capstone.instructor.instructor_id  # source=inferred_metadata, confidence=0.83
sp_352545_capstone.class.room_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.86
sp_352545_capstone.class.room_id -> sp_352545_capstone.room.room_id  # source=inferred_metadata, confidence=0.83
sp_352545_capstone.class.room_id -> sp_352545_capstone.room_type.room_type_id  # source=semantic, confidence=0.90
sp_352545_capstone.class.semester_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.class.semester_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.class.semester_id -> sp_352545_capstone.instructor.instructor_id  # source=semantic, confidence=0.87
sp_352545_capstone.class.start_time -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.85
sp_352545_capstone.class.subject_code -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.class.subject_code -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.class_member.class_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.85
sp_352545_capstone.class_member.class_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.86
sp_352545_capstone.class_member.seat_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.86
sp_352545_capstone.class_member.seat_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.85
sp_352545_capstone.class_member.seat_id -> sp_352545_capstone.instructor.instructor_id  # source=semantic, confidence=0.87
sp_352545_capstone.class_member.status -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.88
sp_352545_capstone.class_member.status -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.86
sp_352545_capstone.class_member.student_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.87
sp_352545_capstone.class_member.student_id -> sp_352545_capstone.instructor.instructor_id  # source=semantic, confidence=0.85
sp_352545_capstone.consultation.class_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.85
sp_352545_capstone.consultation.instructor_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.86
sp_352545_capstone.consultation.instructor_id -> sp_352545_capstone.instructor.instructor_id  # source=inferred_metadata, confidence=0.83
sp_352545_capstone.consultation.remarks -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.86
sp_352545_capstone.consultee.student_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.88
sp_352545_capstone.consultee.student_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.86
sp_352545_capstone.course.course_id -> sp_352545_capstone.instructor.instructor_id  # source=semantic, confidence=0.87
sp_352545_capstone.course.course_name -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.course.course_name -> sp_352545_capstone.instructor.instructor_id  # source=semantic, confidence=0.86
sp_352545_capstone.instructor.position -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.85
sp_352545_capstone.room.room_id -> sp_352545_capstone.room_type.room_type_id  # source=semantic, confidence=0.90
sp_352545_capstone.room.room_type -> sp_352545_capstone.room_type.room_type_id  # source=semantic, confidence=0.90
sp_352545_capstone.room_type.room_type -> sp_352545_capstone.room.room_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.academic_year_id -> sp_352545_capstone.academic_year.academic_year_id  # source=inferred_metadata, confidence=0.83
sp_352545_capstone.semester.academic_year_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.end_date -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.end_date -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.89
sp_352545_capstone.semester.semester -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.semester -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.semester_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.semester_id -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.start_date -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.semester.start_date -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.student.course_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.student.course_id -> sp_352545_capstone.course.course_id  # source=inferred_metadata, confidence=0.83
sp_352545_capstone.student.course_id -> sp_352545_capstone.instructor.instructor_id  # source=semantic, confidence=0.87
sp_352545_capstone.student.student_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.88
sp_352545_capstone.student.year_level -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.student.year_level -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.87
sp_352545_capstone.subject.subject_code -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.subject.subject_code -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.subject.subject_title -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.90
sp_352545_capstone.subject.subject_title -> sp_352545_capstone.course.course_id  # source=semantic, confidence=0.90
sp_352545_capstone.user.mname -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.87
sp_352545_capstone.user.user_id -> sp_352545_capstone.academic_year.academic_year_id  # source=semantic, confidence=0.85
sp_365427_schema.doctor.doctor_id -> sp_365427_schema.patient.patient_id  # source=semantic, confidence=0.90
sp_365427_schema.doctor.doctor_name -> sp_365427_schema.patient.patient_id  # source=semantic, confidence=0.88
sp_365427_schema.patient.date_of_birth -> sp_365427_schema.doctor.doctor_id  # source=semantic, confidence=0.85
sp_365427_schema.patient.patient_first_name -> sp_365427_schema.doctor.doctor_id  # source=semantic, confidence=0.90
sp_365427_schema.patient.patient_last_name -> sp_365427_schema.doctor.doctor_id  # source=semantic, confidence=0.89
sp_365427_schema.patient.patient_password -> sp_365427_schema.doctor.doctor_id  # source=semantic, confidence=0.90
sp_413175_01_tabledesign_exam21oct2018.journeys.destination_spaceport_id -> sp_413175_01_tabledesign_exam21oct2018.spaceports.id  # source=semantic, confidence=0.90
sp_413175_01_tabledesign_exam21oct2018.journeys.id -> sp_413175_01_tabledesign_exam21oct2018.travel_cards.id  # source=semantic, confidence=0.90
sp_413175_01_tabledesign_exam21oct2018.journeys.journey_end -> sp_413175_01_tabledesign_exam21oct2018.travel_cards.id  # source=semantic, confidence=0.86
sp_413175_01_tabledesign_exam21oct2018.journeys.journey_start -> sp_413175_01_tabledesign_exam21oct2018.travel_cards.id  # source=semantic, confidence=0.86
sp_413175_01_tabledesign_exam21oct2018.journeys.purpose -> sp_413175_01_tabledesign_exam21oct2018.travel_cards.id  # source=semantic, confidence=0.86
sp_413175_01_tabledesign_exam21oct2018.journeys.spaceship_id -> sp_413175_01_tabledesign_exam21oct2018.spaceships.id  # source=semantic, confidence=0.90
sp_413175_01_tabledesign_exam21oct2018.spaceports.id -> sp_413175_01_tabledesign_exam21oct2018.spaceships.id  # source=semantic, confidence=0.87
sp_413175_01_tabledesign_exam21oct2018.spaceports.planet_id -> sp_413175_01_tabledesign_exam21oct2018.planets.id  # source=semantic, confidence=0.89
sp_413175_01_tabledesign_exam21oct2018.travel_cards.card_number -> sp_413175_01_tabledesign_exam21oct2018.journeys.id  # source=semantic, confidence=0.86
sp_413175_01_tabledesign_exam21oct2018.travel_cards.colonist_id -> sp_413175_01_tabledesign_exam21oct2018.colonists.id  # source=semantic, confidence=0.88
sp_413175_01_tabledesign_exam21oct2018.travel_cards.job_during_journey -> sp_413175_01_tabledesign_exam21oct2018.journeys.id  # source=semantic, confidence=0.89
sp_413175_01_tabledesign_exam21oct2018.travel_cards.journey_id -> sp_413175_01_tabledesign_exam21oct2018.journeys.id  # source=semantic, confidence=0.90
sp_435873_db.categories.id -> sp_435873_db.items.id  # source=semantic, confidence=0.89
sp_435873_db.categories.name -> sp_435873_db.items.id  # source=semantic, confidence=0.86
sp_435873_db.items.category_id -> sp_435873_db.categories.id  # source=semantic, confidence=0.90
sp_435873_db.items.id -> sp_435873_db.orders.id  # source=semantic, confidence=0.86
sp_435873_db.items.id -> sp_435873_db.users.id  # source=semantic, confidence=0.86
sp_435873_db.items.name -> sp_435873_db.categories.id  # source=semantic, confidence=0.86
sp_435873_db.orders.user_id -> sp_435873_db.users.id  # source=semantic, confidence=0.89
sp_435873_db.orders_items.item_id -> sp_435873_db.items.id  # source=semantic, confidence=0.90
sp_435873_db.orders_items.item_id -> sp_435873_db.orders.id  # source=semantic, confidence=0.90
sp_435873_db.orders_items.order_id -> sp_435873_db.items.id  # source=semantic, confidence=0.88
sp_435873_db.orders_items.order_id -> sp_435873_db.orders.id  # source=semantic, confidence=0.90
sp_435873_db.spots_users.spot_id -> sp_435873_db.spots.id  # source=semantic, confidence=0.90
sp_435873_db.spots_users.spot_id -> sp_435873_db.users.id  # source=semantic, confidence=0.85
sp_435873_db.spots_users.user_id -> sp_435873_db.spots.id  # source=semantic, confidence=0.89
sp_435873_db.spots_users.user_id -> sp_435873_db.users.id  # source=semantic, confidence=0.90
sp_535132_zhuanglang.book.id -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.book.id -> sp_535132_zhuanglang.resources.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.book.id -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.90
sp_535132_zhuanglang.book.name -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.class.belong -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.class.id -> sp_535132_zhuanglang.resources.id  # source=semantic, confidence=0.88
sp_535132_zhuanglang.class.id -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.90
sp_535132_zhuanglang.class.note -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.class.note -> sp_535132_zhuanglang.resources.id  # source=semantic, confidence=0.86
sp_535132_zhuanglang.class.note -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.88
sp_535132_zhuanglang.class.number -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.86
sp_535132_zhuanglang.class.number -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.89
sp_535132_zhuanglang.resources.belong -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.85
sp_535132_zhuanglang.resources.belong -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.88
sp_535132_zhuanglang.resources.bilibili -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.86
sp_535132_zhuanglang.resources.id -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.90
sp_535132_zhuanglang.resources.name -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.86
sp_535132_zhuanglang.resources.name -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.85
sp_535132_zhuanglang.resources.name -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.resources.note -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.resources.note -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.85
sp_535132_zhuanglang.resources.note -> sp_535132_zhuanglang.section.id  # source=semantic, confidence=0.89
sp_535132_zhuanglang.section.belong -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.89
sp_535132_zhuanglang.section.belong -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.89
sp_535132_zhuanglang.section.belong -> sp_535132_zhuanglang.resources.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.section.note -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.88
sp_535132_zhuanglang.section.note -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.section.note -> sp_535132_zhuanglang.resources.id  # source=semantic, confidence=0.87
sp_535132_zhuanglang.section.number -> sp_535132_zhuanglang.book.id  # source=semantic, confidence=0.88
sp_535132_zhuanglang.section.number -> sp_535132_zhuanglang.class.id  # source=semantic, confidence=0.88
sp_535132_zhuanglang.section.number -> sp_535132_zhuanglang.resources.id  # source=semantic, confidence=0.85
sp_594700_takeout.location.order_id -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.90
sp_594700_takeout.location.time -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.86
sp_594700_takeout.location.uuid -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.87
sp_594700_takeout.location.x -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.89
sp_594700_takeout.location.y -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.85
sp_594700_takeout.order_include.attribute -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.87
sp_594700_takeout.order_include.order_id -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.89
sp_594700_takeout.orders.order_id -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.89
sp_594700_takeout.score.type -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.85
sp_594700_takeout.user.store_id -> sp_594700_takeout.store.store_id  # source=inferred_metadata, confidence=0.83
sp_594700_takeout.user_orders.order_id -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.86
sp_594700_takeout.user_orders.state -> sp_594700_takeout.store.store_id  # source=semantic, confidence=0.87
```

---

## Sample values

Format: `schema.table.column: value1, value2, value3, ...`.

```
sp_020891_v01__impl_usuario.tb_grupo.ds_descricao: Backoffice - Cadastros
sp_020891_v01__impl_usuario.tb_grupo.ds_nome: BACKOFFICE
sp_020891_v01__impl_usuario.tb_permissao.ds_descricao: ADMINISTRAÇÂO COMPLETA
sp_020891_v01__impl_usuario.tb_permissao.ds_permissao: ROLE_ADMIN
sp_020891_v01__impl_usuario.tb_permissao_x_grupo.id_grupo: 1
sp_020891_v01__impl_usuario.tb_permissao_x_grupo.id_permissao: 3
sp_020891_v01__impl_usuario.tb_usuario.ds_login: eudes
sp_020891_v01__impl_usuario.tb_usuario.ds_nome: <PERSON>
sp_020891_v01__impl_usuario.tb_usuario.ds_senha: $2a$10$YYe9VtFGZoWvrNSZNV/AeuVSTOMQLxcGia4IQEl/yVaxrfAnPDcuO
sp_020891_v01__impl_usuario.tb_usuario.fl_ativo: True
sp_020891_v01__impl_usuario.tb_usuario_x_grupo.id_grupo: 1
sp_020891_v01__impl_usuario.tb_usuario_x_grupo.id_usuario: 1
sp_169810_init.shoe.brand: <PERSON>, SERVUS BY HONEYWELL, MAUI AND SONS, Josmo, NATIVE
sp_169810_init.shoe.colour: <PERSON>, White, Beige, Yellow, Multi-Colour, Black, Brown, Red, BlackBlue
sp_169810_init.shoe.description: Some descriptions
sp_169810_init.shoe.image_url: www.google.com
sp_169810_init.shoe.name: Josmo 8190 Plain Infant Walking Shoes Navy - Wide - Size 7. 5, Native Miller Men Us 10 Gray Loafer Uk 9 Eu 43, <PERSON>, Servus By Honeywell Shoe Studs Zsr101blmlg, Twisted X Western Boots Mens Buckaroo Spur Ridge Crazy Horse Mbkl012, Mens Faux Leather Business Handbag Messenger Shoulder Briefcase Laptop Bag
sp_169810_init.shoe.price: 41.12, 7.99, 39.89, 55.99, 45.23, 25, 46.26, 9.99, 21.4, 37.99
sp_169810_init.shoe.size: 6, 9, 11, 7, 12, 13, 8, 10
sp_169810_init.shoe.stock: 11, 15, 4, 14, 12, 1, 13, 2, 8, 3
sp_169810_init.shoe.style: Bucks, <PERSON>, Boat shoe, Cantabrian albarcas, Brogue shoe, Blucher shoe, Climbing shoe
sp_352545_capstone.academic_year.academic_year: 2015-2016
sp_352545_capstone.course.course_id: bscs
sp_352545_capstone.course.course_name: Bachelor of Science in Computer Science
sp_352545_capstone.semester.academic_year_id: 1
sp_352545_capstone.semester.end_date: 2015-10-30
sp_352545_capstone.semester.semester: First Semester
sp_352545_capstone.semester.start_date: 2015-06-01
sp_352545_capstone.subject.subject_code: ict136
sp_352545_capstone.subject.subject_title: Database 2
sp_352545_capstone.user.fname: David
sp_352545_capstone.user.lname: <PERSON>
sp_352545_capstone.user.password: 20001231
sp_352545_capstone.user.role_id: 2
sp_352545_capstone.user.user_id: 1122343
sp_352545_capstone.user_role.role: Student
sp_352545_capstone.user_role.role_id: 3
sp_535132_zhuanglang.book.discipline: IT
sp_535132_zhuanglang.book.name: 八年级上, 八年级下
sp_535132_zhuanglang.book.upload: <PERSON>
sp_535132_zhuanglang.class.belong: 1, 2
sp_535132_zhuanglang.class.note: Test
sp_535132_zhuanglang.class.number: 1, 2, 3
sp_535132_zhuanglang.class.upload: <PERSON>
sp_535132_zhuanglang.resources.belong: 1
sp_535132_zhuanglang.resources.name: 1.mp4, 1.pdf
sp_535132_zhuanglang.resources.note: Test
sp_535132_zhuanglang.resources.path: IT/1/1/1/1.mp4, IT/1/1/1/1.pdf
sp_535132_zhuanglang.resources.upload: <PERSON>
sp_535132_zhuanglang.resources.videoorslide: SLIDE, VIDEO
sp_535132_zhuanglang.section.belong: 1, 2
sp_535132_zhuanglang.section.note: Test
sp_535132_zhuanglang.section.number: 1, 2, 3
sp_535132_zhuanglang.section.upload: <PERSON>
sp_535132_zhuanglang.teacher.mail: <EMAIL_ADDRESS>
sp_535132_zhuanglang.teacher.name: 吕
sp_535132_zhuanglang.teacher.nkuorzhuanglang: NKU
sp_535132_zhuanglang.teacher.pwd: 7c403719af397878d6d48c385a3607f9
sp_535132_zhuanglang.teacher.salt: qQT06+5F7zymT7CcJPnhYQ==
sp_535132_zhuanglang.teacher.username: <PERSON>
sp_596038_ddl_proyecto.perfil.nombre: Dueño
```
