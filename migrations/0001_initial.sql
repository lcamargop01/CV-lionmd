-- Contractors table (Lion MD doctors)
CREATE TABLE IF NOT EXISTS contractors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  company TEXT,
  ein_ssn TEXT,
  email TEXT,
  is_active INTEGER DEFAULT 1,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Payment rates (what CareValidate pays LionMD and what LionMD pays contractors)
CREATE TABLE IF NOT EXISTS payment_rates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  visit_type TEXT NOT NULL UNIQUE,  -- ASYNC_TEXT_EMAIL, SYNC_PHONE, SYNC_VIDEO, OrderlyMeds
  carevalidate_rate REAL NOT NULL,  -- What CareValidate pays LionMD
  contractor_rate REAL NOT NULL,    -- What LionMD pays contractor
  label TEXT,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Upload sessions (each Excel file upload)
CREATE TABLE IF NOT EXISTS upload_sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  period_label TEXT NOT NULL,   -- e.g. "January 2026"
  period_month INTEGER NOT NULL, -- 1-12
  period_year INTEGER NOT NULL,
  total_cases INTEGER DEFAULT 0,
  total_carevalidate_amount REAL DEFAULT 0,
  total_contractor_amount REAL DEFAULT 0,
  status TEXT DEFAULT 'active',
  uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  notes TEXT
);

-- Individual consult records parsed from Excel
CREATE TABLE IF NOT EXISTS consults (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  case_id TEXT,
  case_id_short TEXT,
  organization_name TEXT,
  patient_name TEXT,
  doctor_name TEXT,
  decision_date TEXT,
  decision_status TEXT,
  visit_type TEXT,
  carevalidate_fee REAL DEFAULT 0,
  contractor_fee REAL DEFAULT 0,
  contractor_id INTEGER,
  is_flagged INTEGER DEFAULT 0,
  flag_reason TEXT,
  is_override INTEGER DEFAULT 0,
  override_fee REAL,
  notes TEXT,
  FOREIGN KEY (session_id) REFERENCES upload_sessions(id) ON DELETE CASCADE,
  FOREIGN KEY (contractor_id) REFERENCES contractors(id)
);

-- Audit log for edits
CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  record_id INTEGER NOT NULL,
  action TEXT NOT NULL,
  field_name TEXT,
  old_value TEXT,
  new_value TEXT,
  changed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_consults_session ON consults(session_id);
CREATE INDEX IF NOT EXISTS idx_consults_doctor ON consults(doctor_name);
CREATE INDEX IF NOT EXISTS idx_consults_visit_type ON consults(visit_type);
CREATE INDEX IF NOT EXISTS idx_consults_contractor ON consults(contractor_id);

-- Default payment rates
INSERT OR IGNORE INTO payment_rates (visit_type, carevalidate_rate, contractor_rate, label) VALUES
  ('ASYNC_TEXT_EMAIL', 20.00, 15.00, 'Async'),
  ('SYNC_PHONE', 50.00, 35.00, 'Sync Phone'),
  ('SYNC_VIDEO', 50.00, 35.00, 'Sync Video'),
  ('ORDERLY', 17.00, 12.00, 'OrderlyMeds'),
  ('NO_SHOW', 0.00, 0.00, 'No Show'),
  ('SYNC_IN_PERSON', 50.00, 35.00, 'Sync In Person');

-- Default contractors based on known doctors in the system
INSERT OR IGNORE INTO contractors (name, company, ein_ssn, email) VALUES
  ('Amy Gaines', 'Lion MD', '', ''),
  ('Ana Lisa Carr', 'Lion MD', '', ''),
  ('Carissa Kelly', 'Lion MD', '', ''),
  ('Jessica Hicks', 'Lion MD', '', ''),
  ('Jill McLaughlin', 'Lion MD', '', ''),
  ('Juan Bayolo', 'Lion MD', '', ''),
  ('Kelly Tenbrink', 'Lion MD', '', ''),
  ('LaurenMarie Cormier', 'Lion MD', '', ''),
  ('Lea Thomas', 'Lion MD', '', ''),
  ('Miklos Major', 'Lion MD', '', ''),
  ('Muhammad Usman', 'Lion MD', '', ''),
  ('Nichcole Rau', 'Lion MD', '', ''),
  ('Rachel Recore', 'Lion MD', '', ''),
  ('Rashelle Phelps', 'Lion MD', '', ''),
  ('Stefanie Barr', 'Lion MD', '', ''),
  ('Tiffany Alexander', 'Lion MD', '', '');
