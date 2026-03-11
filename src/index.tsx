import { Hono } from 'hono'
import { cors } from 'hono/cors'

type Bindings = {
  DB: D1Database
}

// ──────────────────────────────────────────────
// Helper: detect if a row is an OrderlyMeds consult
//
// Rule: organization_name contains 'orderly' → always OrderlyMeds flat rate.
// ALL visit types (ASYNC, SYNC, NO_SHOW) from OrderlyMeds use CV=$17, CT=$10.
// ──────────────────────────────────────────────
function isOrderlyRow(orgName?: string | null, rawFee?: number | null, visitType?: string | null): boolean {
  if (!orgName || !orgName.toLowerCase().includes('orderly')) return false
  // ALL visit types from OrderlyMeds are orderly-rated (CV=$17, CT=$10)
  return true
}

function computeFee(
  visitType: string | null,
  ratesMap: Record<string, { cv: number; ct: number }>,
  rawFee?: number | null,
  orgName?: string | null
): { cv: number; ct: number } {
  if (!visitType) return { cv: 0, ct: 0 }
  const vt = visitType.toUpperCase()

  // OrderlyMeds flat rate ($17/$10) applies to ALL visit types from this org.
  if (isOrderlyRow(orgName, rawFee, visitType)) {
    const orderly = ratesMap['ORDERLY']
    return orderly ? { cv: orderly.cv, ct: orderly.ct } : { cv: rawFee ?? 0, ct: rawFee ?? 0 }
  }

  // All other (non-OrderlyMeds) rows use their normal rate by visit type
  const rate = ratesMap[vt]
  if (rate) return { cv: rate.cv, ct: rate.ct }
  return { cv: 0, ct: 0 }
}

// Build period_key string from month + year
function periodKey(month: number, year: number) {
  return `${year}-${String(month).padStart(2, '0')}`
}

const app = new Hono<{ Bindings: Bindings }>()
app.use('/api/*', cors())

// ──────────────────────────────────────────────
// STARTUP: ensure new columns exist (idempotent)
// ──────────────────────────────────────────────
async function ensureSchema(db: D1Database) {
  await db.batch([
    db.prepare(`ALTER TABLE upload_sessions ADD COLUMN source_label TEXT`).bind(),
    db.prepare(`ALTER TABLE upload_sessions ADD COLUMN period_key TEXT`).bind(),
    db.prepare(`ALTER TABLE consults ADD COLUMN is_orderly INTEGER DEFAULT 0`).bind(),
  ]).catch(() => {}) // ignore if columns already exist
  // Backfill period_key for existing sessions
  await db.prepare(
    `UPDATE upload_sessions SET period_key = printf('%04d-%02d', period_year, period_month) WHERE period_key IS NULL OR period_key = ''`
  ).run().catch(() => {})
  // Backfill is_orderly=1 for ALL OrderlyMeds rows (CV is always $17 for this org)
  await db.prepare(
    `UPDATE consults SET is_orderly=1
     WHERE is_orderly=0
       AND LOWER(organization_name) LIKE '%orderly%'`
  ).run().catch(() => {})
  // Fix orderly SYNC rows: CV=$17, CT=$10 (same as async — orderly is always flat rate)
  await db.prepare(
    `UPDATE consults SET carevalidate_fee=17, contractor_fee=10
     WHERE is_orderly=1
       AND LOWER(organization_name) LIKE '%orderly%'
       AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON')
       AND (is_override IS NULL OR is_override=0)`
  ).run().catch(() => {})
}

// ──────────────────────────────────────────────
// GET /api/health
// ──────────────────────────────────────────────
app.get('/api/health', async (c) => {
  await ensureSchema(c.env.DB)
  return c.json({ ok: true, ts: Date.now() })
})

// ──────────────────────────────────────────────
// PAYMENT RATES
// ──────────────────────────────────────────────
app.get('/api/rates', async (c) => {
  const rates = await c.env.DB.prepare('SELECT * FROM payment_rates ORDER BY visit_type').all()
  return c.json(rates.results)
})

app.put('/api/rates/:id', async (c) => {
  const id = c.req.param('id')
  const { carevalidate_rate, contractor_rate, label } = await c.req.json()
  await c.env.DB.prepare(
    'UPDATE payment_rates SET carevalidate_rate=?, contractor_rate=?, label=?, updated_at=CURRENT_TIMESTAMP WHERE id=?'
  ).bind(carevalidate_rate, contractor_rate, label, id).run()
  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// CONTRACTOR TYPE RATES
// ──────────────────────────────────────────────
app.get('/api/contractor-type-rates', async (c) => {
  // Ensure table exists
  await c.env.DB.prepare(`CREATE TABLE IF NOT EXISTS contractor_type_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contractor_type TEXT NOT NULL,
    visit_type TEXT NOT NULL,
    contractor_rate REAL NOT NULL DEFAULT 0,
    label TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(contractor_type, visit_type)
  )`).run()
  const rows = await c.env.DB.prepare('SELECT * FROM contractor_type_rates ORDER BY contractor_type, visit_type').all()
  return c.json(rows.results)
})

app.put('/api/contractor-type-rates', async (c) => {
  const { contractor_type, visit_type, contractor_rate } = await c.req.json()
  await c.env.DB.prepare(
    `INSERT INTO contractor_type_rates (contractor_type, visit_type, contractor_rate, updated_at)
     VALUES (?, ?, ?, CURRENT_TIMESTAMP)
     ON CONFLICT(contractor_type, visit_type) DO UPDATE SET contractor_rate=excluded.contractor_rate, updated_at=CURRENT_TIMESTAMP`
  ).bind(contractor_type, visit_type, contractor_rate).run()
  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// RECALCULATE PERIODS — recompute all consult fees using current rates
// ──────────────────────────────────────────────
app.post('/api/recalculate', async (c) => {
  const body = await c.req.json().catch(() => ({}))
  const period_key = (body as any).period_key || null  // optional: limit to one period

  // Load current rates
  const ratesResult = await c.env.DB.prepare('SELECT * FROM payment_rates').all()
  const ratesMap: Record<string, { cv: number; ct: number }> = {}
  for (const r of ratesResult.results as any[]) {
    ratesMap[(r as any).visit_type] = { cv: (r as any).carevalidate_rate, ct: (r as any).contractor_rate }
  }

  // Load contractor type rates
  const ctRatesResult = await c.env.DB.prepare('SELECT * FROM contractor_type_rates').all()
  const ctRatesMap: Record<string, Record<string, number>> = {}
  for (const r of ctRatesResult.results as any[]) {
    if (!ctRatesMap[(r as any).contractor_type]) ctRatesMap[(r as any).contractor_type] = {}
    ctRatesMap[(r as any).contractor_type][(r as any).visit_type] = (r as any).contractor_rate
  }

  // Load contractors with their types — also build name→id map for re-matching unlinked consults
  const contractorsResult = await c.env.DB.prepare('SELECT id, name, contractor_type FROM contractors WHERE is_active=1').all()
  const contractorTypeMap: Record<number, string> = {}
  const contractorNameMap: Record<string, number> = {}  // doctor_name (lowercase) → contractor id
  for (const ct of contractorsResult.results as any[]) {
    contractorTypeMap[(ct as any).id] = (ct as any).contractor_type || 'regular'
    contractorNameMap[(ct as any).name.toLowerCase().trim()] = (ct as any).id
  }

  // Fetch all consults (not overridden), optionally filtered by period
  const whereClause = period_key
    ? 'LEFT JOIN upload_sessions s ON c.session_id=s.id WHERE c.is_override=0 AND s.period_key=?'
    : 'LEFT JOIN upload_sessions s ON c.session_id=s.id WHERE c.is_override=0'
  const params: any[] = period_key ? [period_key] : []

  const PAGE_SIZE = 500
  let offset = 0
  let updatedCount = 0

  while (true) {
    const rows = await c.env.DB.prepare(
      `SELECT c.id, c.visit_type, c.is_orderly, c.contractor_id, c.doctor_name, c.session_id
       FROM consults c ${whereClause} LIMIT ${PAGE_SIZE} OFFSET ${offset}`
    ).bind(...params).all()

    if (!rows.results.length) break

    // Batch updates
    const stmts = []
    for (const row of rows.results as any[]) {
      const vt: string = row.visit_type || ''
      const isOrderly: boolean = row.is_orderly === 1

      // Re-match contractor_id if missing (contractor added after upload)
      let contractorId: number | null = row.contractor_id
      if (!contractorId && row.doctor_name) {
        const matched = contractorNameMap[(row.doctor_name as string).toLowerCase().trim()]
        if (matched) contractorId = matched
      }

      const ctype: string = (contractorId ? contractorTypeMap[contractorId] : null) || 'regular'

      // CV fee: orderly always $17, else lookup by visit_type
      let cvFee = 0
      if (isOrderly) {
        cvFee = ratesMap['ORDERLY']?.cv ?? 17
      } else {
        cvFee = ratesMap[vt.toUpperCase()]?.cv ?? 0
      }

      // CT fee: orderly always $10 (flat rate regardless of visit type), else normal contractor-type rate
      let ctFee = 0
      if (isOrderly) {
        ctFee = ctRatesMap[ctype]?.['ORDERLY'] ?? ratesMap['ORDERLY']?.ct ?? 10
      } else {
        ctFee = ctRatesMap[ctype]?.[vt.toUpperCase()] ?? ratesMap[vt.toUpperCase()]?.ct ?? 0
      }

      // Update fees and re-link contractor_id if it changed
      if (contractorId && contractorId !== row.contractor_id) {
        stmts.push(
          c.env.DB.prepare('UPDATE consults SET carevalidate_fee=?, contractor_fee=?, contractor_id=? WHERE id=?')
            .bind(cvFee, ctFee, contractorId, row.id)
        )
      } else {
        stmts.push(
          c.env.DB.prepare('UPDATE consults SET carevalidate_fee=?, contractor_fee=? WHERE id=?')
            .bind(cvFee, ctFee, row.id)
        )
      }
      updatedCount++
    }

    if (stmts.length > 0) await c.env.DB.batch(stmts)
    offset += PAGE_SIZE
    if (rows.results.length < PAGE_SIZE) break
  }

  // Recompute all session totals
  const sessions = await c.env.DB.prepare(
    period_key
      ? 'SELECT id FROM upload_sessions WHERE period_key=?'
      : 'SELECT id FROM upload_sessions'
  ).bind(...(period_key ? [period_key] : [])).all()

  const sessionStmts = []
  for (const s of sessions.results as any[]) {
    const totals = await c.env.DB.prepare(
      'SELECT COUNT(*) as tc, SUM(carevalidate_fee) as cv, SUM(contractor_fee) as ct FROM consults WHERE session_id=?'
    ).bind(s.id).first() as any
    sessionStmts.push(
      c.env.DB.prepare('UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?')
        .bind(totals?.tc || 0, totals?.cv || 0, totals?.ct || 0, s.id)
    )
  }
  if (sessionStmts.length) await c.env.DB.batch(sessionStmts)

  return c.json({ ok: true, updated: updatedCount, sessions: sessions.results.length })
})

// ──────────────────────────────────────────────
// CONTRACTORS
// ──────────────────────────────────────────────
app.get('/api/contractors', async (c) => {
  // ensure columns exist
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN contractor_type TEXT DEFAULT 'regular'`).run().catch(() => {})
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN gusto_type TEXT DEFAULT 'Individual'`).run().catch(() => {})
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN first_name TEXT DEFAULT ''`).run().catch(() => {})
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN last_name TEXT DEFAULT ''`).run().catch(() => {})
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN earns_commission INTEGER DEFAULT 0`).run().catch(() => {})
  // Default earns_commission=1 for Lion MD, PLLC contractors (Ana Lisa Carr, Kelly Tenbrink)
  await c.env.DB.prepare(
    `UPDATE contractors SET earns_commission=1 WHERE (LOWER(company) LIKE '%lion md%') AND earns_commission=0`
  ).run().catch(() => {})
  const rows = await c.env.DB.prepare('SELECT * FROM contractors WHERE is_active=1 ORDER BY name').all()
  return c.json(rows.results)
})

app.post('/api/contractors', async (c) => {
  const { name, first_name, last_name, company, ein_ssn, email, contractor_type, gusto_type } = await c.req.json()
  const result = await c.env.DB.prepare(
    `INSERT INTO contractors (name, first_name, last_name, company, ein_ssn, email, contractor_type, gusto_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(name, first_name || '', last_name || '', company || '', ein_ssn || '', email || '', contractor_type || 'regular', gusto_type || 'Individual').run()
  return c.json({ id: result.meta.last_row_id, name, first_name, last_name, company, ein_ssn, email, contractor_type, gusto_type })
})

app.put('/api/contractors/:id', async (c) => {
  const id = c.req.param('id')
  const { name, first_name, last_name, company, ein_ssn, email, is_active, contractor_type, gusto_type, earns_commission } = await c.req.json()
  await c.env.DB.prepare(
    `UPDATE contractors SET name=?, first_name=?, last_name=?, company=?, ein_ssn=?, email=?, is_active=?, contractor_type=?, gusto_type=?, earns_commission=? WHERE id=?`
  ).bind(name, first_name || '', last_name || '', company || '', ein_ssn || '', email || '', is_active ?? 1, contractor_type || 'regular', gusto_type || 'Individual', earns_commission ? 1 : 0, id).run()
  return c.json({ ok: true })
})

// Dedicated endpoint to toggle earns_commission flag
app.patch('/api/contractors/:id/earns-commission', async (c) => {
  const id = c.req.param('id')
  const { earns_commission } = await c.req.json()
  await c.env.DB.prepare(
    `UPDATE contractors SET earns_commission=? WHERE id=?`
  ).bind(earns_commission ? 1 : 0, id).run()
  return c.json({ ok: true, id, earns_commission: earns_commission ? 1 : 0 })
})

app.delete('/api/contractors/:id', async (c) => {
  const id = c.req.param('id')
  await c.env.DB.prepare('UPDATE contractors SET is_active=0 WHERE id=?').bind(id).run()
  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// SESSIONS
// Returns grouped periods, each with list of files
// ──────────────────────────────────────────────
app.get('/api/sessions', async (c) => {
  await ensureSchema(c.env.DB)
  const rows = await c.env.DB.prepare(
    `SELECT * FROM upload_sessions ORDER BY period_year DESC, period_month DESC, id DESC`
  ).all()

  // Group by period_key
  const grouped: Record<string, any> = {}
  for (const s of rows.results as any[]) {
    const pk = s.period_key || periodKey(s.period_month, s.period_year)
    if (!grouped[pk]) {
      grouped[pk] = {
        period_key: pk,
        period_label: s.period_label,
        period_month: s.period_month,
        period_year: s.period_year,
        files: [],
        total_cases: 0,
        total_carevalidate_amount: 0,
        total_contractor_amount: 0,
        // Use the first (most recent) session's id as the "primary" for backward compat
        id: s.id
      }
    }
    grouped[pk].files.push({
      id: s.id,
      filename: s.filename,
      source_label: s.source_label || s.filename,
      total_cases: s.total_cases,
      total_carevalidate_amount: s.total_carevalidate_amount,
      total_contractor_amount: s.total_contractor_amount,
      uploaded_at: s.uploaded_at
    })
    grouped[pk].total_cases += s.total_cases || 0
    grouped[pk].total_carevalidate_amount += s.total_carevalidate_amount || 0
    grouped[pk].total_contractor_amount += s.total_contractor_amount || 0
  }

  return c.json(Object.values(grouped))
})

app.get('/api/sessions/:id', async (c) => {
  const id = c.req.param('id')
  const session = await c.env.DB.prepare('SELECT * FROM upload_sessions WHERE id=?').bind(id).first()
  if (!session) return c.json({ error: 'Not found' }, 404)
  return c.json(session)
})

app.delete('/api/sessions/:id', async (c) => {
  const id = c.req.param('id')
  await c.env.DB.prepare('DELETE FROM consults WHERE session_id=?').bind(id).run()
  await c.env.DB.prepare('DELETE FROM upload_sessions WHERE id=?').bind(id).run()
  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// DELETE entire period (all files for a period_key)
// ──────────────────────────────────────────────
app.delete('/api/periods/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const sessions = await c.env.DB.prepare(
    'SELECT id FROM upload_sessions WHERE period_key=?'
  ).bind(pk).all()
  for (const s of sessions.results as any[]) {
    await c.env.DB.prepare('DELETE FROM consults WHERE session_id=?').bind(s.id).run()
  }
  await c.env.DB.prepare('DELETE FROM upload_sessions WHERE period_key=?').bind(pk).run()
  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// Shared helper: load rates + contractors maps
// ──────────────────────────────────────────────
async function loadMaps(db: D1Database) {
  const ratesResult = await db.prepare('SELECT * FROM payment_rates').all()
  const ratesMap: Record<string, { cv: number; ct: number }> = {}
  for (const r of ratesResult.results as any[]) {
    ratesMap[r.visit_type] = { cv: r.carevalidate_rate, ct: r.contractor_rate }
  }
  const contractorsResult = await db.prepare('SELECT id, name, contractor_type FROM contractors WHERE is_active=1').all()
  const contractorMap: Record<string, number> = {}
  const contractorTypeMap: Record<number, string> = {}
  for (const ct of contractorsResult.results as any[]) {
    contractorMap[(ct.name as string).toLowerCase().trim()] = ct.id as number
    contractorTypeMap[ct.id as number] = (ct.contractor_type as string) || 'regular'
  }
  // Load contractor-type-specific rates
  const ctRatesResult = await db.prepare('SELECT * FROM contractor_type_rates').all().catch(() => ({ results: [] }))
  const ctRatesMap: Record<string, Record<string, number>> = {}
  for (const r of ctRatesResult.results as any[]) {
    if (!ctRatesMap[r.contractor_type]) ctRatesMap[r.contractor_type] = {}
    ctRatesMap[r.contractor_type][r.visit_type] = r.contractor_rate
  }
  return { ratesMap, contractorMap, contractorTypeMap, ctRatesMap }
}

// ──────────────────────────────────────────────
// Shared helper: insert ALL rows into DB — no deduplication
// Every row from every file is inserted exactly as provided.
// Returns { added, cvTotal, ctTotal }
// ──────────────────────────────────────────────
async function insertRows(
  db: D1Database,
  sessionId: number,
  rows: any[],
  ratesMap: Record<string, { cv: number; ct: number }>,
  contractorMap: Record<string, number>,
  contractorTypeMap: Record<number, string> = {},
  ctRatesMap: Record<string, Record<string, number>> = {}
) {
  let totalCV = 0
  let totalCT = 0
  const batchSize = 100

  const stmt = db.prepare(
    `INSERT INTO consults (session_id, case_id, case_id_short, organization_name, patient_name,
      doctor_name, decision_date, decision_status, visit_type, carevalidate_fee, contractor_fee,
      contractor_id, is_flagged, is_orderly)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  )

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize)
    const statements = batch.map((row: any) => {
      const fees = computeFee(row.visit_type, ratesMap, row.raw_fee, row.organization_name)
      const orderly = isOrderlyRow(row.organization_name, row.raw_fee, row.visit_type) ? 1 : 0
      const contractorId = contractorMap[(row.doctor_name || '').toLowerCase().trim()] || null
      // Override CT fee with contractor-type-specific rate if available
      const ctype = contractorId ? (contractorTypeMap[contractorId] || 'regular') : 'regular'
      const vtKey = orderly ? 'ORDERLY' : (row.visit_type || '').toUpperCase()
      const ctFee = (ctRatesMap[ctype]?.[vtKey] !== undefined) ? ctRatesMap[ctype][vtKey] : fees.ct
      totalCV += fees.cv
      totalCT += ctFee
      return stmt.bind(
        sessionId, row.case_id || '', row.case_id_short || '',
        row.organization_name || '', row.patient_name || '',
        row.doctor_name || '', row.decision_date || '',
        row.decision_status || '', row.visit_type || '',
        fees.cv, ctFee, contractorId, row.is_flagged ? 1 : 0, orderly
      )
    })
    await db.batch(statements)
  }

  return { added: rows.length, cvTotal: totalCV, ctTotal: totalCT }
}

// ──────────────────────────────────────────────
// POST /api/upload  — Step 1: create session + insert first chunk
// Body: { filename, source_label, period_label, period_month, period_year, rows[], is_last_chunk }
// Returns: { session_id, period_key, new_cases_added, done }
// NO deduplication — every row is inserted exactly as provided.
// ──────────────────────────────────────────────
app.post('/api/upload', async (c) => {
  await ensureSchema(c.env.DB)
  const body = await c.req.json()
  const { filename, period_label, period_month, period_year, rows, source_label, is_last_chunk } = body

  if (!rows || !Array.isArray(rows)) return c.json({ error: 'Invalid payload' }, 400)

  const pk = periodKey(period_month, period_year)
  const { ratesMap, contractorMap, contractorTypeMap, ctRatesMap } = await loadMaps(c.env.DB)

  // Create new session for this file
  const sessionResult = await c.env.DB.prepare(
    `INSERT INTO upload_sessions (filename, period_label, period_month, period_year, period_key, source_label)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).bind(filename, period_label, period_month, period_year, pk, source_label || filename).run()
  const sessionId = sessionResult.meta.last_row_id as number

  const result = await insertRows(c.env.DB, sessionId, rows, ratesMap, contractorMap, contractorTypeMap, ctRatesMap)

  // If this is the only / last chunk, finalise totals now
  if (is_last_chunk !== false) {
    await c.env.DB.prepare(
      'UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?'
    ).bind(result.added, result.cvTotal, result.ctTotal, sessionId).run()
  }

  return c.json({
    session_id: sessionId,
    period_key: pk,
    new_cases_added: result.added,
    total_carevalidate_amount: result.cvTotal,
    total_contractor_amount: result.ctTotal,
    done: is_last_chunk !== false
  })
})

// ──────────────────────────────────────────────
// POST /api/upload/chunk  — Step 2+: append rows to existing session
// Body: { session_id, rows[], is_last_chunk }
// Returns: { new_cases_added, done }
// NO deduplication — every row is inserted exactly as provided.
// ──────────────────────────────────────────────
app.post('/api/upload/chunk', async (c) => {
  const body = await c.req.json()
  const { session_id, rows, is_last_chunk } = body

  if (!session_id || !rows || !Array.isArray(rows)) return c.json({ error: 'Invalid payload' }, 400)

  const { ratesMap, contractorMap, contractorTypeMap, ctRatesMap } = await loadMaps(c.env.DB)

  const result = await insertRows(c.env.DB, session_id, rows, ratesMap, contractorMap, contractorTypeMap, ctRatesMap)

  if (is_last_chunk) {
    // Recompute totals from DB (accurate after all chunks)
    const totals = await c.env.DB.prepare(
      'SELECT COUNT(*) as tc, SUM(carevalidate_fee) as cv, SUM(contractor_fee) as ct FROM consults WHERE session_id=?'
    ).bind(session_id).first() as any
    await c.env.DB.prepare(
      'UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?'
    ).bind(totals?.tc || 0, totals?.cv || 0, totals?.ct || 0, session_id).run()
  }

  return c.json({
    session_id,
    new_cases_added: result.added,
    done: !!is_last_chunk
  })
})

// ──────────────────────────────────────────────
// CONSULTS — accepts period_key OR session_id
// ──────────────────────────────────────────────
app.get('/api/consults', async (c) => {
  const period_key  = c.req.query('period_key')
  const session_id  = c.req.query('session_id')
  const doctor_name  = c.req.query('doctor_name')
  const visit_type   = c.req.query('visit_type')
  const is_orderly   = c.req.query('is_orderly')    // '1' = OrderlyMeds only
  const organization = c.req.query('organization')  // exact org name
  const page         = c.req.query('page')  || '1'
  const limit        = c.req.query('limit') || '50'
  const search       = c.req.query('search')
  const offset       = (parseInt(page) - 1) * parseInt(limit)

  // Sorting — whitelist allowed columns to prevent SQL injection
  const SORT_COLS: Record<string, string> = {
    decision_date:    'c.decision_date',
    patient_name:     'c.patient_name',
    organization_name:'c.organization_name',
    doctor_name:      'c.doctor_name',
    visit_type:       'c.visit_type',
    carevalidate_fee: 'c.carevalidate_fee',
    contractor_fee:   'c.contractor_fee',
    case_id_short:    'c.case_id_short',
    decision_status:  'c.decision_status',
  }
  const rawSortBy  = c.req.query('sort_by')  || 'decision_date'
  const rawSortDir = c.req.query('sort_dir') || 'desc'
  const sortCol = SORT_COLS[rawSortBy] ?? 'c.decision_date'
  const sortDir = rawSortDir.toLowerCase() === 'asc' ? 'ASC' : 'DESC'
  const orderBy = `${sortCol} ${sortDir}, c.id ${sortDir}`

  let where = 'WHERE 1=1'
  const params: any[] = []

  if (period_key) {
    where += ' AND s.period_key=?'; params.push(period_key)
  } else if (session_id) {
    where += ' AND c.session_id=?'; params.push(session_id)
  }
  if (doctor_name)  { where += ' AND c.doctor_name=?';    params.push(doctor_name)  }
  if (visit_type)   { where += ' AND c.visit_type=?';     params.push(visit_type)   }
  if (is_orderly === '1') { where += ' AND c.is_orderly=1' }
  if (is_orderly === '0') { where += ' AND c.is_orderly=0' }
  if (organization) { where += ' AND c.organization_name=?'; params.push(organization) }
  if (search) {
    where += ' AND (c.patient_name LIKE ? OR c.case_id_short LIKE ? OR c.organization_name LIKE ?)'
    params.push(`%${search}%`, `%${search}%`, `%${search}%`)
  }

  const countResult = await c.env.DB.prepare(
    `SELECT COUNT(*) as total FROM consults c
     LEFT JOIN upload_sessions s ON c.session_id = s.id ${where}`
  ).bind(...params).first() as any

  const rows = await c.env.DB.prepare(
    `SELECT c.*, ct.ein_ssn, ct.company, s.period_key, s.source_label as file_label
     FROM consults c
     LEFT JOIN contractors ct ON c.contractor_id = ct.id
     LEFT JOIN upload_sessions s ON c.session_id = s.id
     ${where}
     ORDER BY ${orderBy}
     LIMIT ? OFFSET ?`
  ).bind(...params, parseInt(limit), offset).all()

  return c.json({ total: countResult?.total || 0, page: parseInt(page), limit: parseInt(limit), data: rows.results })
})

// Distinct organizations for the filter dropdown (scoped to current period)
app.get('/api/consults/organizations', async (c) => {
  const period_key = c.req.query('period_key')
  let where = 'WHERE c.organization_name IS NOT NULL AND c.organization_name != \'\''
  const params: any[] = []
  if (period_key) { where += ' AND s.period_key=?'; params.push(period_key) }
  const rows = await c.env.DB.prepare(
    `SELECT DISTINCT c.organization_name
     FROM consults c
     LEFT JOIN upload_sessions s ON c.session_id = s.id
     ${where}
     ORDER BY c.organization_name ASC`
  ).bind(...params).all()
  return c.json((rows.results as any[]).map(r => r.organization_name))
})

app.get('/api/consults/:id', async (c) => {
  const id = c.req.param('id')
  const row = await c.env.DB.prepare(
    `SELECT c.*, ct.ein_ssn, ct.company, s.period_key, s.source_label as file_label
     FROM consults c
     LEFT JOIN contractors ct ON c.contractor_id = ct.id
     LEFT JOIN upload_sessions s ON c.session_id = s.id
     WHERE c.id=?`
  ).bind(id).first()
  if (!row) return c.json({ error: 'Not found' }, 404)
  return c.json(row)
})

app.put('/api/consults/:id', async (c) => {
  const id = c.req.param('id')
  const body = await c.req.json()

  // Fetch current row first so we only overwrite fields that are explicitly provided
  const current = await c.env.DB.prepare('SELECT * FROM consults WHERE id=?').bind(id).first() as any
  if (!current) return c.json({ error: 'Not found' }, 404)

  const {
    doctor_name, patient_name, organization_name, visit_type, decision_date, decision_status,
    carevalidate_fee, contractor_fee, notes, is_override, override_fee, is_orderly, is_flagged, flag_reason
  } = body

  // Merge: use provided value if present, otherwise keep existing value
  const newDoctorName    = doctor_name    !== undefined ? (doctor_name    || null) : current.doctor_name
  const newPatientName   = patient_name   !== undefined ? (patient_name   || null) : current.patient_name
  const newOrgName       = organization_name !== undefined ? (organization_name || null) : current.organization_name
  const newVisitType     = visit_type     !== undefined ? (visit_type     || null) : current.visit_type
  const newDecisionDate  = decision_date  !== undefined ? (decision_date  || null) : current.decision_date
  const newDecisionStatus = decision_status !== undefined ? (decision_status || 'Approved') : (current.decision_status || 'Approved')
  const newCVFee         = carevalidate_fee !== undefined ? (carevalidate_fee ?? 0) : current.carevalidate_fee
  const newCTFee         = contractor_fee  !== undefined ? (contractor_fee  ?? 0) : current.contractor_fee
  const newNotes         = notes          !== undefined ? (notes          || null) : current.notes
  const newIsOverride    = is_override    !== undefined ? (is_override    ? 1 : 0) : current.is_override
  const newOverrideFee   = override_fee   !== undefined ? (override_fee   || null) : current.override_fee
  const newIsFlagged     = is_flagged     !== undefined ? (is_flagged     ? 1 : 0) : current.is_flagged
  const newFlagReason    = flag_reason    !== undefined ? (flag_reason    || null) : current.flag_reason

  // Recompute is_orderly: explicit override > org+visitType rule > keep existing
  const newIsOrderly = is_orderly !== undefined
    ? (is_orderly ? 1 : 0)
    : (isOrderlyRow(newOrgName, null, newVisitType) ? 1 : current.is_orderly)

  await c.env.DB.prepare(`
    UPDATE consults SET
      doctor_name=?, patient_name=?, organization_name=?, visit_type=?,
      decision_date=?, decision_status=?,
      carevalidate_fee=?, contractor_fee=?,
      notes=?, is_override=?, override_fee=?,
      is_orderly=?, is_flagged=?, flag_reason=?
    WHERE id=?
  `).bind(
    newDoctorName, newPatientName, newOrgName, newVisitType,
    newDecisionDate, newDecisionStatus,
    newCVFee, newCTFee,
    newNotes, newIsOverride, newOverrideFee,
    newIsOrderly, newIsFlagged, newFlagReason,
    id
  ).run()

  // Recompute session totals for the session this consult belongs to
  const row = await c.env.DB.prepare('SELECT session_id FROM consults WHERE id=?').bind(id).first() as any
  if (row?.session_id) {
    const totals = await c.env.DB.prepare(
      'SELECT COUNT(*) as tc, SUM(carevalidate_fee) as cv, SUM(contractor_fee) as ct FROM consults WHERE session_id=?'
    ).bind(row.session_id).first() as any
    await c.env.DB.prepare(
      'UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?'
    ).bind(totals?.tc || 0, totals?.cv || 0, totals?.ct || 0, row.session_id).run()
  }

  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// SUMMARY — accepts period_key (aggregates all files) OR session_id
// ──────────────────────────────────────────────
async function buildSummary(db: D1Database, where: string, params: any[]) {
  const [byDoctor, byVisitType, byOrg, totals, flagged] = await Promise.all([
    db.prepare(`
      SELECT c.doctor_name, ct.id as contractor_id, ct.company, ct.ein_ssn,
        COUNT(*) as case_count,
        SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
        SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
        SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END) as orderly_count,
        SUM(c.carevalidate_fee) as total_carevalidate,
        SUM(c.contractor_fee) as total_contractor,
        SUM(c.carevalidate_fee) - SUM(c.contractor_fee) as margin
      FROM consults c
      LEFT JOIN contractors ct ON c.contractor_id = ct.id
      LEFT JOIN upload_sessions s ON c.session_id = s.id
      ${where} GROUP BY c.doctor_name ORDER BY c.doctor_name
    `).bind(...params).all(),
    db.prepare(`
      SELECT visit_type, COUNT(*) as count,
        SUM(carevalidate_fee) as total_cv, SUM(contractor_fee) as total_ct
      FROM consults c
      LEFT JOIN upload_sessions s ON c.session_id = s.id
      ${where} GROUP BY visit_type ORDER BY count DESC
    `).bind(...params).all(),
    db.prepare(`
      SELECT organization_name, COUNT(*) as count, SUM(carevalidate_fee) as total_cv
      FROM consults c
      LEFT JOIN upload_sessions s ON c.session_id = s.id
      ${where} GROUP BY organization_name ORDER BY count DESC LIMIT 20
    `).bind(...params).all(),
    db.prepare(`
      SELECT COUNT(*) as total_cases,
        SUM(carevalidate_fee) as total_carevalidate,
        SUM(contractor_fee) as total_contractor,
        SUM(carevalidate_fee) - SUM(contractor_fee) as total_margin
      FROM consults c
      LEFT JOIN upload_sessions s ON c.session_id = s.id ${where}
    `).bind(...params).first(),
    db.prepare(
      `SELECT COUNT(*) as count FROM consults c
       LEFT JOIN upload_sessions s ON c.session_id = s.id
       ${where} AND c.is_flagged=1`
    ).bind(...params).first()
  ])
  return {
    totals,
    byDoctor: byDoctor.results,
    byVisitType: byVisitType.results,
    byOrg: byOrg.results,
    flaggedCount: (flagged as any)?.count || 0
  }
}

app.get('/api/summary/period/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  return c.json(await buildSummary(c.env.DB, 'WHERE s.period_key=?', [pk]))
})

app.get('/api/summary/:session_id', async (c) => {
  const sid = c.req.param('session_id')
  return c.json(await buildSummary(c.env.DB, 'WHERE c.session_id=?', [sid]))
})

// ──────────────────────────────────────────────
// PAYSTUB — accepts period_key
// ──────────────────────────────────────────────
app.get('/api/paystub/period/:period_key/:contractor_id', async (c) => {
  const pk = c.req.param('period_key')
  const cid = c.req.param('contractor_id')

  const contractor = await c.env.DB.prepare('SELECT * FROM contractors WHERE id=?').bind(cid).first() as any

  // Get all sessions for this period to build a label
  const sessions = await c.env.DB.prepare(
    'SELECT * FROM upload_sessions WHERE period_key=? ORDER BY id'
  ).bind(pk).all()
  const periodLabel = (sessions.results[0] as any)?.period_label || pk

  const consults = await c.env.DB.prepare(`
    SELECT c.*, s.source_label as file_label FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? AND c.contractor_id=?
    ORDER BY c.decision_date ASC
  `).bind(pk, cid).all()

  const summary = await c.env.DB.prepare(`
    SELECT
      COUNT(*) as total_cases, SUM(contractor_fee) as total_pay,
      SUM(CASE WHEN is_orderly=0 AND visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN is_orderly=0 AND visit_type='ASYNC_TEXT_EMAIL' THEN contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN is_orderly=0 AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN is_orderly=0 AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN is_orderly=1 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN is_orderly=1 THEN contractor_fee ELSE 0 END) as orderly_pay
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? AND c.contractor_id=?
  `).bind(pk, cid).first()

  // If this contractor is Christopher Garcia, attach commission data
  let commission = null
  const cname = (contractor?.name || '').toLowerCase()
  if (cname.includes('garcia') && cname.includes('chris')) {
    commission = await calcCommission(c.env.DB, pk)
  }

  return c.json({
    contractor,
    session: { period_label: periodLabel, period_key: pk, files: sessions.results },
    consults: consults.results,
    summary,
    commission
  })
})

// Keep old single-session paystub for backward compat
app.get('/api/paystub/:session_id/:contractor_id', async (c) => {
  const sid = c.req.param('session_id')
  const cid = c.req.param('contractor_id')
  const contractor = await c.env.DB.prepare('SELECT * FROM contractors WHERE id=?').bind(cid).first()
  const session = await c.env.DB.prepare('SELECT * FROM upload_sessions WHERE id=?').bind(sid).first()
  const consults = await c.env.DB.prepare(
    'SELECT * FROM consults WHERE session_id=? AND contractor_id=? ORDER BY decision_date ASC'
  ).bind(sid, cid).all()
  const summary = await c.env.DB.prepare(`
    SELECT COUNT(*) as total_cases, SUM(contractor_fee) as total_pay,
      SUM(CASE WHEN is_orderly=0 AND visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN is_orderly=0 AND visit_type='ASYNC_TEXT_EMAIL' THEN contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN is_orderly=0 AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN is_orderly=0 AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN is_orderly=1 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN is_orderly=1 THEN contractor_fee ELSE 0 END) as orderly_pay
    FROM consults WHERE session_id=? AND contractor_id=?
  `).bind(sid, cid).first()
  return c.json({ contractor, session, consults: consults.results, summary })
})

// ──────────────────────────────────────────────
// CHRIS GARCIA COMMISSION
//
// New formula:
//   Commission = 25% × (total CV pays us − total we pay contractors)
//
// Special adjustment for Ana Lisa Carr:
//   Before computing her net contribution, subtract
//   (count of Ringside Health consults × $20) from her CV total.
//
// Christopher Garcia himself is excluded from the base calculation.
// ──────────────────────────────────────────────
const COMMISSION_RATE          = 0.25   // 25% of net (CV − contractor pay)
const RINGSIDE_DEDUCTION_RATE  = 20     // $20 per Ringside Health consult

async function calcCommission(db: D1Database, pk: string) {
  // 1. Count Ringside Health consults assigned to Ana Lisa Carr
  const ringsideRow = await db.prepare(`
    SELECT COUNT(*) as ringside_count
    FROM consults c
    LEFT JOIN upload_sessions s  ON c.session_id = s.id
    LEFT JOIN contractors     ct ON c.contractor_id = ct.id
    WHERE s.period_key = ?
      AND LOWER(ct.name) LIKE '%ana lisa carr%'
      AND LOWER(c.organization_name) LIKE '%ringside%'
  `).bind(pk).first() as any

  const ringside_count     = ringsideRow?.ringside_count || 0
  const ringside_deduction = ringside_count * RINGSIDE_DEDUCTION_RATE

  // 2. Per-contractor breakdown — all contractors except Garcia
  const rows = await db.prepare(`
    SELECT
      ct.id       as contractor_id,
      ct.name     as contractor_name,
      ct.contractor_type,
      SUM(c.carevalidate_fee)  as cv_total,
      SUM(c.contractor_fee)    as contractor_total,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN c.carevalidate_fee ELSE 0 END) as async_cv,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END)                  as async_cases,
      SUM(CASE WHEN c.is_orderly=1 THEN c.carevalidate_fee ELSE 0 END)                                     as orderly_cv,
      SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END)                                                      as orderly_cases,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN c.carevalidate_fee ELSE 0 END) as sync_cv,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END)                  as sync_cases
    FROM consults c
    LEFT JOIN upload_sessions s  ON c.session_id = s.id
    LEFT JOIN contractors     ct ON c.contractor_id = ct.id
    WHERE s.period_key = ?
      AND LOWER(ct.name) NOT LIKE '%garcia%'
    GROUP BY ct.id, ct.name, ct.contractor_type
    ORDER BY ct.name
  `).bind(pk).all()

  const details = (rows.results as any[]).map(r => {
    const isAnaLisa = (r.contractor_name || '').toLowerCase().includes('ana lisa carr')
    const cv  = r.cv_total         || 0
    const pay = r.contractor_total || 0
    // Formula for every contractor: (cv - pay) * 25%
    // Ana Lisa only: subtract (ringside_count * $20) AFTER applying 25%
    const net        = cv - pay
    const commission = net * COMMISSION_RATE - (isAnaLisa ? ringside_deduction : 0)
    return {
      contractor_id:      r.contractor_id,
      contractor_name:    r.contractor_name,
      contractor_type:    r.contractor_type,
      async_cv:           r.async_cv      || 0,
      async_cases:        r.async_cases   || 0,
      orderly_cv:         r.orderly_cv    || 0,
      orderly_cases:      r.orderly_cases || 0,
      sync_cv:            r.sync_cv       || 0,
      sync_cases:         r.sync_cases    || 0,
      other_cv:           0,
      other_cases:        0,
      cv_total:           cv,
      contractor_total:   pay,
      ringside_count:     isAnaLisa ? ringside_count : 0,
      ringside_deduction: isAnaLisa ? ringside_deduction : 0,
      commission
    }
  })

  // commission_total = sum of all per-contractor commissions
  const commission_total = details.reduce((sum, d) => sum + d.commission, 0)

  // totals for display
  const total_cv         = details.reduce((sum, d) => sum + d.cv_total, 0)
  const total_contractor = details.reduce((sum, d) => sum + d.contractor_total, 0)
  const net_base         = total_cv - total_contractor

  return {
    period_key:          pk,
    commission_total,
    total_cv,
    total_contractor,
    net_base,
    ringside_count,
    ringside_deduction,
    // Legacy fields kept for frontend compatibility
    grand_async_orderly_cv: 0,
    grand_sync_cv:          0,
    grand_owner_cv:         0,
    commission_async_orderly: 0,
    commission_sync:          0,
    commission_owner:         0,
    by_contractor: details.sort((a: any, b: any) => b.commission - a.commission)
  }
}

app.get('/api/commission/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  return c.json(await calcCommission(c.env.DB, pk))
})

// ──────────────────────────────────────────────
// GUSTO EXPORT — accepts period_key
// ──────────────────────────────────────────────
app.get('/api/export/gusto/period/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  // Ensure new columns exist
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN first_name TEXT DEFAULT ''`).run().catch(() => {})
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN last_name TEXT DEFAULT ''`).run().catch(() => {})
  await c.env.DB.prepare(`ALTER TABLE contractors ADD COLUMN gusto_type TEXT DEFAULT 'Individual'`).run().catch(() => {})
  const rows = await c.env.DB.prepare(`
    SELECT ct.name as contractor_name, ct.first_name, ct.last_name, ct.company, ct.ein_ssn, ct.email,
      COALESCE(ct.gusto_type, 'Individual') as gusto_type,
      COALESCE(ct.contractor_type, 'regular') as contractor_type,
      s.period_label,
      COUNT(*) as total_cases, SUM(c.contractor_fee) as total_pay,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END) as orderly_count
    FROM consults c
    LEFT JOIN contractors ct ON c.contractor_id = ct.id
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=?
    GROUP BY c.contractor_id ORDER BY ct.name
  `).bind(pk).all()
  return c.json(rows.results)
})

// Legacy single-session gusto
app.get('/api/export/gusto/:session_id', async (c) => {
  const sid = c.req.param('session_id')
  const rows = await c.env.DB.prepare(`
    SELECT ct.name as contractor_name, ct.company, ct.ein_ssn, ct.email, s.period_label,
      COUNT(*) as total_cases, SUM(c.contractor_fee) as total_pay,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN c.contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN c.contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN c.is_orderly=1 THEN c.contractor_fee ELSE 0 END) as orderly_pay
    FROM consults c LEFT JOIN contractors ct ON c.contractor_id = ct.id
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE c.session_id=? GROUP BY c.contractor_id ORDER BY ct.name
  `).bind(sid).all()
  return c.json(rows.results)
})

// ──────────────────────────────────────────────
// CV SUMMARY — period_key aware
// ──────────────────────────────────────────────
app.get('/api/cv-summary/period/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const sessions = await c.env.DB.prepare(
    'SELECT * FROM upload_sessions WHERE period_key=? ORDER BY id'
  ).bind(pk).all()
  const periodLabel = (sessions.results[0] as any)?.period_label || pk

  const byVisitType = await c.env.DB.prepare(`
    SELECT visit_type, COUNT(*) as count, SUM(carevalidate_fee) as total_amount
    FROM consults c LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? GROUP BY visit_type
  `).bind(pk).all()

  const total = await c.env.DB.prepare(`
    SELECT COUNT(*) as total_cases, SUM(carevalidate_fee) as total_owed
    FROM consults c LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=?
  `).bind(pk).first()

  // Breakdown by contractor: async, sync, orderly counts + CV amounts
  const byContractorRaw = await c.env.DB.prepare(`
    SELECT
      ct.id                                                            AS contractor_id,
      ct.name                                                          AS contractor_name,
      ct.contractor_type,
      COUNT(*)                                                         AS total_cases,
      SUM(c.carevalidate_fee)                                          AS total_cv,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL'
               THEN 1 ELSE 0 END)                                     AS async_cases,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL'
               THEN c.carevalidate_fee ELSE 0 END)                    AS async_cv,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON')
               THEN 1 ELSE 0 END)                                     AS sync_cases,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON')
               THEN c.carevalidate_fee ELSE 0 END)                    AS sync_cv,
      SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END)                 AS orderly_cases,
      SUM(CASE WHEN c.is_orderly=1 THEN c.carevalidate_fee ELSE 0 END) AS orderly_cv
    FROM consults c
    JOIN upload_sessions s  ON c.session_id = s.id
    JOIN contractors ct     ON c.contractor_id = ct.id
    WHERE s.period_key=?
    GROUP BY ct.id, ct.name, ct.contractor_type
    ORDER BY total_cv DESC
  `).bind(pk).all()

  return c.json({
    session: { period_label: periodLabel, period_key: pk, files: sessions.results },
    byVisitType: byVisitType.results,
    byContractor: byContractorRaw.results,
    total
  })
})

app.get('/api/cv-summary/:session_id', async (c) => {
  const sid = c.req.param('session_id')
  const session = await c.env.DB.prepare('SELECT * FROM upload_sessions WHERE id=?').bind(sid).first()
  const byVisitType = await c.env.DB.prepare(
    'SELECT visit_type, COUNT(*) as count, SUM(carevalidate_fee) as total_amount FROM consults WHERE session_id=? GROUP BY visit_type'
  ).bind(sid).all()
  const total = await c.env.DB.prepare(
    'SELECT COUNT(*) as total_cases, SUM(carevalidate_fee) as total_owed FROM consults WHERE session_id=?'
  ).bind(sid).first()
  return c.json({ session, byVisitType: byVisitType.results, total })
})

// Diagnostic: per-contractor totals for a period (fast, single query)
app.get('/api/debug/contractor-totals/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const rows = await c.env.DB.prepare(`
    SELECT
      ct.id as contractor_id,
      ct.name as contractor_name,
      c.doctor_name as raw_doctor_name,
      COUNT(*) as cases,
      SUM(c.carevalidate_fee) as total_cv,
      SUM(c.contractor_fee)  as total_pay
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    LEFT JOIN contractors ct    ON c.contractor_id = ct.id
    WHERE s.period_key = ?
    GROUP BY ct.id, ct.name, c.doctor_name
    ORDER BY ct.name, c.doctor_name
  `).bind(pk).all()
  return c.json(rows.results)
})

// Find unmatched consults (null contractor_id) for a period
app.get('/api/debug/unmatched/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const rows = await c.env.DB.prepare(`
    SELECT c.id, c.doctor_name, c.visit_type, c.is_orderly, c.carevalidate_fee, c.contractor_fee, c.organization_name
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key = ?
      AND (c.contractor_id IS NULL OR c.contractor_id = 0)
      AND c.visit_type != 'NO_SHOW'
    ORDER BY c.doctor_name, c.id
  `).bind(pk).all()
  return c.json(rows.results)
})

// Find non-standard fee consults for a contractor in a period
app.get('/api/debug/bad-fees/:period_key/:contractor_id', async (c) => {
  const pk  = c.req.param('period_key')
  const cid = c.req.param('contractor_id')
  const rows = await c.env.DB.prepare(`
    SELECT c.id, c.visit_type, c.is_orderly, c.carevalidate_fee, c.contractor_fee, c.doctor_name, c.organization_name
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key = ? AND c.contractor_id = ?
      AND c.visit_type != 'NO_SHOW'
      AND NOT (
        (c.is_orderly = 1 AND c.contractor_fee = 10) OR
        (c.is_orderly = 0 AND c.visit_type = 'ASYNC_TEXT_EMAIL' AND c.contractor_fee = 10) OR
        (c.is_orderly = 0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') AND c.contractor_fee = 30)
      )
    ORDER BY c.id
  `).bind(pk, cid).all()
  return c.json(rows.results)
})

// Find SYNC consults wrongly flagged as orderly for a contractor in a period
app.get('/api/debug/orderly-sync/:period_key/:contractor_id', async (c) => {
  const pk  = c.req.param('period_key')
  const cid = c.req.param('contractor_id')
  const rows = await c.env.DB.prepare(`
    SELECT c.id, c.visit_type, c.is_orderly, c.carevalidate_fee, c.contractor_fee, c.doctor_name, c.organization_name
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key = ? AND c.contractor_id = ?
      AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON')
      AND c.is_orderly = 1
    ORDER BY c.id
  `).bind(pk, cid).all()
  return c.json(rows.results)
})

// Find ALL SYNC rows from OrderlyMeds in a period (any is_orderly value)
app.get('/api/debug/orderly-sync-all/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const rows = await c.env.DB.prepare(`
    SELECT c.id, c.contractor_id, c.visit_type, c.is_orderly,
           c.carevalidate_fee, c.contractor_fee, c.doctor_name, c.organization_name
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key = ?
      AND LOWER(c.organization_name) LIKE '%orderly%'
      AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON')
    ORDER BY c.id
  `).bind(pk).all()
  return c.json(rows.results)
})

export default app
