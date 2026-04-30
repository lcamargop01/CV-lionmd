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

// From March 2026 onward, Denied cases are paid at normal rates.
// Earlier periods always treat Denied as $0.
const DENIED_PAID_FROM = '2026-03'
function isDeniedPaid(pk: string | null | undefined): boolean {
  if (!pk) return false
  return pk >= DENIED_PAID_FROM
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
  // Create period_settings table for per-period configuration flags
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS period_settings (
      period_key TEXT PRIMARY KEY,
      denied_paid INTEGER DEFAULT 0,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `).run().catch(() => {})
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

  // Load period-level settings: denied_paid flag per period
  // If period_key is specified, look it up; otherwise load all for per-row lookup
  const periodSettingsMap: Record<string, number> = {}
  await ensureSchema(c.env.DB)
  const psRows = await c.env.DB.prepare('SELECT period_key, denied_paid FROM period_settings').all()
  for (const r of psRows.results as any[]) {
    periodSettingsMap[r.period_key] = r.denied_paid ?? 0
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
      `SELECT c.id, c.visit_type, c.is_orderly, c.contractor_id, c.doctor_name, c.session_id, c.decision_status, s.period_key as period_key
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

      // Denied and No Decision: zero out fees UNLESS the period is 2026-03+
      // (from March 2026 onward, denied cases are paid at normal rates)
      const decisionStatus = (row.decision_status || '').trim()
      const rowPeriodKey = (row.period_key || period_key || '') as string
      if (decisionStatus === 'No Decision' ||
          (decisionStatus === 'Denied' && !isDeniedPaid(rowPeriodKey))) {
        cvFee = 0
        ctFee = 0
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
// PERIOD SETTINGS — per-period flags (e.g. denied_paid)
// ──────────────────────────────────────────────
app.get('/api/period-settings/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  await ensureSchema(c.env.DB)
  const row = await c.env.DB.prepare(
    `SELECT * FROM period_settings WHERE period_key=?`
  ).bind(pk).first() as any
  return c.json({ period_key: pk, denied_paid: row?.denied_paid ?? 0 })
})

app.post('/api/period-settings/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const body = await c.req.json() as any
  const denied_paid = body.denied_paid ? 1 : 0
  await c.env.DB.prepare(`
    INSERT INTO period_settings (period_key, denied_paid, updated_at)
    VALUES (?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(period_key) DO UPDATE SET denied_paid=excluded.denied_paid, updated_at=CURRENT_TIMESTAMP
  `).bind(pk, denied_paid).run()
  return c.json({ ok: true, period_key: pk, denied_paid })
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
// MERGE CONTRACTORS
// Body: { keep_id: number, merge_ids: number[] }
// - Re-points all consults from merge_ids → keep_id
// - Re-runs fee calculation for affected consults (owner/type may differ)
// - Deactivates the merged (duplicate) contractor records
// ──────────────────────────────────────────────
app.post('/api/contractors/merge', async (c) => {
  const body = await c.req.json() as any
  const keepId: number = Number(body.keep_id)
  const mergeIds: number[] = (body.merge_ids || []).map(Number).filter((id: number) => id !== keepId)

  if (!keepId || mergeIds.length === 0) {
    return c.json({ error: 'keep_id and at least one merge_id required' }, 400)
  }

  // Load the canonical contractor info
  const keeper = await c.env.DB.prepare('SELECT * FROM contractors WHERE id=?').bind(keepId).first() as any
  if (!keeper) return c.json({ error: 'keep_id not found' }, 404)

  // Load rates maps to recalculate fees after re-pointing
  const { ratesMap, contractorTypeMap, ctRatesMap } = await loadMaps(c.env.DB)
  const keeperType = keeper.contractor_type || 'regular'

  // Pre-load session → period_key map to avoid N+1 queries inside the loop
  const sessionsAll = await c.env.DB.prepare('SELECT id, period_key FROM upload_sessions').all()
  const sessionPkMap: Record<number, string> = {}
  for (const s of sessionsAll.results as any[]) {
    sessionPkMap[s.id] = s.period_key || ''
  }

  let totalMoved = 0

  for (const mergeId of mergeIds) {
    // Re-point all non-overridden consults from mergeId → keepId and recalculate fees
    const PAGE_SIZE = 500
    let offset = 0

    while (true) {
      const rows = await c.env.DB.prepare(
        `SELECT id, visit_type, is_orderly, decision_status, session_id FROM consults
         WHERE contractor_id=? AND (is_override IS NULL OR is_override=0)
         LIMIT ${PAGE_SIZE} OFFSET ${offset}`
      ).bind(mergeId).all()

      if (!rows.results.length) break

      const stmts = []
      for (const row of rows.results as any[]) {
        const vt: string = (row.visit_type || '').toUpperCase()
        const isOrderly: boolean = row.is_orderly === 1
        const rowPk: string = sessionPkMap[row.session_id] || ''

        // CV fee (same regardless of contractor type)
        let cvFee = isOrderly ? (ratesMap['ORDERLY']?.cv ?? 17) : (ratesMap[vt]?.cv ?? 0)

        // CT fee using the KEEPER's contractor type
        let ctFee: number
        if (isOrderly) {
          ctFee = ctRatesMap[keeperType]?.['ORDERLY'] ?? ratesMap['ORDERLY']?.ct ?? 10
        } else {
          ctFee = ctRatesMap[keeperType]?.[vt] ?? ratesMap[vt]?.ct ?? 0
        }

        // Zero out for No Decision always; zero Denied only for pre-2026-03
        const status = (row.decision_status || '').trim()
        if (status === 'No Decision' || (status === 'Denied' && !isDeniedPaid(rowPk))) {
          cvFee = 0; ctFee = 0
        }

        stmts.push(
          c.env.DB.prepare('UPDATE consults SET contractor_id=?, carevalidate_fee=?, contractor_fee=? WHERE id=?')
            .bind(keepId, cvFee, ctFee, row.id)
        )
      }

      if (stmts.length > 0) await c.env.DB.batch(stmts)
      totalMoved += rows.results.length
      offset += PAGE_SIZE
      if (rows.results.length < PAGE_SIZE) break
    }

    // Also re-point any overridden consults (keep fees as-is, just update the pointer)
    await c.env.DB.prepare('UPDATE consults SET contractor_id=? WHERE contractor_id=? AND is_override=1')
      .bind(keepId, mergeId).run()

    // Deactivate the merged duplicate
    await c.env.DB.prepare('UPDATE contractors SET is_active=0 WHERE id=?').bind(mergeId).run()
  }

  // Recompute session totals for all affected sessions
  const affectedSessions = await c.env.DB.prepare(
    `SELECT DISTINCT session_id FROM consults WHERE contractor_id=?`
  ).bind(keepId).all()

  const sessionStmts = []
  for (const s of affectedSessions.results as any[]) {
    const totals = await c.env.DB.prepare(
      'SELECT COUNT(*) as tc, SUM(carevalidate_fee) as cv, SUM(contractor_fee) as ct FROM consults WHERE session_id=?'
    ).bind(s.session_id).first() as any
    sessionStmts.push(
      c.env.DB.prepare('UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?')
        .bind(totals?.tc || 0, totals?.cv || 0, totals?.ct || 0, s.session_id)
    )
  }
  if (sessionStmts.length) await c.env.DB.batch(sessionStmts)

  return c.json({ ok: true, keep_id: keepId, merged: mergeIds, consults_moved: totalMoved })
})

// GET /api/contractors/all — includes inactive, for merge UI
app.get('/api/contractors/all', async (c) => {
  const rows = await c.env.DB.prepare('SELECT * FROM contractors ORDER BY name, is_active DESC').all()
  return c.json(rows.results)
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
  ctRatesMap: Record<string, Record<string, number>> = {},
  periodKeyForDenied: string | null = null
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
      let cvFee = fees.cv
      let ctFee = (ctRatesMap[ctype]?.[vtKey] !== undefined) ? ctRatesMap[ctype][vtKey] : fees.ct
      // No Decision always $0; Denied is $0 only for periods before 2026-03
      const status = (row.decision_status || '').trim()
      if (status === 'No Decision' ||
          (status === 'Denied' && !isDeniedPaid(periodKeyForDenied))) {
        cvFee = 0
        ctFee = 0
      }
      totalCV += cvFee
      totalCT += ctFee
      return stmt.bind(
        sessionId, row.case_id || '', row.case_id_short || '',
        row.organization_name || '', row.patient_name || '',
        row.doctor_name || '', row.decision_date || '',
        row.decision_status || '', row.visit_type || '',
        cvFee, ctFee, contractorId, row.is_flagged ? 1 : 0, orderly
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

  const result = await insertRows(c.env.DB, sessionId, rows, ratesMap, contractorMap, contractorTypeMap, ctRatesMap, pk)

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

  // Look up period_key so denied fee logic is applied correctly
  const chunkSess = await c.env.DB.prepare('SELECT period_key FROM upload_sessions WHERE id=?').bind(session_id).first() as any
  const chunkPeriodKey = chunkSess?.period_key || null

  const result = await insertRows(c.env.DB, session_id, rows, ratesMap, contractorMap, contractorTypeMap, ctRatesMap, chunkPeriodKey)

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
  const period_key      = c.req.query('period_key')
  const session_id      = c.req.query('session_id')
  const doctor_name     = c.req.query('doctor_name')
  const visit_type      = c.req.query('visit_type')
  const is_orderly      = c.req.query('is_orderly')       // '1' | '0'
  const is_flagged      = c.req.query('is_flagged')       // '1' | '0'
  const decision_status = c.req.query('decision_status')  // 'Approved' | 'Denied' | 'Pending'
  const organization    = c.req.query('organization')
  const page            = c.req.query('page')  || '1'
  const limit           = c.req.query('limit') || '50'
  const search          = c.req.query('search')
  const offset          = (parseInt(page) - 1) * parseInt(limit)

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
  if (visit_type === '_SYNC') {
    // All sync types (phone, video, in-person) — both orderly and non-orderly
    where += ` AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON')`
  } else if (visit_type === '_OTHER') {
    // Anything that is NOT a known type
    where += ` AND (c.visit_type IS NULL OR c.visit_type NOT IN ('ASYNC_TEXT_EMAIL','SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON','NO_SHOW') OR c.visit_type = '')`
  } else if (visit_type) {
    where += ' AND c.visit_type=?';     params.push(visit_type)
  }
  if (is_orderly === '1') { where += ' AND c.is_orderly=1' }
  if (is_orderly === '0') { where += ' AND c.is_orderly=0' }
  if (is_flagged === '1') { where += ' AND c.is_flagged=1' }
  if (is_flagged === '0') { where += ' AND (c.is_flagged=0 OR c.is_flagged IS NULL)' }
  if (decision_status)    { where += ' AND c.decision_status=?'; params.push(decision_status) }
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
// DELETE /api/consults/:id  — remove a single consult, recompute session totals
// ──────────────────────────────────────────────
app.delete('/api/consults/:id', async (c) => {
  const id = c.req.param('id')
  // Grab session_id before deletion so we can recompute totals
  const existing = await c.env.DB.prepare('SELECT session_id FROM consults WHERE id=?').bind(id).first() as any
  if (!existing) return c.json({ error: 'Not found' }, 404)

  await c.env.DB.prepare('DELETE FROM consults WHERE id=?').bind(id).run()

  // Recompute session totals
  if (existing.session_id) {
    const totals = await c.env.DB.prepare(
      'SELECT COUNT(*) as tc, SUM(carevalidate_fee) as cv, SUM(contractor_fee) as ct FROM consults WHERE session_id=?'
    ).bind(existing.session_id).first() as any
    await c.env.DB.prepare(
      'UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?'
    ).bind(totals?.tc || 0, totals?.cv || 0, totals?.ct || 0, existing.session_id).run()
  }

  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// SUMMARY — accepts period_key (aggregates all files) OR session_id
// ──────────────────────────────────────────────
async function buildSummary(db: D1Database, where: string, params: any[], pk?: string | null) {
  // Exclude Denied/No Decision from counts/totals ONLY for periods before 2026-03.
  // From 2026-03 onward, denied cases are paid and must appear in totals.
  // 'No Decision' is always excluded regardless of period.
  const deniedPaid = isDeniedPaid(pk)
  const whereEx = deniedPaid
    ? where + ` AND c.decision_status != 'No Decision'`
    : where + ` AND c.decision_status NOT IN ('Denied','No Decision')`
  const [byDoctor, byVisitType, byOrg, totals, flagged] = await Promise.all([
    db.prepare(`
      SELECT c.doctor_name, ct.id as contractor_id, ct.company, ct.ein_ssn,
        COUNT(*) as case_count,
        SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
        SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
        SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END) as orderly_count,
        SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='NO_SHOW' THEN 1 ELSE 0 END) as no_show_count,
        COUNT(*)
          - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END)
          - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END)
          - SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END)
          - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='NO_SHOW' THEN 1 ELSE 0 END) as other_count,
        SUM(c.carevalidate_fee) as total_carevalidate,
        SUM(c.contractor_fee) as total_contractor,
        SUM(c.carevalidate_fee) - SUM(c.contractor_fee) as margin
      FROM consults c
      LEFT JOIN contractors ct ON c.contractor_id = ct.id
      LEFT JOIN upload_sessions s ON c.session_id = s.id
      ${whereEx} GROUP BY c.doctor_name ORDER BY c.doctor_name
    `).bind(...params).all(),
    db.prepare(`
      SELECT visit_type, COUNT(*) as count,
        SUM(carevalidate_fee) as total_cv, SUM(contractor_fee) as total_ct
      FROM consults c
      LEFT JOIN upload_sessions s ON c.session_id = s.id
      ${whereEx} GROUP BY visit_type ORDER BY count DESC
    `).bind(...params).all(),
    db.prepare(`
      SELECT organization_name, COUNT(*) as count, SUM(carevalidate_fee) as total_cv
      FROM consults c
      LEFT JOIN upload_sessions s ON c.session_id = s.id
      ${whereEx} GROUP BY organization_name ORDER BY count DESC LIMIT 20
    `).bind(...params).all(),
    db.prepare(`
      SELECT COUNT(*) as total_cases,
        SUM(carevalidate_fee) as total_carevalidate,
        SUM(contractor_fee) as total_contractor,
        SUM(carevalidate_fee) - SUM(contractor_fee) as total_margin
      FROM consults c
      LEFT JOIN upload_sessions s ON c.session_id = s.id ${whereEx}
    `).bind(...params).first(),
    db.prepare(
      `SELECT COUNT(*) as count FROM consults c
       LEFT JOIN upload_sessions s ON c.session_id = s.id
       ${whereEx} AND c.is_flagged=1`
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
  return c.json(await buildSummary(c.env.DB, 'WHERE s.period_key=?', [pk], pk))
})

app.get('/api/summary/:session_id', async (c) => {
  const sid = c.req.param('session_id')
  // Look up the period_key for this session so denied_paid logic applies correctly
  const sess = await c.env.DB.prepare('SELECT period_key FROM upload_sessions WHERE id=?').bind(sid).first() as any
  const pk = sess?.period_key || null
  return c.json(await buildSummary(c.env.DB, 'WHERE c.session_id=?', [sid], pk))
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
      SUM(CASE WHEN is_orderly=1 THEN contractor_fee ELSE 0 END) as orderly_pay,
      SUM(CASE WHEN is_orderly=0 AND visit_type='NO_SHOW' THEN 1 ELSE 0 END) as no_show_count,
      COUNT(*)
        - SUM(CASE WHEN is_orderly=0 AND visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_orderly=0 AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_orderly=1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_orderly=0 AND visit_type='NO_SHOW' THEN 1 ELSE 0 END) as other_count
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? AND c.contractor_id=?
      AND c.decision_status != 'No Decision'
      ${isDeniedPaid(pk) ? '' : "AND c.decision_status != 'Denied'"}
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
      SUM(CASE WHEN is_orderly=1 THEN contractor_fee ELSE 0 END) as orderly_pay,
      SUM(CASE WHEN is_orderly=0 AND visit_type='NO_SHOW' THEN 1 ELSE 0 END) as no_show_count,
      COUNT(*)
        - SUM(CASE WHEN is_orderly=0 AND visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_orderly=0 AND visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_orderly=1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_orderly=0 AND visit_type='NO_SHOW' THEN 1 ELSE 0 END) as other_count
    FROM consults WHERE session_id=? AND contractor_id=?
      AND decision_status != 'No Decision'
      ${isDeniedPaid((session as any)?.period_key) ? '' : "AND decision_status != 'Denied'"}
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
      AND c.decision_status != 'No Decision'
      ${isDeniedPaid(pk) ? '' : "AND c.decision_status != 'Denied'"}
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
      AND c.decision_status != 'No Decision'
      ${isDeniedPaid(pk) ? '' : "AND c.decision_status != 'Denied'"}
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
      SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='NO_SHOW' THEN 1 ELSE 0 END) as no_show_count,
      COUNT(*)
        - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END)
        - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END)
        - SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='NO_SHOW' THEN 1 ELSE 0 END) as other_count
    FROM consults c
    LEFT JOIN contractors ct ON c.contractor_id = ct.id
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=?
      AND c.decision_status != 'No Decision'
      ${isDeniedPaid(pk) ? '' : "AND c.decision_status != 'Denied'"}
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
      SUM(CASE WHEN c.is_orderly=1 THEN c.contractor_fee ELSE 0 END) as orderly_pay,
      SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='NO_SHOW' THEN 1 ELSE 0 END) as no_show_count,
      COUNT(*)
        - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END)
        - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END)
        - SUM(CASE WHEN c.is_orderly=1 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN c.is_orderly=0 AND c.visit_type='NO_SHOW' THEN 1 ELSE 0 END) as other_count
    FROM consults c LEFT JOIN contractors ct ON c.contractor_id = ct.id
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE c.session_id=?
      AND c.decision_status != 'No Decision'
      ${isDeniedPaid((await c.env.DB.prepare('SELECT period_key FROM upload_sessions WHERE id=?').bind(sid).first() as any)?.period_key) ? '' : "AND c.decision_status != 'Denied'"}
    GROUP BY c.contractor_id ORDER BY ct.name
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

  const deniedFilter = isDeniedPaid(pk) ? `AND c.decision_status != 'No Decision'` : `AND c.decision_status NOT IN ('Denied','No Decision')`

  const byVisitType = await c.env.DB.prepare(`
    SELECT visit_type, COUNT(*) as count, SUM(carevalidate_fee) as total_amount
    FROM consults c LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? ${deniedFilter}
    GROUP BY visit_type
  `).bind(pk).all()

  const total = await c.env.DB.prepare(`
    SELECT COUNT(*) as total_cases, SUM(carevalidate_fee) as total_owed
    FROM consults c LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? ${deniedFilter}
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
    WHERE s.period_key=? ${deniedFilter}
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
  const sessDeniedFilter = isDeniedPaid((session as any)?.period_key) ? `AND decision_status != 'No Decision'` : `AND decision_status NOT IN ('Denied','No Decision')`
  const byVisitType = await c.env.DB.prepare(
    `SELECT visit_type, COUNT(*) as count, SUM(carevalidate_fee) as total_amount FROM consults WHERE session_id=? ${sessDeniedFilter} GROUP BY visit_type`
  ).bind(sid).all()
  const total = await c.env.DB.prepare(
    `SELECT COUNT(*) as total_cases, SUM(carevalidate_fee) as total_owed FROM consults WHERE session_id=? ${sessDeniedFilter}`
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

// ══════════════════════════════════════════════════════════════════
// ONBOARDING MODULE
// Tables: onboarding_candidates, onboarding_documents, onboarding_meetings,
//         onboarding_availability, contract_templates
// ══════════════════════════════════════════════════════════════════

async function ensureOnboardingSchema(db: D1Database) {
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS onboarding_candidates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      full_name TEXT NOT NULL,
      company_name TEXT,
      email TEXT,
      phone TEXT,
      ein_ssn TEXT,
      contractor_type TEXT DEFAULT 'regular',
      specialty TEXT,
      status TEXT DEFAULT 'new',
      source TEXT,
      notes TEXT,
      resume_text TEXT,
      resume_summary TEXT,
      resume_key_points TEXT,
      -- Hired checklist
      payroll_sent INTEGER DEFAULT 0,
      payroll_sent_at DATETIME,
      contract_sent INTEGER DEFAULT 0,
      contract_sent_at DATETIME,
      contract_signed INTEGER DEFAULT 0,
      contract_signed_at DATETIME,
      training_scheduled INTEGER DEFAULT 0,
      training_scheduled_at DATETIME,
      training_completed INTEGER DEFAULT 0,
      training_completed_at DATETIME,
      docs_received INTEGER DEFAULT 0,
      docs_received_at DATETIME,
      -- Conversion
      converted_contractor_id INTEGER,
      converted_at DATETIME,
      -- Metadata
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `).run().catch(() => {})

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS onboarding_documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      candidate_id INTEGER NOT NULL,
      doc_type TEXT NOT NULL,
      file_name TEXT NOT NULL,
      file_data TEXT NOT NULL,
      file_size INTEGER,
      mime_type TEXT,
      uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (candidate_id) REFERENCES onboarding_candidates(id)
    )
  `).run().catch(() => {})

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS onboarding_meetings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      candidate_id INTEGER NOT NULL,
      title TEXT,
      scheduled_at DATETIME,
      duration_min INTEGER DEFAULT 30,
      meeting_link TEXT,
      meeting_type TEXT DEFAULT 'interview',
      status TEXT DEFAULT 'scheduled',
      notes TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (candidate_id) REFERENCES onboarding_candidates(id)
    )
  `).run().catch(() => {})

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS onboarding_availability (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      day_of_week INTEGER,
      date TEXT,
      start_time TEXT NOT NULL,
      end_time TEXT NOT NULL,
      label TEXT,
      is_active INTEGER DEFAULT 1,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `).run().catch(() => {})

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS contract_templates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      content TEXT NOT NULL,
      is_default INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `).run().catch(() => {})

  // seed a default contract template if none exists
  const tmplCount = await db.prepare('SELECT COUNT(*) as c FROM contract_templates').first() as any
  if ((tmplCount?.c ?? 0) === 0) {
    await db.prepare(`
      INSERT INTO contract_templates (name, content, is_default) VALUES (?, ?, 1)
    `).bind('Standard Independent Contractor Agreement', `INDEPENDENT CONTRACTOR AGREEMENT

This Independent Contractor Agreement ("Agreement") is entered into as of {{date}} between:

COMPANY: Lion MD's, PLLC, a Professional Limited Liability Company ("Company")

CONTRACTOR: {{contractor_name}}
{{#if company_name}}Business Name: {{company_name}}{{/if}}
{{#if ein_ssn}}Tax ID: {{ein_ssn}}{{/if}}
Email: {{email}}

1. SERVICES
Contractor agrees to provide telehealth consultation and medical review services as an independent contractor. Specific services will be outlined in applicable work orders.

2. COMPENSATION
Contractor will be compensated per completed consultation at the rates established in the current Payment Rate Schedule, which may be updated by Company upon 30 days' notice.
  - Async Text/Email: per current rate schedule
  - Sync (Phone/Video/In-Person): per current rate schedule
  - OrderlyMeds consultations: per current rate schedule

3. INDEPENDENT CONTRACTOR STATUS
Contractor is an independent contractor, not an employee. Contractor is responsible for all taxes on compensation received. Company will not withhold taxes or provide employee benefits.

4. CONFIDENTIALITY
Contractor agrees to maintain strict confidentiality regarding all patient information, Company business practices, and proprietary information in compliance with HIPAA and applicable law.

5. TERM AND TERMINATION
This Agreement commences on {{date}} and continues until terminated by either party with 30 days written notice, or immediately for cause.

6. GOVERNING LAW
This Agreement is governed by the laws of the state in which the Company is incorporated.

IN WITNESS WHEREOF, the parties have executed this Agreement as of the date first written above.

LION MD'S, PLLC                    CONTRACTOR

_______________________            _______________________
Authorized Signature               {{contractor_name}}
Date: __________________           Date: __________________`, 1).run().catch(() => {})
  }
}

// ── Candidates CRUD ──────────────────────────────────────────────

app.get('/api/onboarding/candidates', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const status = c.req.query('status')
  const search = c.req.query('search')
  let q = `SELECT oc.*,
    (SELECT COUNT(*) FROM onboarding_documents od WHERE od.candidate_id = oc.id) as doc_count,
    (SELECT COUNT(*) FROM onboarding_meetings om WHERE om.candidate_id = oc.id) as meeting_count
    FROM onboarding_candidates oc WHERE 1=1`
  const params: any[] = []
  if (status && status !== 'all') { q += ' AND oc.status=?'; params.push(status) }
  if (search) { q += ` AND (oc.full_name LIKE ? OR oc.email LIKE ? OR oc.company_name LIKE ?)`; const s = `%${search}%`; params.push(s, s, s) }
  q += ' ORDER BY oc.updated_at DESC'
  const rows = await c.env.DB.prepare(q).bind(...params).all()
  return c.json(rows.results)
})

app.get('/api/onboarding/candidates/:id', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  const candidate = await c.env.DB.prepare(`
    SELECT oc.*,
      (SELECT COUNT(*) FROM onboarding_documents od WHERE od.candidate_id = oc.id) as doc_count,
      (SELECT COUNT(*) FROM onboarding_meetings om WHERE om.candidate_id = oc.id) as meeting_count
    FROM onboarding_candidates oc WHERE oc.id=?
  `).bind(id).first()
  if (!candidate) return c.json({ error: 'Not found' }, 404)
  const docs = await c.env.DB.prepare(
    'SELECT id, doc_type, file_name, file_size, mime_type, uploaded_at FROM onboarding_documents WHERE candidate_id=? ORDER BY uploaded_at DESC'
  ).bind(id).all()
  const meetings = await c.env.DB.prepare(
    'SELECT * FROM onboarding_meetings WHERE candidate_id=? ORDER BY scheduled_at DESC'
  ).bind(id).all()
  return c.json({ ...candidate, documents: docs.results, meetings: meetings.results })
})

app.post('/api/onboarding/candidates', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const body = await c.req.json() as any
  const r = await c.env.DB.prepare(`
    INSERT INTO onboarding_candidates
      (full_name, company_name, email, phone, ein_ssn, contractor_type, specialty, status, source, notes)
    VALUES (?,?,?,?,?,?,?,?,?,?)
  `).bind(
    body.full_name || '', body.company_name || '', body.email || '',
    body.phone || '', body.ein_ssn || '', body.contractor_type || 'regular',
    body.specialty || '', body.status || 'new', body.source || '', body.notes || ''
  ).run()
  return c.json({ ok: true, id: r.meta.last_row_id })
})

app.put('/api/onboarding/candidates/:id', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  const body = await c.req.json() as any
  // Build update dynamically for any fields passed
  const allowed = ['full_name','company_name','email','phone','ein_ssn','contractor_type','specialty',
    'status','source','notes','payroll_sent','payroll_sent_at','contract_sent','contract_sent_at',
    'contract_signed','contract_signed_at','training_scheduled','training_scheduled_at',
    'training_completed','training_completed_at','docs_received','docs_received_at',
    'resume_summary','resume_key_points','resume_text','converted_contractor_id','converted_at']
  const sets: string[] = []
  const vals: any[] = []
  for (const k of allowed) {
    if (body[k] !== undefined) { sets.push(`${k}=?`); vals.push(body[k]) }
  }
  if (sets.length === 0) return c.json({ ok: true })
  sets.push('updated_at=CURRENT_TIMESTAMP')
  vals.push(id)
  await c.env.DB.prepare(`UPDATE onboarding_candidates SET ${sets.join(',')} WHERE id=?`).bind(...vals).run()
  return c.json({ ok: true })
})

app.delete('/api/onboarding/candidates/:id', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  await c.env.DB.prepare('DELETE FROM onboarding_documents WHERE candidate_id=?').bind(id).run()
  await c.env.DB.prepare('DELETE FROM onboarding_meetings WHERE candidate_id=?').bind(id).run()
  await c.env.DB.prepare('DELETE FROM onboarding_candidates WHERE id=?').bind(id).run()
  return c.json({ ok: true })
})

// ── Documents ────────────────────────────────────────────────────

app.get('/api/onboarding/candidates/:id/documents', async (c) => {
  const id = c.req.param('id')
  const rows = await c.env.DB.prepare(
    'SELECT id, doc_type, file_name, file_size, mime_type, uploaded_at FROM onboarding_documents WHERE candidate_id=? ORDER BY uploaded_at DESC'
  ).bind(id).all()
  return c.json(rows.results)
})

// Upload document (base64 encoded in JSON body)
app.post('/api/onboarding/candidates/:id/documents', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  const body = await c.req.json() as any
  const r = await c.env.DB.prepare(`
    INSERT INTO onboarding_documents (candidate_id, doc_type, file_name, file_data, file_size, mime_type)
    VALUES (?,?,?,?,?,?)
  `).bind(id, body.doc_type || 'other', body.file_name || 'document', body.file_data || '',
          body.file_size || 0, body.mime_type || 'application/octet-stream').run()
  return c.json({ ok: true, id: r.meta.last_row_id })
})

// Download document (returns base64 data)
app.get('/api/onboarding/documents/:id', async (c) => {
  const id = c.req.param('id')
  const doc = await c.env.DB.prepare('SELECT * FROM onboarding_documents WHERE id=?').bind(id).first() as any
  if (!doc) return c.json({ error: 'Not found' }, 404)
  return c.json(doc)
})

app.delete('/api/onboarding/documents/:id', async (c) => {
  await c.env.DB.prepare('DELETE FROM onboarding_documents WHERE id=?').bind(c.req.param('id')).run()
  return c.json({ ok: true })
})

// ── Resume AI Analysis ───────────────────────────────────────────
// Stores the resume text and generated summary/key-points on the candidate

app.post('/api/onboarding/candidates/:id/analyze-resume', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  const body = await c.req.json() as any
  const resumeText: string = body.resume_text || ''

  if (!resumeText.trim()) return c.json({ error: 'No resume text provided' }, 400)

  // Simple rule-based extraction when no AI key is available —
  // pattern-match for skills, experience, education etc.
  // The front-end can also call this endpoint after extracting text from a PDF.
  const lines = resumeText.split(/\n/).map((l: string) => l.trim()).filter(Boolean)

  // Heuristic key point extraction
  const keyPoints: string[] = []

  // Specialty / board certs
  const specMatch = resumeText.match(/(?:board[- ]certified|fellowship|residency|specialt|physician|MD|DO|NP|PA|ARNP|LCSW|PharmD)[^.\n]{0,80}/gi)
  if (specMatch) keyPoints.push(...specMatch.slice(0, 3).map((s: string) => '🏥 ' + s.trim()))

  // Years of experience
  const expMatch = resumeText.match(/(\d+)\+?\s*years?\s*(?:of\s*)?(?:experience|practice|clinical)/gi)
  if (expMatch) keyPoints.push(...expMatch.slice(0, 2).map((s: string) => '📅 ' + s.trim()))

  // Telehealth
  const teleMatch = resumeText.match(/(?:telehealth|telemedicine|virtual|remote)[^.\n]{0,60}/gi)
  if (teleMatch) keyPoints.push(...teleMatch.slice(0, 2).map((s: string) => '💻 ' + s.trim()))

  // Availability / hours
  const availMatch = resumeText.match(/(?:available|availability|hours|full[- ]time|part[- ]time|weekends?|evenings?)[^.\n]{0,60}/gi)
  if (availMatch) keyPoints.push(...availMatch.slice(0, 2).map((s: string) => '🕐 ' + s.trim()))

  // Education
  const eduMatch = resumeText.match(/(?:University|College|School of Medicine|Medical School|M\.D\.|D\.O\.|graduated)[^.\n]{0,80}/gi)
  if (eduMatch) keyPoints.push(...eduMatch.slice(0, 2).map((s: string) => '🎓 ' + s.trim()))

  // State licenses
  const licMatch = resumeText.match(/(?:licensed in|license(?:d)?\s+(?:in\s+)?[A-Z]{2}(?:[,\s]+[A-Z]{2})*)[^.\n]{0,60}/gi)
  if (licMatch) keyPoints.push(...licMatch.slice(0, 2).map((s: string) => '📋 ' + s.trim()))

  // Build summary from first 3 substantial lines
  const summaryLines = lines.filter((l: string) => l.length > 30).slice(0, 4)
  const summary = summaryLines.join(' ').substring(0, 500)

  const finalPoints = [...new Set(keyPoints)].slice(0, 8)

  await c.env.DB.prepare(
    'UPDATE onboarding_candidates SET resume_text=?, resume_summary=?, resume_key_points=?, updated_at=CURRENT_TIMESTAMP WHERE id=?'
  ).bind(resumeText.substring(0, 8000), summary, JSON.stringify(finalPoints), id).run()

  return c.json({ ok: true, summary, key_points: finalPoints })
})

// ── Meetings ─────────────────────────────────────────────────────

app.get('/api/onboarding/candidates/:id/meetings', async (c) => {
  const id = c.req.param('id')
  const rows = await c.env.DB.prepare(
    'SELECT * FROM onboarding_meetings WHERE candidate_id=? ORDER BY scheduled_at DESC'
  ).bind(id).all()
  return c.json(rows.results)
})

app.post('/api/onboarding/candidates/:id/meetings', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  const body = await c.req.json() as any
  const r = await c.env.DB.prepare(`
    INSERT INTO onboarding_meetings (candidate_id, title, scheduled_at, duration_min, meeting_link, meeting_type, notes)
    VALUES (?,?,?,?,?,?,?)
  `).bind(id, body.title || 'Interview', body.scheduled_at || null,
          body.duration_min || 30, body.meeting_link || '', body.meeting_type || 'interview',
          body.notes || '').run()
  return c.json({ ok: true, id: r.meta.last_row_id })
})

app.put('/api/onboarding/meetings/:id', async (c) => {
  const id = c.req.param('id')
  const body = await c.req.json() as any
  await c.env.DB.prepare(`
    UPDATE onboarding_meetings SET title=?, scheduled_at=?, duration_min=?, meeting_link=?,
    meeting_type=?, status=?, notes=? WHERE id=?
  `).bind(body.title, body.scheduled_at, body.duration_min, body.meeting_link,
          body.meeting_type, body.status || 'scheduled', body.notes, id).run()
  return c.json({ ok: true })
})

app.delete('/api/onboarding/meetings/:id', async (c) => {
  await c.env.DB.prepare('DELETE FROM onboarding_meetings WHERE id=?').bind(c.req.param('id')).run()
  return c.json({ ok: true })
})

// ── Availability slots ───────────────────────────────────────────

app.get('/api/onboarding/availability', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const rows = await c.env.DB.prepare(
    'SELECT * FROM onboarding_availability WHERE is_active=1 ORDER BY day_of_week, date, start_time'
  ).all()
  return c.json(rows.results)
})

app.post('/api/onboarding/availability', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const body = await c.req.json() as any
  const r = await c.env.DB.prepare(`
    INSERT INTO onboarding_availability (day_of_week, date, start_time, end_time, label)
    VALUES (?,?,?,?,?)
  `).bind(body.day_of_week ?? null, body.date ?? null, body.start_time, body.end_time, body.label || '').run()
  return c.json({ ok: true, id: r.meta.last_row_id })
})

app.delete('/api/onboarding/availability/:id', async (c) => {
  await c.env.DB.prepare('UPDATE onboarding_availability SET is_active=0 WHERE id=?').bind(c.req.param('id')).run()
  return c.json({ ok: true })
})

// ── Contract Templates ───────────────────────────────────────────

app.get('/api/onboarding/templates', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const rows = await c.env.DB.prepare('SELECT id, name, is_default, created_at, updated_at FROM contract_templates ORDER BY is_default DESC, name').all()
  return c.json(rows.results)
})

app.get('/api/onboarding/templates/:id', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const t = await c.env.DB.prepare('SELECT * FROM contract_templates WHERE id=?').bind(c.req.param('id')).first()
  if (!t) return c.json({ error: 'Not found' }, 404)
  return c.json(t)
})

app.post('/api/onboarding/templates', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const body = await c.req.json() as any
  if (body.is_default) await c.env.DB.prepare('UPDATE contract_templates SET is_default=0').run()
  const r = await c.env.DB.prepare(
    'INSERT INTO contract_templates (name, content, is_default) VALUES (?,?,?)'
  ).bind(body.name, body.content, body.is_default ? 1 : 0).run()
  return c.json({ ok: true, id: r.meta.last_row_id })
})

app.put('/api/onboarding/templates/:id', async (c) => {
  const body = await c.req.json() as any
  if (body.is_default) await c.env.DB.prepare('UPDATE contract_templates SET is_default=0').run()
  await c.env.DB.prepare(
    'UPDATE contract_templates SET name=?, content=?, is_default=?, updated_at=CURRENT_TIMESTAMP WHERE id=?'
  ).bind(body.name, body.content, body.is_default ? 1 : 0, c.req.param('id')).run()
  return c.json({ ok: true })
})

app.delete('/api/onboarding/templates/:id', async (c) => {
  await c.env.DB.prepare('DELETE FROM contract_templates WHERE id=?').bind(c.req.param('id')).run()
  return c.json({ ok: true })
})

// Fill template placeholders with candidate data and return the filled text
app.post('/api/onboarding/templates/:id/fill', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const body = await c.req.json() as any
  const tmpl = await c.env.DB.prepare('SELECT * FROM contract_templates WHERE id=?').bind(c.req.param('id')).first() as any
  if (!tmpl) return c.json({ error: 'Template not found' }, 404)
  const today = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
  let content: string = tmpl.content
  content = content.replace(/{{date}}/g, body.date || today)
  content = content.replace(/{{contractor_name}}/g, body.full_name || body.contractor_name || '')
  content = content.replace(/{{company_name}}/g, body.company_name || '')
  content = content.replace(/{{ein_ssn}}/g, body.ein_ssn || '')
  content = content.replace(/{{email}}/g, body.email || '')
  content = content.replace(/{{phone}}/g, body.phone || '')
  content = content.replace(/{{specialty}}/g, body.specialty || '')
  // Handle conditional blocks {{#if field}}...{{/if}}
  content = content.replace(/{{#if (\w+)}}([\s\S]*?){{\/if}}/g, (_: string, field: string, inner: string) => {
    return body[field] ? inner : ''
  })
  return c.json({ ok: true, content, template_name: tmpl.name })
})

// ── Stats / Pipeline overview ────────────────────────────────────

app.get('/api/onboarding/stats', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const byStatus = await c.env.DB.prepare(
    `SELECT status, COUNT(*) as count FROM onboarding_candidates GROUP BY status`
  ).all()
  const total = await c.env.DB.prepare('SELECT COUNT(*) as c FROM onboarding_candidates').first() as any
  const hired = await c.env.DB.prepare(`SELECT COUNT(*) as c FROM onboarding_candidates WHERE status='hired'`).first() as any
  const rejected = await c.env.DB.prepare(`SELECT COUNT(*) as c FROM onboarding_candidates WHERE status='rejected'`).first() as any
  const recentActivity = await c.env.DB.prepare(
    `SELECT id, full_name, status, updated_at FROM onboarding_candidates ORDER BY updated_at DESC LIMIT 5`
  ).all()
  return c.json({
    total: total?.c ?? 0,
    hired: hired?.c ?? 0,
    rejected: rejected?.c ?? 0,
    by_status: byStatus.results,
    recent_activity: recentActivity.results
  })
})

// ════════════════════════════════════════════════════════════════
// AUTH MODULE
// Tables: portal_users
// ════════════════════════════════════════════════════════════════

async function ensureAuthSchema(db: D1Database) {
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS portal_users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL,
      password_hash TEXT,
      role TEXT NOT NULL DEFAULT 'onboarding',
      is_active INTEGER DEFAULT 1,
      must_set_password INTEGER DEFAULT 1,
      invite_token TEXT,
      invite_token_expires DATETIME,
      last_login DATETIME,
      created_by INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `).run().catch(() => {})
}

// ── Crypto helpers (Web Crypto API — available in CF Workers) ────

async function hashPassword(password: string): Promise<string> {
  const enc = new TextEncoder()
  const salt = crypto.getRandomValues(new Uint8Array(16))
  const keyMaterial = await crypto.subtle.importKey('raw', enc.encode(password), 'PBKDF2', false, ['deriveBits'])
  const bits = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', hash: 'SHA-256', salt, iterations: 100000 },
    keyMaterial, 256
  )
  const hashArr = new Uint8Array(bits)
  const saltHex = Array.from(salt).map(b => b.toString(16).padStart(2,'0')).join('')
  const hashHex = Array.from(hashArr).map(b => b.toString(16).padStart(2,'0')).join('')
  return `pbkdf2:${saltHex}:${hashHex}`
}

async function verifyPassword(password: string, stored: string): Promise<boolean> {
  try {
    const [, saltHex, hashHex] = stored.split(':')
    const enc = new TextEncoder()
    const salt = new Uint8Array(saltHex.match(/.{2}/g)!.map(h => parseInt(h, 16)))
    const keyMaterial = await crypto.subtle.importKey('raw', enc.encode(password), 'PBKDF2', false, ['deriveBits'])
    const bits = await crypto.subtle.deriveBits(
      { name: 'PBKDF2', hash: 'SHA-256', salt, iterations: 100000 },
      keyMaterial, 256
    )
    const hashArr = new Uint8Array(bits)
    const newHex = Array.from(hashArr).map(b => b.toString(16).padStart(2,'0')).join('')
    return newHex === hashHex
  } catch { return false }
}

// Simple signed session token: base64(payload).base64(hmac-sha256)
const TOKEN_SECRET = 'lionmd-portal-secret-2026'

async function signToken(payload: object): Promise<string> {
  const enc = new TextEncoder()
  const key = await crypto.subtle.importKey(
    'raw', enc.encode(TOKEN_SECRET), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  )
  const data = btoa(JSON.stringify(payload))
  const sig = await crypto.subtle.sign('HMAC', key, enc.encode(data))
  const sigHex = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2,'0')).join('')
  return `${data}.${sigHex}`
}

async function verifyToken(token: string): Promise<any | null> {
  try {
    const [data, sigHex] = token.split('.')
    if (!data || !sigHex) return null
    const enc = new TextEncoder()
    const key = await crypto.subtle.importKey(
      'raw', enc.encode(TOKEN_SECRET), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']
    )
    const sigBytes = new Uint8Array(sigHex.match(/.{2}/g)!.map(h => parseInt(h, 16)))
    const valid = await crypto.subtle.verify('HMAC', key, sigBytes, enc.encode(data))
    if (!valid) return null
    const payload = JSON.parse(atob(data))
    if (payload.exp && Date.now() > payload.exp) return null
    return payload
  } catch { return null }
}

function generateInviteToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(24))
  return Array.from(bytes).map(b => b.toString(16).padStart(2,'0')).join('')
}

// ── Auth middleware ──────────────────────────────────────────────
async function requireAuth(c: any, next: any) {
  const auth = c.req.header('Authorization') || ''
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : ''
  if (!token) return c.json({ error: 'Unauthorized' }, 401)
  const payload = await verifyToken(token)
  if (!payload) return c.json({ error: 'Invalid or expired token' }, 401)
  c.set('user', payload)
  return next()
}

async function requireAdmin(c: any, next: any) {
  const auth = c.req.header('Authorization') || ''
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : ''
  if (!token) return c.json({ error: 'Unauthorized' }, 401)
  const payload = await verifyToken(token)
  if (!payload || payload.role !== 'admin') return c.json({ error: 'Admin access required' }, 403)
  c.set('user', payload)
  return next()
}

// ── POST /api/auth/bootstrap ─────────────────────────────────────
// Creates first admin account if no users exist at all
app.post('/api/auth/bootstrap', async (c) => {
  await ensureAuthSchema(c.env.DB)
  const count = await c.env.DB.prepare('SELECT COUNT(*) as c FROM portal_users').first() as any
  if ((count?.c ?? 0) > 0) return c.json({ error: 'Already bootstrapped' }, 400)
  const body = await c.req.json() as any
  const { name, email, password } = body
  if (!name || !email || !password) return c.json({ error: 'name, email, and password required' }, 400)
  if (password.length < 8) return c.json({ error: 'Password must be at least 8 characters' }, 400)
  const hash = await hashPassword(password)
  const r = await c.env.DB.prepare(
    `INSERT INTO portal_users (name, email, password_hash, role, is_active, must_set_password) VALUES (?,?,?,'admin',1,0)`
  ).bind(name, email.toLowerCase().trim(), hash).run()
  const token = await signToken({ id: r.meta.last_row_id, email: email.toLowerCase().trim(), name, role: 'admin', exp: Date.now() + 86400000 * 30 })
  return c.json({ ok: true, token, user: { id: r.meta.last_row_id, name, email, role: 'admin' } })
})

// ── POST /api/auth/login ─────────────────────────────────────────
app.post('/api/auth/login', async (c) => {
  await ensureAuthSchema(c.env.DB)
  const body = await c.req.json() as any
  const { email, password } = body
  if (!email || !password) return c.json({ error: 'Email and password required' }, 400)

  const user = await c.env.DB.prepare(
    'SELECT * FROM portal_users WHERE LOWER(email)=? AND is_active=1'
  ).bind(email.toLowerCase().trim()).first() as any

  if (!user) return c.json({ error: 'Invalid email or password' }, 401)

  // If must_set_password, they should use setup flow
  if (user.must_set_password || !user.password_hash) {
    return c.json({ must_set_password: true, invite_token: user.invite_token, user_id: user.id })
  }

  const ok = await verifyPassword(password, user.password_hash)
  if (!ok) return c.json({ error: 'Invalid email or password' }, 401)

  await c.env.DB.prepare('UPDATE portal_users SET last_login=CURRENT_TIMESTAMP WHERE id=?').bind(user.id).run()

  const token = await signToken({ id: user.id, email: user.email, name: user.name, role: user.role, exp: Date.now() + 86400000 * 30 })
  return c.json({ ok: true, token, user: { id: user.id, name: user.name, email: user.email, role: user.role } })
})

// ── POST /api/auth/setup-password ───────────────────────────────
// Used when a new user clicks their invite link to set their password
app.post('/api/auth/setup-password', async (c) => {
  await ensureAuthSchema(c.env.DB)
  const body = await c.req.json() as any
  const { invite_token, password, confirm_password } = body
  if (!invite_token || !password) return c.json({ error: 'invite_token and password required' }, 400)
  if (password !== confirm_password) return c.json({ error: 'Passwords do not match' }, 400)
  if (password.length < 8) return c.json({ error: 'Password must be at least 8 characters' }, 400)

  const user = await c.env.DB.prepare(
    `SELECT * FROM portal_users WHERE invite_token=? AND is_active=1`
  ).bind(invite_token).first() as any

  if (!user) return c.json({ error: 'Invalid or expired invite link' }, 400)

  const hash = await hashPassword(password)
  await c.env.DB.prepare(
    `UPDATE portal_users SET password_hash=?, must_set_password=0, invite_token=NULL,
     invite_token_expires=NULL, last_login=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?`
  ).bind(hash, user.id).run()

  const token = await signToken({ id: user.id, email: user.email, name: user.name, role: user.role, exp: Date.now() + 86400000 * 30 })
  return c.json({ ok: true, token, user: { id: user.id, name: user.name, email: user.email, role: user.role } })
})

// ── GET /api/auth/me ─────────────────────────────────────────────
app.get('/api/auth/me', requireAuth, async (c) => {
  const u = c.get('user')
  const user = await c.env.DB.prepare('SELECT id, name, email, role, is_active FROM portal_users WHERE id=?').bind(u.id).first()
  if (!user) return c.json({ error: 'User not found' }, 404)
  return c.json(user)
})

// ── POST /api/auth/change-password ──────────────────────────────
app.post('/api/auth/change-password', requireAuth, async (c) => {
  const u = c.get('user')
  const body = await c.req.json() as any
  const { current_password, new_password, confirm_password } = body
  if (!current_password || !new_password) return c.json({ error: 'Fields required' }, 400)
  if (new_password !== confirm_password) return c.json({ error: 'Passwords do not match' }, 400)
  if (new_password.length < 8) return c.json({ error: 'Password must be at least 8 characters' }, 400)

  const user = await c.env.DB.prepare('SELECT * FROM portal_users WHERE id=?').bind(u.id).first() as any
  if (!user) return c.json({ error: 'User not found' }, 404)
  const ok = await verifyPassword(current_password, user.password_hash)
  if (!ok) return c.json({ error: 'Current password is incorrect' }, 401)

  const hash = await hashPassword(new_password)
  await c.env.DB.prepare('UPDATE portal_users SET password_hash=?, updated_at=CURRENT_TIMESTAMP WHERE id=?').bind(hash, u.id).run()
  return c.json({ ok: true })
})

// ── GET /api/auth/invite/:token ──────────────────────────────────
// Lets front-end look up who the invite belongs to before showing setup form
app.get('/api/auth/invite/:token', async (c) => {
  await ensureAuthSchema(c.env.DB)
  const token = c.req.param('token')
  const user = await c.env.DB.prepare(
    'SELECT id, name, email, role FROM portal_users WHERE invite_token=? AND is_active=1'
  ).bind(token).first() as any
  if (!user) return c.json({ error: 'Invalid or expired invite link' }, 404)
  return c.json({ id: user.id, name: user.name, email: user.email, role: user.role })
})

// ── GET /api/admin/users ─────────────────────────────────────────
app.get('/api/admin/users', requireAdmin, async (c) => {
  await ensureAuthSchema(c.env.DB)
  const rows = await c.env.DB.prepare(
    'SELECT id, name, email, role, is_active, must_set_password, last_login, created_at FROM portal_users ORDER BY created_at DESC'
  ).all()
  return c.json(rows.results)
})

// ── POST /api/admin/users ────────────────────────────────────────
// Admin creates a new user → generates invite token, no password yet
app.post('/api/admin/users', requireAdmin, async (c) => {
  await ensureAuthSchema(c.env.DB)
  const body = await c.req.json() as any
  const { name, email, role } = body
  if (!name || !email || !role) return c.json({ error: 'name, email, and role required' }, 400)
  const validRoles = ['admin', 'carevalidate', 'onboarding']
  if (!validRoles.includes(role)) return c.json({ error: 'Invalid role' }, 400)

  // Check duplicate
  const existing = await c.env.DB.prepare('SELECT id FROM portal_users WHERE LOWER(email)=?').bind(email.toLowerCase().trim()).first()
  if (existing) return c.json({ error: 'A user with that email already exists' }, 409)

  const inviteToken = generateInviteToken()
  const creator = c.get('user')
  const r = await c.env.DB.prepare(
    `INSERT INTO portal_users (name, email, role, is_active, must_set_password, invite_token, created_by)
     VALUES (?,?,?,1,1,?,?)`
  ).bind(name, email.toLowerCase().trim(), role, inviteToken, creator?.id ?? null).run()

  return c.json({ ok: true, id: r.meta.last_row_id, invite_token: inviteToken })
})

// ── PUT /api/admin/users/:id ─────────────────────────────────────
app.put('/api/admin/users/:id', requireAdmin, async (c) => {
  const id = c.req.param('id')
  const body = await c.req.json() as any
  const validRoles = ['admin', 'carevalidate', 'onboarding']
  if (body.role && !validRoles.includes(body.role)) return c.json({ error: 'Invalid role' }, 400)

  const sets: string[] = []
  const vals: any[] = []
  if (body.name !== undefined)      { sets.push('name=?');      vals.push(body.name) }
  if (body.email !== undefined)     { sets.push('email=?');     vals.push(body.email.toLowerCase().trim()) }
  if (body.role !== undefined)      { sets.push('role=?');      vals.push(body.role) }
  if (body.is_active !== undefined) { sets.push('is_active=?'); vals.push(body.is_active ? 1 : 0) }
  if (body.password !== undefined && body.password.length >= 8) {
    const hash = await hashPassword(body.password)
    sets.push('password_hash=?'); vals.push(hash)
    sets.push('must_set_password=0')
  }
  if (sets.length === 0) return c.json({ ok: true })
  sets.push('updated_at=CURRENT_TIMESTAMP')
  vals.push(id)
  await c.env.DB.prepare(`UPDATE portal_users SET ${sets.join(',')} WHERE id=?`).bind(...vals).run()
  return c.json({ ok: true })
})

// ── DELETE /api/admin/users/:id ──────────────────────────────────
app.delete('/api/admin/users/:id', requireAdmin, async (c) => {
  const requestor = c.get('user')
  const id = parseInt(c.req.param('id'))
  if (requestor?.id === id) return c.json({ error: 'Cannot delete your own account' }, 400)
  await c.env.DB.prepare('DELETE FROM portal_users WHERE id=?').bind(id).run()
  return c.json({ ok: true })
})

// ── POST /api/admin/users/:id/reset-invite ───────────────────────
// Re-generate an invite token so admin can resend the setup link
app.post('/api/admin/users/:id/reset-invite', requireAdmin, async (c) => {
  const inviteToken = generateInviteToken()
  await c.env.DB.prepare(
    `UPDATE portal_users SET invite_token=?, must_set_password=1, password_hash=NULL, updated_at=CURRENT_TIMESTAMP WHERE id=?`
  ).bind(inviteToken, c.req.param('id')).run()
  return c.json({ ok: true, invite_token: inviteToken })
})

// ── GET /api/auth/check-bootstrap ───────────────────────────────
// Returns whether any users exist yet (for first-run detection)
app.get('/api/auth/check-bootstrap', async (c) => {
  await ensureAuthSchema(c.env.DB)
  const count = await c.env.DB.prepare('SELECT COUNT(*) as c FROM portal_users').first() as any
  return c.json({ needs_bootstrap: (count?.c ?? 0) === 0 })
})

// ── Convert hired candidate → active contractor ──────────────────

app.post('/api/onboarding/candidates/:id/convert', async (c) => {
  await ensureOnboardingSchema(c.env.DB)
  const id = c.req.param('id')
  const candidate = await c.env.DB.prepare('SELECT * FROM onboarding_candidates WHERE id=?').bind(id).first() as any
  if (!candidate) return c.json({ error: 'Candidate not found' }, 404)
  if (candidate.converted_contractor_id) {
    return c.json({ ok: true, contractor_id: candidate.converted_contractor_id, already_converted: true })
  }

  // Create contractor record
  const r = await c.env.DB.prepare(`
    INSERT INTO contractors (name, company, ein_ssn, email, contractor_type, is_active)
    VALUES (?,?,?,?,?,1)
  `).bind(
    candidate.full_name,
    candidate.company_name || '',
    candidate.ein_ssn || '',
    candidate.email || '',
    candidate.contractor_type || 'regular'
  ).run()

  const contractorId = r.meta.last_row_id

  // Mark candidate as converted
  await c.env.DB.prepare(
    `UPDATE onboarding_candidates SET converted_contractor_id=?, converted_at=CURRENT_TIMESTAMP, status='hired', updated_at=CURRENT_TIMESTAMP WHERE id=?`
  ).bind(contractorId, id).run()

  return c.json({ ok: true, contractor_id: contractorId })
})

export default app
