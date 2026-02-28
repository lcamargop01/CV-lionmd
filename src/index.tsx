import { Hono } from 'hono'
import { cors } from 'hono/cors'

type Bindings = {
  DB: D1Database
}

// ──────────────────────────────────────────────
// Helper: compute fee from visit type & rates map
// ──────────────────────────────────────────────
function computeFee(visitType: string | null, ratesMap: Record<string, { cv: number; ct: number }>, rawFee?: number | null): { cv: number; ct: number } {
  if (!visitType) return { cv: 0, ct: 0 }
  const vt = visitType.toUpperCase()
  if (rawFee === 17 || (typeof rawFee === 'number' && rawFee > 0 && rawFee < 20)) {
    const orderly = ratesMap['ORDERLY']
    return orderly ? { cv: orderly.cv, ct: orderly.ct } : { cv: rawFee, ct: rawFee }
  }
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
  ]).catch(() => {}) // ignore if columns already exist
  // Backfill any rows missing period_key
  await db.prepare(
    `UPDATE upload_sessions SET period_key = printf('%04d-%02d', period_year, period_month) WHERE period_key IS NULL OR period_key = ''`
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
// CONTRACTORS
// ──────────────────────────────────────────────
app.get('/api/contractors', async (c) => {
  const rows = await c.env.DB.prepare('SELECT * FROM contractors WHERE is_active=1 ORDER BY name').all()
  return c.json(rows.results)
})

app.post('/api/contractors', async (c) => {
  const { name, company, ein_ssn, email } = await c.req.json()
  const result = await c.env.DB.prepare(
    'INSERT INTO contractors (name, company, ein_ssn, email) VALUES (?, ?, ?, ?)'
  ).bind(name, company || '', ein_ssn || '', email || '').run()
  return c.json({ id: result.meta.last_row_id, name, company, ein_ssn, email })
})

app.put('/api/contractors/:id', async (c) => {
  const id = c.req.param('id')
  const { name, company, ein_ssn, email, is_active } = await c.req.json()
  await c.env.DB.prepare(
    'UPDATE contractors SET name=?, company=?, ein_ssn=?, email=?, is_active=? WHERE id=?'
  ).bind(name, company || '', ein_ssn || '', email || '', is_active ?? 1, id).run()
  return c.json({ ok: true })
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
// UPLOAD EXCEL — always creates a NEW session (append mode)
// Deduplicates case_ids across all files in the same period
// ──────────────────────────────────────────────
app.post('/api/upload', async (c) => {
  await ensureSchema(c.env.DB)
  const body = await c.req.json()
  const { filename, period_label, period_month, period_year, rows, source_label } = body

  if (!rows || !Array.isArray(rows)) return c.json({ error: 'Invalid payload' }, 400)

  const pk = periodKey(period_month, period_year)

  // Load rates
  const ratesResult = await c.env.DB.prepare('SELECT * FROM payment_rates').all()
  const ratesMap: Record<string, { cv: number; ct: number }> = {}
  for (const r of ratesResult.results as any[]) {
    ratesMap[r.visit_type] = { cv: r.carevalidate_rate, ct: r.contractor_rate }
  }

  // Load contractors name→id map
  const contractorsResult = await c.env.DB.prepare('SELECT id, name FROM contractors WHERE is_active=1').all()
  const contractorMap: Record<string, number> = {}
  for (const ct of contractorsResult.results as any[]) {
    contractorMap[ct.name.toLowerCase().trim()] = ct.id
  }

  // Collect all case_ids already stored for this period (for deduplication)
  const existingSessionIds = await c.env.DB.prepare(
    'SELECT id FROM upload_sessions WHERE period_key=?'
  ).bind(pk).all()

  const existingCaseIds = new Set<string>()
  if (existingSessionIds.results.length > 0) {
    for (const es of existingSessionIds.results as any[]) {
      const caseRows = await c.env.DB.prepare(
        'SELECT case_id FROM consults WHERE session_id=? AND case_id != ""'
      ).bind(es.id).all()
      for (const cr of caseRows.results as any[]) {
        existingCaseIds.add(cr.case_id)
      }
    }
  }

  // Create a new session for this file
  const sessionResult = await c.env.DB.prepare(
    `INSERT INTO upload_sessions (filename, period_label, period_month, period_year, period_key, source_label)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).bind(filename, period_label, period_month, period_year, pk, source_label || filename).run()
  const sessionId = sessionResult.meta.last_row_id as number

  // Filter out duplicates
  const newRows = rows.filter((row: any) => {
    if (!row.case_id) return true // keep rows without case_id
    return !existingCaseIds.has(row.case_id)
  })
  const skippedCount = rows.length - newRows.length

  // Process new rows in batches of 100
  let totalCV = 0
  let totalCT = 0
  const batchSize = 100

  for (let i = 0; i < newRows.length; i += batchSize) {
    const batch = newRows.slice(i, i + batchSize)
    const stmt = c.env.DB.prepare(
      `INSERT INTO consults (session_id, case_id, case_id_short, organization_name, patient_name,
        doctor_name, decision_date, decision_status, visit_type, carevalidate_fee, contractor_fee,
        contractor_id, is_flagged)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    const statements = batch.map((row: any) => {
      const fees = computeFee(row.visit_type, ratesMap, row.raw_fee)
      totalCV += fees.cv
      totalCT += fees.ct
      const contractorId = contractorMap[(row.doctor_name || '').toLowerCase().trim()] || null
      return stmt.bind(
        sessionId, row.case_id || '', row.case_id_short || '',
        row.organization_name || '', row.patient_name || '',
        row.doctor_name || '', row.decision_date || '',
        row.decision_status || '', row.visit_type || '',
        fees.cv, fees.ct, contractorId, row.is_flagged ? 1 : 0
      )
    })
    await c.env.DB.batch(statements)
  }

  // Update this session's totals
  await c.env.DB.prepare(
    'UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?'
  ).bind(newRows.length, totalCV, totalCT, sessionId).run()

  return c.json({
    session_id: sessionId,
    period_key: pk,
    total_rows_in_file: rows.length,
    new_cases_added: newRows.length,
    skipped_duplicates: skippedCount,
    total_carevalidate_amount: totalCV,
    total_contractor_amount: totalCT
  })
})

// ──────────────────────────────────────────────
// CONSULTS — accepts period_key OR session_id
// ──────────────────────────────────────────────
app.get('/api/consults', async (c) => {
  const period_key = c.req.query('period_key')
  const session_id = c.req.query('session_id')
  const doctor_name = c.req.query('doctor_name')
  const visit_type = c.req.query('visit_type')
  const page = c.req.query('page') || '1'
  const limit = c.req.query('limit') || '50'
  const search = c.req.query('search')
  const offset = (parseInt(page) - 1) * parseInt(limit)

  let where = 'WHERE 1=1'
  const params: any[] = []

  if (period_key) {
    where += ' AND s.period_key=?'; params.push(period_key)
  } else if (session_id) {
    where += ' AND c.session_id=?'; params.push(session_id)
  }
  if (doctor_name) { where += ' AND c.doctor_name=?'; params.push(doctor_name) }
  if (visit_type) { where += ' AND c.visit_type=?'; params.push(visit_type) }
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
     ORDER BY c.decision_date DESC, c.id DESC
     LIMIT ? OFFSET ?`
  ).bind(...params, parseInt(limit), offset).all()

  return c.json({ total: countResult?.total || 0, page: parseInt(page), limit: parseInt(limit), data: rows.results })
})

app.put('/api/consults/:id', async (c) => {
  const id = c.req.param('id')
  const { contractor_fee, carevalidate_fee, notes, is_override, override_fee } = await c.req.json()
  await c.env.DB.prepare(
    'UPDATE consults SET contractor_fee=?, carevalidate_fee=?, notes=?, is_override=?, override_fee=? WHERE id=?'
  ).bind(contractor_fee, carevalidate_fee, notes || null, is_override ? 1 : 0, override_fee || null, id).run()
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
        SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
        SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
        SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as orderly_count,
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

  const contractor = await c.env.DB.prepare('SELECT * FROM contractors WHERE id=?').bind(cid).first()

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
      SUM(CASE WHEN visit_type='ASYNC_TEXT_EMAIL' AND carevalidate_fee >= 20 THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN visit_type='ASYNC_TEXT_EMAIL' AND carevalidate_fee >= 20 THEN contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN carevalidate_fee < 20 AND carevalidate_fee > 0 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN carevalidate_fee < 20 AND carevalidate_fee > 0 THEN contractor_fee ELSE 0 END) as orderly_pay
    FROM consults c
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE s.period_key=? AND c.contractor_id=?
  `).bind(pk, cid).first()

  return c.json({
    contractor,
    session: { period_label: periodLabel, period_key: pk, files: sessions.results },
    consults: consults.results,
    summary
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
      SUM(CASE WHEN visit_type='ASYNC_TEXT_EMAIL' AND carevalidate_fee >= 20 THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN visit_type='ASYNC_TEXT_EMAIL' AND carevalidate_fee >= 20 THEN contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN carevalidate_fee < 20 AND carevalidate_fee > 0 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN carevalidate_fee < 20 AND carevalidate_fee > 0 THEN contractor_fee ELSE 0 END) as orderly_pay
    FROM consults WHERE session_id=? AND contractor_id=?
  `).bind(sid, cid).first()
  return c.json({ contractor, session, consults: consults.results, summary })
})

// ──────────────────────────────────────────────
// GUSTO EXPORT — accepts period_key
// ──────────────────────────────────────────────
app.get('/api/export/gusto/period/:period_key', async (c) => {
  const pk = c.req.param('period_key')
  const rows = await c.env.DB.prepare(`
    SELECT ct.name as contractor_name, ct.company, ct.ein_ssn, ct.email,
      s.period_label,
      COUNT(*) as total_cases, SUM(c.contractor_fee) as total_pay,
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' AND c.carevalidate_fee >= 20 THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' AND c.carevalidate_fee >= 20 THEN c.contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN c.contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 THEN c.contractor_fee ELSE 0 END) as orderly_pay
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
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' AND c.carevalidate_fee >= 20 THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' AND c.carevalidate_fee >= 20 THEN c.contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN c.contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 THEN c.contractor_fee ELSE 0 END) as orderly_pay
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

  return c.json({
    session: { period_label: periodLabel, period_key: pk, files: sessions.results },
    byVisitType: byVisitType.results,
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

export default app
