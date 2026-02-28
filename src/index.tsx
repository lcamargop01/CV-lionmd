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

  // OrderlyMeds has a special numeric fee of 17 in the source
  if (rawFee === 17 || (typeof rawFee === 'number' && rawFee > 0 && rawFee < 20)) {
    const orderly = ratesMap['ORDERLY']
    return orderly ? { cv: orderly.cv, ct: orderly.ct } : { cv: rawFee, ct: rawFee }
  }

  const rate = ratesMap[vt]
  if (rate) return { cv: rate.cv, ct: rate.ct }
  return { cv: 0, ct: 0 }
}

const app = new Hono<{ Bindings: Bindings }>()

// CORS
app.use('/api/*', cors())

// ──────────────────────────────────────────────
// GET /api/health
// ──────────────────────────────────────────────
app.get('/api/health', (c) => c.json({ ok: true, ts: Date.now() }))

// ──────────────────────────────────────────────
// PAYMENT RATES
// ──────────────────────────────────────────────
app.get('/api/rates', async (c) => {
  const rates = await c.env.DB.prepare('SELECT * FROM payment_rates ORDER BY visit_type').all()
  return c.json(rates.results)
})

app.put('/api/rates/:id', async (c) => {
  const id = c.req.param('id')
  const body = await c.req.json()
  const { carevalidate_rate, contractor_rate, label } = body
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
  const body = await c.req.json()
  const { name, company, ein_ssn, email } = body
  const result = await c.env.DB.prepare(
    'INSERT INTO contractors (name, company, ein_ssn, email) VALUES (?, ?, ?, ?)'
  ).bind(name, company || '', ein_ssn || '', email || '').run()
  return c.json({ id: result.meta.last_row_id, name, company, ein_ssn, email })
})

app.put('/api/contractors/:id', async (c) => {
  const id = c.req.param('id')
  const body = await c.req.json()
  const { name, company, ein_ssn, email, is_active } = body
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
// UPLOAD SESSIONS
// ──────────────────────────────────────────────
app.get('/api/sessions', async (c) => {
  const rows = await c.env.DB.prepare(
    'SELECT * FROM upload_sessions ORDER BY period_year DESC, period_month DESC'
  ).all()
  return c.json(rows.results)
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
// UPLOAD EXCEL (POST /api/upload)
// ──────────────────────────────────────────────
app.post('/api/upload', async (c) => {
  const body = await c.req.json()
  const { filename, period_label, period_month, period_year, rows } = body

  if (!rows || !Array.isArray(rows)) return c.json({ error: 'Invalid payload' }, 400)

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

  // Check if session already exists for this period
  const existing = await c.env.DB.prepare(
    'SELECT id FROM upload_sessions WHERE period_month=? AND period_year=?'
  ).bind(period_month, period_year).first()

  let sessionId: number
  if (existing) {
    sessionId = (existing as any).id
    await c.env.DB.prepare('DELETE FROM consults WHERE session_id=?').bind(sessionId).run()
    await c.env.DB.prepare(
      'UPDATE upload_sessions SET filename=?, period_label=?, total_cases=0, total_carevalidate_amount=0, total_contractor_amount=0, uploaded_at=CURRENT_TIMESTAMP WHERE id=?'
    ).bind(filename, period_label, sessionId).run()
  } else {
    const sessionResult = await c.env.DB.prepare(
      'INSERT INTO upload_sessions (filename, period_label, period_month, period_year) VALUES (?, ?, ?, ?)'
    ).bind(filename, period_label, period_month, period_year).run()
    sessionId = sessionResult.meta.last_row_id as number
  }

  // Process rows in batches of 100
  let totalCV = 0
  let totalCT = 0
  const batchSize = 100

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize)
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
        sessionId,
        row.case_id || '',
        row.case_id_short || '',
        row.organization_name || '',
        row.patient_name || '',
        row.doctor_name || '',
        row.decision_date || '',
        row.decision_status || '',
        row.visit_type || '',
        fees.cv,
        fees.ct,
        contractorId,
        row.is_flagged ? 1 : 0
      )
    })
    await c.env.DB.batch(statements)
  }

  // Update session totals
  await c.env.DB.prepare(
    'UPDATE upload_sessions SET total_cases=?, total_carevalidate_amount=?, total_contractor_amount=? WHERE id=?'
  ).bind(rows.length, totalCV, totalCT, sessionId).run()

  return c.json({
    session_id: sessionId,
    total_cases: rows.length,
    total_carevalidate_amount: totalCV,
    total_contractor_amount: totalCT
  })
})

// ──────────────────────────────────────────────
// CONSULTS (with filtering)
// ──────────────────────────────────────────────
app.get('/api/consults', async (c) => {
  const session_id = c.req.query('session_id')
  const doctor_name = c.req.query('doctor_name')
  const visit_type = c.req.query('visit_type')
  const page = c.req.query('page') || '1'
  const limit = c.req.query('limit') || '50'
  const search = c.req.query('search')
  const offset = (parseInt(page) - 1) * parseInt(limit)

  let where = 'WHERE 1=1'
  const params: any[] = []

  if (session_id) { where += ' AND c.session_id=?'; params.push(session_id) }
  if (doctor_name) { where += ' AND c.doctor_name=?'; params.push(doctor_name) }
  if (visit_type) { where += ' AND c.visit_type=?'; params.push(visit_type) }
  if (search) {
    where += ' AND (c.patient_name LIKE ? OR c.case_id_short LIKE ? OR c.organization_name LIKE ?)'
    params.push(`%${search}%`, `%${search}%`, `%${search}%`)
  }

  const countResult = await c.env.DB.prepare(
    `SELECT COUNT(*) as total FROM consults c ${where}`
  ).bind(...params).first() as any

  const rows = await c.env.DB.prepare(
    `SELECT c.*, ct.ein_ssn, ct.company 
     FROM consults c 
     LEFT JOIN contractors ct ON c.contractor_id = ct.id 
     ${where} 
     ORDER BY c.decision_date DESC, c.id DESC 
     LIMIT ? OFFSET ?`
  ).bind(...params, parseInt(limit), offset).all()

  return c.json({
    total: countResult?.total || 0,
    page: parseInt(page),
    limit: parseInt(limit),
    data: rows.results
  })
})

app.put('/api/consults/:id', async (c) => {
  const id = c.req.param('id')
  const body = await c.req.json()
  const { contractor_fee, carevalidate_fee, notes, is_override, override_fee } = body
  await c.env.DB.prepare(
    'UPDATE consults SET contractor_fee=?, carevalidate_fee=?, notes=?, is_override=?, override_fee=? WHERE id=?'
  ).bind(contractor_fee, carevalidate_fee, notes || null, is_override ? 1 : 0, override_fee || null, id).run()
  return c.json({ ok: true })
})

// ──────────────────────────────────────────────
// SUMMARY / ANALYTICS
// ──────────────────────────────────────────────
app.get('/api/summary/:session_id', async (c) => {
  const sid = c.req.param('session_id')

  const byDoctor = await c.env.DB.prepare(`
    SELECT 
      c.doctor_name,
      ct.id as contractor_id,
      ct.company,
      ct.ein_ssn,
      COUNT(*) as case_count,
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 AND c.visit_type='ASYNC_TEXT_EMAIL' THEN 1 ELSE 0 END) as orderly_count,
      SUM(c.carevalidate_fee) as total_carevalidate,
      SUM(c.contractor_fee) as total_contractor,
      SUM(c.carevalidate_fee) - SUM(c.contractor_fee) as margin
    FROM consults c
    LEFT JOIN contractors ct ON c.contractor_id = ct.id
    WHERE c.session_id=?
    GROUP BY c.doctor_name
    ORDER BY c.doctor_name
  `).bind(sid).all()

  const byVisitType = await c.env.DB.prepare(`
    SELECT 
      visit_type,
      COUNT(*) as count,
      SUM(carevalidate_fee) as total_cv,
      SUM(contractor_fee) as total_ct
    FROM consults
    WHERE session_id=?
    GROUP BY visit_type
    ORDER BY count DESC
  `).bind(sid).all()

  const byOrg = await c.env.DB.prepare(`
    SELECT 
      organization_name,
      COUNT(*) as count,
      SUM(carevalidate_fee) as total_cv
    FROM consults
    WHERE session_id=?
    GROUP BY organization_name
    ORDER BY count DESC
    LIMIT 20
  `).bind(sid).all()

  const totals = await c.env.DB.prepare(`
    SELECT 
      COUNT(*) as total_cases,
      SUM(carevalidate_fee) as total_carevalidate,
      SUM(contractor_fee) as total_contractor,
      SUM(carevalidate_fee) - SUM(contractor_fee) as total_margin
    FROM consults WHERE session_id=?
  `).bind(sid).first()

  const flagged = await c.env.DB.prepare(
    'SELECT COUNT(*) as count FROM consults WHERE session_id=? AND is_flagged=1'
  ).bind(sid).first()

  return c.json({
    totals,
    byDoctor: byDoctor.results,
    byVisitType: byVisitType.results,
    byOrg: byOrg.results,
    flaggedCount: (flagged as any)?.count || 0
  })
})

// ──────────────────────────────────────────────
// CONTRACTOR PAYSTUB DATA
// ──────────────────────────────────────────────
app.get('/api/paystub/:session_id/:contractor_id', async (c) => {
  const sid = c.req.param('session_id')
  const cid = c.req.param('contractor_id')

  const contractor = await c.env.DB.prepare('SELECT * FROM contractors WHERE id=?').bind(cid).first()
  const session = await c.env.DB.prepare('SELECT * FROM upload_sessions WHERE id=?').bind(sid).first()

  const consults = await c.env.DB.prepare(`
    SELECT * FROM consults WHERE session_id=? AND contractor_id=?
    ORDER BY decision_date ASC
  `).bind(sid, cid).all()

  const summary = await c.env.DB.prepare(`
    SELECT 
      COUNT(*) as total_cases,
      SUM(contractor_fee) as total_pay,
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
// GUSTO EXPORT DATA
// ──────────────────────────────────────────────
app.get('/api/export/gusto/:session_id', async (c) => {
  const sid = c.req.param('session_id')

  const rows = await c.env.DB.prepare(`
    SELECT 
      ct.name as contractor_name,
      ct.company,
      ct.ein_ssn,
      ct.email,
      s.period_label,
      COUNT(*) as total_cases,
      SUM(c.contractor_fee) as total_pay,
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' AND c.carevalidate_fee >= 20 THEN 1 ELSE 0 END) as async_count,
      SUM(CASE WHEN c.visit_type='ASYNC_TEXT_EMAIL' AND c.carevalidate_fee >= 20 THEN c.contractor_fee ELSE 0 END) as async_pay,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN 1 ELSE 0 END) as sync_count,
      SUM(CASE WHEN c.visit_type IN ('SYNC_PHONE','SYNC_VIDEO','SYNC_IN_PERSON') THEN c.contractor_fee ELSE 0 END) as sync_pay,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 THEN 1 ELSE 0 END) as orderly_count,
      SUM(CASE WHEN c.carevalidate_fee < 20 AND c.carevalidate_fee > 0 THEN c.contractor_fee ELSE 0 END) as orderly_pay
    FROM consults c
    LEFT JOIN contractors ct ON c.contractor_id = ct.id
    LEFT JOIN upload_sessions s ON c.session_id = s.id
    WHERE c.session_id=?
    GROUP BY c.contractor_id
    ORDER BY ct.name
  `).bind(sid).all()

  return c.json(rows.results)
})

// ──────────────────────────────────────────────
// CAREVALIDATE SUMMARY VIEW
// ──────────────────────────────────────────────
app.get('/api/cv-summary/:session_id', async (c) => {
  const sid = c.req.param('session_id')
  const session = await c.env.DB.prepare('SELECT * FROM upload_sessions WHERE id=?').bind(sid).first()

  const byVisitType = await c.env.DB.prepare(`
    SELECT 
      visit_type,
      COUNT(*) as count,
      SUM(carevalidate_fee) as total_amount
    FROM consults
    WHERE session_id=?
    GROUP BY visit_type
  `).bind(sid).all()

  const total = await c.env.DB.prepare(`
    SELECT COUNT(*) as total_cases, SUM(carevalidate_fee) as total_owed
    FROM consults WHERE session_id=?
  `).bind(sid).first()

  return c.json({ session, byVisitType: byVisitType.results, total })
})

export default app
