// api/server.js — InstaReach v3 Enhanced
// Implements comprehensive rate limiting, session management, monitoring, and safety features
require('dotenv').config();
const express  = require('express');
const cors     = require('cors');
const bcrypt   = require('bcryptjs');
const jwt      = require('jsonwebtoken');
const path     = require('path');
const fs       = require('fs');
const { v4: uuidv4 } = require('uuid');
const { initDb }     = require('../lib/db');
const engine         = require('../lib/dmEngine');

const app        = express();
const PORT       = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || process.env.ADMIN_PASSWORD || 'instraeach_v3_secret';

app.use(cors());
app.use(express.json({ limit: '20mb' }));

// ── Safety & Compliance Configuration ─────────────────────────────
const SAFETY_CONFIG = {
  MAX_MESSAGES_PER_HOUR: 20,
  MAX_MESSAGES_PER_DAY: 100,
  MAX_NEW_CONTACTS_PER_DAY: 10,
  MIN_DELAY_BETWEEN_MESSAGES_MS: 5000,  // 5 seconds minimum
  MAX_DELAY_BETWEEN_MESSAGES_MS: 12000, // 12 seconds maximum
  SESSION_REFRESH_INTERVAL_MS: 3600000, // 1 hour
  RATE_LIMIT_WINDOW_MS: 3600000, // 1 hour
};

// ── Rate Limiter with safety thresholds ───────────────────────────
class RateLimiter {
  constructor() {
    this.accountStats = new Map(); // accountId -> { hourly: [], daily: [], contacts: Set }
  }

  getStats(accountId) {
    if (!this.accountStats.has(accountId)) {
      this.accountStats.set(accountId, {
        hourly: [],
        daily: [],
        contacts: new Set(),
        lastCheck: Date.now(),
      });
    }
    return this.accountStats.get(accountId);
  }

  recordMessage(accountId, toUsername) {
    const stats = this.getStats(accountId);
    const now = Date.now();
    
    // Clean old records
    stats.hourly = stats.hourly.filter(ts => now - ts < SAFETY_CONFIG.RATE_LIMIT_WINDOW_MS);
    stats.daily = stats.daily.filter(ts => now - ts < 86400000);
    
    stats.hourly.push(now);
    stats.daily.push(now);
    if (!stats.contacts.has(toUsername)) {
      stats.contacts.add(toUsername);
    }
  }

  check(accountId) {
    const stats = this.getStats(accountId);
    const now = Date.now();
    
    // Clean old records
    stats.hourly = stats.hourly.filter(ts => now - ts < SAFETY_CONFIG.RATE_LIMIT_WINDOW_MS);
    stats.daily = stats.daily.filter(ts => now - ts < 86400000);
    
    const hourlyCount = stats.hourly.length;
    const dailyCount = stats.daily.length;
    const newContactsCount = stats.contacts.size;
    
    const violations = [];
    
    if (hourlyCount >= SAFETY_CONFIG.MAX_MESSAGES_PER_HOUR) {
      violations.push(`Hourly limit exceeded (${hourlyCount}/${SAFETY_CONFIG.MAX_MESSAGES_PER_HOUR})`);
    }
    if (dailyCount >= SAFETY_CONFIG.MAX_MESSAGES_PER_DAY) {
      violations.push(`Daily limit exceeded (${dailyCount}/${SAFETY_CONFIG.MAX_MESSAGES_PER_DAY})`);
    }
    if (newContactsCount >= SAFETY_CONFIG.MAX_NEW_CONTACTS_PER_DAY) {
      violations.push(`New contacts limit exceeded (${newContactsCount}/${SAFETY_CONFIG.MAX_NEW_CONTACTS_PER_DAY})`);
    }
    
    return {
      isBlocked: violations.length > 0,
      violations,
      stats: { hourlyCount, dailyCount, newContactsCount },
    };
  }
}

// ── Session Manager with device fingerprinting ────────────────────
class SessionManager {
  constructor() {
    this.sessions = new Map(); // accountId -> sessionData
  }

  createSession(accountId, userData = {}) {
    const session = {
      sessionId: `session_${accountId}_${Date.now()}`,
      deviceId: this.generateDeviceId(),
      userAgent: this.generateUserAgent(),
      language: 'en-US',
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      createdAt: Date.now(),
      lastActivity: Date.now(),
      ipHistory: [],
      ...userData,
    };
    this.sessions.set(accountId, session);
    return session;
  }

  updateActivity(accountId, ipAddress = '') {
    const session = this.sessions.get(accountId);
    if (session) {
      session.lastActivity = Date.now();
      if (ipAddress && !session.ipHistory.includes(ipAddress)) {
        session.ipHistory.push(ipAddress);
      }
    }
  }

  refreshSession(accountId) {
    const session = this.sessions.get(accountId);
    if (session) {
      session.lastActivity = Date.now();
    }
    return session;
  }

  generateDeviceId() {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    let id = '';
    for (let i = 0; i < 32; i++) {
      id += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return id;
  }

  generateUserAgent() {
    const versions = ['200.0.0.0', '201.0.0.0', '202.0.0.0'];
    const android = ['Android 11', 'Android 12', 'Android 13'];
    const devices = ['SM-G991B', 'SM-G973F', 'Pixel 6'];
    return `Instagram ${versions[Math.floor(Math.random()*versions.length)]} ${android[Math.floor(Math.random()*android.length)]}; ${devices[Math.floor(Math.random()*devices.length)]}`;
  }
}

// ── Metrics Tracker for monitoring ────────────────────────────────
class MetricsTracker {
  constructor() {
    this.metrics = {
      totalMessagesAttempted: 0,
      totalMessagesSuccessful: 0,
      totalMessagesFailed: 0,
      accountsActive: 0,
      accountsBlocked: 0,
      sessionRefreshes: 0,
      rateLimitHits: 0,
      errorsByType: {},
      campaignMetrics: {},
    };
    this.startTime = Date.now();
  }

  recordMessage(success = true, accountId = null, errorType = null) {
    this.metrics.totalMessagesAttempted++;
    if (success) {
      this.metrics.totalMessagesSuccessful++;
    } else {
      this.metrics.totalMessagesFailed++;
      if (errorType) {
        this.metrics.errorsByType[errorType] = (this.metrics.errorsByType[errorType] || 0) + 1;
      }
    }
  }

  recordRateLimitHit(accountId) {
    this.metrics.rateLimitHits++;
  }

  recordSessionRefresh(accountId) {
    this.metrics.sessionRefreshes++;
  }

  getHealth() {
    const uptime = (Date.now() - this.startTime) / 1000 / 60; // minutes
    const successRateNum = this.metrics.totalMessagesAttempted > 0
      ? (this.metrics.totalMessagesSuccessful / this.metrics.totalMessagesAttempted * 100)
      : 0;
    const successRate = successRateNum.toFixed(2);

    const health = {
      uptime,
      totalMetrics: this.metrics.totalMessagesAttempted,
      successRate,
      failureRate: (100 - successRateNum).toFixed(2),
      rateLimitHits: this.metrics.rateLimitHits,
      sessionRefreshes: this.metrics.sessionRefreshes,
      statusCode: 'healthy',
      recommendations: [],
    };

    if (parseFloat(health.successRate) < 80) {
      health.statusCode = 'warning';
      health.recommendations.push('Success rate below 80% - review error logs');
    }
    if (this.metrics.rateLimitHits > 10) {
      health.statusCode = 'warning';
      health.recommendations.push('Multiple rate limit hits detected');
    }
    if (this.metrics.totalMessagesAttempted >= 5 && this.metrics.totalMessagesFailed / this.metrics.totalMessagesAttempted > 0.2) {
      health.statusCode = 'critical';
      health.recommendations.push('Failure rate exceeds 20% - PAUSE OPERATIONS');
    }

    return health;
  }

  reset() {
    this.metrics = {
      totalMessagesAttempted: 0,
      totalMessagesSuccessful: 0,
      totalMessagesFailed: 0,
      accountsActive: 0,
      accountsBlocked: 0,
      sessionRefreshes: 0,
      rateLimitHits: 0,
      errorsByType: {},
      campaignMetrics: {},
    };
    this.startTime = Date.now();
  }
}

// ── Delivery Logger for auditing ──────────────────────────────────
class DeliveryLogger {
  constructor() {
    this.logs = [];
    this.logFile = path.join(process.cwd(), 'data', 'delivery.log');
  }

  logDelivery(messageId, campaignId, fromAccount, toUsername, status, metadata = {}) {
    const entry = {
      timestamp: new Date().toISOString(),
      messageId,
      campaignId,
      fromAccount,
      toUsername,
      status,
      metadata,
    };
    this.logs.push(entry);
    this.writeToFile(entry);
    
    if (status === 'FAILED') {
      this.notifyError(entry);
    }
  }

  writeToFile(entry) {
    try {
      const dir = path.dirname(this.logFile);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.appendFileSync(this.logFile, JSON.stringify(entry) + '\n');
    } catch (e) {
      console.warn('[Logger] Write error:', e.message);
    }
  }

  notifyError(entry) {
    console.error('[DELIVERY ERROR]', entry);
  }

  getLogs(filter = {}) {
    return this.logs.filter(log => {
      if (filter.campaignId && log.campaignId !== filter.campaignId) return false;
      if (filter.status && log.status !== filter.status) return false;
      return true;
    });
  }
}

// ── Initialize components ─────────────────────────────────────────
const rateLimiter = new RateLimiter();
const sessionManager = new SessionManager();
const metricsTracker = new MetricsTracker();
const deliveryLogger = new DeliveryLogger();

// ── Serve frontend static assets ─────────────────────────────────
const publicDir = path.join(__dirname, '..', 'public');
if (fs.existsSync(publicDir)) {
  app.use(express.static(publicDir));
}

// ── Auth ──────────────────────────────────────────────────────────
function auth(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token' });
  try { 
    req.user = jwt.verify(token, JWT_SECRET); 
    next(); 
  }
  catch { res.status(401).json({ error: 'Invalid token' }); }
}

// ── Boot ──────────────────────────────────────────────────────────
initDb().then(db => {
  engine.setDb(db);

  // Seed admin
  const existing = db.prepare('SELECT id FROM admins WHERE username=?').get(process.env.ADMIN_USERNAME || 'admin');
  if (!existing) {
    const hash = require('bcryptjs').hashSync(process.env.ADMIN_PASSWORD || 'admin123', 10);
    db.prepare('INSERT INTO admins (username,password) VALUES (?,?)').run(process.env.ADMIN_USERNAME || 'admin', hash);
    console.log('[InstaReach v3] Admin seeded');
  }

  // ── Auth routes ───────────────────────────────────────────────
  app.post('/api/login', async (req, res) => {
    const { username, password } = req.body;
    const admin = db.prepare('SELECT * FROM admins WHERE username=?').get(username);
    if (!admin || !bcrypt.compareSync(password, admin.password))
      return res.status(401).json({ error: 'Invalid credentials' });
    const token = jwt.sign({ id: admin.id, username: admin.username }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, username: admin.username });
  });

  // ── Health & Monitoring ───────────────────────────────────────
  app.get('/api/health', (_req, res) => {
    const health = metricsTracker.getHealth();
    res.json({ 
      status: health.statusCode,
      health,
      safetyConfig: SAFETY_CONFIG,
    });
  });

  app.get('/api/metrics', auth, (_req, res) => {
    const health = metricsTracker.getHealth();
    res.json(health);
  });

  app.get('/api/delivery-logs', auth, (req, res) => {
    const { campaignId, status, limit = 100 } = req.query;
    const filter = {};
    if (campaignId) filter.campaignId = campaignId;
    if (status) filter.status = status;
    const logs = deliveryLogger.getLogs(filter).slice(-parseInt(limit));
    res.json(logs);
  });

  // ── Stats ─────────────────────────────────────────────────────
  app.get('/api/stats', auth, (_req, res) => {
    const totalAccounts   = db.prepare('SELECT COUNT(*) AS c FROM accounts').get().c;
    const totalSent       = db.prepare('SELECT COUNT(*) AS c FROM dm_sent').get().c;
    const totalInbox      = db.prepare('SELECT COUNT(*) AS c FROM inbox').get().c;
    const unread          = db.prepare('SELECT COUNT(*) AS c FROM inbox WHERE is_read=0').get().c;
    const activeCampaigns = db.prepare("SELECT COUNT(*) AS c FROM campaigns WHERE status='running'").get().c;
    
    // Safety status
    const safetyStatus = {
      blockedAccounts: 0,
      rateLimitViolations: 0,
      warnings: [],
    };

    const accounts = db.prepare('SELECT id FROM accounts').all();
    for (const acc of accounts) {
      const check = rateLimiter.check(acc.id);
      if (check.isBlocked) {
        safetyStatus.blockedAccounts++;
        safetyStatus.rateLimitViolations += check.violations.length;
      }
    }

    res.json({ 
      totalAccounts, 
      totalSent, 
      totalInbox, 
      unread, 
      activeCampaigns,
      safetyStatus,
      metricsTracker: metricsTracker.metrics,
    });
  });

  // ── Accounts ──────────────────────────────────────────────────
  app.get('/api/accounts', auth, (_req, res) => {
    const rows = db.prepare('SELECT * FROM accounts ORDER BY created_at DESC').all();
    rows.forEach(r => {
      const check = rateLimiter.check(r.id);
      r.rateLimitStatus = check;
      r.sessionStatus = sessionManager.sessions.has(r.id) ? 'active' : 'inactive';
    });
    res.json(rows);
  });

  app.post('/api/accounts', auth, (req, res) => {
    const { username, session_id = '', password = '', daily_limit = 50, notes = '' } = req.body;
    if (!username) return res.status(400).json({ error: 'username required' });
    const clean = username.replace('@', '').trim();
    const existing = db.prepare('SELECT id FROM accounts WHERE username=?').get(clean);
    if (existing) return res.status(400).json({ error: 'Account already exists' });
    const total = db.prepare('SELECT COUNT(*) AS c FROM accounts').get().c;
    if (total >= 100) return res.status(400).json({ error: 'Max 100 accounts reached' });
    const id = uuidv4();
    db.prepare('INSERT INTO accounts (id,username,session_id,password,daily_limit,notes) VALUES (?,?,?,?,?,?)').run(id, clean, session_id, password || session_id, daily_limit, notes);
    
    // Create session
    sessionManager.createSession(id, { username: clean });
    
    res.json({ id, username: clean, sessionStatus: 'active' });
  });

  app.post('/api/accounts/bulk', auth, (req, res) => {
    const { accounts } = req.body;
    if (!Array.isArray(accounts)) return res.status(400).json({ error: 'accounts[] required' });
    let added = 0, skipped = 0;
    for (const acc of accounts) {
      try {
        const total = db.prepare('SELECT COUNT(*) AS c FROM accounts').get().c;
        if (total >= 100) break;
        const clean = (acc.username || '').replace('@', '').trim();
        if (!clean) { skipped++; continue; }
        const exists = db.prepare('SELECT id FROM accounts WHERE username=?').get(clean);
        if (exists) { skipped++; continue; }
        const id = uuidv4();
        db.prepare('INSERT INTO accounts (id,username,session_id,password,daily_limit,notes) VALUES (?,?,?,?,?,?)')
          .run(id, clean, acc.session_id || '', acc.password || acc.session_id || '', acc.daily_limit || 50, acc.notes || '');
        sessionManager.createSession(id, { username: clean });
        added++;
      } catch { skipped++; }
    }
    res.json({ added, skipped });
  });

  // ── Download blank template CSV — MUST be before /:id routes ────
  app.get('/api/accounts/template.csv', (_req, res) => {
    const csv = [
      'username,password,daily_limit,notes',
      'account1,my_password,50,Main account',
      'account2,my_password,50,Backup account',
    ].join('\n') + '\n';
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="instareach_template.csv"');
    res.send(csv);
  });

  app.put('/api/accounts/:id', auth, (req, res) => {
    const { session_id, daily_limit, notes } = req.body;
    const fields = [], vals = [];
    if (session_id  !== undefined) { fields.push('session_id=?');  vals.push(session_id); }
    if (daily_limit !== undefined) { fields.push('daily_limit=?'); vals.push(daily_limit); }
    if (notes       !== undefined) { fields.push('notes=?');       vals.push(notes); }
    if (!fields.length) return res.status(400).json({ error: 'Nothing to update' });
    vals.push(req.params.id);
    db.prepare(`UPDATE accounts SET ${fields.join(',')} WHERE id=?`).run(...vals);
    
    // Refresh session
    sessionManager.refreshSession(req.params.id);
    
    res.json({ ok: true });
  });

  app.delete('/api/accounts/:id', auth, (req, res) => {
    db.prepare('DELETE FROM accounts WHERE id=?').run(req.params.id);
    res.json({ ok: true });
  });

  app.delete('/api/accounts', auth, (_req, res) => {
    db.prepare('DELETE FROM accounts').run();
    res.json({ ok: true });
  });

  app.post('/api/accounts/reset-daily', auth, (_req, res) => {
    db.prepare('UPDATE accounts SET dms_today=0').run();
    rateLimiter.accountStats.clear();
    res.json({ ok: true });
  });

  // ── Upload Excel / CSV ────────────────────────────────────────
  app.post('/api/accounts/upload-replace', auth, (req, res) => {
    const { accounts } = req.body;
    if (!Array.isArray(accounts) || !accounts.length)
      return res.status(400).json({ error: 'accounts[] required' });

    db.prepare('DELETE FROM accounts').run();

    let added = 0, skipped = 0;
    const seen = new Set();
    for (const acc of accounts) {
      try {
        const clean = (acc.username || acc.Username || '').replace('@', '').trim().toLowerCase();
        if (!clean || seen.has(clean)) { skipped++; continue; }
        seen.add(clean);
        if (added >= 100) { skipped++; continue; }
        const id = uuidv4();
        const pwd = acc.password || acc.Password || acc.session_id || acc.Session_ID || '';
        const sid = acc.session_id || acc.Session_ID || '';
        db.prepare('INSERT INTO accounts (id,username,session_id,password,daily_limit,notes) VALUES (?,?,?,?,?,?)')
          .run(id, clean, sid, pwd, parseInt(acc.daily_limit || acc.Daily_Limit) || 50, acc.notes || acc.Notes || '');
        sessionManager.createSession(id, { username: clean });
        added++;
      } catch { skipped++; }
    }
    res.json({ ok: true, added, skipped, total: added });
  });

  // ── Campaigns ─────────────────────────────────────────────────
  app.get('/api/campaigns', auth, (_req, res) => {
    const rows = db.prepare('SELECT * FROM campaigns ORDER BY created_at DESC').all();
    rows.forEach(r => { try { r.keywords = JSON.parse(r.keywords); } catch { r.keywords = []; } });
    res.json(rows);
  });

  app.post('/api/campaigns', auth, (req, res) => {
    const {
      name, parent_category, sub_category = '', location = 'India',
      keywords = [], message, max_targets = 500, dms_per_account = 5,
    } = req.body;
    if (!name || !parent_category || !message)
      return res.status(400).json({ error: 'name, parent_category, message required' });
    
    // Enforce safety limits
    const safeDmsPerAccount = Math.min(dms_per_account, 5); // Max 5 DMs per account
    
    const id = uuidv4();
    db.prepare(`INSERT INTO campaigns
      (id,name,parent_category,sub_category,location,keywords,message,max_targets,dms_per_account,status)
      VALUES (?,?,?,?,?,?,?,?,?,?)`)
      .run(id, name, parent_category, sub_category, location, JSON.stringify(keywords), message, max_targets, safeDmsPerAccount, 'pending');
    res.json({ 
      id, 
      name,
      warning: dms_per_account > safeDmsPerAccount ? `DMs per account limited to ${safeDmsPerAccount} for safety` : null,
    });
  });

  app.delete('/api/campaigns/:id', auth, (req, res) => {
    db.prepare('DELETE FROM campaigns WHERE id=?').run(req.params.id);
    res.json({ ok: true });
  });

  // ── Engine ────────────────────────────────────────────────────
  app.get('/api/engine/status', auth, (_req, res) => {
    res.json({ 
      running: engine.isRunning(),
      metrics: metricsTracker.metrics,
    });
  });

  app.post('/api/campaigns/:id/start', auth, (req, res) => {
    if (engine.isRunning()) return res.status(409).json({ error: 'Engine already running' });

    // Auto-reset metrics if in critical state — login failures from previous runs
    // should not permanently block new campaigns.
    const health = metricsTracker.getHealth();
    if (health.statusCode === 'critical') {
      metricsTracker.reset();
      rateLimiter.accountStats.clear();
      engine.addLog('Metrics auto-reset (was critical) before new campaign start', req.params.id, null, 'warn');
    }

    const total = db.prepare('SELECT COUNT(*) AS c FROM accounts').get().c;
    if (!total) return res.status(400).json({ error: 'No accounts loaded' });
    res.json({ ok: true, started: true });
    
    // Non-blocking
    engine.runCampaign(req.params.id, {
      rateLimiter,
      sessionManager,
      metricsTracker,
      deliveryLogger,
    }).catch(e => {
      engine.addLog(`Fatal: ${e.message}`, req.params.id, null, 'error');
      metricsTracker.recordMessage(false, null, 'fatal_error');
    });
  });

  app.post('/api/engine/stop', auth, (_req, res) => {
    engine.stop();
    res.json({ ok: true });
  });

  app.get('/api/engine/logs', auth, (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
    const camp  = req.query.campaign_id;
    let rows = camp
      ? db.prepare('SELECT * FROM logs WHERE campaign_id=? ORDER BY id DESC').all(camp)
      : db.prepare('SELECT * FROM logs ORDER BY id DESC').all();
    rows = rows.slice(0, limit).reverse();
    res.json(rows);
  });

  // ── Sent DMs ──────────────────────────────────────────────────
  app.get('/api/sent', auth, (req, res) => {
    const { account, campaign, search } = req.query;
    const limit = Math.min(parseInt(req.query.limit || '200', 10), 1000);
    let rows = db.prepare('SELECT * FROM dm_sent ORDER BY id DESC').all();

    if (account)  rows = rows.filter(r => String(r.from_account_id) === String(account));
    if (campaign) rows = rows.filter(r => String(r.campaign_id) === String(campaign));
    if (search) {
      const s = String(search).toLowerCase();
      rows = rows.filter(r =>
        String(r.to_username || '').toLowerCase().includes(s) ||
        String(r.message || '').toLowerCase().includes(s)
      );
    }

    res.json(rows.slice(0, limit));
  });

  app.get('/api/dedup', auth, (_req, res) => {
    const rows = db.prepare('SELECT DISTINCT to_username FROM dm_sent ORDER BY to_username').all();
    res.json({ count: rows.length, usernames: rows.map(r => r.to_username) });
  });

  // ── Inbox ─────────────────────────────────────────────────────
  app.get('/api/inbox', auth, (req, res) => {
    const { account, unread, search } = req.query;
    const limit = Math.min(parseInt(req.query.limit || '200', 10), 1000);

    let rows = db.prepare('SELECT * FROM inbox ORDER BY id DESC').all();
    const dmSentRows = db.prepare('SELECT * FROM dm_sent ORDER BY id DESC').all();

    if (account) rows = rows.filter(r => String(r.to_account_id) === String(account));
    if (unread === 'true') rows = rows.filter(r => !r.is_read);

    if (search) {
      const s = String(search).toLowerCase();
      rows = rows.filter(r =>
        String(r.from_username || '').toLowerCase().includes(s) ||
        String(r.message || '').toLowerCase().includes(s)
      );
    }

    rows = rows.slice(0, limit).map(row => {
      const matched = dmSentRows.find(d =>
        String(d.to_username || '').toLowerCase() === String(row.from_username || '').toLowerCase()
      );

      const enriched = {
        ...row,
        sent_via_username: matched?.from_username || null,
        sent_via_account_id: matched?.from_account_id || null,
        original_dm: matched?.message || null,
        dm_campaign_id: matched?.campaign_id || null,
      };

      enriched.replies = db.prepare('SELECT * FROM replies WHERE inbox_id=? ORDER BY id ASC').all(row.id);
      return enriched;
    });

    res.json(rows);
  });

  app.post('/api/inbox', (req, res) => {
    const { from_username, to_account_id, to_username, message, campaign_id } = req.body;
    if (!from_username || !message) return res.status(400).json({ error: 'from_username and message required' });
    db.prepare('INSERT INTO inbox (from_username,to_account_id,to_username,message,campaign_id) VALUES (?,?,?,?,?)')
      .run(from_username, to_account_id || '', to_username || '', message, campaign_id || '');
    res.json({ ok: true });
  });

  app.patch('/api/inbox/:id/read', auth, (req, res) => {
    db.prepare('UPDATE inbox SET is_read=1 WHERE id=?').run(req.params.id);
    res.json({ ok: true });
  });

  app.post('/api/inbox/:id/reply', auth, async (req, res) => {
    const { message } = req.body;
    if (!message) return res.status(400).json({ error: 'message required' });
    try {
      const result = await engine.sendReply(parseInt(req.params.id), message);
      res.json(result);
    } catch (e) {
      res.status(400).json({ error: e.message });
    }
  });

  app.post('/api/inbox/sync', auth, async (_req, res) => {
    const accounts = db.prepare('SELECT id FROM accounts').all();
    res.json({ ok: true, accounts: accounts.length, message: 'Sync started in background' });
    for (const acc of accounts) {
      try { await engine.checkInbox(acc.id); } catch {}
    }
  });

  // ── Categories list ───────────────────────────────────────────
  app.get('/api/categories', (_req, res) => {
    res.json(Object.keys(engine.CATEGORY_KEYWORDS).map(k => ({
      id: k, label: k.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
      keywords: engine.CATEGORY_KEYWORDS[k],
    })));
  });

  // ── Safety compliance endpoint ────────────────────────────────
  app.get('/api/safety/config', auth, (_req, res) => {
    res.json({
      safetyConfig: SAFETY_CONFIG,
      complianceNotes: {
        maxMessagesPerHour: 'Hard limit to prevent detection',
        maxMessagesPerDay: 'Daily safety threshold',
        maxNewContactsPerDay: 'Prevent rapid contact escalation',
        minDelayBetweenMessages: 'Realistic human behavior',
        maxDelayBetweenMessages: 'Avoid suspicious patterns',
        sessionRefreshInterval: 'Periodic session renewal for authenticity',
      },
    });
  });

  app.get('/api/safety/risks', auth, (_req, res) => {
    const risks = [];
    const accounts = db.prepare('SELECT id FROM accounts').all();
    
    for (const acc of accounts) {
      const check = rateLimiter.check(acc.id);
      if (check.isBlocked) {
        risks.push({
          accountId: acc.id,
          type: 'rate_limit',
          violations: check.violations,
          stats: check.stats,
        });
      }
    }

    const health = metricsTracker.getHealth();
    if (health.statusCode === 'critical') {
      risks.push({
        type: 'system_health',
        message: 'System operating at critical threshold',
        recommendation: 'Pause operations immediately',
      });
    }

    res.json({
      riskLevel: risks.length === 0 ? 'low' : risks.length < 3 ? 'medium' : 'critical',
      detectedRisks: risks,
      recommendations: health.recommendations,
    });
  });

  // ── Ping ─────────────────────────────────────────────────────
  app.get('/ping', (req, res) => {
    db.prepare('INSERT INTO ping_log (ip) VALUES (?)').run(req.ip);
    res.json({ ok: true, ts: new Date().toISOString() });
  });

  // ── Reset ─────────────────────────────────────────────────────
  app.post('/api/reset/sent', auth, (_req, res) => {
    db.prepare('DELETE FROM dm_sent').run();
    db.prepare('UPDATE accounts SET dms_today=0,dms_total=0').run();
    rateLimiter.accountStats.clear();
    res.json({ ok: true });
  });

  app.post('/api/metrics/reset', auth, (_req, res) => {
    metricsTracker.reset();
    rateLimiter.accountStats.clear();
    res.json({ ok: true, message: 'Metrics reset' });
  });

  // ── Start server ──────────────────────────────────────────────
  app.listen(PORT, () => {
    console.log(`[InstaReach v3] Server → http://localhost:${PORT}`);
    console.log(`[InstaReach v3] Ping   → http://localhost:${PORT}/ping`);
    console.log(`[InstaReach v3] Health → http://localhost:${PORT}/api/health`);
    console.log(`[InstaReach v3] Safety limits configured:`, SAFETY_CONFIG);
  });

}).catch(err => {
  console.error('[InstaReach v3] Boot failed:', err);
  process.exit(1);
});

module.exports = app;