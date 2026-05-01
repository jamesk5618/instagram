/**
 * lib/dmEngine.js — InstaReach v3 Enhanced
 * Rate limiting, human-like behavior, detection avoidance, and comprehensive safety
 * Uses pure Node.js instagram-private-api (no Python required).
 */

const path = require('path');
const fs   = require('fs');
const os   = require('os');

const { dispatch } = require('./ig_client');

const IS_VERCEL   = !!(process.env.VERCEL || process.env.VERCEL_ENV);
const SESSION_DIR = IS_VERCEL
  ? path.join(os.tmpdir(), 'ig_sessions')
  : path.join(process.cwd(), 'data', 'sessions');

let _db       = null;
let _running  = false;
let _stopFlag = false;
let _safetyComponents = {};

function setDb(db)   { _db = db; }
function isRunning() { return _running; }
function stop()      { _stopFlag = true; addLog('Stop signal received', null, null, 'warn'); }

// ── Logging ───────────────────────────────────────────────────────
function addLog(msg, campaignId, accountId, level = 'info') {
  console.log(`[Engine] ${level.toUpperCase()} ${msg}`);
  if (_db) {
    try {
      _db.prepare('INSERT INTO logs (campaign_id, account_id, level, message) VALUES (?,?,?,?)')
        .run(campaignId || null, accountId || null, level, msg);
    } catch {}
  }
}

// ── Session file path ─────────────────────────────────────────────
function sessionFile(username) {
  if (!fs.existsSync(SESSION_DIR)) {
    try { fs.mkdirSync(SESSION_DIR, { recursive: true }); } catch {}
  }
  return path.join(SESSION_DIR, `${username}.json`);
}

// ── Human-like delay generator ───────────────────────────────────
function generateHumanDelay() {
  const rand = Math.random();
  if (rand < 0.4) return 5000 + Math.random() * 3000;
  if (rand < 0.8) return 8000 + Math.random() * 4000;
  return 12000 + Math.random() * 8000;
}

// ── Detection risk assessment ───────────────────────────────────
function assessDetectionRisk(accountId, stats) {
  const risks = [];
  if (stats.messagesBurst > 3)            risks.push({ level: 'high',   reason: 'Message burst detected' });
  if (stats.identicalMessageCount > 2)    risks.push({ level: 'high',   reason: 'Identical message pattern' });
  if (stats.timingPattern === 'consistent') risks.push({ level: 'medium', reason: 'Consistent timing pattern' });
  return {
    riskLevel:    risks.length === 0 ? 'low' : risks.length === 1 ? 'medium' : 'high',
    detectedRisks: risks,
    shouldPause:  risks.some(r => r.level === 'high'),
  };
}

// ── Instagram operations (via ig_client.js) ──────────────────────
function igLogin(acc) {
  return dispatch({ cmd: 'login', username: acc.username, password: acc.password || acc.session_id || '' });
}

async function igSearch(acc, keyword) {
  const r = await dispatch({ cmd: 'search', username: acc.username, password: acc.password || acc.session_id || '', keyword });
  return r.users || [];
}

function igSendDM(acc, toUsername, message, imageB64 = '', imageExt = 'jpg') {
  return dispatch({ cmd: 'send_dm', username: acc.username, password: acc.password || acc.session_id || '', to_username: toUsername, message, image_b64: imageB64, image_ext: imageExt });
}

async function igInbox(acc) {
  return dispatch({ cmd: 'inbox', username: acc.username, password: acc.password || acc.session_id || '' });
}

// ── Category keywords ─────────────────────────────────────────────
const CATEGORY_KEYWORDS = {
  real_estate:       ['realestateindia','propertyindia','delhirealestate','propertydealerdelhi','realesteagent','homesforsale','indianrealestate','realestate'],
  digital_marketing: ['digitalmarketingindia','socialmediamarketing','seoagency','digitalmarketer','marketingagency','growthhacking','contentmarketing'],
  fashion:           ['fashionbloggerindia','indianfashion','boutiquefashion','streetwearindia','fashiondesignerindia','ootdindia','fashionstyle'],
  food:              ['foodbloggerindia','cloudkitchen','restaurantindia','foodentrepreneur','cafeowner','foodbusiness','indianfood'],
  fitness:           ['fitnessindia','personaltrainerindia','gymlife','yogaindia','fitnesscoach','healthylifestyle','fitnessmotivation'],
  education:         ['edtechindia','onlineeducation','coachingclass','tutoring','educationindia','learningonline','studygram'],
  tech:              ['startupindia','techindia','saas','entrepreneurindia','techstartup','softwaredev','inditech'],
  beauty:            ['beautybloggerindia','makeupartist','skincareindia','beautyinfluencer','salonowner','makeupindia','beautycare'],
  travel:            ['travelbloggerindia','travelphotography','indiatravel','touroperator','travelindia','wanderlust'],
  other:             ['businessindia','entrepreneurlife','smallbusiness','startuplife','businessowner','workfromhome'],
};

function getKeywordsForCategory(parent, sub, custom = []) {
  const base = CATEGORY_KEYWORDS[parent] || CATEGORY_KEYWORDS.other;
  return [...new Set([...custom, ...base])];
}

function getGlobalDmedSet() {
  try {
    const rows = _db.prepare('SELECT DISTINCT to_username FROM dm_sent').all();
    return new Set(rows.map(r => r.to_username.toLowerCase()));
  } catch { return new Set(); }
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Pre-login all accounts with retry ─────────────────────────────
async function loginAllAccounts(accounts, campaignId) {
  addLog(`Logging in ${accounts.length} accounts via instagrapi...`, campaignId);
  const valid = [];
  
  for (const acc of accounts) {
    if (_stopFlag) break;
    const pwd = acc.password || acc.session_id || '';
    if (!pwd) {
      addLog(`@${acc.username} — no password set, skipping`, campaignId, acc.id, 'warn');
      continue;
    }
    
    addLog(`Logging in @${acc.username}...`, campaignId, acc.id);
    
    // Retry logic
    let loginSuccess = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
      const r = await igLogin(acc);
      if (r.ok) {
        addLog(`✓ @${acc.username} logged in (attempt ${attempt})`, campaignId, acc.id, 'success');
        valid.push(acc);
        loginSuccess = true;
        break;
      } else {
        addLog(`✗ @${acc.username} login attempt ${attempt}/3 failed: ${r.error}`, campaignId, acc.id, 'warn');
        if (attempt < 3) {
          await sleep(5000 + Math.random() * 3000);
        }
      }
    }
    
    if (!loginSuccess) {
      if (_safetyComponents.metricsTracker) {
        _safetyComponents.metricsTracker.recordMessage(false, acc.id, 'login_failed');
      }
    }
    
    await sleep(generateHumanDelay());
  }
  
  return valid;
}

// ── Rate limit check before sending ──────────────────────────────
function checkRateLimits(accountId) {
  if (!_safetyComponents.rateLimiter) return { allowed: true };
  
  const check = _safetyComponents.rateLimiter.check(accountId);
  if (check.isBlocked) {
    addLog(`Rate limit enforced for account: ${check.violations.join(', ')}`, null, accountId, 'warn');
    return { allowed: false, reason: check.violations[0] };
  }
  return { allowed: true };
}

// ── MAIN CAMPAIGN ENGINE ──────────────────────────────────────────
async function runCampaign(campaignId, safetyComponents = {}) {
  if (_running) throw new Error('Engine already running');
  if (!_db)     throw new Error('Database not initialized');

  _running  = true;
  _stopFlag = false;
  _safetyComponents = safetyComponents;

  try {
    const campaign = _db.prepare('SELECT * FROM campaigns WHERE id = ?').get(campaignId);
  if (!campaign) { _running = false; throw new Error('Campaign not found'); }

  const allAccounts = _db.prepare('SELECT * FROM accounts ORDER BY created_at ASC LIMIT 100').all();
  if (!allAccounts.length) { _running = false; throw new Error('No accounts loaded'); }

  const dmsPerAccount = campaign.dms_per_account || 5;
  addLog(`Campaign "${campaign.name}" starting with safety checks enabled`, campaignId);
  _db.prepare("UPDATE campaigns SET status='running', started_at=datetime('now') WHERE id=?").run(campaignId);

  // Login all accounts with retry
  const accounts = await loginAllAccounts(allAccounts, campaignId);
  if (!accounts.length) {
    addLog('No accounts could log in — check passwords in your CSV', campaignId, null, 'error');
    _db.prepare("UPDATE campaigns SET status='done',finished_at=datetime('now') WHERE id=?").run(campaignId);
    if (_safetyComponents.metricsTracker) {
      _safetyComponents.metricsTracker.recordMessage(false, null, 'no_valid_accounts');
    }
    _running = false;
    return { sent: 0, skipped: 0 };
  }
  addLog(`${accounts.length} accounts ready`, campaignId, null, 'success');

  // Build keyword list
  let keywords = [];
  try { keywords = JSON.parse(campaign.keywords || '[]'); } catch {}
  const allKeywords = getKeywordsForCategory(campaign.parent_category, campaign.sub_category, keywords);

  // Search for targets using first account
  addLog(`Searching targets (category: ${campaign.parent_category})...`, campaignId);
  const searchAcc = accounts[0];
  const dmedSet   = getGlobalDmedSet();
  const targets   = new Set();

  for (const kw of allKeywords) {
    if (_stopFlag) break;
    const found = await igSearch(searchAcc, kw);
    found.forEach(u => { if (!dmedSet.has(u.toLowerCase())) targets.add(u); });
    addLog(`"${kw}" → ${found.length} found, ${targets.size} fresh targets so far`, campaignId);
    await sleep(generateHumanDelay());
    if (targets.size >= campaign.max_targets) break;
  }

  const targetList = [...targets].slice(0, campaign.max_targets);
  addLog(`Total fresh targets: ${targetList.length}`, campaignId);
  _db.prepare('UPDATE campaigns SET targets_found=? WHERE id=?').run(targetList.length, campaignId);

  if (!targetList.length) {
    addLog('No fresh targets found', campaignId, null, 'warn');
    _db.prepare("UPDATE campaigns SET status='done',finished_at=datetime('now') WHERE id=?").run(campaignId);
    _running = false;
    return { sent: 0, skipped: 0 };
  }

  // ── Round-robin DM rotation with safety checks ─────────────────
  const accountDmCount = {};
  accounts.forEach(a => accountDmCount[a.id] = 0);
  let targetIdx = 0, totalSent = 0, totalSkip = 0, acctIdx = 0;

  addLog('Starting DM rotation with human-like delays...', campaignId);

  while (targetIdx < targetList.length && !_stopFlag) {
    let sentThisCycle = 0;

    for (let i = 0; i < accounts.length; i++) {
      if (_stopFlag || targetIdx >= targetList.length) break;

      // Pick next account under limit
      let acc = null, tries = 0;
      while (tries < accounts.length) {
        const c = accounts[acctIdx % accounts.length]; acctIdx++;
        if (accountDmCount[c.id] < dmsPerAccount) { acc = c; break; }
        tries++;
      }
      if (!acc) break;

      // Rate limit check
      const rateLimitCheck = checkRateLimits(acc.id);
      if (!rateLimitCheck.allowed) {
        addLog(`${acc.username} blocked: ${rateLimitCheck.reason} — waiting`, campaignId, acc.id, 'warn');
        if (_safetyComponents.metricsTracker) {
          _safetyComponents.metricsTracker.recordRateLimitHit(acc.id);
        }
        await sleep(300000); // 5 minute pause for rate limit
        break;
      }

      // Dedup check
      const freshDmed = getGlobalDmedSet();
      while (targetIdx < targetList.length && freshDmed.has(targetList[targetIdx].toLowerCase())) {
        targetIdx++; totalSkip++;
      }
      if (targetIdx >= targetList.length) break;

      const toUsername = targetList[targetIdx++];
      const msg = (campaign.message || '')
        .replace(/\{\{username\}\}/g, `@${toUsername}`)
        .replace(/\{\{account\}\}/g,  `@${acc.username}`);

      // Personalization check
      if (msg.includes('{{username}}') || msg.includes('{{account}}')) {
        addLog(`Message uses placeholders — personalization recommended`, campaignId, acc.id, 'info');
      }

      addLog(`[${acc.username} → @${toUsername}] DM #${accountDmCount[acc.id]+1}/${dmsPerAccount}`, campaignId, acc.id);
      _db.prepare("UPDATE accounts SET status='running',last_active=datetime('now') WHERE id=?").run(acc.id);

      const result = await igSendDM(acc, toUsername, msg, campaign.image_b64 || '', campaign.image_ext || 'jpg');

      if (result.ok) {
        try {
          _db.prepare('INSERT OR IGNORE INTO dm_sent (campaign_id,from_account_id,from_username,to_username,message) VALUES (?,?,?,?,?)')
            .run(campaignId, acc.id, acc.username, toUsername, msg);
        } catch {}
        
        // Record in metrics
        if (_safetyComponents.metricsTracker) {
          _safetyComponents.metricsTracker.recordMessage(true, acc.id);
        }
        
        // Log delivery
        if (_safetyComponents.deliveryLogger) {
          _safetyComponents.deliveryLogger.logDelivery(
            `msg_${Date.now()}`,
            campaignId,
            acc.username,
            toUsername,
            'SUCCESS'
          );
        }
        
        // Update rate limiter
        if (_safetyComponents.rateLimiter) {
          _safetyComponents.rateLimiter.recordMessage(acc.id, toUsername);
        }
        
        accountDmCount[acc.id]++;
        totalSent++;
        _db.prepare('UPDATE campaigns SET dms_sent=? WHERE id=?').run(totalSent, campaignId);
        _db.prepare('UPDATE accounts SET dms_today=dms_today+1,dms_total=dms_total+1 WHERE id=?').run(acc.id);
        addLog(`✓ Sent: @${acc.username} → @${toUsername} (${accountDmCount[acc.id]}/${dmsPerAccount})`, campaignId, acc.id, 'success');
        sentThisCycle++;
      } else if (result.reason === 'rate_limited') {
        addLog(`Rate limited @${acc.username} — pausing 5 min`, campaignId, acc.id, 'warn');
        if (_safetyComponents.metricsTracker) {
          _safetyComponents.metricsTracker.recordMessage(false, acc.id, 'rate_limited');
        }
        await sleep(300000);
        targetIdx--;
      } else if (result.reason === 'session_expired') {
        addLog(`Session expired @${acc.username} — re-logging in`, campaignId, acc.id, 'warn');
        const relogin = await igLogin(acc);
        if (!relogin.ok) {
          addLog(`Re-login failed @${acc.username} — disabling`, campaignId, acc.id, 'error');
          if (_safetyComponents.metricsTracker) {
            _safetyComponents.metricsTracker.recordMessage(false, acc.id, 'session_expired');
          }
          accountDmCount[acc.id] = dmsPerAccount;
        } else {
          if (_safetyComponents.sessionManager) {
            _safetyComponents.sessionManager.refreshSession(acc.id);
          }
          if (_safetyComponents.metricsTracker) {
            _safetyComponents.metricsTracker.recordSessionRefresh(acc.id);
          }
          targetIdx--;
        }
      } else {
        addLog(`Failed @${acc.username} → @${toUsername}: ${result.reason || result.error}`, campaignId, acc.id, 'warn');
        if (_safetyComponents.metricsTracker) {
          _safetyComponents.metricsTracker.recordMessage(false, acc.id, result.reason || 'unknown_error');
        }
        if (_safetyComponents.deliveryLogger) {
          _safetyComponents.deliveryLogger.logDelivery(
            `msg_${Date.now()}`,
            campaignId,
            acc.username,
            toUsername,
            'FAILED',
            { reason: result.reason, error: result.error }
          );
        }
      }

      _db.prepare("UPDATE accounts SET status='idle' WHERE id=?").run(acc.id);
      
      // Human-like variable delay
      const delay = generateHumanDelay();
      await sleep(delay);
    }

    if (sentThisCycle === 0) {
      addLog('All accounts reached DM limit for this run', campaignId, null, 'warn');
      break;
    }
  }

  const finalStatus = _stopFlag ? 'stopped' : 'done';
  _db.prepare(`UPDATE campaigns SET status=?,finished_at=datetime('now') WHERE id=?`).run(finalStatus, campaignId);
  addLog(`Campaign complete — Sent: ${totalSent} | Skipped: ${totalSkip}`, campaignId, null, 'success');
  _running = false;
  return { sent: totalSent, skipped: totalSkip };
  } catch (err) {
    addLog(`Campaign error: ${err.message}`, campaignId, null, 'error');
    _db.prepare("UPDATE campaigns SET status='error',finished_at=datetime('now') WHERE id=?").run(campaignId);
    _running = false;
    return { sent: 0, skipped: 0, error: err.message };
  } finally {
    _running = false;
  }
}

// ── Inbox check ───────────────────────────────────────────────────
async function checkInbox(accountId) {
  if (!_db) return;
  const acc = _db.prepare('SELECT * FROM accounts WHERE id=?').get(accountId);
  if (!acc) return;
  const r = await igInbox(acc);
  if (!r.ok) return;
  for (const msg of (r.messages || [])) {
    const wasDmed = _db.prepare('SELECT id FROM dm_sent WHERE to_username=? COLLATE NOCASE').get(msg.from_username);
    if (!wasDmed) continue;
    const exists = _db.prepare('SELECT id FROM inbox WHERE from_username=? AND message=?').get(msg.from_username, msg.text);
    if (!exists) {
      const sentRow = _db.prepare('SELECT campaign_id FROM dm_sent WHERE to_username=? COLLATE NOCASE LIMIT 1').get(msg.from_username);
      _db.prepare('INSERT INTO inbox (from_username,to_account_id,to_username,message,campaign_id) VALUES (?,?,?,?,?)')
        .run(msg.from_username, accountId, acc.username, msg.text, sentRow?.campaign_id || '');
      addLog(`New reply from @${msg.from_username}`, null, accountId, 'success');
    }
  }
}

// ── Reply from dashboard ──────────────────────────────────────────
async function sendReply(inboxId, message) {
  if (!_db) throw new Error('No DB');
  const inboxMsg  = _db.prepare('SELECT * FROM inbox WHERE id=?').get(inboxId);
  if (!inboxMsg)  throw new Error('Inbox message not found');
  const sentRow   = _db.prepare('SELECT * FROM dm_sent WHERE to_username=? COLLATE NOCASE LIMIT 1').get(inboxMsg.from_username);
  const accountId = sentRow?.from_account_id || inboxMsg.to_account_id;
  const acc       = _db.prepare('SELECT * FROM accounts WHERE id=?').get(accountId);
  if (!acc) throw new Error('Account not found');
  const result = await igSendDM(acc, inboxMsg.from_username, message);
  if (result.ok) {
    _db.prepare('INSERT INTO replies (inbox_id,from_account_id,from_username,to_username,message) VALUES (?,?,?,?,?)')
      .run(inboxId, acc.id, acc.username, inboxMsg.from_username, message);
    _db.prepare('UPDATE inbox SET is_replied=1,is_read=1 WHERE id=?').run(inboxId);
    return { ok: true, via: acc.username };
  }
  throw new Error(result.reason || result.error);
}

module.exports = { 
  setDb, isRunning, stop, runCampaign, checkInbox, sendReply, addLog, 
  CATEGORY_KEYWORDS,
  generateHumanDelay,
  assessDetectionRisk,
};