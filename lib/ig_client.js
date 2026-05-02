/**
 * lib/ig_client.js — Session-Safe Instagram Client
 * OPTIMIZED FOR MANUAL SESSION UPLOADS
 * No device fingerprinting errors - works with exported sessions
 */

const { IgApiClient, IgCheckpointError, IgLoginRequiredError } = require('instagram-private-api');
const fs   = require('fs');
const path = require('path');
const os   = require('os');

const IS_VERCEL   = !!(process.env.VERCEL || process.env.VERCEL_ENV);
const SESSION_DIR = IS_VERCEL
  ? path.join(os.tmpdir(), 'ig_sessions')
  : path.join(process.cwd(), 'data', 'sessions');

function sessionFile(username) {
  if (!fs.existsSync(SESSION_DIR)) {
    try { fs.mkdirSync(SESSION_DIR, { recursive: true }); } catch {}
  }
  return path.join(SESSION_DIR, `session_${username.toLowerCase()}.json`);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function jitter(min, max) { return min + Math.random() * (max - min); }

function classifyError(e) {
  const msg = (e.message || String(e)).toLowerCase();
  if (msg.includes('challenge') || msg.includes('checkpoint')) return 'session_expired';
  if (msg.includes('rate') || msg.includes('429') || msg.includes('throttle')) return 'rate_limited';
  if (msg.includes('invalid') || msg.includes('password') || msg.includes('400')) return 'login_failed';
  if (msg.includes('not found') || msg.includes('user_not_found')) return 'user_not_found';
  if (msg.includes('block') || msg.includes('restricted')) return 'blocked';
  if (msg.includes('spam') || msg.includes('action_blocked')) return 'spam_detected';
  return 'unknown';
}

// ── SAFE SESSION LOADING (No device property errors) ──────────────
async function getClientFromSession(username) {
  const sf = sessionFile(username);
  
  if (!fs.existsSync(sf)) {
    throw new Error(`Session file not found for @${username}. Expected: ${sf}`);
  }
  
  try {
    console.log(`[ig_client] Loading session from: ${sf}`);
    
    const ig = new IgApiClient();
    
    // Load session WITHOUT modifying device properties (prevents errors)
    const state = JSON.parse(fs.readFileSync(sf, 'utf8'));
    ig.state.deserialize(state);
    
    // Test if session is valid
    await sleep(jitter(1000, 2000));
    const user = await ig.account.currentUser();
    
    console.log(`[ig_client] ✅ Session loaded for @${user.username} (id: ${user.pk})`);
    return ig;
  } catch (e) {
    console.error(`[ig_client] Session load error: ${e.message}`);
    return {
  ok: false,
  error: e.message,
  reason: 'session_failed'
};
  }
}

// ── Commands ──────────────────────────────────────────────────────

async function cmdLogin({ username, password }) {
  try {
    // Priority 1: Try loaded session first (for manual uploads)
    if (!password || password.trim() === '') {
      console.log(`[ig_client] No password provided, attempting session load for @${username}`);
      const ig = await getClientFromSession(username);
      const info = await ig.account.currentUser();
      return {
        ok: true,
        username,
        user_id: String(info.pk),
        follower_count: info.follower_count || 0,
      };
    }
    
    // Priority 2: Login with password
    console.log(`[ig_client] Logging in @${username} with password`);
    const ig = new IgApiClient();
    await sleep(jitter(2000, 4000));
    await ig.account.login(username, password);
    await sleep(jitter(800, 1500));
    
    const info = await ig.account.currentUser();
    
    // Save session for future use
    try {
      const state = ig.state.serialize();
      delete state.constants;
      fs.writeFileSync(sessionFile(username), JSON.stringify(state, null, 2));
      console.log(`[ig_client] Session saved for @${username}`);
    } catch (e) {
      console.warn(`[ig_client] Failed to save session: ${e.message}`);
    }
    
    return {
      ok: true,
      username,
      user_id: String(info.pk),
      follower_count: info.follower_count || 0,
    };
  } catch (e) {
    console.error(`[ig_client] Login error: ${e.message}`);
    return { 
      ok: false, 
      error: e.message, 
      reason: classifyError(e) 
    };
  }
}

async function cmdSearch({ username, password, keyword }) {
  try {
    console.log(`[ig_client] Searching for "${keyword}" as @${username}`);
    
    let ig;
    if (!password || password.trim() === '') {
      ig = await getClientFromSession(username);
    } else {
      ig = new IgApiClient();
      await sleep(jitter(2000, 4000));
      await ig.account.login(username, password);
    }
    
    const users = new Set();
    
    // Strategy 1: Hashtag search (most reliable)
    const tags = [
      keyword.replace(/[\s-]/g, '').toLowerCase(),
      keyword.replace(/\s+/g, '_').toLowerCase(),
    ];
    
    for (const tag of tags) {
      try {
        console.log(`[ig_client] Searching hashtag: #${tag}`);
        await sleep(jitter(1500, 3000));
        const feed = ig.feed.tag(tag);
        const posts = await feed.items();
        
        for (const p of posts.slice(0, 40)) {
          if (p.user?.username) {
            users.add(p.user.username.toLowerCase());
          }
          if (users.size >= 25) break;
        }
        
        if (users.size >= 25) break;
      } catch (e) {
        console.warn(`[ig_client] Hashtag search failed for "${tag}": ${e.message}`);
      }
      
      if (users.size >= 25) break;
    }
    
    // Strategy 2: Direct user search (fallback)
    if (users.size < 10) {
      try {
        console.log(`[ig_client] Fallback user search for "${keyword}"`);
        await sleep(jitter(1500, 3000));
        const results = await ig.search.users(keyword);
        
        for (const u of results.slice(0, 20)) {
          if (u.username) {
            users.add(u.username.toLowerCase());
          }
        }
      } catch (e) {
        console.warn(`[ig_client] User search failed: ${e.message}`);
      }
    }
    
    console.log(`[ig_client] Search complete: found ${users.size} users`);
    return { 
      ok: true, 
      users: [...users],
      count: users.size 
    };
  } catch (e) {
    console.error(`[ig_client] Search error: ${e.message}`);
    return { 
      ok: false, 
      error: e.message, 
      users: [], 
      reason: classifyError(e) 
    };
  }
}

async function cmdSendDm({ username, password, to_username, message, image_b64, image_ext }) {
  try {
    console.log(`[ig_client] Sending DM from @${username} to @${to_username}`);
    
    let ig;
    if (!password || password.trim() === '') {
      ig = await getClientFromSession(username);
    } else {
      ig = new IgApiClient();
      await sleep(jitter(2000, 4000));
      await ig.account.login(username, password);
    }
    
    // Resolve recipient user id
    let userId;
    try {
      console.log(`[ig_client] Looking up @${to_username}`);
      await sleep(jitter(1000, 2000));
      const userInfo = await ig.user.searchExact(to_username);
      userId = userInfo.pk;
      console.log(`[ig_client] Found @${to_username} (id: ${userId})`);
    } catch (e) {
      console.warn(`[ig_client] User not found: ${to_username}`);
      return { 
        ok: false, 
        reason: 'user_not_found', 
        error: `User @${to_username} not found` 
      };
    }

    // Send image if provided
    if (image_b64 && image_b64.trim()) {
      try {
        console.log(`[ig_client] Sending image to @${to_username}`);
        await sleep(jitter(1500, 2500));
        const imgBuf = Buffer.from(image_b64, 'base64');
        await ig.directThread.broadcastPhoto({ 
          userIds: [String(userId)], 
          file: imgBuf 
        });
        console.log(`[ig_client] Image sent`);
      } catch (ie) {
        console.warn(`[ig_client] Image send failed: ${ie.message}`);
      }
    }

    // Send text message
    if (message) {
      console.log(`[ig_client] Sending text message (${message.length} chars)`);
      await sleep(jitter(1500, 2500));
      await ig.directThread.broadcastText({ 
        userIds: [String(userId)], 
        text: message 
      });
      console.log(`[ig_client] Message sent successfully`);
    }

    return { 
      ok: true, 
      message_sent: true, 
      to_username 
    };
  } catch (e) {
    const reason = classifyError(e);
    console.error(`[ig_client] Send DM error (${reason}): ${e.message}`);
    return { 
      ok: false, 
      reason, 
      error: e.message 
    };
  }
}

async function cmdInbox({ username, password }) {
  try {
    console.log(`[ig_client] Fetching inbox for @${username}`);
    
    let ig;
    if (!password || password.trim() === '') {
      ig = await getClientFromSession(username);
    } else {
      ig = new IgApiClient();
      await sleep(jitter(2000, 4000));
      await ig.account.login(username, password);
    }
    
    await sleep(jitter(1500, 2500));
    const feed = ig.feed.directInbox();
    const threads = await feed.items();
    const messages = [];

    for (const thread of threads.slice(0, 20)) {
      const other = thread.users?.[0];
      if (!other) continue;
      
      for (const item of (thread.items || [])) {
        if (String(item.user_id) === String(other.pk) && item.text) {
          messages.push({
            from_username: other.username.toLowerCase(),
            text:          item.text,
            timestamp:     String(item.timestamp),
          });
        }
      }
      
      await sleep(jitter(500, 1200));
    }

    console.log(`[ig_client] Inbox check complete: ${messages.length} messages`);
    return { 
      ok: true, 
      messages, 
      message_count: messages.length 
    };
  } catch (e) {
    console.error(`[ig_client] Inbox error: ${e.message}`);
    return { 
      ok: false, 
      error: e.message, 
      messages: [], 
      reason: classifyError(e) 
    };
  }
}

async function cmdCheckSession({ username }) {
  const sf = sessionFile(username);
  if (!fs.existsSync(sf)) {
    return { 
      ok: false, 
      valid: false, 
      reason: 'no_session_file',
      message: `Session file not found: ${sf}`
    };
  }
  
  try {
    const ig = new IgApiClient();
    const state = JSON.parse(fs.readFileSync(sf, 'utf8'));
    ig.state.deserialize(state);
    await sleep(jitter(800, 1500));
    const user = await ig.account.currentUser();
    console.log(`[ig_client] Session valid for @${user.username}`);
    return { 
      ok: true, 
      valid: true, 
      username,
      user_id: String(user.pk)
    };
  } catch (e) {
    console.warn(`[ig_client] Session check failed: ${e.message}`);
    return { 
      ok: false, 
      valid: false, 
      reason: classifyError(e), 
      error: e.message 
    };
  }
}

// ── Dispatch ──────────────────────────────────────────────────────
const COMMANDS = {
  login:         cmdLogin,
  search:        cmdSearch,
  send_dm:       cmdSendDm,
  inbox:         cmdInbox,
  check_session: cmdCheckSession,
};

async function dispatch(payload) {
  const handler = COMMANDS[payload.cmd];
  if (!handler) {
    return { 
      ok: false, 
      error: `Unknown command: ${payload.cmd}` 
    };
  }
  try {
    return await handler(payload);
  } catch (e) {
    console.error(`[ig_client] Dispatch error in ${payload.cmd}:`, e.message);
    return { 
      ok: false, 
      error: e.message,
      reason: classifyError(e)
    };
  }
}

module.exports = { dispatch };