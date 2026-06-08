const express = require('express');
const http = require('http');
const path = require('path');
const { Pool } = require('pg');
const jwt = require('jsonwebtoken');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const port = process.env.PORT || 3000;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const JWT_SECRET = process.env.JWT_SECRET;

const PUBLIC_API_PATHS = new Set(['/health']);
const PUBLIC_PREFIXES = ['/explorer-api/'];

app.use(express.json({ limit: '5mb' }));

app.use((req, res, next) => {
  const token = req.query.token || req.headers['x-usernode-token'];
  if (token && JWT_SECRET) {
    try { req.user = jwt.verify(token, JWT_SECRET); } catch {}
  }
  if (req.method !== 'GET' || req.path.startsWith('/api/')) {
    if (PUBLIC_API_PATHS.has(req.path)) return next();
    if (PUBLIC_PREFIXES.some(p => req.path.startsWith(p))) return next();
    if (!req.user) return res.status(401).json({ error: 'Not authenticated' });
  }
  next();
});

// Socket.io auth
io.use((socket, next) => {
  const token = socket.handshake.auth.token || socket.handshake.query.token;
  if (token && JWT_SECRET) {
    try {
      socket.user = jwt.verify(token, JWT_SECRET);
      return next();
    } catch {}
  }
  next(new Error('Authentication error'));
});

// ─── Auction timer ────────────────────────────────────────────────────────────
let auctionTimer = null;

function clearAuctionTimer() {
  if (auctionTimer) {
    clearTimeout(auctionTimer.timerId);
    clearInterval(auctionTimer.intervalId);
    auctionTimer = null;
  }
}

function startAuctionTimer(sessionId, expiresAt) {
  clearAuctionTimer();
  const expiryMs = new Date(expiresAt).getTime();

  const intervalId = setInterval(() => {
    const remaining = Math.max(0, Math.ceil((expiryMs - Date.now()) / 1000));
    io.to(`session:${sessionId}`).emit('auction-tick', { remaining, expiresAt });
  }, 1000);

  const delay = Math.max(0, expiryMs - Date.now());
  const timerId = setTimeout(() => handleAuctionEnd(sessionId), delay);

  auctionTimer = { sessionId, timerId, intervalId, expiresAt };
}

async function handleAuctionEnd(sessionId) {
  if (auctionTimer) {
    clearInterval(auctionTimer.intervalId);
    clearTimeout(auctionTimer.timerId);
    auctionTimer = null;
  }
  try {
    const { rows } = await pool.query('SELECT * FROM auctions WHERE session_id = $1', [sessionId]);
    if (!rows.length) return;
    const auction = rows[0];
    io.to(`session:${sessionId}`).emit('auction-ended', {
      sessionId,
      winnerId: auction.current_leader_user_id,
      winnerUsername: auction.current_leader_username,
      finalBid: parseInt(auction.current_bid)
    });
  } catch (err) {
    console.error('handleAuctionEnd error:', err);
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
async function ensureUserCredits(userId, username) {
  await pool.query(
    `INSERT INTO user_credits (user_id, username, balance) VALUES ($1, $2, 1000) ON CONFLICT (user_id) DO NOTHING`,
    [userId, username]
  );
}

function parseWords(input) {
  if (Array.isArray(input)) return input.map(w => String(w).trim()).filter(Boolean);
  return String(input || '').split(/[\s,]+/).map(w => w.trim()).filter(Boolean);
}

// ─── Creative Prompts ───────────────────────────────────────────────────────────
// Rule-based suggestion generator. Derives a personal word-frequency map from the
// signed-in user's activity (sessions created / bid on / won / liked), blends in a
// curated word bank for freshness, and falls back to app-wide popular words and a
// built-in seed bank so the endpoint always returns 10 valid, submittable words.

const WORD_BANK = {
  nature:   ['ocean', 'forest', 'mountain', 'river', 'storm', 'meadow', 'desert', 'glacier', 'canyon', 'willow', 'ember', 'tide'],
  cosmic:   ['nebula', 'comet', 'eclipse', 'galaxy', 'starlight', 'void', 'orbit', 'aurora', 'cosmos', 'meteor'],
  emotion:  ['longing', 'serene', 'euphoria', 'melancholy', 'wonder', 'dread', 'tender', 'restless', 'solace', 'reverie'],
  material: ['glass', 'marble', 'silk', 'iron', 'obsidian', 'velvet', 'copper', 'crystal', 'porcelain', 'amber'],
  color:    ['crimson', 'indigo', 'golden', 'emerald', 'violet', 'scarlet', 'azure', 'ivory', 'onyx', 'teal'],
  motion:   ['drift', 'spiral', 'cascade', 'shatter', 'bloom', 'unravel', 'flicker', 'surge', 'dissolve', 'wander'],
};
const SEED_WORDS = Object.values(WORD_BANK).flat();

const TITLE_ADJECTIVES = ['Quiet', 'Restless', 'Hidden', 'Endless', 'Fractured', 'Luminous', 'Ancient', 'Drifting'];

const THEME_TEMPLATES = [
  ({ a, b, c }) => `A dreamlike scene of ${a} and ${b}, lit by ${c}`,
  ({ a, b, c }) => `${cap(a)} meeting ${b} in a haze of ${c}`,
  ({ a, b, c }) => `An abstract study of ${a}, ${b}, and ${c}`,
  ({ a, b, c }) => `Where ${a} dissolves into ${b} beneath ${c}`,
  ({ a, b, c }) => `A surreal portrait built from ${a} and ${b}`,
];
const TITLE_TEMPLATES = [
  ({ a, b }) => `${cap(a)} ${cap(b)}`,
  ({ a, b }) => `The ${cap(a)} of ${cap(b)}`,
  ({ a, adj }) => `${adj} ${cap(a)}`,
  ({ a }) => `Echoes of ${cap(a)}`,
];

function cap(s) {
  s = String(s || '');
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// Normalize a stored comma-joined words string into clean, submittable tokens
// (lowercased, trimmed, dropping anything over the 30-char session limit).
function normWords(wordsStr) {
  return String(wordsStr || '')
    .split(',')
    .map(w => w.trim().toLowerCase())
    .filter(w => w && w.length <= 30);
}

// Small deterministic PRNG so a given (userId, offset) reproduces the same batch
// — lets the client cycle "next" predictably without server-side cursor state.
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffle(arr, rng) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function pickRng(arr, rng) {
  if (!arr.length) return '';
  return arr[Math.floor(rng() * arr.length)];
}

// Aggregate the user's word usage across the four activity sources, weighted by
// signal strength (a like is stronger taste evidence than a passing bid).
async function gatherUserActivity(userId) {
  const freq = new Map();
  const existingNames = new Set();
  const bump = (wordsStr, weight) => {
    for (const w of normWords(wordsStr)) freq.set(w, (freq.get(w) || 0) + weight);
  };

  const [liked, won, created, bid] = await Promise.all([
    pool.query(`SELECT s.words FROM artwork_likes l JOIN sessions s ON s.id = l.session_id WHERE l.user_id = $1`, [userId]),
    pool.query(`SELECT s.words FROM generated_artworks g JOIN sessions s ON s.id = g.session_id WHERE g.owner_user_id = $1`, [userId]),
    pool.query(`SELECT words, name FROM sessions WHERE creator_user_id = $1`, [userId]),
    pool.query(`SELECT DISTINCT s.words FROM bids b JOIN sessions s ON s.id = b.session_id WHERE b.user_id = $1`, [userId]),
  ]);

  liked.rows.forEach(r => bump(r.words, 4));
  won.rows.forEach(r => bump(r.words, 3));
  created.rows.forEach(r => {
    bump(r.words, 2);
    if (r.name) existingNames.add(r.name.trim().toLowerCase());
  });
  bid.rows.forEach(r => bump(r.words, 1));

  return { freq, existingNames, sessionsCreated: created.rows.length };
}

// Most-used words across all completed (locked) sessions — the popular fallback tier.
async function gatherPopularWords() {
  const { rows } = await pool.query(`SELECT words FROM sessions WHERE state = 'locked'`);
  const freq = new Map();
  rows.forEach(r => normWords(r.words).forEach(w => freq.set(w, (freq.get(w) || 0) + 1)));
  return [...freq.entries()].sort((a, b) => b[1] - a[1]).map(e => e[0]);
}

function buildOneSuggestion({ personal, popular, existingNames, rng }) {
  const picked = [];
  const seen = new Set();
  const tryAdd = (w) => {
    w = String(w || '').trim().toLowerCase();
    if (!w || w.length > 30 || seen.has(w)) return;
    seen.add(w); picked.push(w);
  };

  // 1. Personal words (a shuffled subset of the user's most-used words).
  const personalTarget = Math.min(personal.length, 6);
  for (const w of shuffle(personal.slice(0, 20), rng)) {
    if (picked.length >= personalTarget) break;
    tryAdd(w);
  }
  const personalUsed = picked.length;

  // 2. Blend in 2–4 word-bank words so suggestions feel fresh, not echoed back.
  const bankCount = 2 + Math.floor(rng() * 3);
  for (const w of shuffle(SEED_WORDS, rng)) {
    if (picked.length >= personalUsed + bankCount) break;
    tryAdd(w);
  }

  // 3. Fill to 10 from popular words, then the seed bank as a last resort.
  for (const src of [shuffle(popular, rng), shuffle(SEED_WORDS, rng)]) {
    for (const w of src) {
      if (picked.length >= 10) break;
      tryAdd(w);
    }
    if (picked.length >= 10) break;
  }

  const words = picked.slice(0, 10);

  // Source = whichever tier supplied the majority of the 10 words.
  let source;
  if (personalUsed > 5) source = 'personal';
  else if (popular.length > 0) source = 'popular';
  else source = 'seed';

  // Theme line + title woven from the chosen words.
  const themePool = words.length >= 3 ? words : [...words, ...SEED_WORDS];
  const [a, b, c] = shuffle(themePool, rng).slice(0, 3);
  const theme = pickRng(THEME_TEMPLATES, rng)({ a, b, c });

  let title = '';
  for (let attempt = 0; attempt < 6; attempt++) {
    title = pickRng(TITLE_TEMPLATES, rng)({
      a: pickRng(words, rng),
      b: pickRng(words, rng),
      adj: pickRng(TITLE_ADJECTIVES, rng),
    });
    if (!existingNames.has(title.trim().toLowerCase())) break;
  }

  return { title, words, theme, source, personalUsed };
}

async function generateSuggestions(userId, count, offset) {
  const { freq, existingNames, sessionsCreated } = await gatherUserActivity(userId);
  const personal = [...freq.entries()].sort((a, b) => b[1] - a[1]).map(e => e[0]);

  // Only the popular tier requires an extra query, and only when personal data is thin.
  const popular = personal.length < 10 ? await gatherPopularWords() : [];

  const suggestions = [];
  for (let i = 0; i < count; i++) {
    const rng = mulberry32(((userId * 2654435761) ^ ((offset + i + 1) * 40503)) >>> 0);
    suggestions.push(buildOneSuggestion({ personal, popular, existingNames, rng }));
  }

  const basis = {
    wordsFromHistory: freq.size,
    sessionsCreated,
    source: suggestions[0] ? suggestions[0].source : 'seed',
  };

  return { suggestions, basis };
}

// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/me', async (req, res) => {
  try {
    await ensureUserCredits(req.user.id, req.user.username);
    const { rows } = await pool.query('SELECT balance FROM user_credits WHERE user_id = $1', [req.user.id]);
    res.json({ id: req.user.id, username: req.user.username, credits: rows[0]?.balance ?? 1000 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/session/active', async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.*, a.current_leader_user_id, a.current_leader_username, a.current_bid, a.expires_at
      FROM sessions s
      LEFT JOIN auctions a ON a.session_id = s.id
      WHERE s.state IN ('active', 'auction')
      ORDER BY s.created_at DESC
      LIMIT 1
    `);
    res.json({ session: rows[0] || null });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/sessions/locked', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.*, a.current_bid as winning_bid, a.current_leader_username as winner_username,
        g.image_url, g.prompt,
        (SELECT COUNT(*) FROM artwork_likes l WHERE l.session_id = s.id)::int AS like_count,
        EXISTS (SELECT 1 FROM artwork_likes l WHERE l.session_id = s.id AND l.user_id = $1) AS liked_by_me
      FROM sessions s
      LEFT JOIN auctions a ON a.session_id = s.id
      LEFT JOIN generated_artworks g ON g.session_id = s.id
      WHERE s.state = 'locked'
      ORDER BY s.locked_at DESC
      LIMIT 50
    `, [req.user.id]);
    res.json({ sessions: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Highest-resolution square we request from the upstream image source.
// (picsum.photos serves up to 5000px; 2048 is a high-res JPEG that stays
// well within that limit and keeps the proxied download reasonably sized.)
const DOWNLOAD_IMAGE_SIZE = 2048;

// Rewrite the stored image URL to request the largest available JPEG.
// picsum.photos takes the size in the path and a `.jpg` suffix forces JPEG
// output, e.g. https://picsum.photos/seed/<seed>/2048/2048.jpg
function toHighResJpegUrl(url) {
  try {
    const u = new URL(url);
    if (u.hostname.endsWith('picsum.photos')) {
      // picsum paths are /seed/<seed>/<w>/<h>, /id/<id>/<w>/<h>, or /<w>/<h>
      // (optionally with a .jpg/.webp suffix). Keep the image selector
      // (seed/id) intact and only swap the trailing dimensions, so we fetch
      // the SAME artwork at a higher resolution rather than a different image.
      const parts = u.pathname.split('/').filter(Boolean);
      let base = [];
      const marker = parts.findIndex(p => p === 'seed' || p === 'id');
      if (marker !== -1 && parts[marker + 1] !== undefined) {
        base = parts.slice(0, marker + 2); // keep ['seed', '<seed>'] or ['id', '<id>']
      }
      u.pathname = '/' + base.concat([String(DOWNLOAD_IMAGE_SIZE), `${DOWNLOAD_IMAGE_SIZE}.jpg`]).join('/');
      return u.toString();
    }
  } catch { /* fall through to original URL */ }
  return url;
}

// Proxy the generated artwork so the browser can save it as a high-resolution
// JPEG (the image is hosted on a remote origin, so a direct <a download> would
// be blocked cross-origin).
app.get('/api/session/:id/image/download', async (req, res) => {
  try {
    const sessionId = parseInt(req.params.id);
    if (!sessionId) return res.status(400).json({ error: 'Invalid session id' });

    const { rows } = await pool.query(
      `SELECT s.name, g.image_url
       FROM generated_artworks g
       JOIN sessions s ON s.id = g.session_id
       WHERE g.session_id = $1
       ORDER BY g.created_at DESC
       LIMIT 1`,
      [sessionId]
    );
    const artwork = rows[0];
    if (!artwork?.image_url) return res.status(404).json({ error: 'Artwork not found' });

    const upstream = await fetch(toHighResJpegUrl(artwork.image_url));
    if (!upstream.ok) return res.status(502).json({ error: 'Failed to fetch image' });

    const safeName = String(artwork.name || 'artwork').replace(/[^a-z0-9_-]+/gi, '_').slice(0, 60) || 'artwork';
    const filename = `${safeName}.jpg`;

    const buf = Buffer.from(await upstream.arrayBuffer());
    res.setHeader('Content-Type', 'image/jpeg');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', buf.length);
    res.send(buf);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/session', async (req, res) => {
  try {
    const { name, words: rawWords } = req.body;
    if (!name?.trim()) return res.status(400).json({ error: 'Name required' });

    const words = parseWords(rawWords);
    if (words.length !== 10) return res.status(400).json({ error: 'Exactly 10 words required' });
    if (words.some(w => w.length > 30)) return res.status(400).json({ error: 'Each word must be 30 characters or fewer' });

    const { rows: existing } = await pool.query(
      `SELECT id FROM sessions WHERE state IN ('active', 'auction') LIMIT 1`
    );
    if (existing.length) return res.status(409).json({ error: 'A session is already active' });

    const { rows } = await pool.query(
      `INSERT INTO sessions (name, creator_user_id, creator_username, state, words) VALUES ($1, $2, $3, 'active', $4) RETURNING *`,
      [name.trim(), req.user.id, req.user.username, words.join(',')]
    );

    io.emit('session-created', { session: rows[0] });
    res.json({ session: rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/bid', async (req, res) => {
  const client = await pool.connect();
  try {
    const { sessionId, amount } = req.body;
    const bidAmount = parseInt(amount);
    if (!bidAmount || bidAmount < 1) return res.status(400).json({ error: 'Invalid bid amount' });

    await client.query('BEGIN');

    const { rows: sessions } = await client.query(
      `SELECT * FROM sessions WHERE id = $1 AND state IN ('active', 'auction') FOR UPDATE`,
      [sessionId]
    );
    if (!sessions.length) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'Session not available for bidding' });
    }

    const { rows: auctions } = await client.query(
      'SELECT * FROM auctions WHERE session_id = $1',
      [sessionId]
    );
    const currentAuction = auctions[0];

    const minBid = currentAuction ? Math.ceil(parseInt(currentAuction.current_bid) * 1.01) : 1;
    if (bidAmount < minBid) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Minimum bid is ${minBid} credits` });
    }

    await ensureUserCredits(req.user.id, req.user.username);
    const { rows: creditRows } = await client.query(
      'SELECT balance FROM user_credits WHERE user_id = $1 FOR UPDATE',
      [req.user.id]
    );
    if (!creditRows.length || parseInt(creditRows[0].balance) < bidAmount) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Insufficient credits' });
    }

    await client.query('UPDATE user_credits SET balance = balance - $1 WHERE user_id = $2', [bidAmount, req.user.id]);

    if (currentAuction) {
      await client.query(
        'UPDATE user_credits SET balance = balance + $1 WHERE user_id = $2',
        [currentAuction.current_bid, currentAuction.current_leader_user_id]
      );
    }

    await client.query(
      'INSERT INTO bids (session_id, user_id, username, amount) VALUES ($1, $2, $3, $4)',
      [sessionId, req.user.id, req.user.username, bidAmount]
    );

    const expiresAt = new Date(Date.now() + 30000);

    if (currentAuction) {
      await client.query(
        `UPDATE auctions SET current_leader_user_id=$1, current_leader_username=$2, current_bid=$3, expires_at=$4 WHERE session_id=$5`,
        [req.user.id, req.user.username, bidAmount, expiresAt, sessionId]
      );
    } else {
      await client.query(
        `INSERT INTO auctions (session_id, current_leader_user_id, current_leader_username, current_bid, expires_at) VALUES ($1,$2,$3,$4,$5)`,
        [sessionId, req.user.id, req.user.username, bidAmount, expiresAt]
      );
    }

    await client.query(`UPDATE sessions SET state = 'auction' WHERE id = $1`, [sessionId]);
    await client.query('COMMIT');

    const { rows: newCredits } = await pool.query('SELECT balance FROM user_credits WHERE user_id = $1', [req.user.id]);

    startAuctionTimer(sessionId, expiresAt);

    const minNextBid = Math.ceil(bidAmount * 1.01);
    io.to(`session:${sessionId}`).emit('bid-placed', {
      sessionId,
      userId: req.user.id,
      username: req.user.username,
      amount: bidAmount,
      expiresAt,
      minNextBid,
      state: 'auction'
    });

    res.json({ ok: true, credits: newCredits[0]?.balance });
  } catch (err) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.post('/api/session/:id/change-word', async (req, res) => {
  const client = await pool.connect();
  try {
    const sessionId = parseInt(req.params.id);
    const { wordIndex, newWord, bidAmount: rawBid } = req.body;
    const bidAmount = parseInt(rawBid);

    if (wordIndex === undefined || wordIndex < 0 || wordIndex > 9) {
      return res.status(400).json({ error: 'wordIndex must be 0–9' });
    }
    const trimmedWord = String(newWord || '').trim();
    if (!trimmedWord) return res.status(400).json({ error: 'Word cannot be empty' });
    if (trimmedWord.length > 30) return res.status(400).json({ error: 'Word must be 30 characters or fewer' });
    if (!bidAmount || bidAmount < 1) return res.status(400).json({ error: 'Invalid bid amount' });

    await client.query('BEGIN');

    const { rows: sessions } = await client.query(
      `SELECT * FROM sessions WHERE id = $1 AND state IN ('active', 'auction') FOR UPDATE`,
      [sessionId]
    );
    if (!sessions.length) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'Session not available' });
    }
    const session = sessions[0];

    const { rows: auctions } = await client.query(
      'SELECT * FROM auctions WHERE session_id = $1',
      [sessionId]
    );
    const currentAuction = auctions[0];

    const minBid = currentAuction ? Math.ceil(parseInt(currentAuction.current_bid) * 1.01) : 1;
    if (bidAmount < minBid) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `Minimum bid is ${minBid} credits` });
    }

    await ensureUserCredits(req.user.id, req.user.username);
    const { rows: creditRows } = await client.query(
      'SELECT balance FROM user_credits WHERE user_id = $1 FOR UPDATE',
      [req.user.id]
    );
    const balance = parseInt(creditRows[0]?.balance ?? 0);
    if (balance < bidAmount + 1) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Insufficient credits' });
    }

    // Deduct bid + 1 credit word-change fee
    await client.query('UPDATE user_credits SET balance = balance - $1 WHERE user_id = $2', [bidAmount + 1, req.user.id]);

    if (currentAuction) {
      await client.query(
        'UPDATE user_credits SET balance = balance + $1 WHERE user_id = $2',
        [currentAuction.current_bid, currentAuction.current_leader_user_id]
      );
    }

    const wordArr = (session.words || '').split(',');
    wordArr[wordIndex] = trimmedWord;
    const newWordsStr = wordArr.join(',');
    await client.query('UPDATE sessions SET words = $1 WHERE id = $2', [newWordsStr, sessionId]);

    await client.query(
      'INSERT INTO bids (session_id, user_id, username, amount) VALUES ($1, $2, $3, $4)',
      [sessionId, req.user.id, req.user.username, bidAmount]
    );

    const expiresAt = new Date(Date.now() + 30000);

    if (currentAuction) {
      await client.query(
        `UPDATE auctions SET current_leader_user_id=$1, current_leader_username=$2, current_bid=$3, expires_at=$4 WHERE session_id=$5`,
        [req.user.id, req.user.username, bidAmount, expiresAt, sessionId]
      );
    } else {
      await client.query(
        `INSERT INTO auctions (session_id, current_leader_user_id, current_leader_username, current_bid, expires_at) VALUES ($1,$2,$3,$4,$5)`,
        [sessionId, req.user.id, req.user.username, bidAmount, expiresAt]
      );
    }

    await client.query(`UPDATE sessions SET state = 'auction' WHERE id = $1`, [sessionId]);
    await client.query('COMMIT');

    const { rows: newCreditRows } = await pool.query('SELECT balance FROM user_credits WHERE user_id = $1', [req.user.id]);
    const newBalance = newCreditRows[0]?.balance ?? 0;

    startAuctionTimer(sessionId, expiresAt);

    const minNextBid = Math.ceil(bidAmount * 1.01);

    io.to(`session:${sessionId}`).emit('word-changed', {
      sessionId,
      words: newWordsStr.split(','),
      changedBy: req.user.username
    });

    io.to(`session:${sessionId}`).emit('bid-placed', {
      sessionId,
      userId: req.user.id,
      username: req.user.username,
      amount: bidAmount,
      expiresAt,
      minNextBid,
      state: 'auction'
    });

    res.json({ ok: true, words: newWordsStr.split(','), newBalance, highestBid: bidAmount });
  } catch (err) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

app.post('/api/generate', async (req, res) => {
  try {
    const { sessionId } = req.body;

    const { rows: sessions } = await pool.query(
      `SELECT * FROM sessions WHERE id = $1 AND state = 'auction'`,
      [sessionId]
    );
    if (!sessions.length) return res.status(409).json({ error: 'Session not in auction state' });

    const session = sessions[0];

    const { rows: auctions } = await pool.query('SELECT * FROM auctions WHERE session_id = $1', [sessionId]);
    if (!auctions.length) return res.status(404).json({ error: 'Auction not found' });

    const auction = auctions[0];
    if (auction.current_leader_user_id !== req.user.id) {
      return res.status(403).json({ error: 'Only the auction winner can generate artwork' });
    }
    if (new Date(auction.expires_at) > new Date()) {
      return res.status(409).json({ error: 'Auction has not ended yet' });
    }

    const wordsStr = session.words || '';
    const prompt = `A beautiful artwork inspired by: ${wordsStr}`;
    const imageUrl = `https://picsum.photos/seed/${sessionId}/512/512`;

    await pool.query(
      `INSERT INTO generated_artworks (session_id, owner_user_id, owner_username, image_url, prompt, canvas_snapshot)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [sessionId, req.user.id, req.user.username, imageUrl, prompt, null]
    );

    await pool.query(`UPDATE sessions SET state = 'locked', locked_at = NOW() WHERE id = $1`, [sessionId]);

    io.emit('session-locked', {
      sessionId,
      imageUrl,
      prompt,
      words: wordsStr.split(','),
      winnerUsername: req.user.username
    });

    res.json({ ok: true, imageUrl, prompt });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/session/:id/like', async (req, res) => {
  try {
    const sessionId = parseInt(req.params.id);
    if (!sessionId) return res.status(400).json({ error: 'Invalid session id' });

    const { rows: sessions } = await pool.query(
      `SELECT id FROM sessions WHERE id = $1 AND state = 'locked'`,
      [sessionId]
    );
    if (!sessions.length) return res.status(404).json({ error: 'Artwork not found' });

    // Toggle: try to insert; if the like already existed, remove it instead.
    const { rowCount } = await pool.query(
      `INSERT INTO artwork_likes (session_id, user_id, username) VALUES ($1, $2, $3)
       ON CONFLICT (session_id, user_id) DO NOTHING`,
      [sessionId, req.user.id, req.user.username]
    );

    let liked;
    if (rowCount === 0) {
      await pool.query(
        `DELETE FROM artwork_likes WHERE session_id = $1 AND user_id = $2`,
        [sessionId, req.user.id]
      );
      liked = false;
    } else {
      liked = true;
    }

    const { rows: countRows } = await pool.query(
      `SELECT COUNT(*)::int AS like_count FROM artwork_likes WHERE session_id = $1`,
      [sessionId]
    );

    res.json({ liked, likeCount: countRows[0].like_count });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Suggest creative prompts derived from the user's own activity. Always returns
// `count` suggestions (each with 10 submittable words); `offset` cycles "next".
app.get('/api/prompts/suggest', async (req, res) => {
  try {
    const count  = Math.min(Math.max(parseInt(req.query.count) || 3, 1), 6);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    const { suggestions, basis } = await generateSuggestions(req.user.id, count, offset);

    // Log served prompts so a future "used" mark can reference them. Best-effort:
    // if the insert fails the feature still works (the table is optional).
    for (const s of suggestions) {
      try {
        const { rows } = await pool.query(
          `INSERT INTO creative_prompts (user_id, username, title, words, source) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
          [req.user.id, req.user.username, s.title, s.words.join(','), s.source]
        );
        s.id = rows[0].id;
      } catch (_) { /* logging table optional — proceed without an id */ }
      delete s.personalUsed;
    }

    res.json({ suggestions, basis });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Mark a previously-served prompt as used (it populated a created session).
app.post('/api/prompts/:id/used', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid prompt id' });
    await pool.query(
      `UPDATE creative_prompts SET used = true WHERE id = $1 AND user_id = $2`,
      [id, req.user.id]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.on('join-session', (sessionId) => {
    socket.join(`session:${sessionId}`);
  });
});

// ─── Static + HTML shell ──────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

app.get('*', (req, res) => {
  if (!req.user) {
    return res.status(401).send(`<!doctype html><meta charset=utf-8><title>Open in Usernode</title>
<body style="font-family:system-ui;background:#09090b;color:#e4e4e7;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0">
  <div style="max-width:24rem;padding:2rem;text-align:center">
    <h1 style="font-size:1.25rem;margin:0 0 0.5rem">Open this app inside Usernode</h1>
    <p style="color:#a1a1aa;font-size:0.9rem;margin:0 0 1.25rem">This page is served via the platform; direct visits aren't authenticated.</p>
    <a href="https://social-vibecoding.usernodelabs.org" style="display:inline-block;padding:0.5rem 1rem;background:#7c3aed;color:white;border-radius:0.5rem;text-decoration:none;font-size:0.9rem">Go to Usernode</a>
  </div>
</body>`);
  }
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── DB init ──────────────────────────────────────────────────────────────────
async function start() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      creator_user_id INTEGER NOT NULL,
      creator_username VARCHAR(255) NOT NULL,
      state VARCHAR(20) NOT NULL DEFAULT 'active',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      locked_at TIMESTAMPTZ
    )
  `);
  await pool.query(`ALTER TABLE sessions ADD COLUMN IF NOT EXISTS words TEXT`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_credits (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL UNIQUE,
      username VARCHAR(255) NOT NULL,
      balance INTEGER NOT NULL DEFAULT 1000
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS canvas_strokes (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL,
      username VARCHAR(255) NOT NULL,
      stroke_data JSONB NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bids (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL,
      username VARCHAR(255) NOT NULL,
      amount INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS auctions (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL UNIQUE REFERENCES sessions(id) ON DELETE CASCADE,
      current_leader_user_id INTEGER NOT NULL,
      current_leader_username VARCHAR(255) NOT NULL,
      current_bid INTEGER NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS generated_artworks (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL UNIQUE REFERENCES sessions(id) ON DELETE CASCADE,
      owner_user_id INTEGER NOT NULL,
      owner_username VARCHAR(255) NOT NULL,
      image_url TEXT NOT NULL,
      prompt TEXT,
      canvas_snapshot TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS artwork_likes (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL,
      username VARCHAR(255) NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (session_id, user_id)
    )
  `);
  // Public by default — rows hold only a user's own derived word lists (no auth
  // material, DMs, or financial data), so the stranger test passes.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS creative_prompts (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      username VARCHAR(255) NOT NULL,
      title TEXT,
      words TEXT,
      source VARCHAR(20),
      used BOOLEAN DEFAULT false,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Resume active auction if server restarted
  const { rows: activeAuctions } = await pool.query(`
    SELECT a.* FROM auctions a
    JOIN sessions s ON s.id = a.session_id
    WHERE s.state = 'auction'
    LIMIT 1
  `);
  if (activeAuctions.length) {
    const auction = activeAuctions[0];
    if (new Date(auction.expires_at) > new Date()) {
      startAuctionTimer(auction.session_id, auction.expires_at);
    } else {
      handleAuctionEnd(auction.session_id);
    }
  }

  server.listen(port, () => console.log(`Listening on :${port}`));
}

start().catch(err => { console.error(err); process.exit(1); });
