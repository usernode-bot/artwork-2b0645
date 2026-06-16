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
const IS_STAGING = process.env.USERNODE_ENV === 'staging';

// --- Replicate text-to-image generation -----------------------------------
// The artwork image is generated from the session's 10 words via Replicate's
// HTTP predictions API (no SDK — plain fetch). REPLICATE_API_KEY is declared in
// dapp.json as optional; when it's absent (e.g. local dev) we fall back to the
// picsum stub below so the app still works without a key.
const REPLICATE_API_KEY = process.env.REPLICATE_API_KEY;
// Pinned model version so output style / input schema don't drift under us.
// stability-ai/sdxl — a fast, inexpensive text-to-image model.
const REPLICATE_MODEL_VERSION = '7762fd07cf82c948538e41f63f77d685e02b063e37e496e96eefd46c929f9bda';
// Square edge requested from the model. 1024 front-loads enough resolution that
// the OG card (1200) and download proxy (2048) don't need provider-side upscaling.
const GENERATED_IMAGE_SIZE = 1024;
// Overall budget for create + poll before we give up and let the winner retry.
const REPLICATE_TIMEOUT_MS = 60000;
const REPLICATE_POLL_INTERVAL_MS = 1500;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Generate an image from `prompt` via Replicate and return its hosted URL.
// Throws on auth/API error, failed prediction, or timeout — callers treat any
// throw as "generation failed, leave the session recoverable".
async function generateImageWithReplicate(prompt) {
  const startedAt = Date.now();
  const createRes = await fetch('https://api.replicate.com/v1/predictions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${REPLICATE_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      version: REPLICATE_MODEL_VERSION,
      input: {
        prompt,
        width: GENERATED_IMAGE_SIZE,
        height: GENERATED_IMAGE_SIZE,
      },
    }),
  });

  if (!createRes.ok) {
    const detail = await createRes.text().catch(() => '');
    throw new Error(`Replicate create failed (${createRes.status}): ${detail.slice(0, 300)}`);
  }

  let prediction = await createRes.json();
  const pollUrl = prediction?.urls?.get;

  while (prediction.status !== 'succeeded') {
    if (prediction.status === 'failed' || prediction.status === 'canceled') {
      throw new Error(`Replicate prediction ${prediction.status}: ${prediction.error || 'no detail'}`);
    }
    if (Date.now() - startedAt > REPLICATE_TIMEOUT_MS) {
      throw new Error('Replicate prediction timed out');
    }
    if (!pollUrl) throw new Error('Replicate response missing poll URL');
    await sleep(REPLICATE_POLL_INTERVAL_MS);
    const pollRes = await fetch(pollUrl, {
      headers: { 'Authorization': `Bearer ${REPLICATE_API_KEY}` },
    });
    if (!pollRes.ok) {
      const detail = await pollRes.text().catch(() => '');
      throw new Error(`Replicate poll failed (${pollRes.status}): ${detail.slice(0, 300)}`);
    }
    prediction = await pollRes.json();
  }

  // SDXL returns an array of output URLs; some models return a bare string.
  const output = prediction.output;
  const imageUrl = Array.isArray(output) ? output[0] : output;
  if (!imageUrl || typeof imageUrl !== 'string') {
    throw new Error('Replicate succeeded but returned no image URL');
  }
  return imageUrl;
}

const PUBLIC_API_PATHS = new Set(['/health']);
// `/share-api/*` is intentionally public: it backs the unauthenticated
// per-artwork share landing page (`GET /a/:id`), which must be reachable
// by OG/Twitter crawlers that cannot present a platform token. It only
// exposes already-public, locked (completed) artwork rows.
const PUBLIC_PREFIXES = ['/explorer-api/', '/share-api/'];

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

// ─── Science Zone classification ────────────────────────────────────────────────
// Curated (intentionally NOT exhaustive) vocabulary of science terms. A locked
// artwork qualifies for the Science Zone when at least one of its 5 inspiration
// words matches an entry here. Classification is computed at query time from the
// existing `sessions.words` string — no schema change, and all past artworks are
// covered retroactively. Broadening "what counts as science" is a one-line edit.
const SCIENCE_WORDS = new Set([
  // physics
  'atom', 'molecule', 'quantum', 'gravity', 'electron', 'proton', 'neutron',
  'photon', 'particle', 'energy', 'force', 'magnet', 'magnetism', 'plasma',
  'laser', 'radiation', 'velocity', 'momentum', 'entropy', 'relativity',
  // chemistry
  'chemistry', 'chemical', 'element', 'compound', 'reaction', 'acid', 'base',
  'crystal', 'isotope', 'catalyst', 'enzyme', 'protein', 'polymer',
  // biology
  'biology', 'cell', 'dna', 'rna', 'gene', 'genome', 'neuron', 'brain',
  'evolution', 'fossil', 'bacteria', 'virus', 'microbe', 'organism', 'mitochondria',
  'photosynthesis', 'ecosystem', 'species', 'chromosome',
  // astronomy / space
  'galaxy', 'planet', 'star', 'nebula', 'cosmos', 'cosmic', 'comet', 'asteroid',
  'meteor', 'orbit', 'gravity', 'blackhole', 'supernova', 'telescope', 'satellite',
  'space', 'universe', 'astronomy', 'astronaut', 'rocket', 'lunar', 'solar',
  // earth / general
  'geology', 'volcano', 'mineral', 'electricity', 'circuit', 'microscope',
  'experiment', 'laboratory', 'science', 'scientific', 'physics', 'mathematics',
  'equation', 'algorithm', 'data', 'robot', 'spectrum', 'frequency'
]);

// True if any of the artwork's comma-joined words is a science term. Matching is
// case-insensitive and tolerates a single trailing 's' plural (e.g. "atoms").
function isScienceArtwork(wordsStr) {
  const words = String(wordsStr || '').split(',').map(w => w.trim().toLowerCase()).filter(Boolean);
  return words.some(w => {
    if (SCIENCE_WORDS.has(w)) return true;
    if (w.endsWith('s') && SCIENCE_WORDS.has(w.slice(0, -1))) return true;
    return false;
  });
}

// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/me', async (req, res) => {
  try {
    await ensureUserCredits(req.user.id, req.user.username);
    const { rows } = await pool.query('SELECT balance FROM user_credits WHERE user_id = $1', [req.user.id]);
    res.json({
      id: req.user.id,
      username: req.user.username,
      credits: rows[0]?.balance ?? 1000,
      // On-chain wallet address (ut1…) or null if the user hasn't linked one.
      // Distinct from `credits`, which is the in-app bidding balance.
      usernode_pubkey: req.user.usernode_pubkey ?? null,
      isStaging: IS_STAGING,
      // True when no Replicate key is configured, so generation falls back to
      // the picsum stub (images unrelated to the words). Exposes only the
      // boolean, never the key value. Lets the UI warn that output is a
      // placeholder until a key is set.
      generationStubbed: !REPLICATE_API_KEY
    });
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

// Archive list. Despite the `/locked` path (kept for backwards compat), this
// endpoint supports filtering across all session states via query params:
//   q      — case-insensitive substring match on name OR creator_username
//   status — 'completed' (default, state=locked) | 'in_progress' (active/auction) | 'all'
//   sort   — 'newest' (default, created_at DESC) | 'oldest' (created_at ASC)
app.get('/api/sessions/locked', async (req, res) => {
  try {
    // $1 is always req.user.id (used by the liked_by_me subquery). Additional
    // bound params (e.g. the search term) are appended dynamically.
    const params = [req.user.id];
    const where = [];

    const status = String(req.query.status || 'completed');
    if (status === 'in_progress') {
      where.push(`s.state IN ('active', 'auction')`);
    } else if (status === 'all') {
      /* no state restriction */
    } else {
      // 'completed' (default) and any unknown value fall back to locked only.
      where.push(`s.state = 'locked'`);
    }

    const q = String(req.query.q || '').trim();
    if (q) {
      params.push(`%${q}%`);
      const p = `$${params.length}`;
      where.push(`(s.name ILIKE ${p} OR s.creator_username ILIKE ${p})`);
    }

    const order = String(req.query.sort) === 'oldest' ? 'ASC' : 'DESC';
    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const { rows } = await pool.query(`
      SELECT s.*, a.current_bid as winning_bid, a.current_leader_username as winner_username,
        g.image_url, g.prompt, g.owner_pubkey, g.owner_username, g.source,
        (SELECT COUNT(*) FROM artwork_likes l WHERE l.session_id = s.id)::int AS like_count,
        EXISTS (SELECT 1 FROM artwork_likes l WHERE l.session_id = s.id AND l.user_id = $1) AS liked_by_me,
        (SELECT COALESCE(SUM(amount), 0) FROM gifts gf WHERE gf.session_id = s.id AND gf.status <> 'failed') AS gift_total,
        (SELECT COUNT(*) FROM artwork_comments c WHERE c.session_id = s.id)::int AS comment_count
      FROM sessions s
      LEFT JOIN auctions a ON a.session_id = s.id
      LEFT JOIN generated_artworks g ON g.session_id = s.id
      ${whereSql}
      ORDER BY s.created_at ${order}
      LIMIT 50
    `, params);
    res.json({ sessions: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Science Zone gallery: same shape as /api/sessions/locked, filtered to
// science-themed artworks. We pull a generous window of recent locked rows,
// filter by word in JS (keeping the vocabulary in one place — a comma-string
// SQL match would be brittle), then cap the response at 50 like the archive.
app.get('/api/artworks/science', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.*, a.current_bid as winning_bid, a.current_leader_username as winner_username,
        g.image_url, g.prompt, g.owner_pubkey, g.owner_username,
        (SELECT COUNT(*) FROM artwork_likes l WHERE l.session_id = s.id)::int AS like_count,
        EXISTS (SELECT 1 FROM artwork_likes l WHERE l.session_id = s.id AND l.user_id = $1) AS liked_by_me,
        (SELECT COALESCE(SUM(amount), 0) FROM gifts gf WHERE gf.session_id = s.id AND gf.status <> 'failed') AS gift_total,
        (SELECT COUNT(*) FROM artwork_comments c WHERE c.session_id = s.id)::int AS comment_count
      FROM sessions s
      LEFT JOIN auctions a ON a.session_id = s.id
      LEFT JOIN generated_artworks g ON g.session_id = s.id
      WHERE s.state = 'locked'
      ORDER BY s.locked_at DESC
      LIMIT 300
    `, [req.user.id]);
    const science = rows.filter(r => isScienceArtwork(r.words)).slice(0, 50);
    res.json({ sessions: science });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Target square edge for the high-res download. This only applies to picsum
// stub URLs (legacy / no-key fallback), which support path-based resizing.
// Real provider URLs (e.g. Replicate's replicate.delivery) don't support this,
// so they're served at their native generation resolution (GENERATED_IMAGE_SIZE).
const DOWNLOAD_IMAGE_SIZE = 2048;

// For picsum stub URLs, rewrite the path to request a higher-resolution JPEG
// (picsum takes the size in the path and a `.jpg` suffix forces JPEG output,
// e.g. https://picsum.photos/seed/<seed>/2048/2048.jpg). For any other host the
// URL is returned unchanged — the provider serves the image as generated.
function toHighResJpegUrl(url, size = DOWNLOAD_IMAGE_SIZE) {
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
      u.pathname = '/' + base.concat([String(size), `${size}.jpg`]).join('/');
      return u.toString();
    }
  } catch { /* fall through to original URL */ }
  return url;
}

// Square edge for the Open Graph / Twitter card image. summary_large_image
// accepts a square and crops; 1200 is the standard large-card width.
const OG_IMAGE_SIZE = 1200;

// Escape a value for safe interpolation into server-rendered HTML, including
// inside double-quoted attributes (e.g. <meta content="…">). This is the
// server-side counterpart to the client's esc(); it must not be skipped for
// any user-controlled value (session name, usernames, words, prompt).
function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Absolute public origin for this request. Each staging/prod container has
// its own subdomain, so derive it per request from forwarded headers rather
// than hardcoding a domain. A PUBLIC_BASE_URL env var overrides when set.
function publicOrigin(req) {
  if (process.env.PUBLIC_BASE_URL) return process.env.PUBLIC_BASE_URL.replace(/\/+$/, '');
  const proto = (req.headers['x-forwarded-proto'] || req.protocol || 'https').split(',')[0].trim();
  const host = (req.headers['x-forwarded-host'] || req.get('host') || '').split(',')[0].trim();
  return `${proto}://${host}`;
}

// Fetch a single locked (completed) artwork by session id, in the same shape
// the archive uses. Returns null if the session is missing or not locked.
async function getLockedArtwork(sessionId) {
  if (!sessionId) return null;
  const { rows } = await pool.query(`
    SELECT s.id, s.name, s.creator_username, s.state, s.words,
      a.current_bid AS winning_bid, a.current_leader_username AS winner_username,
      g.image_url, g.prompt, g.source,
      (SELECT COUNT(*) FROM artwork_likes l WHERE l.session_id = s.id)::int AS like_count
    FROM sessions s
    LEFT JOIN auctions a ON a.session_id = s.id
    LEFT JOIN generated_artworks g ON g.session_id = s.id
    WHERE s.id = $1 AND s.state = 'locked'
    LIMIT 1
  `, [sessionId]);
  return rows[0] || null;
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
    // Forward the upstream Content-Type rather than assuming JPEG — the image
    // provider (e.g. Replicate) may serve PNG/WebP.
    res.setHeader('Content-Type', upstream.headers.get('content-type') || 'image/jpeg');
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
    if (words.length !== 5) return res.status(400).json({ error: 'Exactly 5 words required' });
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

    if (wordIndex === undefined || wordIndex < 0 || wordIndex > 4) {
      return res.status(400).json({ error: 'wordIndex must be 0–4' });
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
    // Build the prompt from the session's comma-joined words. Guard against an
    // all-whitespace `words` value with a generic fallback so we never send an
    // empty prompt to the model.
    const cleanWords = wordsStr.split(',').map((w) => w.trim()).filter(Boolean);
    const prompt = cleanWords.length
      ? `A beautiful artwork inspired by: ${cleanWords.join(', ')}`
      : 'A beautiful abstract artwork';

    // Real generation when a key is configured; otherwise fall back to the
    // picsum stub so the app still works locally without a Replicate key.
    let imageUrl;
    if (REPLICATE_API_KEY) {
      try {
        imageUrl = await generateImageWithReplicate(prompt);
      } catch (err) {
        // Leave the session in `auction` state so the winner can retry. We have
        // not locked the session or written any row yet, so this is recoverable.
        console.error('Replicate generation failed:', err.message);
        return res.status(502).json({ error: 'Image generation failed, please try again' });
      }
    } else {
      console.warn('REPLICATE_API_KEY not set — using picsum stub for artwork generation');
      imageUrl = `https://picsum.photos/seed/${sessionId}/512/512`;
    }

    // Only persist + lock + broadcast once we have a real image URL in hand.
    await pool.query(
      `INSERT INTO generated_artworks (session_id, owner_user_id, owner_username, image_url, prompt, canvas_snapshot, owner_pubkey, source)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'ai')`,
      [sessionId, req.user.id, req.user.username, imageUrl, prompt, null, req.user.usernode_pubkey ?? null]
    );

    await pool.query(`UPDATE sessions SET state = 'locked', locked_at = NOW() WHERE id = $1`, [sessionId]);

    io.emit('session-locked', {
      sessionId,
      imageUrl,
      prompt,
      words: wordsStr.split(','),
      winnerUsername: req.user.username,
      ownerUserId: req.user.id,
      ownerPubkey: req.user.usernode_pubkey ?? null,
      source: 'ai'
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

// Send an on-chain gift/tip to the owner of a finished artwork. The actual
// wallet-to-wallet transfer happens client-side via the Usernode bridge; this
// endpoint records the gift after the bridge returns a tx hash. Gifts are real
// on-chain value and are completely separate from the in-app `user_credits`
// balance used for bidding.
app.post('/api/session/:id/gift', async (req, res) => {
  try {
    const sessionId = parseInt(req.params.id);
    if (!sessionId) return res.status(400).json({ error: 'Invalid session id' });

    const { amount: rawAmount, txHash, toPubkey, chainId } = req.body;
    const amount = Number(rawAmount);
    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ error: 'Gift amount must be a positive number' });
    }

    // The artwork must exist and be locked (giftable).
    const { rows } = await pool.query(
      `SELECT g.owner_user_id, g.owner_username, g.owner_pubkey
       FROM generated_artworks g
       JOIN sessions s ON s.id = g.session_id
       WHERE g.session_id = $1 AND s.state = 'locked'`,
      [sessionId]
    );
    const artwork = rows[0];
    if (!artwork) return res.status(404).json({ error: 'Artwork not found' });
    if (!artwork.owner_pubkey) {
      return res.status(409).json({ error: "This creator hasn't linked a wallet yet" });
    }

    // Trust the server-stored recipient, not the client-supplied one.
    if (toPubkey && toPubkey !== artwork.owner_pubkey) {
      return res.status(409).json({ error: 'Recipient wallet mismatch' });
    }
    // Server-side self-gift guard.
    if (req.user.usernode_pubkey && req.user.usernode_pubkey === artwork.owner_pubkey) {
      return res.status(400).json({ error: "You can't gift your own artwork" });
    }

    // In staging no real funds move, so the client sends a synthetic tx hash —
    // accept it and record the gift so totals populate for demos. In prod we
    // trust the bridge-returned tx hash (see spec: stricter verification is
    // deferred work).
    if (!IS_STAGING && !txHash) {
      return res.status(400).json({ error: 'Missing transaction hash' });
    }
    const status = 'confirmed';

    await pool.query(
      `INSERT INTO gifts (session_id, from_user_id, from_username, from_pubkey, to_user_id, to_username, to_pubkey, amount, tx_hash, chain_id, status)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
      [
        sessionId,
        req.user.id,
        req.user.username,
        req.user.usernode_pubkey ?? null,
        artwork.owner_user_id,
        artwork.owner_username,
        artwork.owner_pubkey,
        amount,
        txHash || null,
        chainId || null,
        status
      ]
    );

    const { rows: totalRows } = await pool.query(
      `SELECT COALESCE(SUM(amount), 0) AS gift_total FROM gifts WHERE session_id = $1 AND status <> 'failed'`,
      [sessionId]
    );

    res.json({ ok: true, giftTotal: Number(totalRows[0].gift_total) });
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

// ─── Public share endpoints ─────────────────────────────────────────────────
// These are reachable WITHOUT a platform token so OG/Twitter crawlers (which
// can't authenticate) can render rich previews. They expose only locked,
// already-public artwork rows, and are registered BEFORE the static middleware
// and the auth-gated `app.get('*')` catch-all so they bypass that gate.

// Public JSON for one locked artwork (prefix is in PUBLIC_PREFIXES).
app.get('/share-api/:id', async (req, res) => {
  try {
    const artwork = await getLockedArtwork(parseInt(req.params.id));
    if (!artwork) return res.status(404).json({ error: 'Artwork not found' });
    res.json({
      id: artwork.id,
      name: artwork.name,
      creator_username: artwork.creator_username,
      winner_username: artwork.winner_username,
      winning_bid: artwork.winning_bid != null ? parseInt(artwork.winning_bid) : null,
      image_url: artwork.image_url,
      prompt: artwork.prompt,
      words: (artwork.words || '').split(',').filter(Boolean),
      like_count: artwork.like_count || 0,
      source: artwork.source || 'ai'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Minimal public page used for missing/not-yet-locked artworks. Avoids leaking
// the auth-gate "Open in Usernode" 401 for a bad share id.
function renderShareNotFound(res) {
  res.status(404).type('html').send(`<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Artwork not found</title>
<style>body{font-family:'Inter',system-ui,sans-serif;background:#09090b;color:#e4e4e7;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.card{max-width:24rem;padding:2rem;text-align:center}h1{font-size:1.25rem;margin:0 0 .5rem}p{color:#a1a1aa;font-size:.9rem;margin:0 0 1.25rem}
a{display:inline-block;padding:.6rem 1.1rem;background:#7c3aed;color:#fff;border-radius:.6rem;text-decoration:none;font-size:.9rem;font-weight:600}</style>
</head><body><div class="card"><div style="font-size:2.5rem;margin-bottom:.5rem">🖼️</div>
<h1>Artwork not found</h1><p>This artwork doesn't exist or hasn't been completed yet.</p>
<a href="https://social-vibecoding.usernodelabs.org">Go to Usernode</a></div></body></html>`);
}

// Public, server-rendered share landing page for one artwork.
app.get('/a/:id', async (req, res) => {
  try {
    const artwork = await getLockedArtwork(parseInt(req.params.id));
    if (!artwork) return renderShareNotFound(res);

    const origin = publicOrigin(req);
    const shareUrl = `${origin}/a/${artwork.id}`;
    const words = (artwork.words || '').split(',').filter(Boolean);

    const title = `${artwork.name} — Artwork`;
    const description = words.length
      ? words.join(' · ')
      : (artwork.prompt || 'A collaborative AI artwork on Usernode.');
    const ogImage = artwork.image_url ? toHighResJpegUrl(artwork.image_url, OG_IMAGE_SIZE) : '';

    // Pre-escape everything user-controlled for HTML/attribute contexts.
    const eTitle = escapeHtml(title);
    const eDesc = escapeHtml(description);
    const eName = escapeHtml(artwork.name);
    const eCreator = escapeHtml(artwork.creator_username);
    const eWinner = escapeHtml(artwork.winner_username || '');
    const eShareUrl = escapeHtml(shareUrl);
    const eOgImage = escapeHtml(ogImage);
    const eImg = escapeHtml(artwork.image_url || '');
    const eUsernodeUrl = 'https://social-vibecoding.usernodelabs.org';

    const ogImageTags = ogImage
      ? `<meta property="og:image" content="${eOgImage}">
  <meta property="og:image:width" content="${OG_IMAGE_SIZE}">
  <meta property="og:image:height" content="${OG_IMAGE_SIZE}">
  <meta name="twitter:image" content="${eOgImage}">`
      : '';

    const wordChips = words.map(w =>
      `<span class="chip">${escapeHtml(w)}</span>`).join('');

    const winnerLine = artwork.winner_username
      ? `<p class="meta">🏆 <span class="hl">@${eWinner}</span>${artwork.winning_bid != null ? ` · ${parseInt(artwork.winning_bid)} credits` : ''}</p>`
      : '';

    const imgBlock = artwork.image_url
      ? `<img class="art" src="${eImg}" alt="${eName}">`
      : '';

    // Provenance badge — 'human' shows the warm "Human-made" pill; anything
    // else (including 'ai', null, or an unknown value) falls back to the
    // violet "AI-generated" pill.
    const isHuman = artwork.source === 'human';
    const badgeLabel = isHuman ? '🎨 Human-made' : '🤖 AI-generated';
    const badgeTitle = isHuman ? 'Human-made artwork' : 'AI-generated artwork';
    const badgeClass = isHuman ? 'badge badge-human' : 'badge badge-ai';
    const sourceBadge = `<span class="${badgeClass}" title="${escapeHtml(badgeTitle)}" aria-label="${escapeHtml(badgeTitle)}">${badgeLabel}</span>`;

    res.type('html').send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${eTitle}</title>
  <meta name="description" content="${eDesc}">

  <meta property="og:type" content="article">
  <meta property="og:site_name" content="Artwork">
  <meta property="og:title" content="${eTitle}">
  <meta property="og:description" content="${eDesc}">
  <meta property="og:url" content="${eShareUrl}">
  ${ogImageTags}

  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${eTitle}">
  <meta name="twitter:description" content="${eDesc}">

  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root { --accent:#7c3aed; }
    * { box-sizing: border-box; }
    body { font-family:'Inter',system-ui,-apple-system,sans-serif; background:#09090b; color:#fafafa;
      margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center; padding:24px; }
    .wrap { width:100%; max-width:480px; }
    .brand { font-size:.8rem; font-weight:600; letter-spacing:.08em; text-transform:uppercase; color:#a1a1aa; margin:0 0 16px; }
    .card { background:#18181b; border:1px solid #27272a; border-radius:16px; overflow:hidden; box-shadow:0 20px 50px rgba(0,0,0,.5); }
    .art { display:block; width:100%; aspect-ratio:1/1; object-fit:cover; background:#27272a; }
    .body { padding:20px; }
    h1 { font-size:1.35rem; font-weight:700; margin:0 0 4px; letter-spacing:-.01em; }
    .meta { font-size:.85rem; color:#a1a1aa; margin:2px 0; }
    .hl { color:#c4b5fd; }
    .chips { display:flex; flex-wrap:wrap; gap:6px; margin:14px 0 4px; }
    .chip { display:inline-flex; padding:5px 12px; border-radius:9999px; font-size:.78rem; font-weight:500;
      background:rgba(124,58,237,.15); border:1px solid rgba(124,58,237,.35); color:#c4b5fd; }
    .badge { display:inline-flex; align-items:center; gap:4px; padding:4px 10px; border-radius:9999px;
      font-size:.72rem; font-weight:600; margin:6px 0 2px; }
    .badge-ai { background:rgba(124,58,237,.15); border:1px solid rgba(124,58,237,.4); color:#c4b5fd; }
    .badge-human { background:rgba(245,158,11,.15); border:1px solid rgba(245,158,11,.4); color:#fcd34d; }
    .cta { display:block; text-align:center; margin-top:20px; padding:12px 16px; background:var(--accent);
      color:#fff; border-radius:10px; text-decoration:none; font-size:.92rem; font-weight:600; transition:background .12s; }
    .cta:hover { background:#8b5cf6; }
  </style>
</head>
<body>
  <div class="wrap">
    <p class="brand">🎨 Artwork Workspace</p>
    <div class="card">
      ${imgBlock}
      <div class="body">
        <h1>${eName}</h1>
        <p class="meta">by <span class="hl">@${eCreator}</span></p>
        ${sourceBadge}
        ${winnerLine}
        <div class="chips">${wordChips}</div>
        <a class="cta" href="${eUsernodeUrl}">Open in Usernode →</a>
      </div>
    </div>
  </div>
</body>
</html>`);
  } catch (err) {
    console.error('share page error:', err);
    renderShareNotFound(res);
  }
});

// Stories row: most recent locked artwork per unique creator, ordered by recency.
app.get('/api/stories', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT username, image_url, session_id, locked_at
      FROM (
        SELECT DISTINCT ON (s.creator_username)
          s.creator_username AS username,
          g.image_url,
          s.id AS session_id,
          s.locked_at
        FROM sessions s
        LEFT JOIN generated_artworks g ON g.session_id = s.id
        WHERE s.state = 'locked'
        ORDER BY s.creator_username, s.locked_at DESC
      ) sub
      ORDER BY locked_at DESC
      LIMIT 15
    `);
    res.json({ stories: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Current user's completed artworks (for Profile tab).
app.get('/api/me/artworks', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.*, a.current_bid AS winning_bid, a.current_leader_username AS winner_username,
        g.image_url, g.prompt, g.owner_pubkey, g.owner_username,
        (SELECT COUNT(*) FROM artwork_likes l WHERE l.session_id = s.id)::int AS like_count,
        EXISTS (SELECT 1 FROM artwork_likes l WHERE l.session_id = s.id AND l.user_id = $1) AS liked_by_me,
        (SELECT COALESCE(SUM(amount), 0) FROM gifts gf WHERE gf.session_id = s.id AND gf.status <> 'failed') AS gift_total,
        (SELECT COUNT(*) FROM artwork_comments c WHERE c.session_id = s.id)::int AS comment_count
      FROM sessions s
      LEFT JOIN auctions a ON a.session_id = s.id
      LEFT JOIN generated_artworks g ON g.session_id = s.id
      WHERE s.creator_user_id = $1 AND s.state = 'locked'
      ORDER BY s.created_at DESC
      LIMIT 50
    `, [req.user.id]);
    res.json({ sessions: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Notifications: likes on the current user's artworks + new artworks by others.
app.get('/api/notifications', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT 'like' AS type, al.username, al.created_at,
             s.id AS session_id, s.name AS session_name
      FROM artwork_likes al
      JOIN sessions s ON s.id = al.session_id
      WHERE s.creator_user_id = $1 AND al.user_id <> $1

      UNION ALL

      SELECT 'new_artwork' AS type, s.creator_username AS username, s.locked_at AS created_at,
             s.id AS session_id, s.name AS session_name
      FROM sessions s
      WHERE s.state = 'locked' AND s.creator_user_id <> $1

      ORDER BY created_at DESC NULLS LAST
      LIMIT 20
    `, [req.user.id]);
    res.json({ notifications: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
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
  // Wallet address (ut1…) of the artwork owner, captured at generation time so
  // gifts can be routed even after the owner's JWT is gone. Null for artworks
  // generated before this column existed or by owners with no linked wallet.
  await pool.query(`ALTER TABLE generated_artworks ADD COLUMN IF NOT EXISTS owner_pubkey TEXT`);
  // Provenance of the artwork: 'ai' for machine-generated, 'human' for a
  // hand-made upload. Every artwork today is produced by /api/generate, so the
  // default backfills all existing rows to 'ai'. 'human' is supported for a
  // future human-upload path but nothing writes it yet.
  await pool.query(`ALTER TABLE generated_artworks ADD COLUMN IF NOT EXISTS source VARCHAR(20) NOT NULL DEFAULT 'ai'`);
  // On-chain gifts/tips sent to artwork owners. Financial data (wallet
  // addresses + amounts) → private: copied schema-only into staging.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS gifts (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      from_user_id INTEGER NOT NULL,
      from_username VARCHAR(255) NOT NULL,
      from_pubkey TEXT,
      to_user_id INTEGER NOT NULL,
      to_username VARCHAR(255),
      to_pubkey TEXT NOT NULL,
      amount NUMERIC NOT NULL,
      tx_hash TEXT,
      chain_id TEXT,
      status VARCHAR(20) NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`COMMENT ON TABLE gifts IS 'staging:private'`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS artwork_comments (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL,
      username VARCHAR(255) NOT NULL,
      body TEXT NOT NULL,
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

  // Staging seeds: populate Stories row, feed grid, and Notifications tab with
  // demo content from multiple fake users. All idempotent via session name.
  // No-op in production.
  if (IS_STAGING) {
    const DEMO_SESSIONS = [
      { name: '[demo] Quantum Galaxy',  words: 'galaxy,quantum,nebula,dna,gravity',         user: 'demo_scientist',    uid: -10, minsAgo: 5 },
      { name: '[demo] Ocean Bloom',     words: 'ocean,bloom,tide,coral,mist',                user: 'staging-demo-alice', uid: -11, minsAgo: 12 },
      { name: '[demo] Fire Storm',      words: 'fire,storm,ember,thunder,ash',               user: 'staging-demo-bob',  uid: -12, minsAgo: 25 },
      { name: '[demo] Crystal Cave',    words: 'crystal,obsidian,jade,prism,shadow',         user: 'staging-demo-carol', uid: -13, minsAgo: 45 },
      { name: '[demo] Cosmic Dust',     words: 'cosmos,star,nebula,spectral,eclipse',        user: 'staging-demo-dave', uid: -14, minsAgo: 80 },
      { name: '[demo] Desert Mirage',   words: 'desert,mirage,dusk,amber,horizon',           user: 'staging-demo-eve',  uid: -15, minsAgo: 120 },
    ];

    for (const demo of DEMO_SESSIONS) {
      const { rows: existing } = await pool.query('SELECT id FROM sessions WHERE name = $1 LIMIT 1', [demo.name]);
      if (!existing.length) {
        const lockedAt = new Date(Date.now() - demo.minsAgo * 60 * 1000);
        const { rows: sRows } = await pool.query(
          `INSERT INTO sessions (name, creator_user_id, creator_username, state, words, locked_at)
           VALUES ($1, $2, $3, 'locked', $4, $5) RETURNING id`,
          [demo.name, demo.uid, demo.user, demo.words, lockedAt]
        );
        const sid = sRows[0].id;
        await pool.query(
          `INSERT INTO generated_artworks (session_id, owner_user_id, owner_username, image_url, prompt)
           VALUES ($1, $2, $3, $4, $5)`,
          [sid, demo.uid, demo.user,
           `https://picsum.photos/seed/demo-${demo.user}/512/512`,
           `A beautiful artwork inspired by: ${demo.words}`]
        );
        // Seed some likes so heart counts are non-zero
        const likers = [-21, -22, -23].filter(lid => lid !== demo.uid);
        for (const lid of likers) {
          await pool.query(
            `INSERT INTO artwork_likes (session_id, user_id, username) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
            [sid, lid, `staging-liker-${Math.abs(lid)}`]
          );
        }
      }
    }
  }

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
