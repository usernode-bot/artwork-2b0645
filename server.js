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

// Curated, seeded category list for the Art Work platform. Slugs are stable
// identifiers; names are display labels. Seeded idempotently on boot.
const ART_CATEGORIES = [
  { slug: 'painting',     name: 'Painting' },
  { slug: 'illustration', name: 'Illustration' },
  { slug: 'digital-art',  name: 'Digital Art' },
  { slug: 'photography',  name: 'Photography' },
  { slug: 'sculpture',    name: 'Sculpture' },
  { slug: 'mixed-media',  name: 'Mixed Media' },
  { slug: 'other',        name: 'Other' },
];

// Upsert a profile row on first authenticated hit (mirrors ensureUserCredits).
// display_name defaults to the username and can be edited later (Phase 5).
async function ensureProfile(userId, username) {
  await pool.query(
    `INSERT INTO profiles (user_id, username, display_name)
     VALUES ($1, $2, $2)
     ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username`,
    [userId, username]
  );
}

// Accepted upload image mime types and server-side byte ceiling. The client
// downscales/compresses to ~2 MB; this is a hard backstop (base64 of a 2 MB
// image is ~2.7 MB, comfortably under the 5 MB express.json limit).
const ALLOWED_IMAGE_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);
const MAX_IMAGE_BYTES = 4 * 1024 * 1024;

// Parse a data URL ("data:image/jpeg;base64,…") or a {base64, mime} pair into
// a { buffer, mime } pair. Returns null on anything malformed.
function parseImagePayload(image) {
  try {
    let mime, b64;
    if (typeof image === 'string') {
      const m = /^data:([a-zA-Z0-9.+/-]+);base64,(.+)$/s.exec(image.trim());
      if (!m) return null;
      mime = m[1].toLowerCase();
      b64 = m[2];
    } else if (image && typeof image === 'object') {
      mime = String(image.mime || '').toLowerCase();
      b64 = String(image.base64 || '').replace(/^data:[^,]+,/, '');
    } else {
      return null;
    }
    if (!ALLOWED_IMAGE_MIME.has(mime)) return null;
    const buffer = Buffer.from(b64, 'base64');
    if (!buffer.length) return null;
    return { buffer, mime };
  } catch {
    return null;
  }
}

function parseWords(input) {
  if (Array.isArray(input)) return input.map(w => String(w).trim()).filter(Boolean);
  return String(input || '').split(/[\s,]+/).map(w => w.trim()).filter(Boolean);
}

// Normalize a client-supplied tag list into ≤8 trimmed, de-duped strings.
function normalizeTags(input) {
  let arr = [];
  if (Array.isArray(input)) arr = input;
  else if (typeof input === 'string') arr = input.split(',');
  const seen = new Set();
  const out = [];
  for (const t of arr) {
    const tag = String(t || '').trim().replace(/^#/, '').slice(0, 30);
    if (tag && !seen.has(tag.toLowerCase())) { seen.add(tag.toLowerCase()); out.push(tag); }
    if (out.length >= 8) break;
  }
  return out;
}

// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/me', async (req, res) => {
  try {
    await ensureProfile(req.user.id, req.user.username);
    await ensureUserCredits(req.user.id, req.user.username);
    const { rows } = await pool.query(
      `SELECT p.display_name, p.bio, p.avatar_image_id,
              (SELECT COUNT(*)::int FROM artworks a WHERE a.owner_user_id = p.user_id) AS artwork_count
       FROM profiles p WHERE p.user_id = $1`,
      [req.user.id]
    );
    const profile = rows[0] || {};
    res.json({
      id: req.user.id,
      username: req.user.username,
      display_name: profile.display_name || req.user.username,
      bio: profile.bio || '',
      avatar_image_id: profile.avatar_image_id ?? null,
      artwork_count: profile.artwork_count ?? 0,
      // On-chain wallet address (ut1…) or null if the user hasn't linked one.
      usernode_pubkey: req.user.usernode_pubkey ?? null,
      isStaging: IS_STAGING
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Art Work: categories ───────────────────────────────────────────────────
app.get('/api/categories', async (_req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT id, slug, name FROM categories ORDER BY sort_order ASC, id ASC`
    );
    res.json({ categories: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Art Work: create an artwork (upload OR drawing) ──────────────────────────
// Accepts a client-compressed image (data URL or {base64, mime}) plus metadata.
// Stores the binary in `images` and the metadata in `artworks` (one image per
// artwork). Drawings and uploads use this same path — a drawing is just a
// canvas exported via toBlob and sent here.
app.post('/api/artwork', async (req, res) => {
  const client = await pool.connect();
  try {
    const { title, description, categoryId, categorySlug, tags, image, width, height } = req.body || {};

    const cleanTitle = String(title || '').trim();
    if (!cleanTitle) return res.status(400).json({ error: 'Title is required' });
    if (cleanTitle.length > 120) return res.status(400).json({ error: 'Title must be 120 characters or fewer' });

    const cleanDesc = String(description || '').trim().slice(0, 2000);

    const parsed = parseImagePayload(image);
    if (!parsed) return res.status(400).json({ error: 'A valid image (JPEG, PNG or WebP) is required' });
    if (parsed.buffer.length > MAX_IMAGE_BYTES) {
      return res.status(413).json({ error: 'Image is too large — keep it under 4 MB' });
    }

    // Resolve category: explicit id, then slug, else fall back to "Other".
    let resolvedCategoryId = null;
    if (categoryId != null && Number.isFinite(Number(categoryId))) {
      const { rows } = await client.query('SELECT id FROM categories WHERE id = $1', [Number(categoryId)]);
      if (rows.length) resolvedCategoryId = rows[0].id;
    }
    if (resolvedCategoryId == null) {
      const slug = String(categorySlug || 'other').trim() || 'other';
      const { rows } = await client.query(
        `SELECT id FROM categories WHERE slug = $1
         UNION ALL SELECT id FROM categories WHERE slug = 'other'
         LIMIT 1`,
        [slug]
      );
      resolvedCategoryId = rows[0]?.id ?? null;
    }

    const w = Number.isFinite(Number(width)) ? Math.round(Number(width)) : null;
    const h = Number.isFinite(Number(height)) ? Math.round(Number(height)) : null;
    const cleanTags = normalizeTags(tags);

    await client.query('BEGIN');
    const { rows: imgRows } = await client.query(
      `INSERT INTO images (owner_user_id, bytes, mime, width, height, byte_size)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
      [req.user.id, parsed.buffer, parsed.mime, w, h, parsed.buffer.length]
    );
    const imageId = imgRows[0].id;

    const { rows: artRows } = await client.query(
      `INSERT INTO artworks (owner_user_id, owner_username, title, description, category_id, image_id, tags)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id, title, description, category_id, image_id, tags, created_at`,
      [req.user.id, req.user.username, cleanTitle, cleanDesc, resolvedCategoryId, imageId, cleanTags]
    );
    await client.query('COMMIT');

    const artwork = artRows[0];
    res.json({
      artwork: {
        ...artwork,
        owner_username: req.user.username,
        image_url: `/api/artwork/${artwork.id}/image`
      }
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// ─── Art Work: serve an artwork's image bytes ─────────────────────────────────
// Bytes are immutable per artwork id, so we send a long-lived, immutable cache
// header plus a stable ETag and honour conditional requests with a 304.
app.get('/api/artwork/:id/image', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    if (!id) return res.status(400).json({ error: 'Invalid artwork id' });

    const { rows } = await pool.query(
      `SELECT i.bytes, i.mime, i.byte_size, i.id AS image_id
       FROM artworks a JOIN images i ON i.id = a.image_id
       WHERE a.id = $1`,
      [id]
    );
    const img = rows[0];
    if (!img) return res.status(404).json({ error: 'Image not found' });

    const etag = `"aw-${img.image_id}"`;
    if (req.headers['if-none-match'] === etag) {
      res.status(304).end();
      return;
    }
    res.setHeader('Content-Type', img.mime || 'application/octet-stream');
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    res.setHeader('ETag', etag);
    res.setHeader('Content-Length', img.byte_size ?? img.bytes.length);
    res.send(img.bytes);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Art Work: list the signed-in user's own artworks (My Artwork grid) ───────
app.get('/api/artworks/mine', async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT a.id, a.title, a.description, a.tags, a.view_count, a.created_at,
              c.slug AS category_slug, c.name AS category_name
       FROM artworks a
       LEFT JOIN categories c ON c.id = a.category_id
       WHERE a.owner_user_id = $1
       ORDER BY a.created_at DESC, a.id DESC
       LIMIT 200`,
      [req.user.id]
    );
    res.json({
      artworks: rows.map(r => ({ ...r, image_url: `/api/artwork/${r.id}/image` }))
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

app.get('/api/sessions/locked', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.*, a.current_bid as winning_bid, a.current_leader_username as winner_username,
        g.image_url, g.prompt, g.owner_pubkey, g.owner_username,
        (SELECT COUNT(*) FROM artwork_likes l WHERE l.session_id = s.id)::int AS like_count,
        EXISTS (SELECT 1 FROM artwork_likes l WHERE l.session_id = s.id AND l.user_id = $1) AS liked_by_me,
        (SELECT COALESCE(SUM(amount), 0) FROM gifts gf WHERE gf.session_id = s.id AND gf.status <> 'failed') AS gift_total
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
      g.image_url, g.prompt,
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
      `INSERT INTO generated_artworks (session_id, owner_user_id, owner_username, image_url, prompt, canvas_snapshot, owner_pubkey)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
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
      ownerPubkey: req.user.usernode_pubkey ?? null
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
      like_count: artwork.like_count || 0
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
    CREATE TABLE IF NOT EXISTS artwork_likes (
      id SERIAL PRIMARY KEY,
      session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL,
      username VARCHAR(255) NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE (session_id, user_id)
    )
  `);

  // ─── Art Work platform schema (Phase 1) ────────────────────────────────────
  // All public by default — public profiles, reference categories, shared
  // artwork and the binaries behind them. No FK points at a private table.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS categories (
      id SERIAL PRIMARY KEY,
      slug TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      sort_order INTEGER NOT NULL DEFAULT 0
    )
  `);
  // Seed / refresh the curated category list idempotently.
  for (let i = 0; i < ART_CATEGORIES.length; i++) {
    const c = ART_CATEGORIES[i];
    await pool.query(
      `INSERT INTO categories (slug, name, sort_order) VALUES ($1, $2, $3)
       ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name, sort_order = EXCLUDED.sort_order`,
      [c.slug, c.name, i]
    );
  }

  // Uploaded/drawn image binaries, stored directly in Postgres (no object
  // store on the platform). Served via GET /api/artwork/:id/image.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS images (
      id SERIAL PRIMARY KEY,
      owner_user_id INTEGER NOT NULL,
      bytes BYTEA NOT NULL,
      mime TEXT NOT NULL,
      width INTEGER,
      height INTEGER,
      byte_size INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Public profiles. avatar_image_id references the public images table.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS profiles (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL UNIQUE,
      username VARCHAR(255) NOT NULL,
      display_name VARCHAR(255),
      bio TEXT,
      avatar_image_id INTEGER REFERENCES images(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // First-class artworks (uploads and drawings alike).
  await pool.query(`
    CREATE TABLE IF NOT EXISTS artworks (
      id SERIAL PRIMARY KEY,
      owner_user_id INTEGER NOT NULL,
      owner_username VARCHAR(255) NOT NULL,
      title VARCHAR(160) NOT NULL,
      description TEXT,
      category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
      image_id INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
      tags TEXT[] NOT NULL DEFAULT '{}',
      view_count INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`ALTER TABLE artworks ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}'`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_artworks_owner ON artworks (owner_user_id, created_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_artworks_recent ON artworks (created_at DESC, id DESC)`);

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
