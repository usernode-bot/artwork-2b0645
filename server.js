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
let auctionTimer = null; // { sessionId, timerId, intervalId, expiresAt }

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
    `INSERT INTO user_credits (user_id, username, balance) VALUES ($1, $2, 100) ON CONFLICT (user_id) DO NOTHING`,
    [userId, username]
  );
}

// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/me', async (req, res) => {
  try {
    await ensureUserCredits(req.user.id, req.user.username);
    const { rows } = await pool.query('SELECT balance FROM user_credits WHERE user_id = $1', [req.user.id]);
    res.json({ id: req.user.id, username: req.user.username, credits: rows[0]?.balance ?? 100 });
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

app.get('/api/sessions/locked', async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.*, a.current_bid as winning_bid, a.current_leader_username as winner_username,
        g.image_url, g.prompt, g.canvas_snapshot
      FROM sessions s
      LEFT JOIN auctions a ON a.session_id = s.id
      LEFT JOIN generated_artworks g ON g.session_id = s.id
      WHERE s.state = 'locked'
      ORDER BY s.locked_at DESC
      LIMIT 50
    `);
    res.json({ sessions: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/session', async (req, res) => {
  try {
    const { name } = req.body;
    if (!name?.trim()) return res.status(400).json({ error: 'Name required' });

    const { rows: existing } = await pool.query(
      `SELECT id FROM sessions WHERE state IN ('active', 'auction') LIMIT 1`
    );
    if (existing.length) return res.status(409).json({ error: 'A session is already active' });

    const { rows } = await pool.query(
      `INSERT INTO sessions (name, creator_user_id, creator_username, state) VALUES ($1, $2, $3, 'active') RETURNING *`,
      [name.trim(), req.user.id, req.user.username]
    );

    io.emit('session-created', { session: rows[0] });
    res.json({ session: rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/session/:id/strokes', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM canvas_strokes WHERE session_id = $1 ORDER BY created_at ASC',
      [req.params.id]
    );
    res.json({ strokes: rows });
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

    // Deduct from bidder
    await client.query('UPDATE user_credits SET balance = balance - $1 WHERE user_id = $2', [bidAmount, req.user.id]);

    // Refund previous leader
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

app.post('/api/generate', async (req, res) => {
  try {
    const { sessionId, canvasSnapshot } = req.body;

    const { rows: sessions } = await pool.query(
      `SELECT * FROM sessions WHERE id = $1 AND state = 'auction'`,
      [sessionId]
    );
    if (!sessions.length) return res.status(409).json({ error: 'Session not in auction state' });

    const { rows: auctions } = await pool.query('SELECT * FROM auctions WHERE session_id = $1', [sessionId]);
    if (!auctions.length) return res.status(404).json({ error: 'Auction not found' });

    const auction = auctions[0];
    if (auction.current_leader_user_id !== req.user.id) {
      return res.status(403).json({ error: 'Only the auction winner can generate artwork' });
    }
    if (new Date(auction.expires_at) > new Date()) {
      return res.status(409).json({ error: 'Auction has not ended yet' });
    }

    const imageUrl = `https://picsum.photos/seed/${sessionId}/512/512`;
    const prompt = 'A beautiful AI artwork generated from the collaborative drawing.';

    await pool.query(
      `INSERT INTO generated_artworks (session_id, owner_user_id, owner_username, image_url, prompt, canvas_snapshot)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [sessionId, req.user.id, req.user.username, imageUrl, prompt, canvasSnapshot || null]
    );

    await pool.query(`UPDATE sessions SET state = 'locked', locked_at = NOW() WHERE id = $1`, [sessionId]);

    io.emit('session-locked', {
      sessionId,
      imageUrl,
      prompt,
      winnerUsername: req.user.username,
      canvasSnapshot: canvasSnapshot || null
    });

    res.json({ ok: true, imageUrl, prompt });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Socket.io ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  socket.on('join-session', (sessionId) => {
    socket.join(`session:${sessionId}`);
  });

  socket.on('stroke', async ({ sessionId, strokeData }) => {
    try {
      const { rows } = await pool.query(
        `INSERT INTO canvas_strokes (session_id, user_id, username, stroke_data) VALUES ($1,$2,$3,$4) RETURNING id`,
        [sessionId, socket.user.id, socket.user.username, JSON.stringify(strokeData)]
      );
      socket.to(`session:${sessionId}`).emit('stroke', {
        id: rows[0].id,
        userId: socket.user.id,
        username: socket.user.username,
        strokeData
      });
    } catch (err) {
      console.error('stroke error:', err);
    }
  });

  socket.on('clear-canvas', async (sessionId) => {
    try {
      const { rows } = await pool.query('SELECT creator_user_id FROM sessions WHERE id = $1', [sessionId]);
      if (!rows.length || rows[0].creator_user_id !== socket.user.id) return;
      await pool.query('DELETE FROM canvas_strokes WHERE session_id = $1', [sessionId]);
      io.to(`session:${sessionId}`).emit('canvas-cleared');
    } catch (err) {
      console.error('clear-canvas error:', err);
    }
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
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_credits (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL UNIQUE,
      username VARCHAR(255) NOT NULL,
      balance INTEGER NOT NULL DEFAULT 100
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
