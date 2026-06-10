/* Art Work — service worker (Phase 0)
 *
 * Goals:
 *  - Precache the static, always-public PWA assets (manifest, icons, offline page).
 *  - Make the app shell available offline so the start_url responds 200 when the
 *    network is gone (cached after the first authenticated load).
 *  - NEVER cache /api/* data, realtime sockets, or anything carrying ?token=
 *    (the JWT must not be persisted in the Cache Storage).
 */
const CACHE = 'artwork-v1';
const SHELL_URL = '/index.html';

// Static assets safe to precache (served publicly, no auth/token involved).
const PRECACHE = [
  '/offline.html',
  '/manifest.webmanifest',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  '/icons/icon-512-maskable.png',
  '/icons/apple-touch-icon.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE)
      .then((cache) => cache.addAll(PRECACHE))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Requests we must never read from or write to the cache.
function isNeverCache(url) {
  return (
    url.pathname.startsWith('/api/') ||
    url.pathname.startsWith('/socket.io/') ||
    url.pathname.startsWith('/share-api/') ||
    url.pathname.startsWith('/explorer-api/')
  );
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return; // only GETs are cacheable

  const url = new URL(req.url);
  const sameOrigin = url.origin === self.location.origin;

  // Data + realtime: pass straight through, untouched by the cache.
  if (sameOrigin && isNeverCache(url)) return;

  // Navigations (loading the HTML shell): network-first, fall back to the
  // cached shell, then the offline page. We cache the shell under a
  // token-stripped key so a ?token= JWT is never persisted.
  if (req.mode === 'navigate') {
    event.respondWith(networkFirstShell(req));
    return;
  }

  // A token in the query string means a privileged request — never cache it.
  if (url.searchParams.has('token')) return;

  // Same-origin static assets (icons, manifest, future css/js): cache-first.
  if (sameOrigin) {
    event.respondWith(cacheFirst(req));
    return;
  }

  // Cross-origin (Tailwind CDN, Google Fonts, the bridge): stale-while-revalidate.
  event.respondWith(staleWhileRevalidate(req));
});

async function networkFirstShell(req) {
  const cache = await caches.open(CACHE);
  try {
    const res = await fetch(req);
    if (res && res.ok) {
      // Store a clean copy of the shell HTML under a stable, token-free key.
      cache.put(SHELL_URL, res.clone());
    }
    return res;
  } catch (err) {
    return (await cache.match(SHELL_URL)) || (await cache.match('/offline.html')) || Response.error();
  }
}

async function cacheFirst(req) {
  const cache = await caches.open(CACHE);
  const cached = await cache.match(req);
  if (cached) return cached;
  try {
    const res = await fetch(req);
    if (res && res.ok) cache.put(req, res.clone());
    return res;
  } catch (err) {
    return cached || Response.error();
  }
}

async function staleWhileRevalidate(req) {
  const cache = await caches.open(CACHE);
  const cached = await cache.match(req);
  const network = fetch(req)
    .then((res) => {
      if (res && (res.ok || res.type === 'opaque')) cache.put(req, res.clone());
      return res;
    })
    .catch(() => cached);
  return cached || network;
}
