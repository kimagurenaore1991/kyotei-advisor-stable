// Service Worker caching is intentionally disabled.
// If an older worker is still registered in a browser, this one removes itself.
self.addEventListener('install', event => {
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    try {
      const cacheNames = await caches.keys();
      await Promise.all(cacheNames.map(name => caches.delete(name)));
    } catch (_) {}

    try {
      await self.registration.unregister();
    } catch (_) {}
  })());
});

self.addEventListener('fetch', () => {
  // no-op
});
