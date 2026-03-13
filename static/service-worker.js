const CACHE_NAME = 'kyotei-mvp-cache-v1';
const urlsToCache = [
  './index.html',
  './manifest.json',
  // TailwindCDNは外部リソースなのでキャッシュ戦略に含めるか検討が必要ですが
  // MVPとしては最低限のローカルファイルをキャッシュします
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        return cache.addAll(urlsToCache);
      })
  );
});

self.addEventListener('fetch', event => {
  // APIへのリクエストはキャッシュしない（常に最新データを取得）
  if (event.request.url.includes('/api/')) {
    return;
  }
  
  event.respondWith(
    caches.match(event.request)
      .then(response => {
        if (response) {
          return response;
        }
        return fetch(event.request);
      })
  );
});
