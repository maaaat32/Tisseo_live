// Met en cache uniquement la coquille de l'appli ; les données restent toujours en direct.
const CACHE = "tisseo-live-v1";
const SHELL = ["/", "/manifest.json", "/icon.svg"];
self.addEventListener("install", e => e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL))));
self.addEventListener("activate", e => e.waitUntil(
  caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k))))));
self.addEventListener("fetch", e => {
  const url = new URL(e.request.url);
  if (url.pathname.startsWith("/api/")) return;            // jamais de cache sur les données
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
