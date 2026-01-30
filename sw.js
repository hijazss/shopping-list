self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    // Delete all caches
    const keys = await caches.keys();
    await Promise.all(keys.map((k) => caches.delete(k)));

    // Unregister this service worker
    await self.registration.unregister();

    // Refresh any open pages so they reload without the SW
    const clientsList = await self.clients.matchAll({ type: "window" });
    clientsList.forEach((c) => c.navigate(c.url));
  })());
});

// No fetch handler on purpose
