self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data?.json() ?? {}; } catch { data = { body: event.data?.text() ?? '' }; }
  event.waitUntil(self.registration.showNotification(data.title ?? 'New notification', {
    body: data.body ?? 'You have a new notification',
    tag: data.tag ?? 'vussa-notification',
    data: { url: data.url ?? '/' }
  }));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = event.notification.data?.url ?? '/';
  event.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
    for (const client of clients) if ('focus' in client) { client.navigate(url); return client.focus(); }
    return self.clients.openWindow(url);
  }));
});
