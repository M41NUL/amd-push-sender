const admin = require('firebase-admin');
const express = require('express');
const path = require('path');

if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
  console.error('Missing FIREBASE_SERVICE_ACCOUNT environment variable.');
  process.exit(1);
}

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const messaging = admin.messaging();

const BOOT_TIME = Date.now();
const MAX_LOGS = 200;
const MAX_NOTIFICATIONS = 100;

const state = {
  logs: [],
  notifications: [],
  totalCalls: 0,
  totalSuccess: 0,
  totalFailed: 0
};

function pushLog(text, level) {
  state.logs.unshift({
    time: new Date().toISOString(),
    level: level || 'info',
    text
  });
  if (state.logs.length > MAX_LOGS) state.logs.pop();
  if (level === 'error') console.error(text);
  else console.log(text);
}

function pushNotification(entry) {
  state.notifications.unshift(entry);
  if (state.notifications.length > MAX_NOTIFICATIONS) state.notifications.pop();
}

async function sendToAllSubscribers(title, message, image, link) {
  const tokensSnap = await db.collection('push_tokens').get();
  const tokens = tokensSnap.docs.map((doc) => doc.id);

  if (tokens.length === 0) {
    pushLog('No subscribers to send to.', 'warn');
    return { successCount: 0, failureCount: 0 };
  }

  const payload = {
    notification: {
      title: title || 'All Media Downloader',
      body: message || ''
    },
    data: {
      link: link || 'https://all-media-downloader-web.vercel.app',
      image: image || ''
    },
    tokens
  };

  const response = await messaging.sendEachForMulticast(payload);

  const invalidTokens = [];
  response.responses.forEach((res, i) => {
    if (!res.success) {
      const code = res.error && res.error.code;
      if (
        code === 'messaging/invalid-registration-token' ||
        code === 'messaging/registration-token-not-registered'
      ) {
        invalidTokens.push(tokens[i]);
      }
    }
  });

  if (invalidTokens.length > 0) {
    const batch = db.batch();
    invalidTokens.forEach((token) => {
      batch.delete(db.collection('push_tokens').doc(token));
    });
    await batch.commit();
    pushLog(`Removed ${invalidTokens.length} invalid token(s).`, 'warn');
  }

  return {
    successCount: response.successCount,
    failureCount: response.failureCount
  };
}

function startQueueListener() {
  db.collection('push_queue')
    .where('sent', '==', false)
    .onSnapshot(
      (snapshot) => {
        snapshot.docChanges().forEach(async (change) => {
          if (change.type !== 'added' && change.type !== 'modified') return;

          const doc = change.doc;
          const data = doc.data();
          if (data.sent) return;

          state.totalCalls++;
          pushLog(`Processing queued notification: ${doc.id}`);
          try {
            const result = await sendToAllSubscribers(data.title, data.message, data.image, data.link);
            await doc.ref.update({
              sent: true,
              sentAt: admin.firestore.FieldValue.serverTimestamp(),
              successCount: result.successCount,
              failureCount: result.failureCount
            });
            state.totalSuccess += result.successCount;
            state.totalFailed += result.failureCount;
            pushLog(
              `Sent "${data.title}" — success: ${result.successCount}, failed: ${result.failureCount}`,
              'success'
            );
            pushNotification({
              id: doc.id,
              title: data.title || '',
              message: data.message || '',
              image: data.image || null,
              link: data.link || null,
              successCount: result.successCount,
              failureCount: result.failureCount,
              status: result.failureCount > 0 && result.successCount === 0 ? 'failed' : 'success',
              time: new Date().toISOString()
            });
          } catch (err) {
            state.totalFailed++;
            pushLog(`Failed to send notification ${doc.id}: ${err.message}`, 'error');
            pushNotification({
              id: doc.id,
              title: data.title || '',
              message: data.message || '',
              image: data.image || null,
              link: data.link || null,
              successCount: 0,
              failureCount: 1,
              status: 'failed',
              error: err.message,
              time: new Date().toISOString()
            });
            await doc.ref.update({
              sent: true,
              sentAt: admin.firestore.FieldValue.serverTimestamp(),
              error: err.message
            });
          }
        });
      },
      (err) => {
        pushLog(`Queue listener error: ${err.message}`, 'error');
      }
    );

  pushLog('Listening for queued push notifications...');
}

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/api/status', (req, res) => {
  res.json({
    bootTime: BOOT_TIME,
    uptimeSeconds: Math.floor((Date.now() - BOOT_TIME) / 1000),
    totalCalls: state.totalCalls,
    totalSuccess: state.totalSuccess,
    totalFailed: state.totalFailed,
    logs: state.logs,
    notifications: state.notifications
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  pushLog(`Push sender HTTP server listening on port ${PORT}`);
  startQueueListener();
});
