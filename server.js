const admin = require('firebase-admin');
const express = require('express');

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

async function sendToAllSubscribers(title, message) {
  const tokensSnap = await db.collection('push_tokens').get();
  const tokens = tokensSnap.docs.map((doc) => doc.id);

  if (tokens.length === 0) {
    console.log('No subscribers to send to.');
    return { successCount: 0, failureCount: 0 };
  }

  const payload = {
    notification: {
      title: title || 'All Media Downloader',
      body: message || ''
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
    console.log(`Removed ${invalidTokens.length} invalid token(s).`);
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

          console.log(`Processing queued notification: ${doc.id}`);
          try {
            const result = await sendToAllSubscribers(data.title, data.message);
            await doc.ref.update({
              sent: true,
              sentAt: admin.firestore.FieldValue.serverTimestamp(),
              successCount: result.successCount,
              failureCount: result.failureCount
            });
            console.log(
              `Sent "${data.title}" — success: ${result.successCount}, failed: ${result.failureCount}`
            );
          } catch (err) {
            console.error(`Failed to send notification ${doc.id}:`, err.message);
            await doc.ref.update({
              sent: true,
              sentAt: admin.firestore.FieldValue.serverTimestamp(),
              error: err.message
            });
          }
        });
      },
      (err) => {
        console.error('Queue listener error:', err.message);
      }
    );

  console.log('Listening for queued push notifications...');
}

const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send('All Media Downloader push sender is running.');
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.listen(PORT, () => {
  console.log(`Push sender HTTP server listening on port ${PORT}`);
  startQueueListener();
});
