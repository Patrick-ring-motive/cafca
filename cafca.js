// Initialize cache
const cacheName = 'cafca';
async function getCache() {
  return await caches.open(cacheName);
}

// Simple hash for partitioning
function hashCode(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0;
  }
  return hash;
}

// Produce a message
async function produce({
  topic,
  key,
  value
}) {
  const cache = await getCache();
  const partitionCount = 3;
  const partition = key ? Math.abs(hashCode(key) % partitionCount) : 0;
  const metaKey = `https://${topic}/p${partition}/meta`;

  // Get or initialize offset
  let meta = await cache.match(metaKey);
  let offset = meta ? (await meta.json()).offset : -1;
  offset += 1;

  // Store message
  const messageKey = `https://${topic}/p${partition}/${offset}`;
  const message = {
    offset,
    key,
    value,
    timestamp: Date.now()
  };
  await cache.put(messageKey, new Response(JSON.stringify(message)));

  // Update metadata
  await cache.put(metaKey, new Response(JSON.stringify({
    offset
  })));

  return {
    success: true,
    offset,
    partition
  };
}

// Consume messages
async function consume({
  topic,
  group = 'default',
  partition = 0,
  maxMessages = 10
}) {
  const cache = await getCache();
  const offsetKey = `https://consumer/${group}/${topic}/p${partition}`;

  // Get consumer group offset
  let consumerOffset = 0;
  const offsetCache = await cache.match(offsetKey);
  if (offsetCache) {
    consumerOffset = (await offsetCache.json()).offset;
  }

  // Fetch messages
  const messages = [];
  for (let i = consumerOffset; i < consumerOffset + maxMessages; i++) {
    const key = `https://${topic}/p${partition}/${i}`;
    const cached = await cache.match(key);
    if (!cached) break;
    messages.push(await cached.json());
  }

  // Simulate processing with retry
  let newOffset = consumerOffset;
  for (const msg of messages) {
    let attempts = 0;
    const maxAttempts = 3;
    while (attempts < maxAttempts) {
      try {
        // Simulate processing
        console.log(`Processing: ${JSON.stringify(msg)}`);
        newOffset = msg.offset + 1;
        break;
      } catch (e) {
        attempts++;
        if (attempts === maxAttempts) {
          // Move to DLQ
          await cache.put(`https://dlq/${topic}/p${partition}/${msg.offset}`, new Response(JSON.stringify(msg)));
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  // Commit new offset
  if (messages.length > 0) {
    await cache.put(offsetKey, new Response(JSON.stringify({
      offset: newOffset
    })));
  }

  return {
    messages,
    nextOffset: newOffset,
    partition
  };
}

// Bind UI events
document.getElementById('produceForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.target;
  const data = {
    topic: form.topic.value,
    key: form.key.value,
    value: form.value.value,
  };
  try {
    const result = await produce(data);
    document.getElementById('produceOutput').textContent = JSON.stringify(result, null, 2);
  } catch (e) {
    document.getElementById('produceOutput').textContent = `Error: ${e.message}`;
  }
});

document.getElementById('consumeForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const form = e.target;
  const data = {
    topic: form.topic.value,
    group: form.group.value,
    partition: parseInt(form.partition.value),
    maxMessages: parseInt(form.max.value),
  };
  try {
    const result = await consume(data);
    document.getElementById('consumeOutput').textContent = JSON.stringify(result, null, 2);
  } catch (e) {
    document.getElementById('consumeOutput').textContent = `Error: ${e.message}`;
  }
});
