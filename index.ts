import Fastify from 'fastify'
import { createClient } from 'redis'

const fastify = Fastify({
  logger: true
})

const redis = createClient({
  url: 'redis://redis:6379'
})

const defaultProcessorUrl = process.env.PROCESSOR_DEFAULT_URL!
const fallbackProcessorUrl = process.env.PROCESSOR_FALLBACK_URL!

redis.on('error', (err) => console.log('Redis Client Error', err))

await redis.connect()
await redis.del('payment_queue');

await fetch(defaultProcessorUrl + '/admin/purge-payments', {
  method: 'POST',
})
await fetch(fallbackProcessorUrl + '/admin/purge-payments', {
  method: 'POST',
})

type Payment = {
  amount: number,
  correlationId: string,
}

async function saveProcessedPayment(payment: Payment, processor: 'default' | 'fallback') {
  const timestamp = new Date().toISOString()

  await redis.hSet(`p:${payment.correlationId}`, {
    correlationId: payment.correlationId,
    amount: payment.amount.toString(),
    processor,
    processedAt: timestamp,
  })

  await redis.hIncrBy(`stats:${processor}`, 'totalRequests', 1)
  await redis.hIncrByFloat(`stats:${processor}`, 'totalAmount', payment.amount)
}

async function getPaymentsSummary(from?: string, to?: string) {
  if (!from && !to) {
    const defaultStats = await redis.hGetAll('stats:default')
    const fallbackStats = await redis.hGetAll('stats:fallback')

    return {
      default: {
        totalRequests: parseInt(defaultStats.totalRequests || '0'),
        totalAmount: parseFloat(defaultStats.totalAmount || '0')
      },
      fallback: {
        totalRequests: parseInt(fallbackStats.totalRequests || '0'),
        totalAmount: parseFloat(fallbackStats.totalAmount || '0')
      }
    }
  }

  const paymentKeys = await redis.keys('p:*')
  const defaultResult = { totalRequests: 0, totalAmount: 0 }
  const fallbackResult = { totalRequests: 0, totalAmount: 0 }

  const fromDate = from ? new Date(from) : new Date('1970-01-01')
  const toDate = to ? new Date(to) : new Date()

  for (const key of paymentKeys) {
    const payment = await redis.hGetAll(key)
    const processedAt = new Date(payment.processedAt)

    if (processedAt >= fromDate && processedAt <= toDate) {
      const amount = parseFloat(payment.amount)
      if (payment.processor === 'default') {
        defaultResult.totalRequests++
        defaultResult.totalAmount += amount
      } else if (payment.processor === 'fallback') {
        fallbackResult.totalRequests++
        fallbackResult.totalAmount += amount
      }
    }
  }

  return {
    default: defaultResult,
    fallback: fallbackResult
  }
}

async function addPaymentToQueue(paymentData: Payment) {
  await redis.lPush('payment_queue', JSON.stringify(paymentData))
}

async function processPaymentQueue() {
  while (true) {
    try {
      const item = await redis.brPop('payment_queue', 1)
      if (item) {
        const payment = JSON.parse(item.element)
        await processPaymentByDefault(payment)
        console.log('Queue size:', await redis.lLen('payment_queue'))
      }
    } catch (error) {
      console.error('Error processing payment queue:', error)
    }
  }
}

async function tryProcessPayment(payment: Payment, processor: 'default' | 'fallback') {
  let processorUrl = defaultProcessorUrl
  if (processor == 'fallback') {
    processorUrl = fallbackProcessorUrl
  }
  const success = await processWithProcessor(payment, processorUrl)
  if (success) {
    console.log(`${processor} processor success, saving processment`)
    return await saveProcessedPayment(payment, 'default')
  }

  console.log(`${processor} processor failed, trying fallback`)
  return await processPaymentByFallback(payment)
}

async function tryDefaultProcessor(payment: Payment, defaultProcessorUrl: string) {
  const success = await processWithProcessor(payment, defaultProcessorUrl)
  if (success) {
    console.log('Default processor success, saving processment')
    return await saveProcessedPayment(payment, 'default')
  }
  console.log('Default processor failed, trying fallback')
  return await processPaymentByFallback(payment)
}

async function tryFallBackProcessor(payment: Payment, fallbackProcessorUrl: string) {
  const success = await processWithProcessor(payment, fallbackProcessorUrl)
  if (success) {
    console.log('fallbackprocess success, saving processment')
    return await saveProcessedPayment(payment, 'fallback')
  }
  console.log('fallback processor failed, trying default')
  return await processPaymentByDefault(payment)
}

async function processPaymentByFallback(payment: Payment): Promise<boolean | void> {
  let isFallBackAvailable = false
  try {
    console.log(`Trying to process by fallback: ${payment.correlationId}`)
    const alreadyMadeRequest = await redis.get(`${fallbackProcessorUrl}:/payments/service-health`)
    if (alreadyMadeRequest) {
      if (isFallBackAvailable) {
        return await tryFallBackProcessor(payment, fallbackProcessorUrl)
      }
      return await tryDefaultProcessor(payment, defaultProcessorUrl)
    }
    isFallBackAvailable = await isProcessorHealthy(defaultProcessorUrl)
    if (isFallBackAvailable) {
      return await tryFallBackProcessor(payment, fallbackProcessorUrl)
    }
    return await processPaymentByDefault(payment)
  } catch (error) {
    console.error('Error with fallback processor:', payment.correlationId, error)
  }
}

async function processPaymentByDefault(payment: Payment) {
  try {
    let isDefaultAvailable = false
    console.log('Trying to process by default:', payment.correlationId)
    const alreadyMadeRequest = await redis.get(`${defaultProcessorUrl}:/payments/service-health`)
    if (alreadyMadeRequest) {
      if (isDefaultAvailable) {
        return await tryDefaultProcessor(payment, defaultProcessorUrl)
      }
      return await processPaymentByFallback(payment)
    }
    isDefaultAvailable = await isProcessorHealthy(defaultProcessorUrl)
    if (isDefaultAvailable) {
      return await tryDefaultProcessor(payment, defaultProcessorUrl)
    }
    return await processPaymentByFallback(payment)
  } catch (error) {
    console.error('Error processing payment:', payment.correlationId, error)
  }
}

async function isProcessorHealthy(processorUrl: string): Promise<boolean> {
  const response = await fetch(processorUrl + '/payments/service-health')
  await redis.setEx(`${processorUrl}:/payments/service-health`, 5, '1');
  if (!response.ok) return false
  const health = await response.json()
  if (health.failing === false) return true
  return false
}

async function processWithProcessor(payment: any, processorUrl: string): Promise<boolean> {
  try {
    const paymentRequest = {
      correlationId: payment.correlationId,
      amount: payment.amount,
      requestedAt: new Date().toISOString()
    }

    const response = await fetch(processorUrl + '/payments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(paymentRequest)
    })

    return response.ok
  } catch {
    return false
  }
}

processPaymentQueue()

const schema = {
  body: {
    type: 'object',
    required: ['correlationId', 'amount'],
    properties: {
      amount: { type: 'number', minimum: 0.01 },
      correlationId: { type: 'string', format: 'uuid' },
    }
  },
  response: {
    200: {
      type: 'null'
    }
  }
}

fastify.post('/payments', { schema }, async function handler(request, reply) {
  try {
    await addPaymentToQueue(request.body as Payment)
    reply.code(200).send()
  } catch (error) {
    fastify.log.error('Error adding payment to queue:', error)
    reply.code(500).send({ error: 'Internal server error' })
  }
})

fastify.get('/payments-summary', async function handler(request, reply) {
  try {
    const query = request.query as { from?: string; to?: string }
    const summary = await getPaymentsSummary(query.from, query.to)
    reply.code(200).send(summary)
  } catch (error) {
    fastify.log.error('Error getting payments summary:', error)
    reply.code(500).send({ error: 'Internal server error' })
  }
})

try {
  await fastify.listen({ port: 3000, host: '0.0.0.0' })
} catch (err) {

  fastify.log.error(err)
  process.exit(1)
}