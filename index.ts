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
console.log('Cleaning Redis data...')
await redis.flushDb()
await redis.del('payment_queue');
console.log('Redis cleaned!')

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
  const pipeline = redis.multi()

  // Stats globais (sem data)
  pipeline.hIncrBy(`stats:${processor}`, 'totalRequests', 1)
  pipeline.hIncrByFloat(`stats:${processor}`, 'totalAmount', payment.amount)

  await pipeline.exec()
}

async function getPaymentsSummary(from?: string, to?: string) {
  if (!from && !to) {
    // Sem filtro de data - usa stats globais (super rápido)
    const [defaultStats, fallbackStats] = await Promise.all([
      redis.hGetAll('stats:default'),
      redis.hGetAll('stats:fallback')
    ])

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

async function processPaymentWorker(workerId: number) {
  while (true) {
    try {
      const batchSize = 20
      const batch = []

      for (let i = 0; i < batchSize; i++) {
        const item = await redis.rPop('payment_queue')
        if (item) {
          batch.push(JSON.parse(item))
        } else {
          break
        }
      }

      if (batch.length > 0) {
        const promises = batch.map(payment =>
          processPaymentWithProcessor(payment, 'default')
        )

        await Promise.all(promises)
        const queueSize = await redis.lLen('payment_queue')
        console.log(`Processed batch of ${batch.length}, queue size: ${queueSize}`)

      } else {
        // Se não há itens, espera um pouco antes de tentar novamente
        await new Promise(resolve => setTimeout(resolve, 10))
      }

    } catch (error) {
      console.error(`Worker ${workerId} error:`, error)
      await new Promise(resolve => setTimeout(resolve, 100))
    }
  }
}

async function startPaymentWorkers() {
  const numWorkers = 10 // 4 workers por API (total 8)

  console.log(`Starting ${numWorkers} payment workers...`)

  const workers = Array.from({ length: numWorkers }, (_, i) =>
    processPaymentWorker(i + 1)
  )

  await Promise.all(workers)
}

async function tryProcessor(
  payment: Payment,
  processorUrl: string,
  processorType: 'default' | 'fallback'
) {
  const success = await processWithProcessor(payment, processorUrl)
  if (success) {
    // console.log(`${processorType} processor success, saving processment`)
    return await saveProcessedPayment(payment, processorType)
  }
  console.log(`${processorType} processor failed, trying ${processorType === 'default' ? 'fallback' : 'default'}`)
  if (processorType === 'default') {
    return await processPaymentWithProcessor(payment, 'fallback')
  } else {
    return await processPaymentWithProcessor(payment, 'default')
  }
}

async function processPaymentWithProcessor(
  payment: Payment,
  processorType: 'default' | 'fallback'
): Promise<boolean | void> {
  try {
    const processorUrl = processorType === 'default' ? defaultProcessorUrl : fallbackProcessorUrl
    const fallbackProcessorType = processorType === 'default' ? 'fallback' : 'default'

    let isProcessorAvailable = false
    // console.log(`Trying to process by ${processorType}: ${payment.correlationId}`)

    const alreadyMadeRequest = await redis.get(`${processorUrl}:/payments/service-health`)
    if (alreadyMadeRequest) {
      if (isProcessorAvailable) {
        return await tryProcessor(payment, processorUrl, processorType)
      }
      const fallbackUrl = processorType === 'default' ? fallbackProcessorUrl : defaultProcessorUrl
      return await tryProcessor(payment, fallbackUrl, fallbackProcessorType)
    }

    isProcessorAvailable = await isProcessorHealthy(processorUrl)
    if (isProcessorAvailable) {
      return await tryProcessor(payment, processorUrl, processorType)
    }
    return await processPaymentWithProcessor(payment, fallbackProcessorType)
  } catch (error) {
    console.error(`Error with ${processorType} processor:`, payment.correlationId, error)
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

startPaymentWorkers()

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