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
  const timestamp = Date.now()

  // Stats globais (sem data)
  pipeline.hIncrBy(`stats:${processor}`, 'totalRequests', 1)
  pipeline.hIncrByFloat(`stats:${processor}`, 'totalAmount', payment.amount)

  // Sorted set para consultas por data (timestamp como score)
  const paymentData = JSON.stringify({
    correlationId: payment.correlationId,
    amount: payment.amount,
    processor,
    processedAt: new Date(timestamp).toISOString()
  })
  pipeline.zAdd(`payments:${processor}:by_date`, { score: timestamp, value: paymentData })

  await pipeline.exec()

  // Log do pagamento salvo
  console.log(`💾 Payment saved: ${payment.correlationId} | $${payment.amount} | ${processor} processor | ${new Date(timestamp).toISOString()}`)
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

  // Com filtro de data - usa sorted sets (performático)
  const fromTimestamp = from ? new Date(from).getTime() : 0
  const toTimestamp = to ? new Date(to).getTime() : Date.now()

  const [defaultPayments, fallbackPayments] = await Promise.all([
    redis.zRangeByScore('payments:default:by_date', fromTimestamp, toTimestamp),
    redis.zRangeByScore('payments:fallback:by_date', fromTimestamp, toTimestamp)
  ])

  const calculateStats = (payments: string[]) => {
    let totalRequests = 0
    let totalAmount = 0

    for (const paymentStr of payments) {
      const payment = JSON.parse(paymentStr)
      totalRequests++
      totalAmount += payment.amount
    }

    return { totalRequests, totalAmount }
  }

  return {
    default: calculateStats(defaultPayments),
    fallback: calculateStats(fallbackPayments)
  }
}

async function addPaymentToQueue(paymentData: Payment) {
  await redis.lPush('payment_queue', JSON.stringify(paymentData))
}

async function processPaymentWorker(workerId: number) {
  while (true) {
    try {
      const queueSize = await redis.lLen('payment_queue')
      const batchSize = Math.min(50, Math.max(5, Math.floor(queueSize / 20)))
      const batch = []

      for (let i = 0; i < batchSize; i++) {
        const item = await redis.rPop('payment_queue')
        if (!item) {
          break
        }
        batch.push(JSON.parse(item))
      }

      if (batch.length > 0) {
        // Select optimal processor based on health and minResponseTime
        const optimalProcessor = await selectOptimalProcessor()

        const promises = batch.map(payment =>
          processPaymentDirect(payment, optimalProcessor)
        )

        await Promise.all(promises)
        console.log(`Worker ${workerId}: Processed batch of ${batch.length} via ${optimalProcessor}, queue size: ${queueSize}`)

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
  const numWorkers = 12

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

type ProcessorHealth = {
  failing: boolean
  minResponseTime: number
}

async function getProcessorHealth(processorUrl: string): Promise<ProcessorHealth | null> {
  try {
    const response = await fetch(processorUrl + '/payments/service-health')

    await redis.setEx(`health_check:${processorUrl}`, 5, '1')

    if (!response.ok) return null

    const health = await response.json() as ProcessorHealth

    // Cache health info for 5 seconds
    await redis.setEx(`health_data:${processorUrl}`, 5, JSON.stringify(health))

    return health
  } catch {
    return null
  }
}

async function isProcessorHealthy(processorUrl: string): Promise<boolean> {
  const health = await getProcessorHealth(processorUrl)
  return health ? !health.failing : false
}

async function selectOptimalProcessor(): Promise<'default' | 'fallback'> {
  try {
    // Check if we can get cached health data first (avoid rate limits)
    const [defaultCache, fallbackCache] = await Promise.all([
      redis.get(`health_data:${defaultProcessorUrl}`),
      redis.get(`health_data:${fallbackProcessorUrl}`)
    ])

    let defaultHealth: ProcessorHealth | null = null
    let fallbackHealth: ProcessorHealth | null = null

    // Parse cached data
    if (defaultCache) {
      defaultHealth = JSON.parse(defaultCache)
    }
    if (fallbackCache) {
      fallbackHealth = JSON.parse(fallbackCache)
    }

    // If no cached data, check if we can make health requests (respect rate limits)
    const [canCheckDefault, canCheckFallback] = await Promise.all([
      redis.get(`health_check:${defaultProcessorUrl}`),
      redis.get(`health_check:${fallbackProcessorUrl}`)
    ])

    // Get fresh health data if not rate limited
    if (!canCheckDefault && !defaultHealth) {
      defaultHealth = await getProcessorHealth(defaultProcessorUrl)
    }
    if (!canCheckFallback && !fallbackHealth) {
      fallbackHealth = await getProcessorHealth(fallbackProcessorUrl)
    }

    // Select best processor based on health and minResponseTime
    const defaultHealthy = defaultHealth && !defaultHealth.failing
    const fallbackHealthy = fallbackHealth && !fallbackHealth.failing

    // If both are healthy, choose the one with lower response time
    if (defaultHealthy && fallbackHealthy) {
      const defaultTime = defaultHealth!.minResponseTime
      const fallbackTime = fallbackHealth!.minResponseTime

      console.log(`Both processors healthy - Default: ${defaultTime}ms, Fallback: ${fallbackTime}ms`)
      return defaultTime <= fallbackTime ? 'default' : 'fallback'
    }

    // If only one is healthy, use it
    if (defaultHealthy) {
      console.log(`Only default processor healthy (${defaultHealth!.minResponseTime}ms)`)
      return 'default'
    }
    if (fallbackHealthy) {
      console.log(`Only fallback processor healthy (${fallbackHealth!.minResponseTime}ms)`)
      return 'fallback'
    }

    // If neither is confirmed healthy, default to 'default' (better fees)
    console.log('No health info available, defaulting to default processor')
    return 'default'

  } catch (error) {
    console.error('Error selecting optimal processor:', error)
    return 'default'
  }
}

async function processPaymentDirect(payment: Payment, processorType: 'default' | 'fallback'): Promise<void> {
  try {
    const processorUrl = processorType === 'default' ? defaultProcessorUrl : fallbackProcessorUrl
    const fallbackProcessorType = processorType === 'default' ? 'fallback' : 'default'

    // Try primary processor
    const success = await processWithProcessor(payment, processorUrl)
    if (success) {
      await saveProcessedPayment(payment, processorType)
      return
    }

    // Try fallback processor
    const fallbackUrl = processorType === 'default' ? fallbackProcessorUrl : defaultProcessorUrl
    const fallbackSuccess = await processWithProcessor(payment, fallbackUrl)
    if (fallbackSuccess) {
      await saveProcessedPayment(payment, fallbackProcessorType)
      return
    }

    console.error(`Both processors failed for payment: ${payment.correlationId}`)
  } catch (error) {
    console.error(`Payment processing error: ${payment.correlationId}`, error)
  }
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
      body: JSON.stringify(paymentRequest),
    })
    return response.ok
  } catch (error) {
    if (error instanceof Error && error.name !== 'TimeoutError') {
      console.error('Payment processing error:', error.message)
    }
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