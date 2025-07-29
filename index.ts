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

async function saveProcessedPayment(payment: any, processor: 'default' | 'fallback', success: boolean) {
  const timestamp = new Date().toISOString()

  await redis.hSet(`p:${payment.correlationId}`, {
    correlationId: payment.correlationId,
    amount: payment.amount.toString(),
    processor,
    success: success.toString(),
    processedAt: timestamp,
  })

  await redis.hIncrBy(`stats:${processor}`, 'totalRequests', 1)
  await redis.hIncrByFloat(`stats:${processor}`, 'totalAmount', success ? payment.amount : 0)
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
      const success = payment.success === 'true'

      if (payment.processor === 'default') {
        defaultResult.totalRequests++
        if (success) {
          defaultResult.totalAmount += amount
        }
      } else if (payment.processor === 'fallback') {
        fallbackResult.totalRequests++
        if (success) {
          fallbackResult.totalAmount += amount
        }
      }
    }
  }

  return {
    default: defaultResult,
    fallback: fallbackResult
  }
}

async function addPaymentToQueue(paymentData: any) {
  await redis.lPush('payment_queue', JSON.stringify(paymentData))
}

async function processPaymentQueue() {
  while (true) {
    try {
      const item = await redis.rPop('payment_queue')
      if (item) {
        const payment = JSON.parse(item)
        await processPayment(payment)
      }
    } catch (error) {
      console.error('Error processing payment queue:', error)
      await new Promise(resolve => setTimeout(resolve, 1000))
    }
  }
}

async function processPayment(payment: any) {
  try {
    console.log('Processing payment:', payment.correlationId)

    // Check if default processor is available
    const isDefaultAvailable = await isProcessorHealthy(defaultProcessorUrl)
    if (!isDefaultAvailable) {
      console.log('Default processor not available, trying fallback')
      return await tryFallbackProcessor(payment)
    }

    // Try to process with default processor
    console.log('Default processor is healthy')
    const success = await processWithProcessor(payment, defaultProcessorUrl)
    if (success) {
      return await saveProcessedPayment(payment, 'default', true)
    }

    // Default processing failed, try fallback
    console.log('Default processor failed, trying fallback')
    await tryFallbackProcessor(payment)

  } catch (error) {
    console.error('Error processing payment:', payment.correlationId, error)
    await saveProcessedPayment(payment, 'default', false)
  }
}

async function isProcessorHealthy(processorUrl: string): Promise<boolean> {
  try {
    const response = await fetch(processorUrl + '/payments/service-health')
    if (!response.ok) return false

    const health = await response.json()
    return health.failing === false
  } catch {
    return false
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
      body: JSON.stringify(paymentRequest)
    })

    return response.ok
  } catch {
    return false
  }
}

async function tryFallbackProcessor(payment: any) {
  try {
    console.log(`Trying fallback processor for payment: ${payment.correlationId}`)

    const success = await processWithProcessor(payment, fallbackProcessorUrl)
    await saveProcessedPayment(payment, 'fallback', success)

    if (success) {
      console.log(`Payment ${payment.correlationId} processed with fallback`)
    } else {
      console.log(`Payment ${payment.correlationId} failed on both processors`)
    }

  } catch (error) {
    console.error('Error with fallback processor:', payment.correlationId, error)
    await saveProcessedPayment(payment, 'fallback', false)
  }
}

processPaymentQueue()

const schema = {
  body: {
    type: 'object',
    required: ['correlationId', 'amount'],
    properties: {
      amount: { type: 'number', multipleOf: 0.01, minimum: 0.01 },
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
    await addPaymentToQueue(request.body)
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