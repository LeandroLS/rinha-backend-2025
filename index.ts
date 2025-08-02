import Fastify from 'fastify'
import { Pool } from 'pg'
import { startProcessPaymentWorkers } from './src/worker.js'
import { HealthChecker } from './src/health-checker.js'

const fastify = Fastify({
  logger: false,
})

const pgPool = new Pool({
  host: 'postgres',
  port: 5432,
  database: 'rinha',
  user: 'rinha',
  password: 'rinha',
  max: 35,
  min: 2,
  idleTimeoutMillis: 15000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 10000,
  query_timeout: 10000,
})

const defaultProcessorUrl = process.env.PROCESSOR_DEFAULT_URL!
const fallbackProcessorUrl = process.env.PROCESSOR_FALLBACK_URL!

pgPool.on('error', (err) => console.log('PostgreSQL Pool Error', err))

await Promise.all([
  fetch(defaultProcessorUrl + '/admin/purge-payments', {
    method: 'POST',
  }),
  fetch(fallbackProcessorUrl + '/admin/purge-payments', {
    method: 'POST',
  })
])

type Payment = {
  amount: number,
  correlationId: string,
}

async function getPaymentsSummary(from?: string, to?: string) {
  const query = `
    SELECT processor_type, COUNT(*) AS total_requests, SUM(amount) AS total_amount
    FROM payment_queue
    WHERE processed = true 
    AND created_at >= $1 AND created_at <= $2
    GROUP BY processor_type
  `
  const values = [from || '1970-01-01', to || new Date().toISOString()]
  const { rows } = await pgPool.query(query, values)

  const result = {
    default: { totalRequests: 0, totalAmount: 0 },
    fallback: { totalRequests: 0, totalAmount: 0 }
  }

  rows.forEach(row => {
    const processorType = row.processor_type as 'default' | 'fallback'
    result[processorType] = {
      totalRequests: parseInt(row.total_requests, 10),
      totalAmount: parseFloat(row.total_amount || '0')
    }
  })

  return result
}
const healthChecker = new HealthChecker(pgPool, defaultProcessorUrl, fallbackProcessorUrl)
healthChecker.startHealthMonitoring(6)

startProcessPaymentWorkers(pgPool, defaultProcessorUrl, fallbackProcessorUrl, healthChecker, 6)

async function addPaymentToQueue(paymentData: Payment) {
  try {
    await pgPool.query('INSERT INTO payment_queue (correlation_id, amount) VALUES ($1, $2)', [paymentData.correlationId, paymentData.amount])
  } catch (error) {
    fastify.log.error('Error adding payment to queue:', error)
  }
}

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