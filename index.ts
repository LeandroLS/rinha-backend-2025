import Fastify from 'fastify'
import { createClient } from 'redis'

const fastify = Fastify({
  logger: true
})

const redis = createClient({
  url: 'redis://redis:6379'
})

redis.on('error', (err) => console.log('Redis Client Error', err))

await redis.connect()

// Queue functions
async function addPaymentToQueue(paymentData: any) {
  await redis.lPush('payment_queue', JSON.stringify(paymentData))
}

async function processPaymentQueue() {
  while (true) {
    try {
      const item = await redis.brPop('payment_queue', 5)
      if (item) {
        const payment = JSON.parse(item.element)
        await processPayment(payment)
      }
    } catch (error) {
      console.error('Error processing payment queue:', error)
      await new Promise(resolve => setTimeout(resolve, 1000))
    }
  }
}

async function processPayment(payment: any) {
  // TODO: Implement actual payment processing logic
  console.log('Processing payment:', payment)

  // Simulate processing time
  await new Promise(resolve => setTimeout(resolve, 100))

  console.log('Payment processed:', payment.correlationId)
}

// Start queue processor
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

try {
  await fastify.listen({ port: 3000, host: '0.0.0.0' })
} catch (err) {

  fastify.log.error(err)
  process.exit(1)
}