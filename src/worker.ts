import { Pool } from 'pg'
import { HealthChecker } from './health-checker.js'

export type PaymentQueueItem = {
  id: number,
  correlation_id: string,
  amount: number,
  processor_type: 'default' | 'fallback',
  created_at: Date,
  processed: boolean,
}

export async function processPayment(
  pgPool: Pool,
  paymentQueueItem: PaymentQueueItem,
  processorUrl: string,
  processorType: 'default' | 'fallback'
): Promise<void> {
  try {
    const response = await fetch(processorUrl + '/payments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        correlationId: paymentQueueItem.correlation_id,
        amount: paymentQueueItem.amount,
        requestedAt: new Date().toISOString(),
      }),
      signal: AbortSignal.timeout(500)
    })

    if (!response.ok) {
      console.error(`Failed to process payment with ${processorUrl}: ${response.statusText}`)
      return await addPaymentBackToQueue(pgPool, false, paymentQueueItem.id)
    }

    await setPaymentAsProcessed(pgPool, paymentQueueItem.id, processorType)
  } catch (error) {
    console.error(`Error processing payment with ${processorUrl}:`, error)
  }
}

async function processPaymentsWorker(
  pgPool: Pool,
  workerNumber: number,
  defaultProcessorUrl: string,
  fallbackProcessorUrl: string,
  healthChecker: HealthChecker
): Promise<void> {
  console.log(`Worker ${workerNumber} started`)
  while (true) {
    try {
      const { rows } = await pgPool.query(`
        UPDATE payment_queue 
        SET processed = true 
        WHERE id = ANY(
          SELECT id FROM payment_queue 
          WHERE processed = false 
          ORDER BY id 
          LIMIT 20
          FOR UPDATE SKIP LOCKED
        ) 
        RETURNING *
      `)

      if (rows.length === 0) {
        await new Promise(resolve => setTimeout(resolve, 50))
        continue
      }

      const optimalProcessor = healthChecker.selectOptimalProcessor()
      const processorUrl = optimalProcessor === 'default' ? defaultProcessorUrl : fallbackProcessorUrl

      const batchPromises = rows.map(payment =>
        processPayment(pgPool, payment, processorUrl, optimalProcessor)
      )

      await Promise.all(batchPromises)
    } catch (error) {
      console.error(`Worker ${workerNumber} error:`, error)
      await new Promise(resolve => setTimeout(resolve, 100))
    }
  }
}

export function startProcessPaymentWorkers(
  pgPool: Pool,
  defaultProcessorUrl: string,
  fallbackProcessorUrl: string,
  healthChecker: HealthChecker,
  numWorkers: number = 5
): void {
  for (let i = 0; i < numWorkers; i++) {
    processPaymentsWorker(pgPool, i + 1, defaultProcessorUrl, fallbackProcessorUrl, healthChecker)
  }
}

async function setPaymentAsProcessed(
  pgPool: Pool,
  paymentQueueItemId: number,
  processorType: 'default' | 'fallback'
): Promise<void> {
  try {
    await pgPool.query(`
      UPDATE payment_queue
      SET processed = true, processor_type = $1
      WHERE id = $2
    `, [processorType, paymentQueueItemId])
  } catch (error) {
    console.error('Error setting payment as processed:', error)
  }
}

async function addPaymentBackToQueue(
  pgPool: Pool,
  isProcessed: boolean,
  paymentQueueItemId: number
): Promise<void> {
  try {
    await pgPool.query(`
      UPDATE payment_queue
      SET processed = $1
      WHERE id = $2
    `, [isProcessed, paymentQueueItemId])
  } catch (error) {
    console.error('Error updating payment in queue:', error)
  }
}