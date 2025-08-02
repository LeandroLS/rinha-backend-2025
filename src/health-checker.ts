import { Pool } from 'pg'

export type ProcessorHealth = {
  failing: boolean
  minResponseTime: number
}

export type HealthStatus = {
  default: ProcessorHealth
  fallback: ProcessorHealth
  lastChecked: Date
}

export class HealthChecker {
  private cachedProcessor: 'default' | 'fallback' = 'default'
  private lastCacheUpdate = 0
  private cacheUpdatePromise: Promise<void> | null = null

  constructor(
    private pgPool: Pool,
    private defaultProcessorUrl: string,
    private fallbackProcessorUrl: string
  ) { }

  async getProcessorHealth(): Promise<HealthStatus> {
    const cached = await this.getCachedHealth()
    if (cached) {
      return cached
    }

    const [defaultHealth, fallbackHealth] = await Promise.all([
      this.checkSingleProcessor('default', this.defaultProcessorUrl),
      this.checkSingleProcessor('fallback', this.fallbackProcessorUrl)
    ])

    const healthStatus: HealthStatus = {
      default: defaultHealth,
      fallback: fallbackHealth,
      lastChecked: new Date()
    }

    await this.cacheHealth(healthStatus)

    return healthStatus
  }

  private async checkSingleProcessor(
    name: string,
    processorUrl: string
  ): Promise<ProcessorHealth> {
    try {
      const response = await fetch(`${processorUrl}/payments/service-health`, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(500)
      })

      if (!response.ok) {
        console.log(`❌ ${name} processor unhealthy: HTTP ${response.status}`)
        return { failing: true, minResponseTime: 999999 }
      }

      const healthData = await response.json()

      return {
        failing: healthData.failing || false,
        minResponseTime: Math.max(healthData.minResponseTime || 0)
      }
    } catch (error) {
      console.log(`❌ ${name} processor error:`, error)
      return { failing: true, minResponseTime: 999999 }
    }
  }

  private async getCachedHealth(): Promise<HealthStatus | null> {
    try {
      const { rows } = await this.pgPool.query(`
        SELECT value, created_at FROM health_check_cache WHERE key = 'health_status'
      `)

      if (rows.length > 0) {
        const cached = rows[0].value
        const createdAt = new Date(rows[0].created_at)
        const ageSeconds = (Date.now() - createdAt.getTime()) / 1000

        if (ageSeconds > 5) {
          await this.pgPool.query('DELETE FROM health_check_cache WHERE key = $1', ['health_status'])
          return null
        }

        return {
          ...cached,
          lastChecked: new Date(cached.lastChecked)
        }
      }
      return null
    } catch (error) {
      console.error('Error getting cached health:', error)
      return null
    }
  }

  private async cacheHealth(healthStatus: HealthStatus): Promise<void> {
    try {
      await this.pgPool.query(`
        INSERT INTO health_check_cache (key, value) 
        VALUES ('health_status', $1) 
        ON CONFLICT (key) DO UPDATE SET 
          value = EXCLUDED.value,
          created_at = NOW()
      `, [JSON.stringify(healthStatus)])
    } catch (error) {
      console.error('Error caching health status:', error)
    }
  }

  selectOptimalProcessor(): 'default' | 'fallback' {
    this.updateCacheIfNeeded()

    return this.cachedProcessor
  }

  async selectOptimalProcessorAsync(): Promise<'default' | 'fallback'> {
    try {
      const health = await this.getProcessorHealth()

      if (!health.default.failing) {
        return 'default'
      }

      if (!health.fallback.failing) {
        return 'fallback'
      }

      if (health.default.minResponseTime <= health.fallback.minResponseTime) {
        return 'default'
      }

      return 'fallback'
    } catch (error) {
      console.error('Error selecting optimal processor:', error)
      return 'default'
    }
  }

  private updateCacheIfNeeded(): void {
    const now = Date.now()

    if (now - this.lastCacheUpdate > 5000 && !this.cacheUpdatePromise) {
      this.cacheUpdatePromise = this.updateCache().finally(() => {
        this.cacheUpdatePromise = null
      })
    }
  }

  private async updateCache(): Promise<void> {
    try {
      const optimalProcessor = await this.selectOptimalProcessorAsync()
      this.cachedProcessor = optimalProcessor
      this.lastCacheUpdate = Date.now()
    } catch (error) {
      console.error('Error updating processor cache:', error)
    }
  }

  // Start background health monitoring
  startHealthMonitoring(intervalSeconds: number = 7): void {
    const monitor = async () => {
      try {
        return await this.getProcessorHealth()
      } catch (error) {
        console.error('Health monitoring error:', error)
      }
    }

    // Initial check
    monitor()

    // Periodic checks
    setInterval(monitor, intervalSeconds * 1000)
  }
}