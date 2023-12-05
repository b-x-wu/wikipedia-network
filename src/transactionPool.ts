import Neo4j, { Driver, ManagedTransaction, SessionMode, driver } from 'neo4j-driver'

export interface TransactionPoolOptions {
    maxConcurrenTransactions: number
    onRejected: (reason: any) => void
    killOnError: boolean
}

export class TransactionPool {
    public maxConcurrentTransactions: number
    public onRejected: (reason: any) => void = () => {}
    public killOnError: boolean = false
    private _driver: Driver
    private _transactionQueue: Array<(tx: ManagedTransaction) => Promise<void>> = []
    private _transactionsRunning: number = 0
    private _onRejected: (reason: any) => void
    private _killed: boolean = false

    constructor(driver: Driver, options?: Partial<TransactionPoolOptions>) {
        this._driver = driver
        this.maxConcurrentTransactions = options?.maxConcurrenTransactions ?? 1000
        this.onRejected = options?.onRejected ?? (() => {})
        this.killOnError = options?.killOnError ?? false
        this._onRejected = (reason: any) => {
            this.onRejected(reason)
            if (this.killOnError) this.kill()
        }
    }

    push(job: (tx: ManagedTransaction) => Promise<void>): this {
        if (this._killed) {
            throw new Error('Cannot push job to a killed transaction pool.')
        }

        this._transactionQueue.push(job)
        if (this._transactionsRunning < this.maxConcurrentTransactions) {
            const jobToRun = this._transactionQueue.shift()
            if (jobToRun != null) this._runJob(jobToRun)
        }

        return this
    }

    kill(): void {
        this._killed = true
    }

    private _jobCallback(): void {
        this._transactionsRunning -= 1
        if (this._killed) return

        const job = this._transactionQueue.shift()
        if (job != null) this._runJob(job)
    }
    
    private _runJob(job: (tx: ManagedTransaction) => Promise<void>) {
        const session = this._driver.session({ defaultAccessMode: Neo4j.session.WRITE })
        this._transactionsRunning += 1
        session.executeWrite(job)
            .then(() => session.close().catch(this._onRejected))
            .then(this._jobCallback.bind(this))
            .catch(this._onRejected)
    }
}