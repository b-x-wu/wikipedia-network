import Neo4j, { Driver, ManagedTransaction, SessionMode, driver } from 'neo4j-driver'

export interface JobPoolOptions {
    maxConcurrentJobs: number
    onRejected: (reason: any) => void
    killOnError: boolean
    onPause: () => void
    onResume: () => void 
}

export class JobPool {
    public maxConcurrentJobs: number = 1000
    public killOnError: boolean = false
    public onPause: () => void = () => {}
    public onResume: () => void = () => {}
    private _transactionQueue: Array<() => Promise<void>> = []
    public jobsRunning: number = 0
    private _onRejected: (reason: any) => void
    public isKilled: boolean = false

    constructor(options?: Partial<JobPoolOptions>) {
        this.maxConcurrentJobs = options?.maxConcurrentJobs ?? this.maxConcurrentJobs
        this.killOnError = options?.killOnError ?? this.killOnError
        this.onPause = options?.onPause ?? this.onPause
        this.onResume = options?.onResume ?? this.onResume

        this._onRejected = (reason: any) => {
            (options?.onRejected ?? (() => {}))(reason)
            if (this.killOnError) this.kill()
        }
    }

    push(job: () => Promise<void>): this {
        if (this.isKilled) {
            throw new Error('Cannot push job to a killed transaction pool.')
        }

        this._transactionQueue.push(job)
        if (this.jobsRunning < this.maxConcurrentJobs) {
            const jobToRun = this._transactionQueue.shift()
            if (jobToRun != null) this._runJob(jobToRun)
        }

        return this
    }

    kill(): void {
        this.isKilled = true
    }

    private _jobCallback(): void {
        this.jobsRunning -= 1
        if (this.isKilled) return

        if (this.jobsRunning < this.maxConcurrentJobs) {
            this.onResume()
        }

        const job = this._transactionQueue.shift()
        if (job != null) this._runJob(job)
    }
    
    private _runJob(job: () => Promise<void>) {
        this.jobsRunning += 1
        job().then(this._jobCallback.bind(this))
            .catch(this._onRejected)

        if (this.jobsRunning >= this.maxConcurrentJobs) {
            this.onPause()
        }
    }
}