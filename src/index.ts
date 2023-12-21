import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream } from 'fs'
import * as dotenv from "dotenv"
import Neo4j, { ManagedTransaction, Neo4jError, type Session } from 'neo4j-driver'
import { WikipediaStream } from "./wikipediaStream"
import { getPageNodeFromPage, getQueriesFromPageNode } from "./pageUtils"
import { type ThroughStream } from 'through'
import { JobPool } from "./jobPool"
import * as ConsoleStamp from 'console-stamp'
ConsoleStamp.default(console)
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'

const main = async () => {
    const driver = Neo4j.driver(
        process.env.NEO4J_URI ?? '',
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? ''),
        {
            maxConnectionPoolSize: 200,
            maxConnectionLifetime: 1000 * 60, // one minute
        }
    )
    let readStream: ReadStream | undefined
    let bz2Stream: ThroughStream | undefined

    const transactionPool: JobPool = new JobPool({
        maxConcurrentJobs: 100,
        killOnError: true,
        onPause: () => {
            // console.log('Pausing job pool.')
            if (readStream != null && !readStream.isPaused()) {
                readStream.pause()
            }

            // if (bz2Stream != null && !bz2Stream.isPaused) {
            //     bz2Stream.pause()
            // }
        },
        onResume: () => {
            // console.log('Resuming job pool.')
            if (readStream != null && readStream.isPaused()) {
                readStream.resume()
            }

            // if (bz2Stream != null && !bz2Stream.isPaused()) {
            //     bz2Stream.resume()
            // }
        },
        onRejected: (error) => {
            console.log(error)
        }
    })

    const onProgramEnd = async (err?: Error) => {
        console.log('Ending program')
        if (err != null) console.error(err)
        await driver.close()
        if (readStream != null && !readStream.closed) readStream.close()
        transactionPool.kill()
    }

    const onProgramEndWithMessage = (message: string) => {
        return async () => {
            await onProgramEnd(new Error(message))
        }
    }

    const wikipediaStream = new WikipediaStream()
    wikipediaStream.on("page", (page) => {
        if (page.namespace !== 0) return
        transactionPool.push(async () => {
            const pageNode = getPageNodeFromPage(page)
            // const queries = getQueriesFromPageNode(pageNode)
            const query = {
                text: 'MERGE (p: Page { title: $title }) SET p.isRedirect = $isRedirect SET p.namespace = $namespace',
                parameters: pageNode
            }
            let session: Session = driver.session({
                defaultAccessMode: Neo4j.session.WRITE
            })

            try {
                if (Math.random() < 0.001) console.log(`Writing out page node: ${pageNode.title}`)
                const executeWrite = async () => {
                    await session.executeWrite(async (tx: ManagedTransaction) => {
                        await tx.run(query)
                    })
                }
                
                let retry: boolean = true
                while (retry) {
                    try {
                        await executeWrite()
                        retry = false
                    } catch (e: any) {
                        if (e instanceof Neo4jError) {
                            retry = e.retriable
                            continue
                        }
                        retry = false
                    }
                }
            } catch (e) {
                throw e
            } finally {
                await session.close()
            }
        })
    })

    wikipediaStream.on("close", onProgramEndWithMessage("Wikipedia stream closed."))
    wikipediaStream.on("error", (e) => {
        onProgramEndWithMessage("Wikipedia stream error: " + e.message)
        process.emit('SIGINT')
    })

    readStream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    readStream.on('close', onProgramEndWithMessage("Read stream closed"))
    readStream.on('error', (e) => onProgramEndWithMessage("Read stream error: " + e.message))
    bz2Stream = bz2()
    bz2Stream.on('close', onProgramEndWithMessage("bz2 stream closed"))
    bz2Stream.on('error', (e) => onProgramEndWithMessage("bz2 stream error: " + e.message))
    readStream.pipe(bz2()).pipe(wikipediaStream)
}

main().catch(console.error)
