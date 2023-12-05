import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream } from 'fs'
import * as dotenv from "dotenv"
import Neo4j, { ManagedTransaction, type Session } from 'neo4j-driver'
import { WikipediaStream } from "./wikipediaStream"
import { getPageNodeFromPage, getQueriesFromPageNode } from "./pageUtils"
import { TransactionPool } from "./transactionPool"
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'

const main = async () => {
    const driver = Neo4j.driver(
        process.env.NEO4J_URI ?? '',
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? ''),
        {
            maxConnectionPoolSize: 100
        }
    )
    let stream: ReadStream | undefined

    const transactionPool: TransactionPool = new TransactionPool(driver, {
        maxConcurrenTransactions: 100,
        killOnError: true,
        onRejected: (error) => {
            console.log(error)
            process.emit("SIGTERM")
        }
    })

    const onProgramEnd = async (err?: Error) => {
        console.log('ending program')
        if (err != null) console.error(err)
        await driver.close()
        if (stream != null) stream.destroy()
        transactionPool.kill()
    }

    process.on('SIGINT', onProgramEnd)
    process.on('SIGTERM', onProgramEnd)

    const wikipediaStream: WikipediaStream = new WikipediaStream()
    wikipediaStream.on("page", (page) => {
        const pageNode = getPageNodeFromPage(page)
        const queries = getQueriesFromPageNode(pageNode)
        transactionPool.push(async (tx: ManagedTransaction) => {
            if (Math.random() < 0.05) console.log(`Writing [${pageNode.title}] to db`)
            for (const query of queries) {
                await tx.run(query)
            }
        })
    })

    stream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    stream.pipe(bz2()).pipe(wikipediaStream)
        .on('close', onProgramEnd)
        .on('error', onProgramEnd)
}

main().catch(console.error)
