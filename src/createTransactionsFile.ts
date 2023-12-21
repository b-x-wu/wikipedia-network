import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream, createWriteStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaStream } from "./wikipediaStream"
import { getPageNodeFromPage, getQueryStringsFromPageNode } from "./pageUtils"
import * as ConsoleStamp from 'console-stamp'
ConsoleStamp.default(console)
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'
const TRANSACTIONS_FILE_NAME = 'transactions.csv'
const TRANSACTIONS_FILE_DELIMITER = String.fromCharCode(31)
const BUFFER_CAPACITY = 1000

const hrtimeToString = (startTime: bigint, percision: number): string => {
    // in seconds
    const current: bigint = process.hrtime.bigint()
    return Number((current - startTime) / 1000000000n).toFixed(percision)
} 


const main = async () => {
    let timeBetweenWrites: bigint = 0n
    let timeBetweenReads: bigint = 0n

    let stream: ReadStream | undefined
    let transactionsWriteStream = createWriteStream(TRANSACTIONS_FILE_NAME)
    transactionsWriteStream.cork()

    let bufferSize: number = 0

    const onProgramEnd = async (err?: Error) => {
        console.log('ending program')
        if (err != null) console.error(err)
        console.log(`Writing out ${bufferSize} transactions. Operation took ${hrtimeToString(timeBetweenWrites, 3)}s`)
        transactionsWriteStream.close()
        if (stream != null) stream.destroy()
    }

    process.on('exit', onProgramEnd)
    process.on('uncaughtException', onProgramEnd)
    process.on('SIGTERM', onProgramEnd)

    const wikipediaStream: WikipediaStream = new WikipediaStream()
    wikipediaStream.on("page", (page) => {
        if (process.hrtime.bigint() - timeBetweenReads > 1000000000n) {
            console.warn('Time between page reads is over 1 second.')
        }
        timeBetweenReads = process.hrtime.bigint()
        if (page.namespace !== 0) return
        const pageNode = getPageNodeFromPage(page)
        const queries = getQueryStringsFromPageNode(pageNode)
        const queriesString = queries.join(TRANSACTIONS_FILE_DELIMITER) + '\n'

        transactionsWriteStream.write(queriesString)
        bufferSize += 1

        if (bufferSize === BUFFER_CAPACITY) {
            process.nextTick(() => {
                stream?.pause()
                transactionsWriteStream.uncork()
            })
            console.log(`Writing out ${bufferSize} transactions. Operation took ${hrtimeToString(timeBetweenWrites, 3)}s`)
            timeBetweenWrites = process.hrtime.bigint()
            bufferSize = 0
            process.nextTick(() => {
                transactionsWriteStream.cork()
                stream?.resume()
            })
            
        }
        stream?.resume()
    })

    timeBetweenWrites = process.hrtime.bigint()
    timeBetweenReads = process.hrtime.bigint()
    stream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    stream.pipe(bz2()).pipe(wikipediaStream)
        .on('close', onProgramEnd)
        .on('error', onProgramEnd)
}

main().catch(console.error)
