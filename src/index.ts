import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaParser, WikipediaStream } from "./wikipediaStream"
import { getPageNodeFromPage, pageNodeToArticledbType } from "./pageUtils"
import { type ThroughStream } from 'through'
import { JobPool } from "./jobPool"
import * as ConsoleStamp from 'console-stamp'
import { ClientSession, MongoClient, ServerApiVersion } from "mongodb"
import { ArticleDbType, Page, PageNode } from "./types"
import { Writable } from "stream"
ConsoleStamp.default(console)
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'
const MAX_PAGE_NODE_BUFFER_SIZE = 20000

const main = async () => {
    const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASSWORD}@cluster0.cftdtes.mongodb.net/?retryWrites=true&w=majority`
    let readStream: ReadStream | undefined
    let bz2Stream: ThroughStream | undefined
    const mongoClient = await new MongoClient(uri, {
        serverApi: {
            version: ServerApiVersion.v1,
            strict: true,
            deprecationErrors: false
        }
    }).connect()
    const collection = mongoClient.db('wikipedia-network').collection<ArticleDbType>('page-nodes')

    const onProgramEnd = async (err?: Error) => {
        console.log('Ending program')
        await mongoClient.close()
        if (err != null) console.error(err)
        if (readStream != null && !readStream.closed) readStream.close()
    }

    const onProgramEndWithMessage = (message: string) => {
        return async () => {
            await onProgramEnd(new Error(message))
        }
    }

    const wikipediaStream = new WikipediaStream()
    wikipediaStream.on("error", (e) => {
        onProgramEndWithMessage("Wikipedia stream error: " + e.message)
        process.emit('SIGINT')
    })

    readStream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    readStream.on('error', (e) => onProgramEndWithMessage("Read stream error: " + e.message))
    bz2Stream = bz2()
    bz2Stream.on('error', (e) => onProgramEndWithMessage("bz2 stream error: " + e.message))

    
    let articleDbTypeBuffer: ArticleDbType[] = []
    const wikipediaParser = new WikipediaParser()
    wikipediaParser.onpage((page) => {
        if (page.namespace !== 0) return
        const pageNode = getPageNodeFromPage(page)
        if (Math.random() < 0.001) console.log(`Adding page ${pageNode.title} to buffer.`)
        articleDbTypeBuffer.push(pageNodeToArticledbType(pageNode))
    })
    const mongoWriteStream = new Writable({
        write: (chunk, encoding, callback) => {

            (new Promise<void>(async (resolve, reject) => {
                wikipediaParser.write(chunk.toString())
                if (articleDbTypeBuffer.length >= MAX_PAGE_NODE_BUFFER_SIZE) {
                    console.log('Starting mongo session. Writing out pages...')
                    let session: ClientSession | undefined
                    try {
                        session = mongoClient.startSession()
                        await collection.insertMany(articleDbTypeBuffer, { session })
                    } catch (e) {
                        reject(e)
                    } finally {
                        console.log(`Closing mongo session. Wrote out ${articleDbTypeBuffer.length} pages.`)
                        if (session != null) await session.endSession()
                        articleDbTypeBuffer = []
                        resolve()
                    }
                } else {
                    resolve()
                }
            })).then(() => callback()).catch(callback)
        },
        objectMode: true
    })
    mongoWriteStream.on("finish", async () => {
        let session: ClientSession | undefined
        try {
            const session = mongoClient.startSession()
            await collection.insertMany(articleDbTypeBuffer, { session })
        } finally {
            console.log(`Closing mongo session. Wrote out ${articleDbTypeBuffer.length} pages.`)
            if (session != null) await session.endSession()
            await mongoClient.close()
        }
    })
    readStream.pipe(bz2()).pipe(mongoWriteStream)
}

main().catch(console.error)
