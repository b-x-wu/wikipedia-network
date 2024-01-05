import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaParser } from "../wikipediaParser"
import { pageToPageNode } from "../pageUtils"
import Neo4j, { Session } from "neo4j-driver"
import * as ConsoleStamp from 'console-stamp'
import { Writable } from "stream"
import { PageNode } from "../types"
import * as fs from 'fs/promises'
import { existsSync } from "fs"
import { pathToFileURL } from "url"
import path from 'path'
ConsoleStamp.default(console)
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = process.env.WIKIPEDIA_ZIP_FILE_NAME ?? 'enwiki-20230820-pages-articles-multistream.xml.bz2'
const PAGE_NODE_FILE_NAME = 'page_nodes.csv'
const MAX_PAGE_NODE_BUFFER_SIZE = 20000

const main = async () => {
    let readStream: ReadStream | undefined
    const driver = Neo4j.driver(
        process.env.NEO4J_URI ?? '',
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? ''),
        {
            maxConnectionLifetime: 1000 * 60 * 5, // five minutes
        }
    )

    // set page title uniqueness constraint
    await driver.executeQuery('CREATE CONSTRAINT page_title FOR (p:Page) REQUIRE p.title IS UNIQUE')

    let pageNodeBuffer: PageNode[] = []
    const writePageNodeBufferToFile = async () => {
        console.log('Writing out to csv...')
        await fs.writeFile(
            PAGE_NODE_FILE_NAME,
            pageNodeBuffer
                .map((pageNode) => {
                    return `${String.fromCharCode(31)}${pageNode.title}${String.fromCharCode(31)}${pageNode.isRedirect}`
                }).join('\n')
        )
        console.log('Wrote to csv.')
    }

    const writeCsvToNeo4j = () => {
        return new Promise<void>(async (resolve, reject) => {
            console.log('Loading to Neo4j...')
            let session: Session | undefined
            try {
                session = driver.session({
                    defaultAccessMode: Neo4j.session.WRITE
                })
                await session.executeWrite(async (tx): Promise<void> => {
                    await tx.run(`LOAD CSV FROM '${pathToFileURL(path.resolve(PAGE_NODE_FILE_NAME))}' AS line FIELDTERMINATOR '\\u001F' WITH line[1] AS title, toBoolean(line[2]) AS isRedirect MERGE (p:Page {title: title}) SET p.isRedirect = isRedirect RETURN count(p)`)
                    console.log('Loaded to Neo4j.')
                })
                resolve()
            } catch (e) {
                reject(e)
            } finally {
                if (session != null) await session.close()
            }
        })
    }

    const cleanUp = async (err?: Error) => {
        console.log('Ending program')
        await driver.close()
        if (err != null) console.error(err)
        if (readStream != null && !readStream.closed) readStream.close()
        if (existsSync(PAGE_NODE_FILE_NAME)) await fs.unlink(PAGE_NODE_FILE_NAME)
    }

    readStream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    
    const wikipediaParser = new WikipediaParser()
    wikipediaParser.onpage((page) => {
        if (page.namespace !== 0) return
        const pageNode = pageToPageNode(page)
        if (Math.random() < 0.001) console.log(`Adding page ${pageNode.title} to buffer.`)
        pageNodeBuffer.push(pageNode)
    })

    const dbWriteStream = new Writable({
        write: (chunk, encoding, callback) => {
            (new Promise<void>(async (resolve, reject) => {
                wikipediaParser.write(chunk.toString())
                if (pageNodeBuffer.length >= MAX_PAGE_NODE_BUFFER_SIZE) {
                    await writePageNodeBufferToFile()
                    pageNodeBuffer = []

                    writeCsvToNeo4j().then(resolve).catch(reject)
                } else {
                    resolve()
                }
            })).then(() => callback()).catch(callback)
        },
        objectMode: true
    })

    dbWriteStream.on("finish", async () => {
        await writePageNodeBufferToFile()
        await writeCsvToNeo4j()
        await cleanUp()
    })

    dbWriteStream.on("error", cleanUp);
    readStream.pipe(bz2()).pipe(dbWriteStream)
}

main().catch(console.error)
