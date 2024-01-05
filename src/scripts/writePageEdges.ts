import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaParser } from "../wikipediaParser"
import { pageToPageEdges, pageToPageNode } from "../pageUtils"
import Neo4j, { Session } from "neo4j-driver"
import * as ConsoleStamp from 'console-stamp'
import { Writable } from "stream"
import { PageEdge, PageNode } from "../types"
import * as fs from 'fs/promises'
import { existsSync } from "fs"
import { pathToFileURL } from "url"
import path from 'path'
ConsoleStamp.default(console)
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = process.env.WIKIPEDIA_ZIP_FILE_NAME ?? 'enwiki-20230820-pages-articles-multistream.xml.bz2'
const PAGE_EDGE_FILE_NAME = 'page_edges.csv'
const MAX_PAGE_EDGE_BUFFER_SIZE = 20000

const main = async () => {
    let readStream: ReadStream | undefined
    const driver = Neo4j.driver(
        process.env.NEO4J_URI ?? '',
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? ''),
        {
            maxConnectionLifetime: 1000 * 60 * 5, // five minutes
        }
    )

    let pageEdgeBuffer: PageEdge[] = []
    const writePageEdgeBufferToFile = async () => {
        console.log('Writing out to csv...')
        await fs.writeFile(
            PAGE_EDGE_FILE_NAME,
            pageEdgeBuffer
                .map((pageEdge) => {
                    return `${String.fromCharCode(31)}${pageEdge.from}${String.fromCharCode(31)}${pageEdge.to}`
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
                    await tx.run(`LOAD CSV FROM '${pathToFileURL(path.resolve(PAGE_EDGE_FILE_NAME))}' AS line FIELDTERMINATOR '\\u001F' MATCH (from:Page {title: line[1]}), (to:Page {title: line[2]}) MERGE (from)-[:LINKS_TO]->(to)`)
                    console.log('Loaded relationships to Neo4j.')
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
        if (existsSync(PAGE_EDGE_FILE_NAME)) await fs.unlink(PAGE_EDGE_FILE_NAME)
    }

    readStream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    
    const wikipediaParser = new WikipediaParser()
    wikipediaParser.onpage((page) => {
        if (page.namespace !== 0) return
        const pageEdges = pageToPageEdges(page)
        if (Math.random() < 0.001) console.log(`Adding edges for ${page.title} to buffer.`)
        pageEdgeBuffer.push(...pageEdges)
    })

    const dbWriteStream = new Writable({
        write: (chunk, encoding, callback) => {
            (new Promise<void>(async (resolve, reject) => {
                wikipediaParser.write(chunk.toString())
                if (pageEdgeBuffer.length >= MAX_PAGE_EDGE_BUFFER_SIZE) {
                    await writePageEdgeBufferToFile()
                    pageEdgeBuffer = []

                    writeCsvToNeo4j().then(resolve).catch(reject)
                } else {
                    resolve()
                }
            })).then(() => callback()).catch(callback)
        },
        objectMode: true
    })

    dbWriteStream.on("finish", async () => {
        await writePageEdgeBufferToFile()
        await writeCsvToNeo4j()
        await cleanUp()
    })

    dbWriteStream.on("error", cleanUp);
    readStream.pipe(bz2()).pipe(dbWriteStream)
}

main().catch(console.error)
