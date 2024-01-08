import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream, createWriteStream, unlinkSync } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaParser } from "../wikipediaParser"
import { pageToPageEdges } from "../pageUtils"
import Neo4j, { Session } from "neo4j-driver"
import * as ConsoleStamp from 'console-stamp'
import { Writable } from "stream"
import { PageEdge } from "../types"
import * as fs from 'fs/promises'
import { existsSync } from "fs"
import { pathToFileURL } from "url"
import path from 'path'
ConsoleStamp.default(console)
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = process.env.WIKIPEDIA_ZIP_FILE_NAME ?? 'enwiki-20230820-pages-articles-multistream.xml.bz2'
const PAGE_EDGE_FILE_NAME = 'page_edges.csv'
const MAX_PAGE_EDGE_BUFFER_SIZE = 100000

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
    const writePageEdgeBufferToFile = () => {
        console.log('Writing out to csv...')
        if (existsSync(PAGE_EDGE_FILE_NAME)) unlinkSync(PAGE_EDGE_FILE_NAME)
        const fileWriteStream = createWriteStream(PAGE_EDGE_FILE_NAME, {
            flags: 'a'
        })
        while (pageEdgeBuffer.length > 0) {
            const pageEdge = pageEdgeBuffer.shift()
            if (pageEdge != null) {
                fileWriteStream.write(`${String.fromCharCode(31)}${pageEdge.from}${String.fromCharCode(31)}${pageEdge.to}\n`)
            }
        }
        fileWriteStream.end()
        console.log('Wrote to csv.')
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
            wikipediaParser.write(chunk.toString());
            
            if (pageEdgeBuffer.length < MAX_PAGE_EDGE_BUFFER_SIZE) {
                callback()
                return
            }

            const session = driver.session({ defaultAccessMode: Neo4j.session.WRITE })
            writePageEdgeBufferToFile()
            console.log('Loading to Neo4j...')
            session.executeWrite(async (tx) => {
                await tx.run(`LOAD CSV FROM '${pathToFileURL(path.resolve(PAGE_EDGE_FILE_NAME))}' AS line FIELDTERMINATOR '\\u001F' MATCH (from:Page {title: line[1]}), (to:Page {title: line[2]}) MERGE (from)-[:LINKS_TO]->(to)`)
            }).then(() => {
                console.log('Loaded to Neo4j.')
            }).catch(console.error).finally(() => {
                return session.close()
            }).then(() => { callback() }).catch(callback)
        },
        objectMode: true
    })

    dbWriteStream.on("finish", () => {
        const session = driver.session({ defaultAccessMode: Neo4j.session.WRITE })
        writePageEdgeBufferToFile()
        console.log('Loading to Neo4j...')
        session.executeWrite(async (tx) => {
            await tx.run(`LOAD CSV FROM '${pathToFileURL(path.resolve(PAGE_EDGE_FILE_NAME))}' AS line FIELDTERMINATOR '\\u001F' MATCH (from:Page {title: line[1]}), (to:Page {title: line[2]}) MERGE (from)-[:LINKS_TO]->(to)`)
        }).then(() => {
            console.log('Loaded to Neo4j.')
        }).catch(console.error).finally(() => {
            return session.close()
        }).finally(cleanUp)
    })

    dbWriteStream.on("error", cleanUp)
    readStream.on("error", cleanUp)
    readStream.pipe(bz2()).pipe(dbWriteStream)
}

main().catch(console.error)
