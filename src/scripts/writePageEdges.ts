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
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? ''), {
            maxConnectionLifetime: 0,
            logging: {
                level: 'debug',
                logger: (level, message) => {
                    if (level === 'debug') {
                        console.debug(message)
                        return
                    }

                    if (level === 'info') {
                        console.info(message)
                        return
                    }

                    if (level === 'warn') {
                        console.warn(message)
                        return
                    }

                    console.error(message)
                }
            }
        }
    )

    let pageEdgeBuffer: PageEdge[] = []
    const writePageEdgeBufferToFile = () => {
        console.log('Writing out to csv...')
        if (existsSync(PAGE_EDGE_FILE_NAME)) unlinkSync(PAGE_EDGE_FILE_NAME)
        const fileWriteStream = createWriteStream(PAGE_EDGE_FILE_NAME, {
            flags: 'a'
        })
        let writeCount = 0
        while (pageEdgeBuffer.length > 0) {
            const pageEdge = pageEdgeBuffer.shift()
            if (pageEdge != null) {
                fileWriteStream.write(`${String.fromCharCode(31)}${pageEdge.from}${String.fromCharCode(31)}${pageEdge.to}\n`)
                writeCount++
            }
        }
        fileWriteStream.end()
        console.log(`Wrote ${writeCount} edges to csv.`)
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
        try {
            console.log(`Processing edges for ${page.title}.`)
            const pageEdges = pageToPageEdges(page)
            console.log(`Adding edges for ${page.title} to buffer.`)
            pageEdgeBuffer.push(...pageEdges)
        } catch (e: any) {
            cleanUp(e)
        }
    })

    wikipediaParser.onerror(cleanUp)

    const dbWriteStream = new Writable({
        write: (chunk, encoding, callback) => {
            if (pageEdgeBuffer.length === 0) console.log("Pushing to clean page edge buffer.")
            try {
                console.log('Writing chunk to parser.')
                wikipediaParser.write(chunk.toString());
            } catch (e: any) {
                console.error(`Error while writing chunk to parser: ${e.toString()}`)
                callback(e as Error)
                return
            }
            
            if (pageEdgeBuffer.length < MAX_PAGE_EDGE_BUFFER_SIZE) {
                console.log(`Current pageEdgeBuffer length is ${pageEdgeBuffer.length}. Continuing.`)
                callback()
                return
            }

            console.log('PageEdgeBuffer is full. Writing out to database.')
            try {
                writePageEdgeBufferToFile()
            } catch (e: any) {
                console.error(`Error while writing page edge buffer to file: ${e.toString()}`)
                callback(e as Error)
                return
            }
            console.log('Loading to Neo4j...')
            let callbackArg: Error | undefined
            let session: Session | undefined
            try {
                session = driver.session({ defaultAccessMode: Neo4j.session.WRITE })
            } catch (e: any) {
                callback(e)
                return
            }
            session.executeWrite(async (tx) => {
                const results = await tx.run(`LOAD CSV FROM '${pathToFileURL(path.resolve(PAGE_EDGE_FILE_NAME))}' AS line FIELDTERMINATOR '\\u001F' MATCH (from:Page {title: line[1]}), (to:Page {title: line[2]}) MERGE (from)-[:LINKS_TO]->(to)`)
                return results.summary.updateStatistics.updates().relationshipsCreated
            }).then((relationshipsCreated) => {
                console.log(`Loaded ${relationshipsCreated} relationships to Neo4j.`)
            }).catch((e: any) => {
                callbackArg = e as Error
            }).finally(() => {
                if (session == null) return
                return session.close()
            }).then(() => console.log("Closed session.")).then(() => { callback(callbackArg) }).catch(callback)
        },
        objectMode: true
    })

    dbWriteStream.on("finish", () => {
        const session = driver.session({ defaultAccessMode: Neo4j.session.WRITE })
        writePageEdgeBufferToFile()
        console.log('Loading to Neo4j...')
        session.executeWrite(async (tx) => {
            const results = await tx.run(`LOAD CSV FROM '${pathToFileURL(path.resolve(PAGE_EDGE_FILE_NAME))}' AS line FIELDTERMINATOR '\\u001F' MATCH (from:Page {title: line[1]}), (to:Page {title: line[2]}) MERGE (from)-[:LINKS_TO]->(to)`)
            return results.summary.updateStatistics.updates().relationshipsCreated
        }).then((relationshipsCreated) => {
            console.log(`Loaded ${relationshipsCreated} relationships to Neo4j.`)
        }).catch(console.error).finally(() => {
            return session.close()
        }).finally(cleanUp)
    })

    dbWriteStream.on("error", cleanUp)
    dbWriteStream.on("close", () => console.log('Closed dbWriteStream'))
    readStream.on("error", cleanUp)
    dbWriteStream.on("drain", () => console.log('Drained dbWriteStream'))
    readStream.on("close", () => console.log('Closed readStream'))
    readStream.on("end", () => console.log('Ended readStream'))
    readStream.on("pause", () => console.log('Paused readStream'))
    readStream.on("resume", () => console.log('Resumed readStream'))
    const bz2Stream = bz2()
    bz2Stream.on("error", cleanUp)
    bz2Stream.on("pause", () => console.log('Paused bz2Stream'))
    bz2Stream.on("resume", () => console.log('Resumed bz2Stream'))
    bz2Stream.on("drain", () => console.log("Drained bz2Stream"))
    readStream.pipe(bz2Stream).pipe(dbWriteStream)
}

main().catch(console.error)
