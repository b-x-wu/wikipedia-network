import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream } from 'fs'
import * as dotenv from "dotenv"
import Neo4j, { type Session } from 'neo4j-driver'
import { WikipediaStream } from "./wikipediaStream"
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'

const main = async () => {
    const driver = Neo4j.driver(
        process.env.NEO4J_URI ?? '',
        Neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? '')
    )
    let stream: ReadStream | undefined
    let session: Session | undefined = undefined

    const onProgramEnd = async (err?: Error) => {
        if (err != null) console.error(err)
        if (session != null) await session.close()
        await driver.close()
        if (stream != null) stream.destroy()
    }

    session = driver.session({
        defaultAccessMode: Neo4j.session.WRITE
    })

    process.on('SIGINT', onProgramEnd)
    process.on('SIGTERM', onProgramEnd)

    const wikipediaStream: WikipediaStream = new WikipediaStream()
    wikipediaStream.on("page", (page) => {
        console.log(page.title)
    })

    stream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    stream.pipe(bz2()).pipe(wikipediaStream)
        .on('close', onProgramEnd)
        .on('error', onProgramEnd)
}

main().catch(console.error)
