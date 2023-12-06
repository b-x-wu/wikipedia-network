import bz2 from "unbzip2-stream"
import { ReadStream, createReadStream, createWriteStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaStream } from "./wikipediaStream"
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'
const ARTICLE_TITLE_FILE_NAME = 'article-titles.txt'
const BUFFER_CAPACITY = 10000

const main = async () => {
    let stream: ReadStream | undefined
    let articleTitleWriteStream = createWriteStream(ARTICLE_TITLE_FILE_NAME)
    articleTitleWriteStream.cork()
    let bufferSize: number = 0

    const onProgramEnd = async (err?: Error) => {
        console.log('ending program')
        if (err != null) console.error(err)
        console.log(`FLUSHING BUFFER [bufferSize=${bufferSize}]`)
        articleTitleWriteStream.close()
        if (stream != null) stream.close()
    }

    process.on('SIGINT', onProgramEnd)
    process.on('SIGTERM', onProgramEnd)

    const wikipediaStream: WikipediaStream = new WikipediaStream()
    wikipediaStream.on("page", (page) => {
        if (page.namespace !== 0) return

        if (Math.random() < 0.001) console.log(`Writing out ${page.title}`)

        articleTitleWriteStream.write(page.title + '\n')
        bufferSize += 1

        if (bufferSize === BUFFER_CAPACITY) {
            console.log(`FLUSHING BUFFER [bufferSize=${bufferSize}]`)
            process.nextTick(() => articleTitleWriteStream.uncork())
            bufferSize = 0
            process.nextTick(() => articleTitleWriteStream.cork())
        }
    })

    stream = createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
    stream.pipe(bz2()).pipe(wikipediaStream)
        .on('close', onProgramEnd)
        .on('error', onProgramEnd)
}

main().catch(console.error)
