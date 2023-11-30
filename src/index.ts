import bz2 from "unbzip2-stream"
import { createReadStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaPageEventListener } from "./wikipediaPageEventListener"
import { SAXStream } from "sax"
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'

const main = async () => {
    const saxStream: SAXStream = new SAXStream(true)
    const wikipediaStream = new WikipediaPageEventListener(saxStream)
    wikipediaStream.registerWikipediaPageEventHandler((page) => {
        console.log(page.title)
    })

    createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
        .pipe(bz2())
        .pipe(saxStream)
}

main().catch(console.error)
