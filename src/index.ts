import bz2 from "unbzip2-stream"
import { createReadStream } from 'fs'
import * as dotenv from "dotenv"
import { WikipediaPageEventListener } from "./wikipediaPageEventListener"
import { SAXStream } from "sax"
import { getPageNodeFromPage } from "./pageUtils"
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'

const main = async () => {
    const skip = 1000
    let count = 1000
    const saxStream: SAXStream = new SAXStream(true)
    const wikipediaStream = new WikipediaPageEventListener(saxStream)
    wikipediaStream.registerWikipediaPageEventHandler((page) => {
        if (count < skip) {
            count++
            return
        }
        console.log(getPageNodeFromPage(page))
        count = 0
    })

    createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
        .pipe(bz2())
        .pipe(saxStream)
}

main().catch(console.error)
