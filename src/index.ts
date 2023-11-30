import sax from "sax"
import bz2 from "unbzip2-stream"
import { createReadStream } from 'fs'
import * as dotenv from "dotenv"
dotenv.config()

const WIKIPEDIA_ZIP_FILE_NAME = 'enwiki-20230820-pages-articles-multistream.xml.bz2'

const main = async () => {
    const saxStream = sax.createStream(true)

    let isParsingTitle = false
    saxStream.on('opentag', (tag) => {
        if (tag.name === 'title') isParsingTitle = true
    })

    saxStream.on('text', (text) => {
        if (isParsingTitle) console.log(text)
    })

    saxStream.on('closetag', (tagName) => {
        if (tagName === 'title') isParsingTitle = false
    })

    createReadStream(WIKIPEDIA_ZIP_FILE_NAME)
        .pipe(bz2())
        .pipe(saxStream)
}

main().catch(console.error)
