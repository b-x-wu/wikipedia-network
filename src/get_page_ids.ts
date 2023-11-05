import * as cheerio from 'cheerio'
import { createWriteStream } from 'fs'

const OUT_FILE_NAME = "page_urls.txt"

const main = async (): Promise<void> => {
    let allPagesUrl: string | undefined = "/wiki/Special:AllPages"
    let kill: boolean = false
    const pageIds: string[] = []
    const outFileStream = createWriteStream(OUT_FILE_NAME, { flags: 'a' })

    process.on('SIGINT', () => { outFileStream.end(); kill = true; })
    process.on('SIGABRT', () => { outFileStream.end(); kill = true; })

    while (!kill && allPagesUrl != null) {
        const res = await fetch(`https://en.wikipedia.org${allPagesUrl}`)
        const $ = cheerio.load(await res.text())

        // get page links
        const pageLinkEles = $('ul.mw-allpages-chunk li a')
        for (const pageLinkEle of pageLinkEles) {
            const pageUrl = $(pageLinkEle).attr('href')
            const pageId = pageUrl == null ? undefined : pageUrl.match(/^\/wiki\/(.*)$/)?.at(1)
            if (pageId != null) pageIds.push(pageId)
        }

        // dump contents
        outFileStream.write(pageIds.join('\n') + '\n')
        if (pageIds.length > 0) {
            console.log(
                `Wrote out ${pageIds.length} pages: `
                + `${decodeURIComponent(pageIds.at(0) ?? '')} to `
                + `${decodeURIComponent(pageIds.at(-1) ?? '')}`
            )
        }
        pageIds.splice(0)

        // get next nave page
        const navLinkEles = $('div.mw-allpages-nav a')
        allPagesUrl = undefined
        for (const navLinkEle of navLinkEles) {
            if ($(navLinkEle).text().includes("Next page")) {
                allPagesUrl = $(navLinkEle).attr('href')
                break
            }
        }
    }

    outFileStream.end()
}

main().catch(console.error)
