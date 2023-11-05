import { getUrlFromPageSlug, type PageNode, type PageEdge, type PageSlug } from './types'
import * as cheerio from 'cheerio'

const getPageNode = async (pageSlug: string): Promise<PageNode> => {
    const res = await fetch(getUrlFromPageSlug(pageSlug))
    if (res.redirected) {
        const redirectPageSlug = res.url.match(/\/wiki\/(.*)$/)?.at(1)
        if (redirectPageSlug == null) throw new Error(`Cannot find redirect page slug: ${pageSlug}`)
        return {
            value: { slug: pageSlug, isRedirect: true },
            outEdges: [{ from: pageSlug, to: redirectPageSlug }]
        }
    }

    const $ = cheerio.load(await res.text())
    const linkEles = $('div.mw-parser-output > *:not(.navbox, [role="navigation"], .reflist) a')
    const outEdges: PageEdge[] = [...new Set(
        linkEles.map<cheerio.Element, string | undefined>((_, ele) => $(ele).attr('href'))
            .map<string | undefined, string | undefined>((_, href) => href?.match(/^\/wiki\/(.*?)(?:#.*)?$/)?.at(1))
            .filter<string | undefined, PageSlug>((_, val): val is PageSlug =>  val != null)
            .toArray<PageSlug>())
            .values()
    ].map<PageEdge>((outSlug: PageSlug) => ({ from: pageSlug, to: outSlug }))

    return {
        value: { slug: pageSlug, isRedirect: false },
        outEdges,
    }
}

const main = async () => {
    console.log(JSON.stringify(await getPageNode("Uniform_Resource_Identifier"), null, 2))
}

main().catch(console.error)
