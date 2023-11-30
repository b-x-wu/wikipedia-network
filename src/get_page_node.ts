import { getUrlFromPageSlug, type PageNode, type PageEdge, type PageSlug, Page, PAGE_SLUGS_NAME } from './types'
import * as cheerio from 'cheerio'
import * as neo4j from 'neo4j-driver'
import * as dotenv from 'dotenv'
import { createReadStream } from 'fs'
import { createInterface } from 'readline'
import { once } from 'events'
dotenv.config()

const getPageNode = async (pageSlug: string): Promise<PageNode> => {
    let res: Response | undefined = undefined
    try {
        res = await fetch(getUrlFromPageSlug(pageSlug))
    } catch {
        console.log(`Fetch failed for slug: ${pageSlug}\nTrying again.`)
        return getPageNode(pageSlug)
    }

    if (res == null || res.status >= 400) {
        throw new Error(`Invalid response for slug: ${pageSlug}\nTerminating.`)
    }

    if (res != null && res.redirected) {
        const redirectPageSlug = res.url.match(/\/wiki\/(.*)$/)?.at(1)
        if (redirectPageSlug == null) throw new Error(`Cannot find redirect page slug: ${pageSlug}`)
        return {
            page: { slug: pageSlug, isRedirect: true },
            outSlugs: [redirectPageSlug]
        }
    }

    const $ = cheerio.load(await res.text())
    const linkEles = $('div.mw-parser-output > *:not(.navbox, [role="navigation"], .metadata, .reflist, .reflist~*) a')
    const outSlugs: PageSlug[] = [...new Set(
        linkEles.map<cheerio.Element, string | undefined>((_, ele) => $(ele).attr('href'))
            .map<string | undefined, string | undefined>((_, href) => href?.match(/^\/wiki\/(.*?)(?:#.*)?$/)?.at(1))
            .filter<string | undefined, PageSlug>((_, val): val is PageSlug =>  val != null)
            .toArray<PageSlug>())
            .values()
    ]

    return {
        page: { slug: pageSlug, isRedirect: false },
        outSlugs,
    }
}

const storePageNode = async (pageNode: PageNode, session: neo4j.Session): Promise<void> => {
    await session.executeWrite<void>(async (tx: neo4j.ManagedTransaction) => {
        await tx.run(`
            MERGE (p:Page { slug: '${pageNode.page.slug}' })
                ON CREATE SET p.isRedirect = ${pageNode.page.isRedirect}
                ON MATCH SET p.isRedirect = ${pageNode.page.isRedirect}
        `)
        for (const outSlug of pageNode.outSlugs) {
            await tx.run(`
                MERGE (outPage:Page { slug: '${outSlug}' })
                WITH outPage
                MATCH (page:Page { slug: '${pageNode.page.slug}' })
                CREATE (page)-[rel:LINKS_TO]->(outPage)
            `)
        }
    })
}

const main = async () => {
    const driver = neo4j.driver(
        process.env.NEO4J_URI ?? '',
        neo4j.auth.basic(process.env.NEO4J_USERNAME ?? '', process.env.NEO4J_PASSWORD ?? '')
    )
    const readStream = createReadStream(PAGE_SLUGS_NAME)
    let session: neo4j.Session | undefined = undefined
    try {
        session = driver.session({
            defaultAccessMode: neo4j.session.WRITE
        })

        const rl = createInterface({
            input: readStream,
            crlfDelay: Infinity
        })

        // rl.on('line', async (line) => {
        //     const pageNode = await getPageNode(line)
        //     await storePageNode(pageNode, session as neo4j.Session)
        //     console.log(`Stored page: ${decodeURIComponent(line)}`)
        // })

        process.on('SIGINT', async () => {
            rl.close()
            if (session != null) await session.close()
            await driver.close()
            readStream.close()
        })

        // await once(rl, 'close')
        const START_FROM = ".ch_(newspaper)"
        let go = false
        for await (const line of rl) {
            if (go) {
                const pageNode = await getPageNode(line)
                await storePageNode(pageNode, session)
                console.log(`Stored page: ${decodeURIComponent(line)}`)
            }
            if (line === START_FROM) go = true
        }

    } catch (e: any) {
        console.log(e)
    } finally {
        if (session != null) await session.close()
        await driver.close()
        readStream.close()
    }
}

main().catch(console.error)
