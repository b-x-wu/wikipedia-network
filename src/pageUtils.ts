import { Query, type Page, type PageNode, PageEdge } from './types'
import he from 'he'

export const cannonizePageTitle = (uncannonizedPageTitle: string): string => {
    // if (uncannonizedPageTitle.length === 0) throw new Error(`Attempting to cannonize illegal page title: ${uncannonizedPageTitle}`)

    // taken from https://en.wikipedia.org/wiki/Help:Link#Conversion_to_canonical_form
    // although we do 
    const encodedCannonizedPageTitle = 
        `${uncannonizedPageTitle.charAt(0).toUpperCase()}${uncannonizedPageTitle.slice(1)}` // capitalize first letter
            .split('#').at(0)! // only grab the page name and not section links
            .replace(' ', '_') // convert spaces to underscores
            .replace(/(_)+/, '_') // group underscores together
            .replace(/^_+|_+$/g, '') // strip underscores from before/after the title

    // decode html elements
    return he.decode(encodedCannonizedPageTitle)
}

export const pageToPageNode = (page: Readonly<Page>): PageNode => {
    return {
        title: encodeURIComponent(cannonizePageTitle(page.title)),
        namespace: page.namespace,
        isRedirect: page.redirect != null
    }
}

export const pageToPageEdges = (page: Readonly<Page>): PageEdge[] => {
    console.log(`pageToPageEdges(Page: title=${page.title})...`)
    const regExpMatchArrays = Array.from(page.text.matchAll(/\[\[(.*?)(?:\|.*?)*\]\]/g), (v, k) => {
        console.log(`Array.from-ing the ${k}-th match array`)
        return v
    })
    console.log(`Matched page text against regex. Found ${regExpMatchArrays.length} matches.`)
    const linksWithRepeats = regExpMatchArrays
        .map((regExpMatchArray) => {
            console.log(`regExpMatchArray: ${regExpMatchArray}`)
            return regExpMatchArray.at(1)
        })
        .filter((matchGroup): matchGroup is string => matchGroup != null)
    console.log("Retrieved links from reg exp match arrays.")
    const links = new Set(linksWithRepeats) // unique
    console.log("Uniquified links.")
    return Array.from(links).map((to) => ({
        from: encodeURIComponent(cannonizePageTitle(page.title)),
        to: encodeURIComponent(cannonizePageTitle(to))
    }))
}

export const isPage = (partialPage: Partial<Page>): partialPage is Page => {
    if (
        partialPage.text == null ||
        partialPage.namespace == null ||
        partialPage.title == null
    ) {
        return false
    }

    return true
}
