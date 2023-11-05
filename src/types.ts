export type PageSlug = string

export interface Page {
    readonly slug: PageSlug // encoded fragment of url identifying the page
    readonly isRedirect: boolean // true if fetching the page gives 302. false otherwise
}

export interface PageEdge {
    readonly from: PageSlug
    readonly to: PageSlug
}

export interface PageNode {
    readonly value: Page
    outEdges: PageEdge[]
}

export const PAGE_PREFIX = 'https://en.wikipedia.org/wiki/'

export const getUrlFromPageSlug = (pageSlug: PageSlug): string => {
    return PAGE_PREFIX + pageSlug
}

export const getUrlFromPage = (page: Page): string => {
    return getUrlFromPageSlug(page.slug)
}