import { Query, type Page, type PageNode, PageEdge } from './types'

export const pageToPageNode = (page: Readonly<Page>): PageNode => {
    return {
        title: encodeURIComponent(page.title),
        namespace: page.namespace,
        isRedirect: page.redirect != null
    }
}

export const pageToPageEdges = (page: Readonly<Page>): PageEdge[] => {
    return Array.from(new Set<string>(
        [...page.text.matchAll(/\[\[(.*?)(?:\|.*?)*\]\]/g)]
            .map<string | undefined>((regExpMatchArray) => regExpMatchArray.at(1))
            .filter<string>((matchGroup): matchGroup is string => matchGroup != null)
            .map<string>((unnormalizedPageName: string) => `${unnormalizedPageName.charAt(0).toUpperCase()}${unnormalizedPageName.slice(1)}`)
    )).map((to: string) => ({ from: encodeURIComponent(page.title), to: encodeURIComponent(to) }))
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
