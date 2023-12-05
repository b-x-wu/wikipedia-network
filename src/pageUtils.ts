import { Query, type Page, type PageNode } from './types'

export const getPageNodeFromPage = (page: Readonly<Page>): PageNode => {
    return {
        title: page.title,
        namespace: page.namespace,
        isRedirect: page.redirect != null,
        pageLinks: new Set<string>(
            [...page.text.matchAll(/\[\[(.*?)(?:\|.*?)*\]\]/g)]
                .map<string | undefined>((regExpMatchArray) => regExpMatchArray.at(1))
                .filter<string>((matchGroup): matchGroup is string => matchGroup != null)
        )
    }
}

export const getQueriesFromPageNode = (pageNode: Readonly<PageNode>): Query[] => {
    const queries = []
    queries.push({
        text: 'MERGE (p: Page { title: $title }) SET p.isRedirect = $isRedirect SET p.namespace = $namespace',
        parameters: pageNode
    })
    queries.push(...[...pageNode.pageLinks].map<Query>((title) => ({
        text: `
            MERGE (linkedPage: Page { title: $linkedPageTitle }) 
            WITH linkedPage 
            MATCH (page: Page { title: $title }) 
            CREATE (page)-[r: LINKS_TO]->(linkedPage)
        `,
        parameters: { linkedPageTitle: title, title: pageNode.title }
    })))

    return queries
}

export const getQueryStringsFromPageNode = (pageNode: Readonly<PageNode>): string[] => {
    const queryStrings: string[] = []
    queryStrings.push(
        `MERGE (p: Page { title: ${pageNode.title}}) SET p.isRedirect = ${pageNode.isRedirect} SET p.namespace = ${pageNode.namespace}`
    )
    queryStrings.push(...[...pageNode.pageLinks].map<string>((title) => {
        return `MERGE (linkedPage: Page { title: ${title} }) WITH linkedPage MATCH (page: Page { title: ${pageNode.title} }) CREATE (page)-[r: LINKS_TO]->(linkedPage)`
    }))

    return queryStrings
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
