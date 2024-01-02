import { Query, type Page, type PageNode, ArticleDbType } from './types'

export const getPageNodeFromPage = (page: Readonly<Page>): PageNode => {
    return {
        title: page.title,
        namespace: page.namespace,
        isRedirect: page.redirect != null,
        // pageLinks: new Set<string>(
        //     [...page.text.matchAll(/\[\[(.*?)(?:\|.*?)*\]\]/g)]
        //         .map<string | undefined>((regExpMatchArray) => regExpMatchArray.at(1))
        //         .filter<string>((matchGroup): matchGroup is string => matchGroup != null)
        // )
    }
}

export const pageNodeToArticledbType = (pageNode: PageNode): ArticleDbType => {
    return {
        _id: pageNode.title,
        isRedirect: pageNode.isRedirect
    }
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
