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
