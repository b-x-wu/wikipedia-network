import { type Page, type PageNode } from './types'

export const getPageNodeFromPage = (page: Readonly<Page>): PageNode => {
    return {
        id: page.id,
        title: page.title,
        namespace: page.namespace,
        isRedirect: page.redirect != null,
        links: new Set<string>(
            [...page.text.matchAll(/\[\[(.*?)(?:\|.*?)*\]\]/g)]
                .map<string | undefined>((regExpMatchArray) => regExpMatchArray.at(1))
                .filter<string>((matchGroup): matchGroup is string => matchGroup != null)
        )
    }
}
