export interface Page {
    title: string
    namespace: number
    text: string
    redirect?: string
}

export interface PageNode {
    title: string
    namespace: number
    isRedirect: boolean
    pageLinks: Set<string> // titles of pages this page links to
}

export interface Query {
    text: string
    parameters?: any
}
