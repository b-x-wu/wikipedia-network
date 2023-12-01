export interface Page {
    id: number
    title: string
    namespace: number
    text: string
    redirect?: string
}

export interface PageNode {
    id: number
    title: string
    namespace: number
    isRedirect: boolean
    links: Set<string> // titles of pages this page links to
}
