export interface Page {
    title: string
    namespace: number
    text: string
    redirect?: string
}

export interface PageNode {
    title: string // URI encoded
    namespace: number
    isRedirect: boolean
}

export interface PageEdge {
    to: string // URI encoded
    from: string // URI encoded
}

export interface Query {
    text: string
    parameters?: any
}
