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
}

export interface ArticleDbType {
    _id: string
    isRedirect: boolean
}

export interface Query {
    text: string
    parameters?: any
}
