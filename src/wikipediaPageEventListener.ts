import { Page } from './types'
import { QualifiedTag, SAXStream, Tag } from 'sax'

/**
 * With a given SAX stream, listens for when a new page is processed and fires off
 * an event handler (if it has been registered).
 */
export class WikipediaPageEventListener {

    private _pageEventHandler: (page: Readonly<Page>) => void = (page: Page) => {}

    // state
    private _isProcessingPage: boolean = false
    private _isProcessingPageField: { [field in keyof Required<Page>]: boolean } = {
        title: false,
        namespace: false,
        text: false,
        redirect: false
    }
    private _processingPage: Partial<Page> = {}

    constructor(stream: SAXStream) {
        stream.on('opentag', (tag: Tag | QualifiedTag) => {
            if (tag.name === 'page') {
                this._isProcessingPage = true
                return
            }

            if (tag.name === 'title') {
                this._isProcessingPageField.title = true
                return
            }

            if (tag.name === 'ns') {
                this._isProcessingPageField.namespace = true
                return
            }

            if (tag.name === 'text') {
                this._isProcessingPageField.text = true
                return
            }

            if (tag.name === 'redirect' && tag.isSelfClosing && tag.attributes['title'] != null) {
                this._processingPage.redirect = tag.attributes['title'].toString()
            }
        })

        stream.on('closetag', (tagName: string) => {
            if (tagName === 'page') {
                // Fire off page listener if the page is valid
                if (WikipediaPageEventListener._isPage(this._processingPage)) {
                    this._pageEventHandler(this._processingPage)
                }

                this._clearState()
                return
            }

            if (tagName === 'title') {
                this._isProcessingPageField.title = false
                return
            }

            if (tagName === 'ns') {
                this._isProcessingPageField.namespace = false
                return
            }

            if (tagName === 'text') {
                this._isProcessingPageField.text = false
                return
            }
        })

        stream.on('text', (text: string) => {
            if (!this._isProcessingPage) {
                return
            }

            if (this._isProcessingPageField.title) {
                this._processingPage.title = text
                return
            }

            if (this._isProcessingPageField.namespace) {
                const parsedNamespace = parseInt(text)
                this._processingPage.namespace = Number.isInteger(parsedNamespace) ? parsedNamespace : undefined
                return
            }

            if (this._isProcessingPageField.text) {
                this._processingPage.text = text
            }
        })
    }

    private static _isPage(partialPage: Partial<Page>): partialPage is Page {
        if (
            partialPage.text == null ||
            partialPage.namespace == null ||
            partialPage.title == null
        ) {
            return false
        }

        return true
    }

    private _clearState() {
        this._isProcessingPage = false
        this._isProcessingPageField = {
            title: false,
            namespace: false,
            text: false,
            redirect: false
        }
        this._processingPage = {}
    }

    public registerWikipediaPageEventHandler(handler: (page: Readonly<Page>) => void) {
        this._pageEventHandler = handler
    }
}