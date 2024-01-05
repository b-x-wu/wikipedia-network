import { QualifiedTag, SAXParser, Tag } from "sax";
import { Page } from "./types";
import { isPage } from "./pageUtils";

export class WikipediaParser {
    private parser: SAXParser = new SAXParser(true)
    private _isProcessingPage: boolean = false
    private _isProcessingPageField: { [field in keyof Required<Page>]: boolean } = {
        title: false,
        namespace: false,
        text: false,
        redirect: false
    }
    private _processingPage: Partial<Page> = {}
    private _pageListener: (page: Page) => void = (page) => {}
    constructor() {
        this.parser.onopentag = (tag: Tag | QualifiedTag) => {
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
        }

        this.parser.onclosetag =  (tagName: string) => {
            if (tagName === 'page') {
                // Fire off page listener if the page is valid
                if (isPage(this._processingPage)) {
                    this._pageListener(this._processingPage)
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
        }

        this.parser.ontext = (text: string) => {
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
        }
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

    onpage(callback: (page: Page) => void) {
        this._pageListener = callback
    }

    write(data: string): void {
        this.parser.write(data)
    }
}
