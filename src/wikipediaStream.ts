import { QualifiedTag, SAXParser, SAXStream, Tag } from "sax";
import { Readable, Writable } from "stream";
import { Page } from "./types";
import { isPage } from "./pageUtils";

export class WikipediaStream extends Writable {

    public readonly parser: SAXParser
    private _isProcessingPage: boolean = false
    private _isProcessingPageField: { [field in keyof Required<Page>]: boolean } = {
        title: false,
        namespace: false,
        text: false,
        redirect: false
    }
    private _processingPage: Partial<Page> = {}
    private _pageListener: (page: Readonly<Page>) => void = (page) => {}

    constructor() {
        super()
        this.parser = new SAXParser(true)
        this.parser.onerror = (e) => super.emit("error", e)

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
                    this.emit("data", this._processingPage)
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

    _write(chunk: any, encoding: BufferEncoding, callback: (error?: Error | null | undefined) => void): void {
        let callbackError: Error | undefined
        try {
            const data = chunk.toString()
            this.parser.write(data)
            this.emit("data", data)
        } catch (e: any) {
            callbackError = new Error(e.toString())
        } finally {
            callback(callbackError)
        }
    }

    on(event: "close", listener: () => void): this;
    on(event: "data", listener: (chunk: any) => void): this;
    on(event: "drain", listener: () => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (err: Error) => void): this;
    on(event: "finish", listener: () => void): this;
    on(event: "pause", listener: () => void): this;
    on(event: "pipe", listener: (src: Readable) => void): this;
    on(event: "readable", listener: () => void): this;
    on(event: "resume", listener: () => void): this;
    on(event: "unpipe", listener: (src: Readable) => void): this;
    on(event: "page", listener: (page: Readonly<Page>) => void): this;
    on(event: string | symbol, listener: (...args: any[]) => void): this {
        if (event === "page") {
            this._pageListener = listener
            return this
        }

        super.on(event, listener)
        return this
    }
}

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
