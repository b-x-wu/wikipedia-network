type TrieNode = {
    [key: string]: TrieNode
}

/**
 * Container for strings optimized for insertion and 'contains' operation
 * with a trie implementation.
 */
export class StringContainer {
    private static PRESENT_KEY: string = 'PRESENT'
    private _root: TrieNode
    public size: number = 0

    constructor() {
        this._root = {};
    }

    insert(s: string): void {
        this.size += 1
        let current: TrieNode = this._root
        for (let i = 0; i < s.length; i++) {
            const char = s.at(i)!
            if (current[char] == null) {
                current[char] = {}
                current = current[char]
            } else {
                current = current[char]
            }
        }
        current[StringContainer.PRESENT_KEY] = {}
    }

    contains(s: string): boolean {
        let current: TrieNode = this._root
        for (let i = 0; i < s.length; i++) {
            const char = s.at(i)!
            if (current[char] == null) return false
            current = current[char]
        }
        return current[StringContainer.PRESENT_KEY] != null
    }
}