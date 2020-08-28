const path = require('path')
const fs = require('fs').promises

const rescue = require('rescue')

class Memento {
    constructor (directory) {
        this.directory = directory
    }

    async open (upgrade = null, version = 1) {
        const list = async () => {
            try {
                return await fs.readdir(this.directory)
            } catch (error) {
                rescue(error, [{ code: 'ENOENT' }])
                await fs.mdkir(this.directory, { recursive: true })
                return await list()
            }
        }
        const subdirs = [ 'versions', 'stores' ].sort()
        const dirs = await list()
        if (dirs.length == 0) {
            for (const dir of subdirs) {
                await fs.mkdir(path.resolve(this.directory, dir))
            }
            await fs.mkdir(path.resolve(this.directory, './versions/0'))
        } else {
            for (const dir of (await list()).sort()) {
            }
        }
        const versions = await fs.readdir(path.resolve(this.directory, 'versions'))
        const latest = versions.map(version => +version).sort().pop()
        if (latest < version) {
        }
        if (latest < version && upgrade != null) {
            console.log('upgrade')
        }
    }
}

module.exports = Memento
