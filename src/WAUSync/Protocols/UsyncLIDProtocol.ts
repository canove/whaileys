import type { USyncQueryProtocol } from '../../Types/USync'
import type { BinaryNode } from '../../WABinary'
import type { USyncUser } from '../USyncUser'

export const makeUSyncLIDProtocol = (): USyncQueryProtocol => {
    return {
        name: 'lid',
        getQueryElement: (): BinaryNode => {
            return {
                tag: 'lid',
                attrs: {}
            }
        },
        getUserElement: (user: USyncUser): BinaryNode | null => {
            if (user.lid) {
                return {
                    tag: 'lid',
                    attrs: { jid: user.lid }
                }
            } else {
                return null
            }
        },
        parser: (node: BinaryNode): string | null => {
            if (node.tag === 'lid') {
                return node.attrs.val!
            }

            return null
        }
    }
}
