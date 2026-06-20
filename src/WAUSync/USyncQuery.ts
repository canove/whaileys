import type { USyncQueryProtocol } from '../Types/USync'
import { type BinaryNode, getBinaryNodeChild } from '../WABinary'
import { makeUSyncBotProfileProtocol } from './Protocols/UsyncBotProfileProtocol'
import { makeUSyncLIDProtocol } from './Protocols/UsyncLIDProtocol'
import {
    makeUSyncContactProtocol,
    makeUSyncDeviceProtocol,
    makeUSyncDisappearingModeProtocol,
    makeUSyncStatusProtocol
} from './Protocols'
import { USyncUser } from './USyncUser'

export type USyncQueryResultList = { [protocol: string]: unknown; id: string }

export type USyncQueryResult = {
    list: USyncQueryResultList[]
    sideList: USyncQueryResultList[]
}

export const makeUSyncQuery = () => {
    const protocols: USyncQueryProtocol[] = []
    const users: USyncUser[] = []
    let context = 'interactive'
    let mode = 'query'

    const self = {
        protocols,
        users,
        
        withMode(newMode: string) {
            mode = newMode
            return self
        },

        withContext(newContext: string) {
            context = newContext
            return self
        },

        withUser(user: USyncUser) {
            users.push(user)
            return self
        },

        withDeviceProtocol() {
            protocols.push(makeUSyncDeviceProtocol())
            return self
        },

        withContactProtocol() {
            protocols.push(makeUSyncContactProtocol())
            return self
        },

        withStatusProtocol() {
            protocols.push(makeUSyncStatusProtocol())
            return self
        },

        withDisappearingModeProtocol() {
            protocols.push(makeUSyncDisappearingModeProtocol())
            return self
        },

        withBotProfileProtocol() {
            protocols.push(makeUSyncBotProfileProtocol())
            return self
        },

        withLIDProtocol() {
            protocols.push(makeUSyncLIDProtocol())
            return self
        },

        parseUSyncQueryResult(result: BinaryNode | undefined): USyncQueryResult | undefined {
            if (!result || result.attrs.type !== 'result') {
                return
            }

            const protocolMap = Object.fromEntries(
                protocols.map(protocol => {
                    return [protocol.name, protocol.parser]
                })
            )

            const queryResult: USyncQueryResult = {
                // TODO: implement errors etc.
                list: [],
                sideList: []
            }

            const usyncNode = getBinaryNodeChild(result, 'usync')

            //TODO: implement error backoff, refresh etc.
            //TODO: see if there are any errors in the result node
            //const resultNode = getBinaryNodeChild(usyncNode, 'result')

            const listNode = usyncNode ? getBinaryNodeChild(usyncNode, 'list') : undefined

            if (listNode?.content && Array.isArray(listNode.content)) {
                queryResult.list = listNode.content.reduce((acc: USyncQueryResultList[], node) => {
                    const id = node?.attrs.jid
                    if (id) {
                        const data = Array.isArray(node?.content)
                            ? Object.fromEntries(
                                node.content
                                    .map(content => {
                                        const protocol = content.tag
                                        const parser = protocolMap[protocol]
                                        if (parser) {
                                            return [protocol, parser(content)]
                                        } else {
                                            return [protocol, null]
                                        }
                                    })
                                    .filter(([, b]) => b !== null) as [string, unknown][]
                            )
                            : {}
                        acc.push({ ...data, id })
                    }

                    return acc
                }, [])
            }

            //TODO: implement side list
            //const sideListNode = getBinaryNodeChild(usyncNode, 'side_list')
            return queryResult
        }
    }

    return self
}
