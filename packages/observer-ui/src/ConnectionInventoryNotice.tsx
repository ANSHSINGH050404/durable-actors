import type { ActorInventory } from "./client.js"

export function ConnectionInventoryNotice({ inventory }: { inventory?: ActorInventory }) {
    if (inventory?.connectionsComplete !== false) return null
    return (
        <p className="overview-alert" role="status">
            Connection inventory incomplete. Some gateways are unavailable; connection counts may be lower than actual. Retrying automatically.
        </p>
    )
}
