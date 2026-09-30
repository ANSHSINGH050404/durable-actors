import * as React from "react"

import { Drawer as DrawerPrimitive } from "vaul"

import { cn } from "../../lib/utils.js"

function Drawer({ ...props }: React.ComponentProps<typeof DrawerPrimitive.Root>) {
    return <DrawerPrimitive.Root data-slot="drawer" {...props} />
}

function DrawerTrigger({ ...props }: React.ComponentProps<typeof DrawerPrimitive.Trigger>) {
    return <DrawerPrimitive.Trigger data-slot="drawer-trigger" {...props} />
}

function DrawerPortal({ ...props }: React.ComponentProps<typeof DrawerPrimitive.Portal>) {
    return <DrawerPrimitive.Portal data-slot="drawer-portal" {...props} />
}

function DrawerClose({ ...props }: React.ComponentProps<typeof DrawerPrimitive.Close>) {
    return <DrawerPrimitive.Close data-slot="drawer-close" {...props} />
}

function DrawerOverlay({ className, ...props }: React.ComponentProps<typeof DrawerPrimitive.Overlay>) {
    return (
        <DrawerPrimitive.Overlay
            data-slot="drawer-overlay"
            className={cn(
                "la:fixed la:inset-0 la:z-50 la:bg-black/50 la:data-[state=closed]:animate-out la:data-[state=closed]:fade-out-0 la:data-[state=open]:animate-in la:data-[state=open]:fade-in-0",
                className
            )}
            {...props}
        />
    )
}

function DrawerContent({ className, children, container, ...props }: React.ComponentProps<typeof DrawerPrimitive.Content> & { container?: HTMLElement | null }) {
    return (
        <DrawerPortal container={container}>
            <DrawerOverlay />
            <DrawerPrimitive.Content
                data-slot="drawer-content"
                className={cn(
                    "la-observer la:group/drawer-content la:fixed la:z-50 la:flex la:h-auto la:flex-col la:bg-background",
                    "la:data-[vaul-drawer-direction=top]:inset-x-0 la:data-[vaul-drawer-direction=top]:top-0 la:data-[vaul-drawer-direction=top]:mb-24 la:data-[vaul-drawer-direction=top]:max-h-[80vh] la:data-[vaul-drawer-direction=top]:rounded-b-lg la:data-[vaul-drawer-direction=top]:border-b",
                    "la:data-[vaul-drawer-direction=bottom]:inset-x-0 la:data-[vaul-drawer-direction=bottom]:bottom-0 la:data-[vaul-drawer-direction=bottom]:mt-24 la:data-[vaul-drawer-direction=bottom]:max-h-[80vh] la:data-[vaul-drawer-direction=bottom]:rounded-t-lg la:data-[vaul-drawer-direction=bottom]:border-t",
                    "la:data-[vaul-drawer-direction=right]:inset-y-0 la:data-[vaul-drawer-direction=right]:right-0 la:data-[vaul-drawer-direction=right]:w-3/4 la:data-[vaul-drawer-direction=right]:border-l la:data-[vaul-drawer-direction=right]:sm:max-w-sm",
                    "la:data-[vaul-drawer-direction=left]:inset-y-0 la:data-[vaul-drawer-direction=left]:left-0 la:data-[vaul-drawer-direction=left]:w-3/4 la:data-[vaul-drawer-direction=left]:border-r la:data-[vaul-drawer-direction=left]:sm:max-w-sm",
                    className
                )}
                {...props}
            >
                <div className="la:mx-auto la:mt-4 la:hidden la:h-2 la:w-[100px] la:shrink-0 la:rounded-full la:bg-muted la:group-data-[vaul-drawer-direction=bottom]/drawer-content:block" />
                {children}
            </DrawerPrimitive.Content>
        </DrawerPortal>
    )
}

function DrawerHeader({ className, ...props }: React.ComponentProps<"div">) {
    return (
        <div
            data-slot="drawer-header"
            className={cn(
                "la:flex la:flex-col la:gap-0.5 la:p-4 la:group-data-[vaul-drawer-direction=bottom]/drawer-content:text-center la:group-data-[vaul-drawer-direction=top]/drawer-content:text-center la:md:gap-1.5 la:md:text-left",
                className
            )}
            {...props}
        />
    )
}

function DrawerFooter({ className, ...props }: React.ComponentProps<"div">) {
    return <div data-slot="drawer-footer" className={cn("la:mt-auto la:flex la:flex-col la:gap-2 la:p-4", className)} {...props} />
}

function DrawerTitle({ className, ...props }: React.ComponentProps<typeof DrawerPrimitive.Title>) {
    return <DrawerPrimitive.Title data-slot="drawer-title" className={cn("la:font-semibold la:text-foreground", className)} {...props} />
}

function DrawerDescription({ className, ...props }: React.ComponentProps<typeof DrawerPrimitive.Description>) {
    return <DrawerPrimitive.Description data-slot="drawer-description" className={cn("la:text-sm la:text-muted-foreground", className)} {...props} />
}

export { Drawer, DrawerPortal, DrawerOverlay, DrawerTrigger, DrawerClose, DrawerContent, DrawerHeader, DrawerFooter, DrawerTitle, DrawerDescription }
