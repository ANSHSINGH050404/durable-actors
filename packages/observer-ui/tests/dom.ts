import { JSDOM } from "jsdom"

// Import this module first, then load React DOM, Radix, and cmdk with `await import()` so they
// initialise against a browser-like global scope; a static import evaluates them too early.
const dom = new JSDOM("<!doctype html><html><body></body></html>", { pretendToBeVisual: true })
const { window } = dom

class ResizeObserverStub {
    observe() {}
    unobserve() {}
    disconnect() {}
}

// jsdom has no layout, so the scrolling and observer APIs used by cmdk and Radix popovers are no-ops.
window.HTMLElement.prototype.scrollIntoView = () => {}
Object.assign(globalThis, {
    window,
    document: window.document,
    HTMLElement: window.HTMLElement,
    HTMLInputElement: window.HTMLInputElement,
    Element: window.Element,
    Node: window.Node,
    NodeFilter: window.NodeFilter,
    MutationObserver: window.MutationObserver,
    Event: window.Event,
    CustomEvent: window.CustomEvent,
    KeyboardEvent: window.KeyboardEvent,
    MouseEvent: window.MouseEvent,
    FocusEvent: window.FocusEvent,
    FormData: window.FormData,
    getComputedStyle: window.getComputedStyle.bind(window),
    requestAnimationFrame: (callback: FrameRequestCallback) => setTimeout(() => callback(Date.now()), 0) as unknown as number,
    cancelAnimationFrame: (handle: number) => clearTimeout(handle),
    ResizeObserver: ResizeObserverStub,
    IS_REACT_ACT_ENVIRONMENT: true
})

export { dom }
