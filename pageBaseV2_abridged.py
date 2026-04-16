
"""
======================================================================================
 ABRIDGED PageBase (pageBaseV2.js) - PUBLIC API REFERENCE
======================================================================================

 SOURCE:  src/pageBase/js/pageBaseV2.js
 PURPOSE: LLM context document - provides the public API surface for the PageBase
          class, which is the foundation for all pages (homepage, pt, upload, etc.).
          Widgets access this via `this.context.page`.

 THIS IS NOT THE FULL FILE.
 - Internal methods and cleanup logic are summarized, not reproduced.
 - DOM wiring, error boundary, and eager column cache internals are omitted.
 - For the full implementation, see: src/pageBase/js/pageBaseV2.js (~1010 lines)

 LAST SYNCED: 2026-04-16 from bugfix/engine_changes branch
======================================================================================
"""


# ====================================================================================
#  PageBase
# ====================================================================================
#
#  The base class for all pages in the application. Provides:
#  - Lifecycle hooks (init -> bind -> ready -> cleanup)
#  - ObservableDictionary-based reactive state stores
#  - Shared cross-page state (shared stores + shared contexts)
#  - Lifecycle-managed DOM event listeners (auto-cleanup on page destroy)
#  - WebSocket message handler registration
#  - Scoped timers (setTimeout/setInterval with auto-cleanup)
#  - RxJS integration (destroy$, safeSubscribe, fromEvent$)
#
#  In widgets, accessed as:  this.context.page
#
# ====================================================================================

class PageBase:
    """
    constructor(name, { url_context, context, config, container })

    Static Properties
    -----------------
    PageBase._shared         - Map<KEY, {dict, ref}> - Cross-page shared stores
    PageBase._sharedContext   - Map<KEY, value>       - Cross-page shared singletons
    PageBase._persistedStores - Set<storageKey>       - Keys persisted to localStorage
    PageBase._pages           - Map<name, PageBase>   - All active page instances

    Instance Properties
    -------------------
    this.name            - Page name string ('pt', 'homepage', 'frame', etc.)
    this.context         - { name, page: this, isFrame, url_context, destroy$, ... }
    this.page$           - ObservableDictionary for page-level reactive state
    this.emitter         - CadesEmitter for page-level event pub/sub
    this.container       - Root DOM container element
    this.drawer          - The #drawer-content element
    """

    # ==========================================================================
    #  LIFECYCLE HOOKS (override in subclasses)
    # ==========================================================================
    #
    #  Execution order during init():
    #    1. _init()            [internal - creates page$, registers page]
    #    2. _onCacheDom()      [internal - caches drawer element]
    #    3. onCacheDom()       [override - cache your DOM elements]
    #    4. onBeforeInit()     [override - pre-init setup]
    #    5. onInit()           [override - main initialization]
    #    6. onAfterInit()      [override - post-init setup]
    #    7. onBeforeBind()     [override - pre-bind setup]
    #    8. _onBind()          [internal - tooltip wiring]
    #    9. onBind()           [override - bind events]
    #   10. onAfterBind()      [override - post-bind setup]
    #   11. setupHotkeys()     [override - keyboard shortcuts]
    #   12. _onReady()         [internal - sets initialized=true]
    #   13. onReady()          [override - page is ready]
    #
    #  Cleanup order during cleanup():
    #    1. onBeforeCleanup()
    #    2. _cleanup()         [internal - unsubscribes all, clears stores, timers, etc.]
    #    3. onCleanup()        [override]
    #    4. onAfterCleanup()   [override]
    #    5. _afterCleanup()    [internal - disposes page$, nulls context]

    async_def_onBeforeInit = "async onBeforeInit() {}"
    async_def_onInit = "async onInit() {}"
    async_def_onAfterInit = "async onAfterInit() {}"
    async_def_onCacheDom = "async onCacheDom() {}"
    async_def_onBeforeBind = "async onBeforeBind() {}"
    async_def_onBind = "async onBind() {}"
    async_def_onAfterBind = "async onAfterBind() {}"
    async_def_onReady = "async onReady() {}"
    async_def_onError = "async onError(e) {}"
    async_def_onBeforeCleanup = "async onBeforeCleanup() {}"
    async_def_onCleanup = "async onCleanup() {}"
    async_def_onAfterCleanup = "async onAfterCleanup() {}"
    async_def_setupHotkeys = "async setupHotkeys() {}"

    # ==========================================================================
    #  STATE HELPERS
    # ==========================================================================

    def isInitialized(self):
        """Returns page$.get('initialized') - True after onReady completes."""

    def isAlive(self):
        """Returns page$.get('alive') - False after error."""

    def isConnected(self):
        """Returns True if WebSocket is currently connected."""

    # ==========================================================================
    #  MANAGER ACCESSORS
    # ==========================================================================
    #
    #  These return singleton manager instances shared across all pages.
    #  All use getSharedContext() internally.
    #

    def modalManager(self):
        """Returns the global ModalManager instance."""

    def serialManager(self):
        """Returns the SerialManager (sequential async task runner)."""

    def socketManager(self):
        """Returns the WebSocket SocketManager."""

    def settingsManager(self):
        """Returns the SettingsManager."""

    def subscriptionManager(self):
        """Returns the SubscriptionManager (WebSocket room subscriptions)."""

    def themeManager(self):
        """Returns the ThemeManager (theme$, getTheme(), etc.)."""

    def toastManager(self):
        """Returns the ToastManager (toast notifications)."""

    def userManager(self):
        """Returns the UserManager (userProfile$, getRoles(), etc.)."""

    def colorManager(self):
        """Returns the ColorManager (getStateColor(), etc.)."""

    def tooltipManager(self):
        """Returns the TooltipManager (add(), remove(), etc.)."""

    def scratchPad(self):
        """Returns the ScratchPad shared context."""

    def frame(self):
        """Returns the 'frame' page instance (PageBase._pages.get('frame'))."""

    # ==========================================================================
    #  OBSERVABLE DICTIONARY STORES
    # ==========================================================================
    #
    #  The reactive state system. Stores are ObservableDictionary instances that
    #  support key-level subscriptions, change detection, and persistence.
    #

    def createStore(self, key, initial=None, opts=None):
        """
        Create a page-scoped reactive store.

        Parameters
        ----------
        key     : string  - Store name (auto-uppercased)
        initial : dict    - Initial key-value pairs
        opts    : dict
            persist    : bool   - Enable localStorage persistence
            storageKey : string - localStorage key
            version    : any    - Version stamp (stored as _version_ key)

        Returns: ObservableDictionary instance

        The store is automatically disposed on page cleanup.
        If a store with the same key already exists, returns the existing one.
        """

    def createSharedStore(self, key, initial=None, opts=None):
        """
        Create a store shared across ALL pages (ref-counted).

        Same parameters as createStore, but stored in PageBase._shared.
        Disposed when all referencing pages are cleaned up.

        Returns: ObservableDictionary instance
        """

    def getSharedStore(self, key, createOnMissing=False):
        """
        Get an existing shared store by key.
        Returns the ObservableDictionary, or undefined if not found.
        """

    def getSharedStoreWithRetry(self, key, maxAttempts=5, retryTimer=1000):
        """
        Get a shared store, retrying if not yet created.
        Useful during page init when store creation order is uncertain.
        """

    def sharedStoreExists(self, key):
        """Returns True if a shared store with the given key exists."""

    def createDebouncedStore(self, name, ms, initial=None, opts=None):
        """
        Create a store whose change notifications are debounced by `ms` milliseconds.
        Returns the debounced ObservableDictionary.
        """

    def createThrottledStore(self, name, ms, initial=None, opts=None):
        """
        Create a store whose change notifications are throttled by `ms` milliseconds.
        """

    def createPipedStore(self, name, initial=None, opts=None, *operators):
        """
        Create a store with custom RxJS pipe operators applied.
        """

    # ==========================================================================
    #  SHARED CONTEXT (singletons)
    # ==========================================================================

    def createSharedContext(self, key, value=None):
        """
        Store a singleton value accessible from any page.
        Used for managers (socketManager, themeManager, etc.).
        """

    def getSharedContext(self, key, fallback=None):
        """Retrieve a shared context value. Returns fallback if not found."""

    def destroySharedContext(self, key):
        """Remove a shared context entry (only if this page created it)."""

    # ==========================================================================
    #  SERIALIZATION
    # ==========================================================================

    def serializeState(self):
        """Snapshot all stores as { KEY: [[k,v], ...], ... }."""

    def restoreState(self, snapshot):
        """Restore stores from a snapshot created by serializeState()."""

    # ==========================================================================
    #  DOM & EVENT MANAGEMENT
    # ==========================================================================

    def addEventListener(self, target, type, fn, opts=None, uuid=None):
        """
        Add a lifecycle-managed DOM event listener.

        Automatically cleaned up on page destroy via AbortController.
        Returns a UUID that can be used with removeEventListener().

        Parameters
        ----------
        target : Element  - DOM element
        type   : string   - Event type ('click', 'input', etc.)
        fn     : function - Handler
        opts   : dict     - addEventListener options (passive, capture, etc.)
        uuid   : string   - Optional explicit UUID (auto-generated if omitted)
        """

    def removeEventListener(self, uuid):
        """Remove a previously registered listener by UUID."""

    def onClickOutside(self, elOrSelector, handler, once=False):
        """
        Register a handler that fires when clicking outside the given element.
        Returns a UUID (can be removed with removeEventListener).
        """

    def onStorage(self, key, fn, type='local', opts=None):
        """
        Listen for localStorage/sessionStorage changes to a specific key.
        Used for cross-tab synchronization of persisted stores.
        """

    def qs(self, selector, scope=None):
        """Safe querySelector - returns null on error instead of throwing."""

    def qsa(self, selector, scope=None):
        """Safe querySelectorAll - returns [] on error."""

    def getElement(self, elementOrSelector):
        """
        Resolve a CSS selector or element to an Element.
        Throws if not found.
        """

    def linkStoreToInput(self, dom, key=None, store=None, default_val=False,
                          persist=False, storageKey=None, cb=None):
        """
        Two-way bind an <input> checkbox to an ObservableDictionary key.
        Optionally persists to localStorage. Callback fires on change.
        """

    # ==========================================================================
    #  WEBSOCKET MESSAGE HANDLERS
    # ==========================================================================

    def addMessageHandler(self, room, type, handler):
        """
        Register a WebSocket message handler for a specific room and message type.

        Parameters
        ----------
        room    : string   - Room name (e.g., 'ABC123.PORTFOLIO')
        type    : string   - Message type ('subscribe', 'publish', 'ack')
        handler : function - async (message) => void

        Automatically cleaned up on page destroy.
        """

    def onMessage(self, room, type, handler):
        """Alias for addMessageHandler()."""

    def removeMessageHandler(self, room, type, handler):
        """Remove a specific message handler."""

    def removeAllMessageHandlers(self):
        """Remove all registered message handlers for this page."""

    # ==========================================================================
    #  ROOM SUBSCRIPTIONS
    # ==========================================================================

    def updateRoomFilters(self, room, context=None, options=None):
        """
        Subscribe to a WebSocket room (or update existing subscription filters).
        Delegated to subscriptionManager.

        Parameters
        ----------
        room    : string - Room name
        context : dict   - { grid_id, grid_filters, columns, ... }
        options : dict   - Subscription options
        """

    # ==========================================================================
    #  RXJS INTEGRATION
    # ==========================================================================

    def untilDestroyed(self):
        """Returns an RxJS operator: takeUntil(this.context.destroy$)."""

    def safeSubscribe(self, observable, next, error=None, complete=None):
        """
        Subscribe to an observable with automatic cleanup on page destroy.
        Returns the subscription.
        """

    def fromEvent(self, target, type, options=None):
        """
        Create an RxJS observable from a DOM event, auto-piped through untilDestroyed().
        """

    def onMergedChange(self, sourcesObject, fn):
        """
        Subscribe to changes from multiple ObservableDictionary stores.
        sourcesObject: { name: dictInstance, ... }
        fn: (sourceName, change) => void
        """

    # ==========================================================================
    #  SCOPED TIMERS
    # ==========================================================================

    def setTimeoutScoped(self, fn, ms):
        """setTimeout that auto-clears on page cleanup. Returns timer ID."""

    def setIntervalScoped(self, fn, ms):
        """setInterval that auto-clears on page cleanup. Returns interval ID."""

    def clearTimers(self):
        """Clear all scoped timeouts and intervals."""

    def sleep(self, ms):
        """Promise-based sleep using setTimeoutScoped."""

    def scheduleRaf(self, key, fn):
        """
        requestAnimationFrame with deduplication by key.
        If called again with the same key before the frame fires, the first
        request is kept and the second is ignored.
        """

    # ==========================================================================
    #  PAGE CHROME
    # ==========================================================================

    def setWindowTitle(self, title="Portfolio Webtool"):
        """Set document.title."""

    def setFavicon(self, src="/assets/ico/pt.ico"):
        """Set the page favicon."""

    # ==========================================================================
    #  ROLE GUARDS
    # ==========================================================================

    def requireRole(self, elSelOrEl, role):
        """
        Hide a DOM element unless the current user has the specified role.
        Auto-updates when user roles change.
        """

    # ==========================================================================
    #  GRID HELPERS (available on PageBase, used by subclasses)
    # ==========================================================================

    def create_generic_field(self, field, template, engine):
        """
        Create an ag-Grid column definition from an Arrow field name.
        Auto-detects data type from the Arrow schema.
        """


# ====================================================================================
#  ObservableDictionary QUICK REFERENCE
# ====================================================================================
#
#  Stores created by createStore() / createSharedStore() are ObservableDictionary
#  instances. Key methods widget authors use:
#
#  READS:
#    store.get(key)                       - Get a value
#    store.has(key)                       - Check if key exists
#    store.asObject()                     - Get all key-values as plain object
#
#  WRITES:
#    store.set(key, value)                - Set a value (emits change)
#    store.update(obj)                    - Merge multiple key-values
#    store.delete(key)                    - Remove a key
#    store.clear()                        - Remove all keys
#
#  SUBSCRIPTIONS:
#    store.onChanges(fn)                  - fn({ current, previous, changes }) on ANY change
#    store.onRawChanges(fn)               - Same but fires synchronously
#    store.onValueChanged(key, fn)        - fn(currentValue, previousValue) when specific key changes
#    store.onValueAddedOrChanged(key, fn) - fn(value) when key is added or changed
#    store.onChange(key, fn)              - Alias for onValueChanged
#    store.pick(key)                      - Returns a sub-store scoped to one key
#    store.selectKeys(keys)               - Returns a sub-store scoped to multiple keys
#
#  PERSISTENCE:
#    store.bindPersistentKeys(keys, opts) - Persist specific keys to localStorage
#      opts: { namespace, key, storage, reconcile, listen }
#
#  DISPOSAL:
#    store.dispose()                      - Unsubscribe all, stop persistence
#
#  EXAMPLE:
#    const store = this.context.page.createStore('myWidget', { count: 0 });
#    store.onValueChanged('count', (newVal, oldVal) => {
#        console.log(`Count changed from ${oldVal} to ${newVal}`);
#    });
#    store.set('count', 42);
#
"""

