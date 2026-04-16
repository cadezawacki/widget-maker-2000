
"""
======================================================================================
 ABRIDGED PortfolioPage (pt.js) - PUBLIC API REFERENCE
======================================================================================

 SOURCE:  src/pt/js/pt.js
 PURPOSE: LLM context document - provides the structure and key APIs of the
          PortfolioPage class, which is the main PT page that hosts all widgets.
          Widgets access this as `this.context.page` (it IS a PageBase subclass).

 THIS IS NOT THE FULL FILE.
 - DOM caching, dropdown wiring, and visual helpers are summarized.
 - Full pill configuration, meta event binding, and tooltip setup are omitted.
 - Market column generation logic is summarized.
 - For the full implementation, see: src/pt/js/pt.js (~3500 lines)

 LAST SYNCED: 2026-04-16 from bugfix/engine_changes branch
======================================================================================
"""


# ====================================================================================
#  IMPORTS (what pt.js pulls in - useful for understanding dependencies)
# ====================================================================================
#
#  External:
#    overlayscrollbars, date-fns, date-fns-tz, interactjs, canvas-confetti
#
#  Internal managers:
#    PageBase          from '@/pageBase/js/pageBaseV2.js'
#    WidgetManager     from '@/pt/js/widgets/widgetManager.js'
#    GridSettings      from '@/pt/js/grids/portfolioSettings.js'
#    PillManager       from '@/global/js/pillManager.js'
#    MicroGridManager  from '@/global/js/microGridManager.js'
#
#  Widgets (imported and registered):
#    OverviewWidget    from '@/pt/js/widgets/overview.js'
#    PivotWidget       from '@/pt/js/widgets/pivotArrowWidget.js'
#    ScriptWidget      from '@/pt/js/widgets/scriptWidget.js'
#    RefreshWidget     from '@/pt/js/widgets/refreshWidget.js'
#
#  Grid/Engine:
#    ArrowEngine, ArrowAgGridAdapter  from '@/grids/js/arrow/arrowEngine.js'
#    portfolioColumns                 from '@/pt/js/grids/portfolio/portfolioColumns.js'
#    marketColumnFactory              from '@/pt/js/grids/portfolio/marketColumnFactory.js'
#
#  Utilities:
#    ACTION_MAP        from '@/global/actionMap.js'
#    EnhancedPayloadBatcher from '@/global/enhancedPayloadBatcher.js'
#    helpers, typeHelpers, NumberFormatter, StringFormatter, AnimationHelper,
#    clipboardHelpers, ENumberFlow, ConfigurableDropdown
#
#  CSS:
#    '@/pt/css/pt.css'


# ====================================================================================
#  MODULE-LEVEL CONSTANTS
# ====================================================================================
#
#  VENUE_MEDIUM = { 'BBG': 'BBG', 'MX': 'MarketAxess', 'TW': 'Tradeweb', ... }
#  MARKET_PRIORITY = { "BVAL (Latest)": 0, "MACP": 1, "AM": 2, ... }
#  PORTFOLIO_KEY_REGEX = /([a-f0-9]{32})/


# ====================================================================================
#  PortfolioPage extends PageBase
# ====================================================================================

class PortfolioPage:  # (PageBase)
    """
    constructor(name='pt', { url_context, context, config, container })

    Extracts portfolio_key from URL context (must be a 32-char hex string).
    Sets up room names for WebSocket subscriptions.

    Key Constructor Properties
    --------------------------
    # Identity
    this.context.portfolio_key  - The 32-char hex portfolio key
    this.context.portfolioRoom  - '{KEY}.PORTFOLIO' room name
    this.context.metaRoom       - '{KEY}.META' room name

    # Grid
    this.grid_id      = 'portfolio'
    this.selector     = '#portfolio-grid'
    this.ptGrid       = null     # ArrowAgGridAdapter (set during onInit)
    this.engine        -> this.ptGrid.engine (ArrowEngine, available after grid init)

    # Defaults
    this.defaultQuoteType = 'client'
    this.defaultMarket    = 'bval'
    this.defaultSide      = 'Mid'
    this.defaultWaterfall = True
    this.defaultWidget    = 'overviewWidget'

    # Managers
    this.widgetManager    = null  # WidgetManager (set during _initializeWidgets)
    this.pillManager      = null  # PillManager (set during _setupPills)
    this._microGridManager = null # MicroGridManager

    # Market data
    this.marketDataMap    = Map() # market key -> column metadata
    """

    # ==========================================================================
    #  LIFECYCLE (overrides from PageBase)
    # ==========================================================================

    def onBeforeInit(self):
        """Sets up DOM references and reactive stores."""

    def onCacheDom(self):
        """
        Caches ~40 DOM element references used throughout the page.
        Key elements widgets might reference via this.context.page:

        this.context.page.searchInput         - Quick filter text input
        this.context.page.headerNameElem      - Client name in header
        this.context.page.headerSizeElem      - Size display in header
        this.context.page.headerNetElem       - Net display in header
        this.context.page.headerCountElem     - Count display in header
        this.context.page.progressBar         - Pricing progress bar
        this.context.page.contentArea         - Widget content area
        this.context.page.toolbarButtons      - Toolbar button NodeList
        this.context.page.stateDisplay        - Portfolio state badge
        """

    def onInit(self):
        """
        Main initialization:
        1. Creates GridSettings manager
        2. Sets up WebSocket subscriptions (meta + portfolio rooms)
        3. Initializes MicroGridManager
        """

    def onBind(self):
        """
        Binds UI interactions:
        - Scroll shadow effects
        - Reference market dropdown
        - Grid resizer (interact.js)
        - DOM event listeners
        - Auto-switch tab behavior
        - DOM animation tracking
        """

    def onReady(self):
        """Shows portfolio body and pricing container."""

    def onCleanup(self):
        """
        Full teardown:
        - Disconnects resize observer and overlay scrollbars
        - Destroys progress bar flows and skew displays
        - Aborts controllers
        - Tears down grid (engine, adapter, pills, widgets)
        - Destroys micro-grid manager
        """

    # ==========================================================================
    #  REACTIVE STORES (set up in setupStores)
    # ==========================================================================
    #
    #  These are ObservableDictionary stores that widgets can subscribe to.
    #  Access via this.context.page.<storeName>
    #
    #  PAGE-LEVEL (this.context.page.page$):
    #    'activeQuoteType'     - Current quote type ('client', 'house', etc.)
    #    'activeMarket'        - Current reference market ('bval', 'macp', etc.)
    #    'activeSide'          - Current side ('Bid', 'Mid', 'Ask', 'Auto')
    #    'waterfallRef'        - Boolean: waterfall mode enabled
    #    'quickSearch'         - Current search string
    #    'linkedPivotFilters'  - Boolean: pivot filters linked to main grid
    #    'lockPivotTotals'     - Boolean: lock pivot total row
    #    'colorizePivot'       - Boolean: colorize pivot cells
    #    'lastAgFilterModel'   - Last ag-Grid filter model object
    #    'bulkSkewMode'        - Boolean: bulk skew editing mode
    #    'username'            - Current user's username
    #    'displayName'         - Current user's display name
    #
    #  DERIVED/PICKED STORES (convenience accessors):
    #    this.activeQuoteType$  = page$.pick('activeQuoteType')
    #    this.activeMarket$     = page$.pick('activeMarket')
    #    this.activeSide$       = page$.pick('activeSide')
    #    this.activeRefMarket$  = page$.selectKeys(['activeMarket', 'activeSide'])
    #    this.activeRefSettings$ = page$.selectKeys(['activeMarket', 'activeSide', 'activeQuoteType'])
    #    this.waterfallRef$     = page$.pick('waterfallRef')
    #    this.quickSearch$      = page$.pick('quickSearch')
    #    this.linkedPivotFilters$ = page$.pick('linkedPivotFilters')
    #
    #  SHARED STORES:
    #    this.qtSigFigs$        - Sig figs per quote type (persisted to localStorage)
    #    this.gridSettings$     - Grid settings (highlight, stats, etc.)
    #    this.sideText$         = gridSettings$.pick('sideText')
    #    this.sideColor$        = gridSettings$.pick('sideColor')
    #
    #  PORTFOLIO META:
    #    this.portfolioMeta$    - Portfolio metadata (client, state, dueTime, etc.)
    #    this._metaStore        - Same as portfolioMeta$
    #
    #  SKEW:
    #    this.overallSkew$      - { bid: {value, unit}, ask: {value, unit} }

    # ==========================================================================
    #  WIDGET SYSTEM
    # ==========================================================================

    def getWidget(self, name):
        """
        Get a widget instance by name.
        Returns the widget instance, or undefined if not found.

        Names: 'overviewWidget', 'pivotWidget', 'scriptWidget', 'refreshWidget'
        """

    def _initializeWidgets(self):
        """
        Creates the WidgetManager and registers all widgets:

            widgetManager.register(OverviewWidget, 'overviewWidget', this.ptGrid, '#overviewWidget')
            widgetManager.register(PivotWidget,    'pivotWidget',    this.ptGrid, '#pivotWidget')
            widgetManager.register(ScriptWidget,   'scriptWidget',   this.ptGrid, '#scriptWidget')
            widgetManager.register(RefreshWidget,  'refreshWidget',  this.ptGrid, '#refreshWidget')
            widgetManager.mount(this.ptGrid)

        Then switches to this.defaultWidget ('overviewWidget').

        TO ADD A NEW WIDGET:
        1. Import your WidgetClass at top of pt.js
        2. Add register() call here
        3. Add <div id="yourWidgetId" class="tab-content"></div> in pt.html
        4. Add toolbar button in pt.html toolbar section
        5. Optionally add hotkey in setupHotkeys()
        """

    def switchTab(self, targetWidgetKey):
        """
        Switch the active widget tab with CSS transition.
        Calls widgetManager.switch() internally.
        Handles the show/hide animation between old and new widget containers.
        """

    # ==========================================================================
    #  WEBSOCKET SUBSCRIPTIONS
    # ==========================================================================

    def _setupWebSocketSubscriptions(self):
        """
        Subscribes to two rooms:
        1. Meta room   - portfolio metadata (single row, all columns)
        2. Portfolio room - portfolio bond data (filtered by portfolio_key)

        Registers message handlers for 'subscribe', 'publish', 'ack' events.
        """

    def _handleMetaSubscription(self, message):
        """
        Handles initial meta data from WebSocket subscription.
        Populates portfolioMeta$ store, updates header, pills, and state badge.
        """

    def _handlePortfolioSubscription(self, message):
        """
        Handles initial portfolio data from WebSocket subscription.
        This is where the ArrowEngine and ArrowAgGridAdapter are created:

        1. Decodes payload (Arrow columnar format)
        2. Creates ArrowEngine from the Arrow Table
        3. Creates ArrowAgGridAdapter with portfolioColumns definitions
        4. Mounts the grid to '#portfolio-grid'
        5. Initializes widgets via _initializeWidgets()
        6. Sets up grid reactives (epoch listeners, skew, pricing %)
        7. Sets up micro-grid flag columns
        """

    def _parsePublishedPortfolioData(self, message):
        """
        Handles live portfolio data updates (incremental pushes).
        Applies row updates to the existing engine (addRows with updateIfExists).
        """

    def _parsePublishedMetaData(self, message):
        """
        Handles live meta data updates.
        Updates portfolioMeta$ store, triggers header/pill refresh.
        """

    # ==========================================================================
    #  GRID REACTIVES
    # ==========================================================================

    def setupGridReactives(self):
        """
        Sets up reactions between grid state and UI:
        - activeRefSettings$ changes -> recompute derived market columns
        - quickSearch$ changes -> notify grid filter
        - Grid filter model changes -> update filter icon
        """

    def setupGridEpochReactions(self):
        """
        Subscribes to engine epoch changes to update:
        - Pricing progress bar (% priced)
        - Filter icon state
        """

    def setupSkewReactions(self):
        """
        Subscribes to engine column changes on skew-related columns.
        Updates the overall skew display in the header.
        """

    def updateSkew(self, bid=None, ask=None):
        """Update the header skew ENumberFlow displays."""

    # ==========================================================================
    #  HEADER & STATUS
    # ==========================================================================

    def update_site_title(self, client=None, state=None):
        """Update document.title with client name and state."""

    def update_site_icon(self, state):
        """Update favicon based on portfolio state."""

    def _metaUpdateHeader(self, newState, animate=True):
        """
        Update the full header bar from meta state:
        - Client name, size, net, count, direction
        - State badge color
        - Pricing progress bar
        - Confetti animation on WON state
        """

    def _setProgressBar(self, percentage, count, total):
        """Update the pricing progress bar width and counter displays."""

    def _refresh_status_color(self, state=None):
        """Update state badge color from colorManager."""

    # ==========================================================================
    #  REFERENCE MARKET SYSTEM
    # ==========================================================================

    def _handleRefMarketChange(self, newState, otherDropdown):
        """
        Called when user changes reference market or side in dropdown.
        Updates page$ stores, ensures market columns are loaded,
        syncs the paired dropdown.
        """

    def _computeMarketAvailability(self, metricsData, state, adapter):
        """
        Compute coverage and usage percentages for each market.
        Used by the dropdown to show which markets have data.
        """

    def _ensureMarketColumns(self, markets):
        """
        Ensure the ArrowAgGridAdapter has column definitions for the
        requested market(s). Uses generateAllMarketColumns() from
        marketColumnFactory.js.
        """

    # ==========================================================================
    #  MICRO-GRID FLAGS
    # ==========================================================================

    def _setupMicroGridFlags(self):
        """
        Registers derived columns on the engine for micro-grid flag data.
        Creates isFlagged, isFlagged<Severity>, isFlagged<Tag> columns
        that widgets can use for conditional formatting and filtering.
        """

    # ==========================================================================
    #  COPY / CLIPBOARD
    # ==========================================================================

    def _setupCopyButtons(self):
        """
        Wires up the 4 copy buttons in the header:
        - Full summary string
        - Short summary
        - Portfolio link
        - Portfolio key
        Each copies to clipboard with success/error feedback animation.
        """

    # ==========================================================================
    #  PUSH / PULL
    # ==========================================================================

    def send_full_push(self, event="", silent=False):
        """
        Send a full push to the server via WebSocket.
        Pushes all current grid data (overlays included) to the backend.
        """

    # ==========================================================================
    #  HOTKEYS (Ctrl+1 through Ctrl+4 switch widget tabs)
    # ==========================================================================
    #
    #  Ctrl+F       - Focus search input
    #  Ctrl+S       - Save column preset
    #  Ctrl+Shift+S - Save pivot preset
    #  Ctrl+O       - Open load preset dialog
    #  Ctrl+L       - Clear filters / search
    #  Ctrl+M       - Toggle side panel
    #  Ctrl+9       - Toggle minimize grid
    #  Ctrl+1       - Switch to Overview
    #  Ctrl+2       - Switch to Pivot
    #  Ctrl+3       - Switch to Script
    #  Ctrl+4       - Switch to Refresh

    # ==========================================================================
    #  PILLS SYSTEM
    # ==========================================================================

    def _setupPills(self):
        """
        Creates the PillManager and registers header pills:
        - venueShort (market type: BBG/MX/TW/TR)
        - numDealers (X in Comp)
        - isAon (AON/PARTIALS)
        - wire (Wire time)
        - isFwdStrike
        - fwdStrikeTimeMnemonic
        - barcSalesName
        - clientTraderName
        - isClientTtt (golden pill)
        - isClientM100 (silver pill)
        """

    # ==========================================================================
    #  UI HELPERS (commonly accessed by widgets)
    # ==========================================================================

    def createInfoModal(self, title, content, type="info", subtitle=None, classes=None):
        """
        Create an info modal dialog.
        Uses <dialog> element with .modal class.
        Returns the dialog element.
        """

    def checkFilterIcon(self, force=None):
        """Update the filter icon visibility based on current filter state."""

    def clearPivotFilters(self):
        """Clear all filters on the pivot widget."""

    def _toggleMinimize(self, force=None):
        """Toggle the grid minimize state (collapse/expand the content row)."""

    def _quoteTypeDisplay(self, val):
        """Convert quote type value to display string."""


# ====================================================================================
#  NOTES FOR WIDGET AUTHORS
# ====================================================================================
#
#  ACCESSING THE PAGE FROM A WIDGET:
#    const page = this.context.page;    // The PortfolioPage instance
#    const engine = this.engine;        // The ArrowEngine (same as page.ptGrid.engine)
#    const adapter = this.adapter;      // The ArrowAgGridAdapter (same as page.ptGrid)
#
#  READING PAGE STATE:
#    const qt = page.page$.get('activeQuoteType');
#    const mkt = page.page$.get('activeMarket');
#    const side = page.page$.get('activeSide');
#    const meta = page.portfolioMeta$.asObject();
#    const key = this.context.portfolio_key;
#
#  REACTING TO STATE CHANGES:
#    page.activeQuoteType$.onChanges((ch) => {
#        const newQt = ch.current.get('activeQuoteType');
#        this.rebuild(newQt);
#    });
#
#  SENDING WEBSOCKET MESSAGES:
#    const sm = page.socketManager();
#    await sm._sendWebSocketMessage({
#        action: ACTION_MAP.get("refresh"),
#        context: page._ptRaw.context,
#        options: { broadcast: false, log: false }
#    });
#
#  GETTING OTHER WIDGETS:
#    const overview = page.getWidget('overviewWidget');
#    const pivot = page.getWidget('pivotWidget');
#
#  ACCESSING GRID API (ag-Grid):
#    const gridApi = page.ptGrid.api;
#    gridApi.forEachNodeAfterFilter(node => { ... });
#    gridApi.refreshCells({ columns: ['myCol'] });
#
"""
