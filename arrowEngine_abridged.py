
"""
======================================================================================
 ABRIDGED ArrowEngine & ArrowAgGridAdapter - PUBLIC API REFERENCE
======================================================================================

 SOURCE:  src/grids/js/arrow/arrowEngine.js
 PURPOSE: LLM context document - provides the public API surface for building
          new PT widgets without requiring the full ~5800-line source file.

 THIS IS NOT THE FULL FILE.
 - Internal/private methods (prefixed with _) are omitted unless commonly
   used by widgets (e.g., _getValueGetter for performance-critical loops).
 - Implementation bodies are replaced with docstrings describing behavior.
 - Overlay internals, materialization pipeline, sort internals, and ag-Grid
   wiring details are omitted.
 - For the full implementation, see: src/grids/js/arrow/arrowEngine.js

 LAST SYNCED: 2026-04-16 from bugfix/engine_changes branch
======================================================================================
"""


# ====================================================================================
#  ArrowEngine
# ====================================================================================
#
#  The core data engine. Wraps an Apache Arrow Table with:
#  - Cell-level reads (with overlay/derived column support)
#  - Overlay system (in-memory edits layered on top of immutable Arrow data)
#  - Derived columns (computed columns with dependency tracking)
#  - Epoch-based reactivity (change notifications for rows/columns)
#  - Row mutation (add/remove/replace)
#  - Index-based querying and statistics
#
#  Widgets access this via:  this.engine  (set in BaseWidget constructor as adapter.engine)
#
# ====================================================================================

class ArrowEngine:
    """
    constructor(context, table, opts={})

    Parameters
    ----------
    context : object
        The page context object (context.page = PageBase instance).
    table : Apache Arrow Table
        The initial Arrow table backing this engine.
    opts : dict
        idProperty        : str   = '__rowId'     - Column name used as row ID
        idxProperty       : str   = '__srcIndex'  - Column name for source index
        rowIdGetter       : fn    = null           - Custom (rowIndex, engine) => string
        autoCommitThreshold : float = 0.25         - Fraction of rows edited before auto-materialize
        useEpochCache     : bool  = false
        yieldEvery        : int   = 8192
        decodeDictionaries: bool  = false
        scaleTimestampsToMs: bool = true
        master_schema     : Schema = null          - Master schema (superset of all columns)

    Key Properties
    --------------
    engine.table          - The underlying Arrow Table
    engine.columnDefs     - Map<string, colDef> of registered column definitions
    engine.emitter        - CadesEmitter instance for event pub/sub
    engine.context        - The context object
    """

    # --------------------------------------------------------------------------
    #  SCHEMA & METADATA
    # --------------------------------------------------------------------------

    def numRows(self):
        """Returns the number of rows (int)."""

    def numCols(self):
        """Returns the number of columns (int)."""

    def getRowCount(self):
        """Alias for numRows()."""

    def schema(self):
        """Returns the Arrow table schema object."""

    def fields(self):
        """Returns list of all column names (string[])."""

    def fieldSet(self):
        """Returns Set<string> of all column names. Cached."""

    def dtypes(self):
        """Returns Map<string, ArrowType> of column name -> Arrow data type."""

    def getDtype(self, column):
        """Returns the Arrow data type for a single column name."""

    # --------------------------------------------------------------------------
    #  CELL-LEVEL READS
    # --------------------------------------------------------------------------

    def getCell(self, rowIndex, column, params=None):
        """
        Read a single cell value.

        Resolves in priority order: overlay > derived > physical Arrow column.
        Handles dictionary decoding, timestamp scaling, etc.

        Parameters
        ----------
        rowIndex : int     - 0-based row index
        column   : string  - Column name
        params   : object  - Optional params (passed to derived column getters)

        Returns: The cell value (number, string, null, etc.)
        """

    def getCellById(self, id, column, params=None):
        """
        Read a cell by row ID (not index).
        Internally resolves ID -> index via getRowIndexById, then calls getCell.
        """

    def getRowObject(self, rowIndex, columns=None):
        """
        Returns a plain object { colName: value, ... } for one row.
        If columns is None, returns ALL columns. Includes __rowId.
        """

    # --------------------------------------------------------------------------
    #  ROW ID <-> INDEX MAPPING
    # --------------------------------------------------------------------------

    def getRowIdByIndex(self, rowIndex):
        """Returns the row ID (string) for the given 0-based row index."""

    def getRowIndexById(self, rowId):
        """
        Returns the 0-based row index for the given row ID.
        Builds an internal lookup map on first call (lazy).
        Returns -1 if not found.
        """

    # --------------------------------------------------------------------------
    #  COLUMN-LEVEL READS
    # --------------------------------------------------------------------------

    def getColumnValues(self, column):
        """
        Returns an Array of all values for the given column(s).

        If column is a single string:  returns Array<value>
        If column is an array:         returns { colName: Array<value>, ... }
        """

    def getFormattedColumnValues(self, column):
        """
        Like getColumnValues but applies valueFormatter from columnDefs.
        Returns Array<string>.
        """

    def getDistinctColumnValues(self, columns, asArr=False):
        """
        Returns unique values for given column(s).
        If asArr=False: returns Map<colName, Set<value>>
        If asArr=True:  returns Map<colName, Array<value>>
        """

    def getColumnType(self, columns):
        """Returns the column type metadata for the given column(s)."""

    # --------------------------------------------------------------------------
    #  BULK ROW READS
    # --------------------------------------------------------------------------

    def getAllRows(self, format='objects', columns=None, useActiveColumns=True):
        """
        Returns all rows.
        format='objects': list of { colName: value } dicts
        format='array':   list of [value, value, ...] arrays
        """

    def getRows(self, startRow, endRow, format='objects', columns=None):
        """Returns rows in range [startRow, endRow)."""

    def getRowWindowObjects(self, startRow, endRow, columns=None, includeId=True):
        """Returns array of row objects for the given range."""

    def getDisjointRowObjectsFast(self, row_idxs, columns=None, includeId=True,
                                   useFormats=False, gridApi=None, useHeaders=False):
        """
        High-performance version of getDisjointRowObjects.
        Hoists getter creation outside the row loop for better perf.
        Use this for large selections (e.g., clipboard copy).
        """

    def getDisjointRowObjects(self, row_idxs, columns=None, includeId=True,
                               useFormats=False, gridApi=None, useHeaders=False):
        """
        Returns row objects for arbitrary (non-contiguous) row indices.
        Supports formatted output and header-name keys when gridApi provided.
        """

    def getDisjointRowArrays(self, row_idxs, columns=None):
        """Returns rows as arrays (not objects) for given indices."""

    def getColumns(self, columnOrColumns):
        """Alias: getRowWindowObjects(0, numRows(), columnOrColumns)."""

    # --------------------------------------------------------------------------
    #  TABLE ACCESS
    # --------------------------------------------------------------------------

    def getArrowTable(self):
        """Returns the raw Arrow Table, or null."""

    def asTable(self):
        """Alias for getArrowTable()."""

    def toMap(self, k='portfolioKey'):
        """
        Returns a Map<keyValue, rowObject> keyed by the given column.
        Useful for quick lookups by portfolio key, ISIN, etc.
        """

    # --------------------------------------------------------------------------
    #  HIGH-LEVEL CONVENIENCES
    # --------------------------------------------------------------------------

    def mapColumn(self, column, mapFn, startRow=0, endRow=None):
        """
        Applies mapFn(value, rowIndex) to each cell in the column.
        Returns Array of results.
        """

    def computeStats(self, column, startRow=0, endRow=None):
        """
        Single-pass Welford statistics for a numeric column.
        Returns: { count, nulls, min, max, sum, mean, std, variance }
        """

    # --------------------------------------------------------------------------
    #  OVERLAY SYSTEM (in-memory edits)
    # --------------------------------------------------------------------------
    #
    #  Overlays are edits layered on top of the immutable Arrow data.
    #  When you call getCell(), overlays take priority over physical data.
    #  Two storage strategies: dense typed arrays (for numeric) and Maps (for other).
    #

    def setOverlayValue(self, rowIndex, column, value, bump=True):
        """
        Set an overlay value for a single cell.
        bump=True triggers epoch change notification.
        Returns self for chaining.
        """

    def setOverlayValuesByColumnBatch(self, column, rowIndices, values,
                                       freeze=False, bump=True):
        """
        Batch-set overlay values for one column across many rows.
        Much faster than calling setOverlayValue in a loop.
        Returns { applied: int, rowsChanged: Int32Array }
        """

    def setOverlayValuesByRowBatch(self, rowIndex, changes, freezeDerived=True, bump=True):
        """
        Set multiple column overlays for a single row at once.
        changes: { colName: value, ... } or [[colName, value], ...]
        """

    def clearOverlayValue(self, rowIndex, column, bump=True):
        """Remove the overlay for a single cell (reverts to physical data)."""

    def clearOverlayColumn(self, column, bump=True):
        """Remove all overlays for an entire column."""

    def hasEdits(self, column=None):
        """
        Returns True if any overlay edits exist.
        If column is specified, checks only that column.
        """

    def resetOverlays(self):
        """Clear ALL overlays for all columns. Does not emit epochs."""

    # --------------------------------------------------------------------------
    #  DERIVED COLUMNS (computed columns with dependency tracking)
    # --------------------------------------------------------------------------

    def registerDerivedColumn(self, name, getter, meta=None):
        """
        Register a computed column.

        Parameters
        ----------
        name   : string   - Column name
        getter : function - (rowIndex, engine, settings?) => value
        meta   : dict
            deps     : string[] or (engine) => string[]  - columns this depends on
            kind     : string  - data type hint ('number', 'text', etc.)
            inverse  : fn      - inverse function for edits
            settings : dict    - { dict, keys } for reactive settings integration
            async    : bool    - if getter is async

        When a dependency column changes, the derived column's cache is
        invalidated and epochs are bumped automatically.
        """

    def addDependencies(self, derivedColName, newDepsArray):
        """Add additional dependencies to an existing derived column."""

    def listDerivedColumns(self):
        """Returns Array<string> of all registered derived column names."""

    def getDependenciesClosure(self, columns):
        """
        Returns the full transitive closure of dependencies for the given columns.
        Useful for understanding what columns feed into a computation.
        """

    def getDependentsClosure(self, columns):
        """
        Returns the full transitive closure of dependents (downstream columns).
        Useful for understanding what will be affected by a column change.
        """

    # --------------------------------------------------------------------------
    #  EDIT PIPELINE
    # --------------------------------------------------------------------------

    def applyValue(self, rowIndex, column, raw, colDef):
        """
        High-level edit: applies a value through the column's edit plan.
        Handles coercion, inverse functions, side-effect columns, freezing, etc.
        This is what ag-Grid's valueSetter calls internally.
        """

    def persistEdit(self, rowIndex, column, value):
        """
        Broadcasts an edit over WebSocket for server persistence.
        Called automatically after applyValue.
        """

    def flushPendingEdits(self):
        """Force-drain any queued edit broadcasts."""

    async def materializeEdits(self, commit='auto'):
        """
        Bake all overlays into a new Arrow Table.
        commit='auto': only materializes if overlay count exceeds threshold.
        Debounced internally (200ms).
        """

    # --------------------------------------------------------------------------
    #  REACTIVITY / EVENTS
    # --------------------------------------------------------------------------
    #
    #  The engine uses an epoch-based system. Each column tracks an "epoch"
    #  counter. When data changes, the epoch increments and listeners fire.
    #

    def onEpochChange(self, fn):
        """
        Subscribe to ANY data change (row add/remove, overlay edit, derived recompute).
        fn receives an epoch payload. Returns an unsubscribe function.
        """

    def onColumnEpochChange(self, columns, fn, opts=None):
        """
        Subscribe to changes on SPECIFIC columns only.
        columns: string or string[]
        fn: callback
        opts: { debounceMs, leading, trailing }
        Returns an unsubscribe function.
        """

    def onMaterialize(self, fn):
        """Subscribe to table materialization events. Returns unsub fn."""

    def onDerivedDirty(self, fn):
        """Subscribe to derived column invalidation. fn(Set<dirtyNames>). Returns unsub fn."""

    def onTableWillReplace(self, fn):
        """Fires before replaceTable(). fn(prevTable, nextTable). Returns unsub fn."""

    def onTableDidReplace(self, fn):
        """Fires after replaceTable(). fn(prevTable, nextTable). Returns unsub fn."""

    def onColumnEvent(self, event, fn):
        """Subscribe to grid-level column events (visibility, resize, etc.)."""

    # --------------------------------------------------------------------------
    #  ROW MUTATIONS
    # --------------------------------------------------------------------------

    def addRows(self, rows, updateIfExists=True):
        """
        Add rows to the engine.
        rows: array of { colName: value } objects
        updateIfExists: if a row with the same ID exists, update it instead.
        Returns { addedIndices: Int32Array, addedIds: string[] }
        """

    def removeRowsById(self, idsOrRows):
        """
        Remove rows by their row IDs.
        idsOrRows: array of ID strings, or array of row objects (uses __rowId).
        Returns { removedIndices, removedIds }
        """

    def removeRowsByIndices(self, rowIndices):
        """
        Remove rows by their 0-based indices.
        Returns { removedIndices, removedIds }
        """

    def removeAllRows(self, resetOverlays=True, resetFrozen=True,
                       rebuildIndexes=True, emitGlobalEpoch=True):
        """Remove all rows, creating an empty table with the same schema."""

    def replaceTable(self, newTable):
        """
        Replace the entire backing Arrow Table.
        Rebuilds internal indexes, clears caches, emits table-will/did-replace events.
        """

    def replaceAllRows(self, rowsOrTable, preserveSchema=True, resetOverlays=False,
                        rebuildIndexes=True, emitGlobalEpoch=False, useMasterSchema=True):
        """
        Replace all rows with new data.
        resetOverlays='byId' preserves overlays by remapping via row IDs.
        """

    def resetWithRows(self, rows, rebuildIndexes=True, emitGlobalEpoch=True):
        """Convenience: replaceAllRows + resetOverlays in one call."""

    # --------------------------------------------------------------------------
    #  INDEXING & QUERYING
    # --------------------------------------------------------------------------

    def enableIndexes(self, numericMinMax=True):
        """Enable block-based min/max indexes for numeric columns. Returns self."""

    def rebuildAllIndexes(self, columns=None):
        """Force rebuild indexes for given columns (or all)."""

    def refreshIndexes(self, columns=None):
        """Refresh indexes after data changes."""

    def queryRange(self, column=None, op=None, value=None, lo=None, hi=None):
        """
        Query rows using indexed range filters.
        op: 'eq', 'lt', 'lte', 'gt', 'gte', 'between', 'ne'
        Returns Int32Array of matching row indices.
        """

    def setActiveColumnsFromProjection(self, projection):
        """
        Restrict which columns are 'active' for getAllRows() etc.
        projection: string[] of column names.
        """

    def getActiveColumns(self):
        """Returns the current active column set, or null if unrestricted."""

    def isColumnActive(self, column):
        """Returns True if the column is in the active set (or if unrestricted)."""

    # --------------------------------------------------------------------------
    #  SORTING
    # --------------------------------------------------------------------------

    def buildComparatorByColumns(self, sortModel, opts=None):
        """
        Build a comparator function from ag-Grid sort model.
        sortModel: [{ colId: 'foo', sort: 'asc' }, ...]
        Returns a comparator function (a, b) => number
        """

    def sortIndex(self, indexArray, comparator):
        """Sort an Int32Array of row indices using the given comparator."""

    def sortIndexByModel(self, sortModel, idx, columnMap):
        """
        Sort an index array using the ag-Grid sort model.
        Builds comparator internally.
        """

    # --------------------------------------------------------------------------
    #  COLUMN REGISTRATION
    # --------------------------------------------------------------------------

    def registerColumnsFromDefs(self, columnDefs, settingsDict):
        """
        Register column definitions with the engine.
        Processes derived columns (context.compute + context.deps) and
        value setters (context.edit) from the column def context.

        columnDefs: standard ag-Grid-style column definition array
        settingsDict: ObservableDictionary for reactive settings
        """

    # --------------------------------------------------------------------------
    #  LIFECYCLE
    # --------------------------------------------------------------------------

    def dispose(self):
        """
        Full cleanup: cancels timers, clears all caches/overlays/derived state,
        releases Arrow table reference, disposes emitter and coalescer.
        """


# ====================================================================================
#  ArrowAgGridAdapter
# ====================================================================================
#
#  Bridges ArrowEngine <-> ag-Grid Enterprise.
#  Handles: mounting, SSR datasource, column defs, sorting, filtering,
#  epoch-driven refresh, copy/paste, keyboard navigation, undo/redo.
#
#  Widgets access this via:  this.adapter  (the 'adapter' param in BaseWidget constructor)
#  The adapter's engine:     this.adapter.engine  (same as this.engine)
#
# ====================================================================================

class ArrowAgGridAdapter:
    """
    constructor(context, engine, columnDefs, opts={}, config={})

    Parameters
    ----------
    context    : object    - The page context
    engine     : ArrowEngine
    columnDefs : array     - ag-Grid column definitions
    opts       : dict
        rowModelType        : str = 'serverSide'
        cacheBlockSize      : int = engine.numRows()
        refreshDebounceMs   : int = 10
        settingsDict        : ObservableDictionary = null
        gridOptions         : dict = {}     - Extra ag-Grid options to merge
    config     : dict
        name                : str = 'portfolio'
        globalViews         : array = []    - Preset column layouts
        enableFilterMemory  : bool = True
        enablePasteOverride : bool = False

    Key Properties
    --------------
    adapter.engine          - The ArrowEngine instance
    adapter.api             - ag-Grid GridApi (available after mount)
    adapter.gridOptions     - The ag-Grid gridOptions object
    adapter.grid$           - ObservableDictionary store for grid state
    adapter.name            - Grid name (e.g., 'portfolio')
    adapter.selector        - CSS selector of mounted element
    adapter.emitter         - CadesEmitter (same as context.page.emitter)
    adapter.filterManager   - ArrowAgGridFilter instance
    adapter.searchManager   - GlobalSearchManager instance
    adapter.columnRegistry  - AgColumnRegistry instance
    """

    # --------------------------------------------------------------------------
    #  STATIC EVENT CONSTANTS
    # --------------------------------------------------------------------------
    #
    #  Use these with adapter.emitter.on(EVENT, fn) to subscribe:
    #
    #  ArrowAgGridAdapter.FIRST_EVENT       = "grid:first"
    #  ArrowAgGridAdapter.API_READY         = "grid:ready"
    #  ArrowAgGridAdapter.SORT_EVENT        = "grid:sort"
    #  ArrowAgGridAdapter.FILTER_EVENT      = "grid:filter"
    #  ArrowAgGridAdapter.FILTER_MODEL_EVENT = "grid:filter-model"
    #  ArrowAgGridAdapter.SEARCH_EVENT      = "grid:search"
    #  ArrowAgGridAdapter.COLUMNS_EVENT     = "grid:columns"
    #  ArrowAgGridAdapter.COLUMN_RESIZE_EVENT = "grid:column-resize"

    # --------------------------------------------------------------------------
    #  MOUNTING & LIFECYCLE
    # --------------------------------------------------------------------------

    def mount(self, elementOrSelector):
        """
        Mount the ag-Grid to a DOM element.

        - Builds ag-Grid column defs from the registered definitions
        - Creates the serverSide datasource backed by the ArrowEngine
        - Sets up epoch-driven auto-refresh
        - Configures keyboard handlers, hover effects, tree service
        - Sets adapter.api to the live ag-Grid GridApi

        After mount, the grid is live and reactive to engine changes.
        """

    def afterMount(self):
        """Post-mount setup (called via requestAnimationFrame)."""

    def dispose(self):
        """
        Full cleanup:
        - Cancels debounces, timers, subscriptions
        - Destroys search/filter managers, undo/redo, paste override
        - Disposes the underlying engine
        - Destroys the ag-Grid api
        """

    # --------------------------------------------------------------------------
    #  COLUMN MANAGEMENT
    # --------------------------------------------------------------------------

    def registerColumns(self, columnDefs):
        """
        Register column definitions with the adapter's column registry.
        Also registers with engine.registerColumnsFromDefs().
        """

    def moveColumn(self, key, index, opts=None):
        """
        Move a column to a new position in the grid.
        opts: { breakPins, toRight, pinned, ensureScroll }
        """

    def getGlobalPresets(self):
        """Returns the globalViews array (preset column layouts)."""

    def setPasteOverride(self, flag):
        """Enable/disable paste override mode."""

    # --------------------------------------------------------------------------
    #  COMMONLY ACCESSED BY WIDGETS
    # --------------------------------------------------------------------------
    #
    #  Widgets typically interact with the adapter through:
    #
    #  1. this.adapter.engine   - Direct ArrowEngine access (most common)
    #  2. this.adapter.api      - ag-Grid API for grid operations
    #  3. this.adapter.grid$    - Observable store for grid state
    #     - grid$.get('initialized')
    #     - grid$.get('weight')
    #     - grid$.onChanges(fn)
    #  4. this.adapter.emitter  - Event bus
    #     - emitter.on('grid:first', fn)
    #     - emitter.on('grid:filter', fn)
    #
    #  Example widget pattern (in afterMount or onActivate):
    #
    #     const engine = this.engine;  // === this.adapter.engine
    #     const n = engine.numRows();
    #
    #     // Read all values for a column
    #     const prices = engine.getColumnValues('bvalMidPx');
    #
    #     // React to data changes
    #     const unsub = engine.onEpochChange(() => this.rebuild());
    #     this.addSubscription({ unsubscribe: unsub });
    #
    #     // React to specific columns only
    #     const unsub2 = engine.onColumnEpochChange(
    #         ['grossSize', 'netSize'],
    #         () => this.updateKPIs()
    #     );
    #     this.addSubscription({ unsubscribe: unsub2 });
    #
    #     // Read a cell
    #     const val = engine.getCell(rowIndex, 'description');
    #
    #     // Compute stats
    #     const stats = engine.computeStats('grossDv01');
    #     // stats = { count, nulls, min, max, sum, mean, std, variance }
    #
    #     // Performance: hoist getter outside loop
    #     const getter = engine._getValueGetter('grossSize');
    #     for (let i = 0; i < n; i++) {
    #         const val = getter(i);  // much faster than getCell in tight loops
    #     }
    #


# ====================================================================================
#  NOTES FOR WIDGET AUTHORS
# ====================================================================================
#
#  _getValueGetter(name, params)
#  -----------------------------
#  Although prefixed with _, this is commonly used by widgets for performance.
#  Returns a closure:  (rowIndex) => value
#  Hoisting this outside a loop avoids repeated Map lookups and overlay checks
#  per cell. Use getCell() for one-off reads; use _getValueGetter for loops.
#
#
#  Epoch System
#  ------------
#  Every column has an epoch counter (incrementing integer). When data changes
#  (overlay set, derived recompute, table replace), the epoch bumps. Listeners
#  registered via onEpochChange / onColumnEpochChange fire on the NEXT animation
#  frame (batched via RAF for performance).
#
#  Pattern for widgets:
#    afterMount() {
#        const unsub = this.engine.onEpochChange(() => this.rebuildUI());
#        this.addSubscription({ unsubscribe: unsub });
#    }
#
#
#  Subscription Cleanup
#  --------------------
#  BaseWidget.addSubscription(sub) stores subscriptions and auto-cleans them
#  on deactivate(). Wrap engine unsub functions as { unsubscribe: fn }.
#
#
#  Context Access
#  --------------
#  this.context.page          - The PageBase instance (PortfolioPage for PT)
#  this.context.portfolio_key - The portfolio key string
#  this.context.portfolioRoom - The WebSocket room name
#  this.context.page.socketManager() - WebSocket manager for sending messages
#  this.context.page.createStore(key, defaults) - Create reactive ObservableDictionary
#  this.context.page.emitter  - CadesEmitter for page-level events
#  this.context.page.addEventListener(el, event, fn) - Lifecycle-managed DOM listener
