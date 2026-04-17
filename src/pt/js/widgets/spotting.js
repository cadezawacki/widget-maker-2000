import '../../css/spottingWidget.css';

import {BaseWidget} from './baseWidget.js';
import {createGrid} from 'ag-grid-enterprise';
import {writeObjectToClipboard} from '@/utils/clipboardHelpers.js';
import {CustomCellEditor} from "src/grids/js/genericCellEditor.js";

const SRC_COLUMNS = [
    'benchmarkIsin',
    'benchmarkName',
    'benchDescription',
    'benchmarkTerm',
    'benchmarkMidPx',
    'benchmarkMidYld',
    'benchmarkRefreshTime',
    'netHedgeSize',
    'isReal',
];

const MISSING_KEY = '**MISSING**';

function _fmtRefreshTime(raw) {
    if (raw == null || raw === '') return '';
    const iso = String(raw).replace(' ', 'T');
    const d = new Date(iso);
    if (isNaN(d.getTime())) return String(raw);
    const now = new Date();
    const sameDay = d.getFullYear() === now.getFullYear()
        && d.getMonth() === now.getMonth()
        && d.getDate() === now.getDate();
    const timeStr = d.toLocaleTimeString([], {hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'});
    if (sameDay) return timeStr;
    const dateStr = d.toLocaleDateString([], {year: 'numeric', month: '2-digit', day: '2-digit'});
    return `${dateStr} ${timeStr}`;
}

function _fmtSize(v) {
    if (v == null || !isFinite(v)) return '';
    return Number(v).toLocaleString(undefined, {maximumFractionDigits: 0});
}

function _fmtPx(v) {
    if (v == null || !isFinite(v)) return '';
    return Number(v).toLocaleString(undefined, {minimumFractionDigits: 3, maximumFractionDigits: 3});
}

function _round3(v) {
    if (v == null || !isFinite(v)) return v;
    return Math.round(Number(v) * 1000) / 1000;
}

function _splitPx(px) {
    if (px == null || !isFinite(px)) return {handle: null, n32: null};
    const sign = px < 0 ? -1 : 1;
    const abs = Math.abs(px);
    const handle = Math.floor(abs);
    const n32 = (abs - handle) * 32;
    return {handle: sign * handle, n32: sign < 0 ? -n32 : n32};
}

function _joinPx(handle, n32) {
    if (handle == null || !isFinite(handle)) return null;
    const h = Number(handle);
    const t = (n32 == null || !isFinite(n32)) ? 0 : Number(n32);
    const sign = (h < 0 || (h === 0 && t < 0)) ? -1 : 1;
    return sign * (Math.abs(h) + Math.abs(t) / 32);
}

export class SpottingWidget extends BaseWidget {
    constructor(context, widgetId, adapter, selector, manager, config = {}) {
        super(context, widgetId, adapter, selector, manager, config);

        this.gridSelector = config.gridSelector || '#spot-grid';
        this.gridApi = null;
        this._grid = null;

        this._disposers = [];
        this._rebuildRafId = 0;
        this._copyResetTimer = null;

        this.priceMode = 'benchmarkMidPx'; // or 'benchmarkMidYld'
        this.splitPricing = false;

        this.edits = new Map();      // isin -> new numeric value (in current priceMode units)
        this.originalByIsin = new Map(); // isin -> {px, yld}
        this.rowData = [];
    }

    // ---------------- Lifecycle ----------------

    async onInit() {
    }

    async afterMount() {
        this._cacheDom();
        this._bindEvents();
        this._buildGrid();
        this._subscribeToEngine();
        this._scheduleRebuild();
    }

    onResumeSubscriptions() {
        this._subscribeToEngine();
        this._scheduleRebuild();
    }

    async onActivate() {
        if (this.gridApi) {
            this.gridApi.sizeColumnsToFit();
        }
    }

    onDeactivate() {
    }

    async onCleanup() {
        for (const d of this._disposers) {
            try { if (typeof d === 'function') d(); } catch (_) {}
        }
        this._disposers.length = 0;

        if (this._rebuildRafId) {
            cancelAnimationFrame(this._rebuildRafId);
            this._rebuildRafId = 0;
        }
        if (this._copyResetTimer) {
            clearTimeout(this._copyResetTimer);
            this._copyResetTimer = null;
        }
        try { this.gridApi?.destroy(); } catch (_) {}
        this.gridApi = null;
        this._grid = null;

        if (this.widgetDiv) this.widgetDiv.innerHTML = '';
    }

    // ---------------- DOM ----------------

    onRender() {
        this.widgetDiv.innerHTML = `
        <div class="spot-body">
        <div class="spot-controls">
            <div class="spot-controls-top">
                <div class="spot-control-group spot-title-group">
                    <div class="spot-left">
                        <p>Spotting</p>
                    </div>
                </div>

                <div class="spot-control-group spot-mode-group">
                    <span class="spot-label-text"></span>
                    <div id="spot-mode" class="spot-mode-radios">
                        <label class="spot-mode-option">
                            <input type="radio" name="spotMode" value="benchmarkMidPx" checked />
                            <span>Price</span>
                        </label>
                        <label class="spot-mode-option">
                            <input type="radio" name="spotMode" value="benchmarkMidYld" />
                            <span>Yield</span>
                        </label>
                    </div>
                </div>

                <div class="spot-control-group spot-split-group">
                    <label class="spot-label">
                        <input id="spot-split-toggle" type="checkbox" class="toggle"/>
                        <span class="spot-label-text">Split Pricing</span>
                    </label>
                </div>

                <div class="spot-control-group spot-actions-group">
                    <button id="spot-copy-btn" class="spot-btn spot-btn-secondary" title="Copy grid to clipboard">
                        <span class="spot-copy-icon" aria-hidden="true">
                            <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"><path fill="currentColor" d="M16 1H4a2 2 0 0 0-2 2v14h2V3h12zm3 4H8a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2m0 16H8V7h11z"/></svg>
                        </span>
                        <span class="spot-check-icon" aria-hidden="true">
                            <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"><path fill="currentColor" d="M9 16.17L4.83 12l-1.42 1.41L9 19L21 7l-1.41-1.41z"/></svg>
                        </span>
                        <span class="spot-btn-label">COPY</span>
                    </button>
                    <button id="spot-clear-btn" class="spot-btn spot-btn-secondary">CLEAR</button>
                    <button id="spot-apply-btn" class="spot-btn spot-btn-primary" disabled>APPLY</button>
                </div>
            </div>
        </div>

        <div id="spot-grid" class="ag-theme-alpine spot-grid"></div>
        </div>
        `;
    }

    _cacheDom() {
        const root = this.widgetDiv;
        this.modeGroup = root.querySelector('#spot-mode');
        this.splitToggle = root.querySelector('#spot-split-toggle');
        this.copyBtn = root.querySelector('#spot-copy-btn');
        this.clearBtn = root.querySelector('#spot-clear-btn');
        this.applyBtn = root.querySelector('#spot-apply-btn');
        this.gridEl = root.querySelector(this.gridSelector);
    }

    _bindEvents() {
        const page = this.context.page;

        page.addEventListener(this.modeGroup, 'change', (e) => {
            if (!e.target || e.target.name !== 'spotMode') return;
            const next = e.target.value;
            if (next === this.priceMode) return;
            this.priceMode = next;
            this.edits.clear();
            if (this.priceMode !== 'benchmarkMidPx' && this.splitPricing) {
                this.splitPricing = false;
                if (this.splitToggle) this.splitToggle.checked = false;
            }
            this._applySplitToggleEnabled();
            this._rebuildColumns();
            this._scheduleRebuild();
            this._updateApplyEnabled();
        });

        page.addEventListener(this.splitToggle, 'change', (e) => {
            if (this.priceMode !== 'benchmarkMidPx') {
                e.target.checked = false;
                return;
            }
            this.splitPricing = !!e.target.checked;
            this._rebuildColumns();
            this._rebuild();
        });

        page.addEventListener(this.clearBtn, 'click', () => this._handleClear());
        page.addEventListener(this.applyBtn, 'click', () => this._handleApply());
        page.addEventListener(this.copyBtn, 'click', () => this._handleCopy());

        this._applySplitToggleEnabled();
    }

    _applySplitToggleEnabled() {
        if (!this.splitToggle) return;
        const enabled = this.priceMode === 'benchmarkMidPx';
        this.splitToggle.disabled = !enabled;
        this.splitToggle.closest('.spot-split-group')?.classList.toggle('spot-disabled', !enabled);
    }

    // ---------------- Engine subscriptions ----------------

    _subscribeToEngine() {
        const engine = this.engine;
        if (!engine) return;

        const unsub = engine.onColumnEpochChange(
            SRC_COLUMNS,
            () => this._scheduleRebuild(),
            {debounceMs: 50, trailing: true}
        );
        const wrapped = {unsubscribe: unsub};
        this.addSubscription(wrapped);
        this._disposers.push(unsub);
    }

    _scheduleRebuild() {
        if (this._rebuildRafId) return;
        this._rebuildRafId = requestAnimationFrame(() => {
            this._rebuildRafId = 0;
            this._rebuild();
        });
    }

    // ---------------- Aggregation ----------------

    _rebuild() {
        const engine = this.engine;
        if (!engine || !this.gridApi) return;

        const n = engine.numRows() | 0;
        if (!n) {
            this.rowData = [];
            this.originalByIsin = new Map();
            this._refreshGridData();
            this._updateApplyEnabled();
            return;
        }

        const getIsin = engine._getValueGetter('benchmarkIsin');
        const getName = engine._getValueGetter('benchmarkName');
        const getDesc = engine._getValueGetter('benchDescription');
        const getTerm = engine._getValueGetter('benchmarkTerm');
        const getPx = engine._getValueGetter('benchmarkMidPx');
        const getYld = engine._getValueGetter('benchmarkMidYld');
        const getRefresh = engine._getValueGetter('benchmarkRefreshTime');
        const getSide = engine._getValueGetter('hedgeDirection');
        const getSize = engine._getValueGetter('netHedgeSize');
        const getReal = engine._getValueGetter('isReal');

        const groups = new Map();

        for (let i = 0; i < n; i++) {
            const real = getReal(i);
            if (real === 0 || real === false) continue;

            const isinRaw = getIsin(i);
            const isin = (isinRaw == null || isinRaw === '') ? MISSING_KEY : isinRaw;

            let g = groups.get(isin);
            if (!g) {
                g = {
                    benchmarkIsin: isin,
                    benchmarkName: isin === MISSING_KEY ? MISSING_KEY : getName(i),
                    benchDescription: isin === MISSING_KEY ? '' : getDesc(i),
                    benchmarkTerm: isin === MISSING_KEY ? -Infinity : getTerm(i),
                    benchmarkMidPx: isin === MISSING_KEY ? null : getPx(i),
                    benchmarkMidYld: isin === MISSING_KEY ? null : getYld(i),
                    benchmarkRefreshTime: isin === MISSING_KEY ? null : getRefresh(i),
                    hedgeDirection: isin === MISSING_KEY ? null : getRefresh(i),
                    netHedgeSize: 0,
                };
                groups.set(isin, g);
            }
            const sz = getSize(i);
            if (sz != null && isFinite(sz)) g.netHedgeSize += Number(sz);
        }

        // Rebuild originals snapshot (applied state is whatever is currently in engine)
        const originals = new Map();
        const rows = [];
        for (const g of groups.values()) {
            originals.set(g.benchmarkIsin, {
                benchmarkMidPx: g.benchmarkMidPx,
                benchmarkMidYld: g.benchmarkMidYld,
            });

            const edited = this.edits.get(g.benchmarkIsin);
            const row = {...g, _isMissing: g.benchmarkIsin === MISSING_KEY};
            if (edited != null) {
                row[this.priceMode] = edited;
                row._edited = true;
            } else {
                row._edited = false;
            }
            if (this.splitPricing && this.priceMode === 'benchmarkMidPx') {
                const s = _splitPx(row.benchmarkMidPx);
                row._handle = s.handle;
                row._n32 = s.n32;
            }
            rows.push(row);
        }

        rows.sort((a, b) => {
            const at = a.benchmarkTerm, bt = b.benchmarkTerm;
            if (at == null && bt == null) return 0;
            if (at == null) return 1;
            if (bt == null) return -1;
            return bt - at;
        });

        this.rowData = rows;
        this.originalByIsin = originals;
        this._refreshGridData();
        this._updateApplyEnabled();
    }

    _refreshGridData() {
        if (!this.gridApi) return;
        this.gridApi.setGridOption('rowData', this.rowData);
        this.gridApi.refreshCells({force: true});
    }

    // ---------------- Grid setup ----------------

    _buildGrid() {
        const widget = this;

        const gridOptions = {
            rowData: [],
            columnDefs: this._buildColumnDefs(),
            suppressCellFocus: false,
            singleClickEdit: true,
            getRowId: (p) => String(p.data.benchmarkIsin ?? MISSING_KEY),
            defaultColDef: {
                hide: false,
                sortingOrder: ['desc', 'asc', null],
                menuTabs: ["filterMenuTab"],
                resizable: true,
                sortable: true,
                filter: true,
                editable: false,
                lockPinned: true,
                floatingFilter: false,
                autoHeight: false,
                wrapText: false,
                suppressMovable: false,
                suppressSizeToFit: true,
                suppressColumnsToolPanel: false,
                suppressSpanHeaderHeight: true,
                cellEditor: CustomCellEditor,
                // suppressKeyboardEvent: (params) => {
                //     return isArrowKey(params.event);
                // },
            },
            animateRows: false,
            suppressColumnVirtualisation: false,
            suppressChangeDetection: false,
            suppressAggFuncInHeader: true,
            suppressMaintainUnsortedOrder: true,
            suppressDragLeaveHidesColumns: true,
            enableRangeSelection: true,
            deltaSort: true,
            suppressAnimationFrame: false,
            suppressAutoSize: true,
            suppressTouch: true,
            suppressColumnMoveAnimation: true,
            suppressRowHoverHighlight: false,
            pagination: false,
            enterNavigatesVerticallyAfterEdit: true,
            suppressMenuHide: false,
            suppressScrollOnNewData: true,
            stopEditingWhenCellsLoseFocus: true,
            suppressClipboardPaste: true,
            suppressCutToClipboard: true,
            copyHeadersToClipboard: false,
            rowHeight: 28,
            headerHeight: 32,
            autoSizeStrategy: {
                type: 'fitCellContents'
            },
            onCellValueChanged: (ev) => widget._onCellValueChanged(ev),
            onGridReady: (p) => {
                widget.gridApi = p.api;
                widget._refreshGridData();
            },
        };

        this._grid = createGrid(this.gridEl, gridOptions);
        this.gridApi = this._grid;
        this.api = this._grid;
    }

    _rebuildColumns() {
        if (!this.gridApi) return;
        this.gridApi.setGridOption('columnDefs', this._buildColumnDefs());
    }

    _buildColumnDefs() {
        const widget = this;
        const priceHeader = this.priceMode === 'benchmarkMidPx' ? 'Mid Px' : 'Mid Yld';

        const defs = [
            {
                field: 'benchmarkName',
                headerName: 'Benchmark',
                minWidth: 110,
                flex: 1,
                cellClassRules: {
                    'spot-missing-cell': (p) => p?.data?._isMissing === true,
                },
            },
            {
                field: 'benchDescription',
                headerName: 'Description',
                minWidth: 180,
                flex: 2,
            },
            {
                field: 'benchmarkIsin',
                headerName: 'ISIN',
                minWidth: 120,
                flex: 1,
                cellClassRules: {
                    'spot-missing-cell': (p) => p?.data?._isMissing === true,
                },
            },
            {
                field: 'netHedgeSize',
                headerName: 'Net Hedge',
                type: 'numericColumn',
                cellDataType: 'number',
                filter: 'agNumberColumnFilter',
                minWidth: 110,
                flex: 1,
                valueGetter: (p) => {
                    const v = p?.data?.netHedgeSize;
                    return (v == null || !isFinite(v)) ? null : Number(v);
                },
                valueFormatter: (p) => _fmtSize(p.value),
                cellClass: 'spot-numeric',
            },
        ];

        if (this.splitPricing && this.priceMode === 'benchmarkMidPx') {
            defs.push({
                headerName: 'Handle',
                colId: 'spot-handle',
                field: '_handle',
                type: 'numericColumn',
                minWidth: 80,
                flex: 1,
                editable: (p) => !p.data._isMissing,
                valueParser: (p) => {
                    const v = parseFloat(p.newValue);
                    return isFinite(v) ? Math.trunc(v) : null;
                },
                valueFormatter: (p) => (p.value == null ? '' : String(p.value)),
                cellClass: (p) => {
                    const cls = ['spot-numeric', 'spot-editable'];
                    if (p?.data?._edited) cls.push('spot-edited');
                    return cls;
                },
            });
            defs.push({
                headerName: '32nds',
                colId: 'spot-32nds',
                field: '_n32',
                type: 'numericColumn',
                minWidth: 80,
                flex: 1,
                editable: (p) => !p.data._isMissing,
                valueParser: (p) => {
                    const v = parseFloat(p.newValue);
                    return isFinite(v) ? v : null;
                },
                valueFormatter: (p) => (p.value == null ? '' : Number(p.value).toLocaleString(undefined, {maximumFractionDigits: 3})),
                cellClass: (p) => {
                    const cls = ['spot-numeric', 'spot-editable'];
                    if (p?.data?._edited) cls.push('spot-edited');
                    return cls;
                },
            });
        } else {
            defs.push({
                headerName: priceHeader,
                colId: 'spot-price',
                field: this.priceMode,
                type: 'numericColumn',
                minWidth: 110,
                flex: 1,
                editable: (p) => !p.data._isMissing,
                valueParser: (p) => {
                    const v = parseFloat(p.newValue);
                    return isFinite(v) ? v : null;
                },
                valueFormatter: (p) => _fmtPx(p.value),
                cellClass: (p) => {
                    const cls = ['spot-numeric', 'spot-editable'];
                    if (p?.data?._edited) cls.push('spot-edited');
                    return cls;
                },
            });
        }

        defs.push({
            field: 'benchmarkRefreshTime',
            headerName: 'Refresh',
            minWidth: 140,
            flex: 1,
            valueFormatter: (p) => _fmtRefreshTime(p.value),
            cellClass: 'spot-refresh-cell',
        });

        // Hidden sort helper
        defs.push({
            field: 'benchmarkTerm',
            hide: true,
            sort: 'desc',
            sortIndex: 0,
        });

        return defs;
    }

    // ---------------- Edit flow ----------------

    _onCellValueChanged(ev) {
        const data = ev.data;
        if (!data || data._isMissing) return;
        const isin = data.benchmarkIsin;
        if (!isin) return;

        const col = ev.column?.getColId?.() || ev.colDef?.colId || ev.colDef?.field;

        let newPrice;
        if (this.splitPricing && this.priceMode === 'benchmarkMidPx' && (col === 'spot-handle' || col === 'spot-32nds')) {
            const h = (col === 'spot-handle') ? ev.newValue : data._handle;
            const t = (col === 'spot-32nds') ? ev.newValue : data._n32;
            newPrice = _joinPx(h, t);
        } else {
            newPrice = ev.newValue;
        }
        newPrice = _round3(newPrice);

        if (newPrice == null || !isFinite(newPrice)) {
            // Revert display
            const orig = this.originalByIsin.get(isin);
            if (orig) {
                data[this.priceMode] = orig[this.priceMode];
                if (this.splitPricing && this.priceMode === 'benchmarkMidPx') {
                    const s = _splitPx(orig.benchmarkMidPx);
                    data._handle = s.handle;
                    data._n32 = s.n32;
                }
            }
            this.edits.delete(isin);
            data._edited = false;
        } else {
            const orig = this.originalByIsin.get(isin);
            const baseline = orig ? orig[this.priceMode] : null;
            if (baseline != null && Math.abs(newPrice - baseline) < 1e-9) {
                this.edits.delete(isin);
                data[this.priceMode] = baseline;
                if (this.splitPricing && this.priceMode === 'benchmarkMidPx') {
                    const s = _splitPx(baseline);
                    data._handle = s.handle;
                    data._n32 = s.n32;
                }
                data._edited = false;
            } else {
                this.edits.set(isin, newPrice);
                data[this.priceMode] = newPrice;
                if (this.splitPricing && this.priceMode === 'benchmarkMidPx') {
                    const s = _splitPx(newPrice);
                    data._handle = s.handle;
                    data._n32 = s.n32;
                }
                data._edited = true;
            }
        }

        if (this.gridApi) {
            const node = this.gridApi.getRowNode(String(isin));
            if (node) node.setData(data);
        }
        this._updateApplyEnabled();
    }

    _updateApplyEnabled() {
        if (!this.applyBtn) return;
        const has = this.edits.size > 0;
        this.applyBtn.disabled = !has;
        this.applyBtn.classList.toggle('spot-dim', !has);
    }

    _handleClear() {
        if (!this.edits.size) return;
        this.edits.clear();
        this._rebuild();
    }

    async _handleApply() {
        if (!this.edits.size) return;
        const engine = this.engine;
        const adapter = this.adapter;
        if (!engine || !adapter) return;

        const id_col = engine._idProperty;
        const idx_col = adapter._idxProperty;
        const targetCol = this.priceMode;

        const n = engine.numRows() | 0;
        const getIsin = engine._getValueGetter('benchmarkIsin');
        const getReal = engine._getValueGetter('isReal');

        const updates = [];
        for (let i = 0; i < n; i++) {
            const real = getReal(i);
            if (real === 0 || real === false) continue;
            const isin = getIsin(i);
            if (isin == null || isin === '') continue;
            if (!this.edits.has(isin)) continue;

            const rid = engine.getRowIdByIndex(i);
            updates.push({
                [idx_col]: i,
                [id_col]: rid,
                [targetCol]: this.edits.get(isin),
            });
        }

        if (!updates.length) {
            this.edits.clear();
            this._updateApplyEnabled();
            return;
        }

        try {
            if (typeof adapter.applyServerUpdateTransaction === 'function') {
                await adapter.applyServerUpdateTransaction(updates, {emitAsEdit: true});
            } else {
                const rowIndices = updates.map(u => u[idx_col]);
                const values = updates.map(u => u[targetCol]);
                engine.setOverlayValuesByColumnBatch(targetCol, rowIndices, values, false, true);
            }
            this.context.page.toastManager?.().info?.(
                'Spotting',
                `Applied ${this.edits.size} level${this.edits.size !== 1 ? 's' : ''} across ${updates.length} bond${updates.length !== 1 ? 's' : ''}.`
            );
        } catch (err) {
            console.error('[spotting-apply]', err);
            this.context.page.toastManager?.().error?.('Spotting', 'Failed to apply edits.');
            return;
        }

        this.edits.clear();
        this._scheduleRebuild();
    }

    async _handleCopy() {
        if (!this.rowData.length) return;
        const priceField = this.priceMode;
        const priceHeader = this.priceMode === 'benchmarkMidPx' ? 'Mid Px' : 'Mid Yld';

        const headerOverride = {
            benchmarkName: 'Benchmark',
            benchDescription: 'Description',
            benchmarkIsin: 'ISIN',
            netHedgeSize: 'Net Hedge',
            hedgeDirection: 'Hedge Direction',
            [priceField]: priceHeader,
            benchmarkRefreshTime: 'Refresh',
        };

        const out = this.rowData.map(r => ({
            benchmarkName: r.benchmarkName ?? '',
            benchDescription: r.benchDescription ?? '',
            benchmarkIsin: r.benchmarkIsin ?? '',
            netHedgeSize: r.netHedgeSize ?? 0,
            hedgeDirection: r.hedgeDirection ?? '',
            [priceField]: r[priceField] ?? '',
            benchmarkRefreshTime: _fmtRefreshTime(r.benchmarkRefreshTime),
        }));

        try {
            await writeObjectToClipboard(out, {headerOverride});
            this._showCopySuccess();
        } catch (err) {
            console.error('[spotting-copy]', err);
            this.context.page.toastManager?.().error?.('Spotting', 'Copy failed.');
        }
    }

    _showCopySuccess() {
        if (!this.copyBtn) return;
        this.copyBtn.classList.add('spot-copied');
        if (this._copyResetTimer) clearTimeout(this._copyResetTimer);
        this._copyResetTimer = setTimeout(() => {
            this.copyBtn?.classList.remove('spot-copied');
            this._copyResetTimer = null;
        }, 1500);
    }
}
