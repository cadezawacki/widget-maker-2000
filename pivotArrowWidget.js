// pt/js/widgets/pivotWidget.js

import {BaseWidget} from './baseWidget.js';
import {combineLatest, mergeAll} from 'rxjs';
import {debounceTime, filter, map} from 'rxjs/operators';
import '../../css/pivot.css';
import * as utils from '@stdlib/utils';
import {pivotColumns} from "@/pt/js/grids/portfolio/portfolioColumns.js";
import {writeObjectToClipboard, getVenueCopyColumns, mapVenueFromRfq} from '@/utils/clipboardHelpers.js';
import {ArrowAgPivotAdapter} from '@/grids/js/arrow/arrowPivotEngine.js';
import {asArray, wait} from "@/utils/helpers.js";
import {ArrowAgGridAdapter} from "@/grids/js/arrow/arrowEngine.js";
import {ACTION_MAP} from "@/global/actionMap.js";
import interact from "interactjs";

function _escHtml(v) {
    if (v == null) return '';
    const s = typeof v === 'number' ? v.toLocaleString(undefined, {maximumFractionDigits: 4}) : String(v);
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}


function wavgFromObjects(rows, value, weight) {
    let weightedSum = 0
    let weightSum = 0
    for (let i = 0, n = rows.length; i < n; i++) {
        const row = rows[i]
        const w = row[weight]
        weightedSum += row[value] * w
        weightSum += w
    }
    return weightSum !== 0 ? weightedSum / weightSum : NaN
}

function groupWavgFromObjects(rows, keyProp, valueProp, weightProp) {
    const metrics = normalizeMetrics(valueProp, weightProp)
    const m = metrics.length

    const weightsMode = classifyWeights(weightProp, m)

    const acc = new Map() // key -> { sumVW: Float64Array(m), sumW: Float64Array(m) }

    const keyIsArray = Array.isArray(keyProp)
    const keyProps = keyIsArray ? keyProp : null
    const keyPropsLen = keyIsArray ? keyProps.length : 0

    for (let r = 0, n = rows.length; r < n; r++) {
        const row = rows[r]

        let key
        if (keyIsArray) {
            key = ''
            for (let k = 0; k < keyPropsLen; k++) {
                if (k) key += '_'
                key += row[keyProps[k]]
            }
        } else {
            key = row[keyProp]
        }

        let state = acc.get(key)
        if (state === undefined) {
            state = { sumVW: new Float64Array(m), sumW: new Float64Array(m) }
            acc.set(key, state)
        }

        const sumVW = state.sumVW
        const sumW = state.sumW

        switch (weightsMode.type) {
            case 0: { // scalar numeric constant
                const wConst = weightsMode.wConst
                if (wConst === 0 || wConst == null) break
                for (let j = 0; j < m; j++) {
                    const v = row[metrics[j].prop]
                    if (v == null) continue
                    sumVW[j] += v * wConst
                    sumW[j] += wConst
                }
                break
            }

            case 1: { // scalar weight property name
                const w = row[weightsMode.wProp]
                if (w === 0 || w == null) break
                for (let j = 0; j < m; j++) {
                    const v = row[metrics[j].prop]
                    if (v == null) continue
                    sumVW[j] += v * w
                    sumW[j] += w
                }
                break
            }

            case 2: { // per-metric numeric constants
                const wArr = weightsMode.wArr
                for (let j = 0; j < m; j++) {
                    const w = wArr[j]
                    if (w === 0 || w == null) continue
                    const v = row[metrics[j].prop]
                    if (v == null) continue
                    sumVW[j] += v * w
                    sumW[j] += w
                }
                break
            }

            case 3: { // per-metric weight property names
                const wPropArr = weightsMode.wPropArr
                for (let j = 0; j < m; j++) {
                    const w = row[wPropArr[j]]
                    if (w === 0 || w == null) continue
                    const v = row[metrics[j].prop]
                    if (v == null) continue
                    sumVW[j] += v * w
                    sumW[j] += w
                }
                break
            }
        }
    }

    const out = Object.create(null)
    for (const [key, state] of acc) {
        const res = Object.create(null)
        const sumVW = state.sumVW
        const sumW = state.sumW
        for (let j = 0; j < m; j++) {
            const w = sumW[j]
            res[metrics[j].as] = w !== 0 ? (sumVW[j] / w) : NaN
        }
        out[key] = res
    }
    return out
}

function normalizeMetrics(valueProp, weightProp) {
    const valueArr = Array.isArray(valueProp) ? valueProp : [valueProp]

    // Allow explicit {prop, as} entries
    const metrics = new Array(valueArr.length)
    for (let i = 0; i < valueArr.length; i++) {
        const v = valueArr[i]
        if (v && typeof v === 'object') {
            const prop = v.prop
            const as = v.as != null ? v.as : prop
            metrics[i] = { prop, as }
        } else {
            metrics[i] = { prop: v, as: v }
        }
    }

    // Ensure unique output names; if duplicates, suffix using per-metric weight name if available
    const seen = new Map()
    const weightSuffixes = getWeightSuffixes(weightProp, metrics.length)

    for (let i = 0; i < metrics.length; i++) {
        let name = metrics[i].as
        const count = seen.get(name)
        if (count === undefined) {
            seen.set(name, 1)
            continue
        }

        // Duplicate: create deterministic suffix
        const suffix = weightSuffixes[i] != null ? String(weightSuffixes[i]) : String(count)
        name = name + '__' + suffix

        // If still colliding, keep bumping with numeric postfix
        let bump = 2
        while (seen.has(name)) {
            name = metrics[i].as + '__' + suffix + '__' + bump
            bump++
        }
        metrics[i].as = name

        seen.set(metrics[i].as, 1)
        seen.set(metrics[i].as.split('__')[0], count + 1) // keep original base count moving
    }

    return metrics
}

function getWeightSuffixes(weightProp, m) {
    // Only meaningful when weightProp is an array of strings (per-metric prop names)
    if (Array.isArray(weightProp) && weightProp.length === m && typeof weightProp[0] === 'string') {
        for (let i = 1; i < m; i++) {
            if (typeof weightProp[i] !== 'string') return null
        }
        return weightProp
    }
    return null
}

function classifyWeights(weightProp, m) {
    // type:
    // 0 -> scalar numeric const
    // 1 -> scalar prop name
    // 2 -> per-metric numeric consts
    // 3 -> per-metric prop names

    if (Array.isArray(weightProp)) {
        if (weightProp.length !== m) {
            throw new RangeError(`weightProp array length (${weightProp.length}) must match valueProp length (${m})`)
        }
        if (m === 0) return { type: 2, wArr: weightProp }

        const first = weightProp[0]
        const isStringArray = typeof first === 'string'
        const isNumberArray = typeof first === 'number'

        if (!isStringArray && !isNumberArray) {
            throw new TypeError('weightProp array must contain either strings (property names) or numbers (constants)')
        }

        for (let i = 1; i < m; i++) {
            const t = typeof weightProp[i]
            if (isStringArray) {
                if (t !== 'string') throw new TypeError('weightProp array must be all strings or all numbers (no mixing)')
            } else {
                if (t !== 'number') throw new TypeError('weightProp array must be all numbers or all strings (no mixing)')
            }
        }

        return isStringArray
            ? { type: 3, wPropArr: weightProp }
            : { type: 2, wArr: weightProp }
    }

    if (typeof weightProp === 'number') {
        return { type: 0, wConst: weightProp }
    }

    return { type: 1, wProp: weightProp }
}

//context, widgetId, manager, feederId, selector, config = {}
export class PivotWidget extends BaseWidget {
    constructor(context, widgetId, adapter, selector, config = {}) {
        super(context, widgetId, adapter, selector, config);

        // Debug
        this.tableId = config.tableId || "pivotWidget-table";
        this.pivotSelector = config.pivotSelector || '#pivot-grid-container';
        this.ptPivot = null;

        this.currentGroups = [];
        this.required_groups = [];
        this._applyRafId = 0;
        this._applyTries = 0;
        this._current_active_presets = new Set();
        this._em = this.context.page.emitter || this.context.emitter;

        // Subscription disposers drained in onCleanup
        this._disposers = [];
        // In-flight guard for applyAllBuckets
        this._applyInFlight = false;
        // RAF id for coalesced alignSkewButton
        this._alignRafId = 0;
        // Tracked timeout for onPresetLoaded
        this._presetTimeoutId = 0;

        this._first_activation = false;
        // In-flight guard for pivot lock toggle
        this._lockToggleInFlight = false;

    }

    // --- Widget Lifecycle ---

    async onInit() {

        const required_groups = ['userSide', 'QT'];
        this.required_groups = required_groups;
        const required_aggs = [
            {isPriced: {func: 'PERCENT_OF_ROW_COUNT', name: '%'}},
            {refSkew: {func: 'wavg', name: 'Skew', weight: 'grossSize'}},
            {grossSize: {func: 'sum', name: 'Gross'}},
            {refSkewPx: {func: 'wavg', name: 'refSkewPx', weight: 'grossSize', DROP_NULLS: false, FILL_NULL: 0, ZERO_AS_NULL: false}},
            {refSkewSpd: {func: 'wavg', name: 'refSkewSpd', weight: 'grossSize', DROP_NULLS: false, FILL_NULL: 0, ZERO_AS_NULL: false}},
            {skewScore: {func: 'wavg', name: 'skewScore', weight: 'grossSize', DROP_NULLS: true, ZERO_AS_NULL: true}},

            {current_skew: {func: 'wavg', name: 'current_skew', weight: 'grossSize', headerName: 'Current', DROP_NULLS: false, FILL_NULL: 0, ZERO_AS_NULL: false}},
            {input_skew: {func: 'wavg', name: 'input_skew', weight: 'grossSize', headerName: '∆'}},
            {proposed_skew: {func: 'wavg', name: 'proposed_skew', weight: 'grossSize', headerName: 'Prop'}},
            {isLocked: {func: 'mean', name: 'isLocked', headerName: '🔒', FILL_NULL: 0, ZERO_AS_NULL: false}},
        ];

        // SVG icons for lock states (sized for grid cells, colored via currentColor)
        const _lockSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"><path fill="currentColor" d="M6 22q-.825 0-1.412-.587T4 20V10q0-.825.588-1.412T6 8h1V6q0-2.075 1.463-3.537T12 1t3.538 1.463T17 6v2h1q.825 0 1.413.588T20 10v10q0 .825-.587 1.413T18 22zm7.413-5.587Q14 15.825 14 15t-.587-1.412T12 13t-1.412.588T10 15t.588 1.413T12 17t1.413-.587M9 8h6V6q0-1.25-.875-2.125T12 3t-2.125.875T9 6z"/></svg>`;
        const _semiSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"><path fill="currentColor" d="M13.413 16.413Q14 15.825 14 15t-.587-1.412T12 13t-1.412.588T10 15t.588 1.413T12 17t1.413-.587M6 22q-.825 0-1.412-.587T4 20V10q0-.825.588-1.412T6 8h7V6q0-2.075 1.463-3.537T18 1q1.875 0 3.263 1.213T22.925 5.2q.05.325-.225.563T22 6t-.7-.175t-.4-.575q-.275-.95-1.062-1.6T18 3q-1.25 0-2.125.875T15 6v2h3q.825 0 1.413.588T20 10v10q0 .825-.587 1.413T18 22z"/></svg>`;
        const _unlockSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"><path fill="currentColor" d="M13.413 16.413Q14 15.825 14 15t-.587-1.412T12 13t-1.412.588T10 15t.588 1.413T12 17t1.413-.587M6 22q-.825 0-1.412-.587T4 20V10q0-.825.588-1.412T6 8h7V6q0-2.075 1.463-3.537T18 1q1.875 0 3.263 1.213T22.925 5.2q.05.325-.225.563T22 6t-.7-.175t-.4-.575q-.275-.95-1.062-1.6T18 3q-1.25 0-2.125.875T15 6v2h3q.825 0 1.413.588T20 10v10q0 .825-.587 1.413T18 22z"/></svg>`;

        // Build lock column def for pivot isLocked toggle
        const lockColDef = {
            field: 'isLocked',
            headerName: '🔒',
            headerClass: ["text-center", 'small-header'],
            width: 50,
            maxWidth: 60,
            minWidth: 40,
            suppressMenu: true,
            sortable: false,
            cellClass: ["center-cell"],
            cellStyle: params => {
                const v = params.value;
                let c;
                if (v == null || v === 0) c = 'var(--gray-500, #6b7280)';
                else if (v === 1) c = 'var(--red-500, #ef4444)';
                else c = 'var(--amber-500, #f59e0b)';
                return {
                    cursor: 'pointer',
                    textAlign: 'center',
                    color: c,
                    fontWeight: 600,
                    userSelect: 'none',
                };
            },
            cellRenderer: params => {
                const v = params.value;
                if (v === 1) return `<span title="Fully locked - click to unlock">${_lockSvg}</span>`;
                if (v == null || v === 0) return `<span title="Unlocked - click to lock">${_unlockSvg}</span>`;
                return `<span title="Partially locked - click to lock all">${_semiSvg}</span>`;
            },
            onCellClicked: params => {
                this._handlePivotLockToggle(params);
            },
        };

        const allCustomDefs = [...(pivotColumns || []), lockColDef];

        this.ptPivot = new ArrowAgPivotAdapter(this.context, this.adapter, {
            requiredGroups: required_groups,
            requiredAggregations: required_aggs,
            customDefs: allCustomDefs
        });

        this.ptPivot.mount(this.pivotSelector);

        // Store SVG strings for reuse in context menu
        this._lockSvg = _lockSvg;
        this._semiSvg = _semiSvg;
        this._unlockSvg = _unlockSvg;

        // Set up context menu on pivot grid
        this._setupPivotContextMenu();

        this.currentGroups = [];
        this._initControls();
        this._cacheDom();
        await this._setupReactions();
        this._setupHotkeys();
        this._setupResizer();
        this.setupTooltips();
        this._em.emitSync('pivot-initialized');

    }

    _setupPivotContextMenu() {
        const pivot = this.ptPivot;
        if (!pivot?.api) return;
        const widget = this;
        pivot.api.setGridOption('getContextMenuItems', (params) => {
            const api = params.api;
            const ranges = api.getCellRanges();
            // Collect all visible row nodes in selection range
            const selectedNodes = [];
            if (ranges && ranges.length > 0) {
                for (const rng of ranges) {
                    const sr = Math.min(rng.startRow.rowIndex, rng.endRow.rowIndex);
                    const er = Math.max(rng.startRow.rowIndex, rng.endRow.rowIndex);
                    for (let i = sr; i <= er; i++) {
                        const node = api.getDisplayedRowAtIndex(i);
                        if (node?.data) selectedNodes.push(node);
                    }
                }
            } else if (params.node?.data) {
                selectedNodes.push(params.node);
            }
            if (selectedNodes.length === 0) return ['copy', 'copyWithHeaders', 'export'];

            // Determine lock state across all selected pivot rows
            const lockValues = selectedNodes.map(n => n.data.isLocked);
            const allLocked = lockValues.every(v => v === 1);
            const allUnlocked = lockValues.every(v => v == null || v === 0);

            const lockIcon = `<span style="color:var(--red-500,#ef4444)">${widget._lockSvg}</span>`;
            const unlockIcon = `<span style="color:var(--gray-500,#6b7280)">${widget._unlockSvg}</span>`;
            const semiIcon = `<span style="color:var(--amber-500,#f59e0b)">${widget._semiSvg}</span>`;

            const items = [];
            if (allUnlocked) {
                items.push({
                    name: 'Lock Rows',
                    icon: lockIcon,
                    action: () => widget._bulkLockFromContextMenu(selectedNodes, 1),
                });
            } else if (allLocked) {
                items.push({
                    name: 'Unlock Rows',
                    icon: unlockIcon,
                    action: () => widget._bulkLockFromContextMenu(selectedNodes, 0),
                });
            } else {
                items.push({
                    name: 'Toggle Locks',
                    icon: semiIcon,
                    action: () => {
                        // Per-row toggle: locked rows unlock, unlocked rows lock
                        for (const node of selectedNodes) {
                            const newVal = node.data.isLocked === 1 ? 0 : 1;
                            widget._handlePivotLockToggle({node, value: node.data.isLocked, _forceValue: newVal});
                        }
                    },
                });
                items.push({
                    name: 'Lock All',
                    icon: lockIcon,
                    action: () => widget._bulkLockFromContextMenu(selectedNodes, 1),
                });
                items.push({
                    name: 'Unlock All',
                    icon: unlockIcon,
                    action: () => widget._bulkLockFromContextMenu(selectedNodes, 0),
                });
            }
            items.push('separator');
            items.push('copy', 'copyWithHeaders', 'export');
            return items;
        });
    }

    async _bulkLockFromContextMenu(nodes, lockValue) {
        if (this._lockToggleInFlight) return;
        this._lockToggleInFlight = true;
        try {
            const engine = this.engine;
            const adapter = this.adapter;
            const id_col = engine._idProperty;
            const idx_col = adapter._idxProperty;
            const groupBy = this.ptPivot.pivotConfig?.groupBy || [];
            const updates = [];

            for (const node of nodes) {
                const pivotData = node.data || {};
                const groupFilters = {};
                for (const col of groupBy) {
                    if (pivotData[col] !== undefined && pivotData[col] !== null) {
                        groupFilters[col] = pivotData[col];
                    }
                }
                for (const col of this.required_groups) {
                    if (pivotData[col] !== undefined && pivotData[col] !== null) {
                        groupFilters[col] = pivotData[col];
                    }
                }
                const allCols = [...new Set([...Object.keys(groupFilters), 'isLocked', id_col])];
                const allRows = engine.getAllRows({columns: allCols});
                for (const row of allRows) {
                    let match = true;
                    for (const [col, val] of Object.entries(groupFilters)) {
                        if (String(row[col] ?? '') !== String(val ?? '')) { match = false; break; }
                    }
                    if (!match) continue;
                    const rid = row[id_col];
                    const ri = engine.getRowIndexById(rid);
                    if (ri == null) continue;
                    updates.push({[idx_col]: ri, [id_col]: rid, isLocked: lockValue});
                }
            }

            if (updates.length > 0) {
                await adapter.applyServerUpdateTransaction(updates, {emitAsEdit: true});
                const toast = this.context.page.toastManager();
                const action = lockValue === 1 ? 'Locked' : 'Unlocked';
                toast.info('Lock Toggle', `${action} ${updates.length} bond${updates.length !== 1 ? 's' : ''}.`);
            }
        } catch (err) {
            console.error('[pivot-lock-context]', err);
        } finally {
            this._lockToggleInFlight = false;
        }
    }

    /**
     * Handles clicking the isLocked cell on a pivot row.
     * Determines which bonds belong to that pivot group and toggles their lock state.
     * If all bonds are locked (mean === 1), unlocks all. Otherwise locks all.
     */
    async _handlePivotLockToggle(params) {
        if (this._lockToggleInFlight) return;
        this._lockToggleInFlight = true;

        try {
            const node = params.node;
            if (!node) return;

            const pivotData = node.data || {};
            const groupBy = this.ptPivot.pivotConfig?.groupBy || [];
            const engine = this.engine;
            const adapter = this.adapter;
            const id_col = engine._idProperty;
            const idx_col = adapter._idxProperty;

            // Determine the group filter criteria from the pivot row data
            const groupFilters = {};
            for (const col of groupBy) {
                if (pivotData[col] !== undefined && pivotData[col] !== null) {
                    groupFilters[col] = pivotData[col];
                }
            }
            // Also include the required groups (userSide, QT) which are always present
            for (const col of this.required_groups) {
                if (pivotData[col] !== undefined && pivotData[col] !== null) {
                    groupFilters[col] = pivotData[col];
                }
            }

            // Get all rows from the source engine that match the group filter
            const allCols = [...new Set([...Object.keys(groupFilters), 'isLocked', id_col])];
            const allRows = engine.getAllRows({columns: allCols});

            // Filter to only rows matching all group column values
            const matchingRows = allRows.filter(row => {
                for (const [col, val] of Object.entries(groupFilters)) {
                    if (String(row[col] ?? '') !== String(val ?? '')) return false;
                }
                return true;
            });

            if (matchingRows.length === 0) return;

            // Determine new lock value (support forced value from context menu)
            const newLockValue = params._forceValue != null
                ? params._forceValue
                : (pivotData.isLocked === 1 ? 0 : 1);

            // Build update payloads
            const updates = [];
            for (const row of matchingRows) {
                const rid = row[id_col];
                const ri = engine.getRowIndexById(rid);
                if (ri == null) continue;
                updates.push({[idx_col]: ri, [id_col]: rid, isLocked: newLockValue});
            }

            if (updates.length > 0) {
                await adapter.applyServerUpdateTransaction(updates, {emitAsEdit: true});
                const toast = this.context.page.toastManager();
                const action = newLockValue === 1 ? 'Locked' : 'Unlocked';
                toast.info('Lock Toggle', `${action} ${updates.length} bond${updates.length !== 1 ? 's' : ''}.`);
            }
        } catch (err) {
            console.error('[pivot-lock-toggle]', err);
        } finally {
            this._lockToggleInFlight = false;
        }
    }

    flipLock(force = null) {
        let v;
        if (force != null) {
            v = force
        } else {
            v = !this.context.page.page$.get('proceedsLocked');
        }
        if (this.tt) {
            if (v) {
                this.tt.enable();
                this.tt.flash();
            } else {
                this.tt.stopFlash();
                this.tt.disable();
            }
        }

        this.context.page.page$.set('proceedsLocked', v);
        return v
    }

    // removed solver code to reduce length (if you see missing dom elements thats OK)

    _cacheDom() {
        this.filterToggle = document.getElementById('filter-pivot-by-grid');
        this.colorizeToggle = document.getElementById('pivot-colorize-toggle');
        this.bulkSkewMode = document.getElementById('pivot-bulkskew-toggle');
        this.lockTotalsToggle = document.getElementById('pivot-locktotals-toggle');
        this.gridDiv = document.querySelector(this.pivotSelector);
        this.bulkSkewClear = document.getElementById('pivot-bulk-skew-clear');
        this.bulkSkewApply = document.getElementById('pivot-bulk-skew-apply');
        this.venueCopy = document.getElementById('venue-copy-btn');
        this.venueDropdown = document.querySelector('#venue-caret');
        this.venueOptions = this.venueDropdown ? this.venueDropdown.querySelectorAll('.quote-types') : [];
        this.widgetDom = document.getElementById('pivotWidget');
        this.bulkBar = document.querySelector('.pt-left');
        this.controls = document.querySelector('.pivot-controls');
        this.presetButtons = document.getElementById("pivot-quick-presets");
        this.contentRow = document.getElementById('content-row');
        this.refSection = document.querySelector('.upper-ref-market-section');
        this.topBar = document.getElementById('pivot-topbar');
        this.pivotWeight = document.getElementById('pivot-weight-group');
        this.weightNotional = document.getElementById('weightNotional');
        this.weightDv01 = document.getElementById('weightDv01');
        this.weightCount = document.getElementById('weightCount');
        this.topApply = document.getElementById('pivot-top-apply');
        this.topLock = document.getElementById('pivot-top-lock');
        this.lockIcon = document.querySelector('.icon-wrap');
        this.topBtnWrapper = document.getElementById('pivot-top-btn-wrapper')
        this.topDiscard = document.getElementById('pivot-top-discard');

        // Skew mode elements
        this.skewModeGroup = document.getElementById('pivot-skew-mode');
        this.pctMarketWrap = document.getElementById('pivot-pct-market-wrap');
        this.pctMarketSelect = document.getElementById('pivot-pct-market');
    }

    _updatePendingBar(count = 0) {
        const n = Number(count) | 0;
        const has = n > 0;
        if (this.topApply) this.topApply.disabled = !has;
        // if (this.topDiscard) this.topDiscard.disabled = !has;

        if (this.topApply) {
            if (has) {
                this.topApply.textContent = `Apply ${n} Pending Skew${n > 1 ? 's' : ''}`;
                if (this.topDiscard) this.topDiscard.textContent = 'Discard';
            } else {
                this.topApply.textContent = `No Pending Skews`
                if (this.topDiscard) this.topDiscard.textContent = 'Clear';
            }
        }
    }

    currentWidth() {
        return this.ptPivot.element.getBoundingClientRect().width
    }

    maxWidth() {
        return this.contentRow.getBoundingClientRect().width - (this.controls.getBoundingClientRect().width + this.refSection.getBoundingClientRect().width)
    }

    proposeWidth() {
        if (!this.ptPivot) return
        let headerWidth = Array.from(this.ptPivot.element.querySelectorAll('.ag-header-cell')).reduce((acc, dom) => {
            acc += (dom.getBoundingClientRect().width ?? 0)
            return acc;
        }, 0);
        if (!headerWidth) return;
        headerWidth += 30 // sidebar + small buffer
        const maxWidth = this.maxWidth();
        return Math.max(400, Math.min(headerWidth, maxWidth));
    }

    async alignWidthIfNeeded() {
        const c = this.currentWidth()
        const p = this.proposeWidth();
        if (c && p && (p > c)) this.ptPivot.setWidth(p);
    }

    alignWidth() {
        const w = this.proposeWidth();
        if (w != null) this.ptPivot.setWidth(w);
    }

    alignWidthOnTap() {
        const c = this.currentWidth()
        const p = this.proposeWidth();
        const m = this.maxWidth();
        if (Math.abs(c - p) <= 50) {
            this.ptPivot.setWidth(m);
        } else {
            this.ptPivot.setWidth(p);
        }
    }

    _setupResizer() {
        const target = document.querySelector(".pivot-controls");
        const grid = this.ptPivot.element;
        const tbl = this.ptPivot;
        this.interactable = interact(target).resizable({
            edges: {top: false, left: true, bottom: false, right: true},
            listeners: {
                move: function (event) {
                    let base = event.client.x - grid.getBoundingClientRect().left;
                    tbl.setWidth(base)
                }
            }
        });

        // Expand pivot grid to fill available width when the container resizes
        const widget = this;
        let resizeRafId = 0;
        this._resizeObserver = new ResizeObserver(() => {
            if (resizeRafId) return;
            resizeRafId = requestAnimationFrame(() => {
                resizeRafId = 0;
                const maxW = widget.maxWidth();
                if (maxW > 0) tbl.setWidth(maxW);
            });
        });
        if (this.contentRow) this._resizeObserver.observe(this.contentRow);
    }


    /* ------------------------ GroupBy persistence helpers ------------------------ */
    _groupByStorageKey() {
        const k = (this.context?.portfolio_key || this.context?.url_context || 'default').toUpperCase();
        return `pt:pivotGroupBy:${k}`;
    }

    _readGroupByCache() {
        try {
            const raw = localStorage.getItem(this._groupByStorageKey());
            const v = raw ? JSON.parse(raw) : [];
            return Array.isArray(v) ? v : [];
        } catch {
            return [];
        }
    }

    _writeGroupByCache(values) {
        try {
            const dedup = Array.from(new Set((values || []).filter(Boolean)));
            localStorage.setItem(this._groupByStorageKey(), JSON.stringify(dedup));
        } catch {
        }
    }

    _validGroupFields() {
        // Mirrors your existing getAllValidGroupDefs filter.  [oai_citation:0‡Pivot.txt](sediment://file_00000000215061f6be6fe4f880c65325)
        const defs = this.ptPivot.getAllValidGroupDefs().filter(cd => !(cd?.context?.suppressColumnMenu && cd?.context?.suppressColumnMenu.includes('pivotWidget-table')));
        const fields = new Set(defs.map(cd => cd.field));
        return {defs, fields};
    }

    // async _applyGroupByFromCache() {
    //     const pivot = this.ptPivot;    //     if (!pivot?.api) return false;    //    //     const { fields } = this._validGroupFields();    //     const saved = this._readGroupByCache().filter(v => fields.has(v));    //     if (!saved.length) return true; // nothing to do, but grid is ready    //    //     const MAX_GROUPS = 4;    //     const next = saved.slice(0, MAX_GROUPS);    //    //     const curSet = new Set(this.currentGroups || []);    //     const nextSet = new Set(next);    //     const removed = Array.from(curSet).filter(x => !nextSet.has(x));    //     const added = Array.from(nextSet).filter(x => !curSet.has(x));    //    //     if (added.length || removed.length) {    //         await pivot.addRemovePivotGroups({ added, removed });    //         this.currentGroups = next;    //     }    //     if (this.groupBySelect?.addSelections) this.groupBySelect.addSelections(next);    //     return true;    // }
    // _scheduleApplyGroupByFromCache() {    //     // Bounded RAF retry in case API becomes ready a tick later.    //     const MAX_TRIES = 60; // ~1s worst-case    //     const tick = async () => {    //         this._applyTries++;    //         const ok = await this._applyGroupByFromCache();    //         if (ok || this._applyTries >= MAX_TRIES) { this._applyRafId = 0; return; }    //         this._applyRafId = requestAnimationFrame(tick);    //     };    //     this._applyRafId = requestAnimationFrame(tick);    // }
    async _setupReactions() {
        const pivot = this.ptPivot;
        const page$ = this.context.page.page$;
        const widget = this;

        this._disposers.push(this.engine.onColumnEvent('columnResized', () => widget._scheduleAlignSkewButton()));
        const isInitialized = pivot.grid$.pick('pivotInitialized');

        if (isInitialized && pivot.api) {
            widget.api = pivot.api;
            await this.onPresetLoaded()
        } else {
            this._disposers.push(this._em.once('pivot-initialized', async () => {
                widget.api = pivot.api;
                await this.onPresetLoaded()
            }));
        }

        this._disposers.push(this._em.once('pivot-initialized', async () => {
            this.setBulkBars(this.page.page$.get('bulkSkewMode'))
        }));

        this._disposers.push(this._em.on(ArrowAgGridAdapter.COLUMNS_EVENT, () => {
            widget._scheduleAlignSkewButton()
        }));

        // this.context.page.addEventListener(this.filterToggle, 'input', (v) => {
        //     page$.set('linkedPivotFilters', v?.target?.checked ?? false)        // }, {passive: true});
        // Link toggles
        this.context.page.linkStoreToInput(this.filterToggle, 'linkedPivotFilters', page$, {persist:true, storageKey:'linkedPivotFilters'});
        this.context.page.linkStoreToInput(this.lockTotalsToggle, 'lockPivotTotals', page$, {persist: true, storageKey: 'lockPivotTotals'});
        this.context.page.linkStoreToInput(this.colorizeToggle, 'colorizePivot', page$, {persist: true, storageKey: 'colorizePivot'});

        // React to changes
        this._disposers.push(this.context.page.page$.onValueChanged('linkedPivotFilters', (link) => pivot.setRespectSourceFilters(link)));
        this._disposers.push(this.context.page.page$.onValueChanged('lockPivotTotals', (ev) => pivot.setLockGridTotals(ev)));

        // Skew mode: outright vs percent
        this._setupSkewModeControls(pivot);

        // initialize adapter with current persisted values
        pivot.setRespectSourceFilters(!!page$.get('linkedPivotFilters'));
        pivot.setLockGridTotals(!!page$.get('lockPivotTotals'));
        this.context.page.addEventListener(this.bulkSkewClear, 'click', (v) => {
            if (pivot._bucketState.size === 0) return pivot.input_clear_on_each_row();
            pivot.resetAllBuckets();
        });

        this.context.page.addEventListener(this.bulkSkewApply, 'click', async () => {
            if (this._applyInFlight) return;
            this._applyInFlight = true;
            try {
                await pivot.applyAllBuckets();
            } finally {
                this._applyInFlight = false;
            }
        });

        if (this.venueOptions) {
            const pg = this.context.page
            this.venueOptions.forEach(item => {
                this.context.page.addEventListener(item, 'mousedown', async (e) => {
                    const venue = e.currentTarget.getAttribute('data-venue');
                    if (venue) {
                        await widget.writeVenueToClipboard(venue);
                        await pg.send_full_push("Submission", true);
                    } else {
                        widget.context.page.toastManager().error('Submission Copy', `Failed to write to clipboard.`);
                    }
                });
            });
        }

        if (this.topApply) {
            this.context.page.addEventListener(this.topApply, 'click', async () => {
                if (this._applyInFlight) return;
                this._applyInFlight = true;
                try {
                    await pivot.applyAllBuckets();
                    this._updatePendingBar(0);
                } finally {
                    this._applyInFlight = false;
                }
            });
        }
        if (this.topDiscard) {
            this.context.page.addEventListener(this.topDiscard, 'click', () => {
                if (pivot._bucketState.size === 0) {
                    return pivot.input_clear_on_each_row()
                }
                pivot.resetAllBuckets();
                this._updatePendingBar(0);
            });
        }

        this.context.page.addEventListener(this.presetButtons, 'click', async (e) => {
            const nearestBtn = e.target.closest('.pill-button');
            if (nearestBtn) {
                const groupsThen = [...(pivot.pivotConfig.groupBy || [])];
                const group = nearestBtn.getAttribute('data-column');

                if (group === 'CLEAR') {
                    widget.clearActiveQuickPreset();
                    await pivot.updateGroups([], {hard: true});
                } else {
                    if (!e.ctrlKey) {
                        widget.clearActiveQuickPreset();
                        this._current_active_presets.add(nearestBtn);
                        nearestBtn.classList.toggle('active', true);
                        await pivot.updateGroups([group], {hard: true});
                    } else {
                        if (this._current_active_presets.has(nearestBtn)) {
                            this._current_active_presets.delete(nearestBtn);
                            await pivot.removeGroups([group], {hard: true});
                            nearestBtn.classList.toggle('active', false);
                        } else {
                            this._current_active_presets.add(nearestBtn);
                            await pivot.addGroups([group], {hard: true});
                            nearestBtn.classList.toggle('active', true);
                        }
                    }
                }
                const groupsNow = pivot.pivotConfig.groupBy;
                const groupsSet = new Set(groupsNow);
                const removals = groupsThen.filter(x => !groupsSet.has(x))
                pivot.api.setColumnsVisible(removals, false);
                pivot.api.setColumnsVisible(groupsNow, true);
            }
        });

        this.context.page.addEventListener(this.venueCopy, 'click', async () => {
            if (this._applyInFlight) return;
            this._applyInFlight = true;
            try {
                const venue = widget.context.page.portfolioMeta$.get("venue") || 'ib';
                await widget.writeVenueToClipboard(venue);
                await widget.context.page.send_full_push("Submission", true);
            } finally {
                this._applyInFlight = false;
            }
        });

        const applyBtn = this.bulkSkewApply;
        this._disposers.push(this.ptPivot.onComputed(() => {
            applyBtn.classList.toggle('dirtySkews', pivot._bucketState.size > 0);
        }));

        const node_event = 'treeColumnChooser-pivot-NODE_TOGGLE';
        this._disposers.push(this._em.on(node_event, () => {
            widget.clearActiveQuickPreset()
        }))

        this._disposers.push(this.ptPivot.onBucketChange((info) => {
            widget._updatePendingBar(info?.size || 0);
        }));

        this._disposers.push(this.ptPivot.onGroupChange((info) => {
            widget.clearSpan();
            widget._scheduleAlignSkewButton();
        }));
        // initialize banner from current state
        widget._updatePendingBar(this.ptPivot.getPendingBucketCount ? this.ptPivot.getPendingBucketCount() : 0);

    }

    clearSpan() {
        this.ptPivot.resetAllBuckets();
        this._updatePendingBar(0);
    }

    clearActiveQuickPreset() {
        const a = this.presetButtons.querySelectorAll('.active');
        if (a) a.forEach(aa => aa.classList.toggle('active', false));
        this._current_active_presets.clear();
        this.clearSpan();
    }

    setBulkBars(v, realign=true) {
            const pivot = this.ptPivot;
            this.ptPivot.domTimeout = false;
            pivot?.api.setColumnsVisible(['current_skew', 'input_skew', 'proposed_skew'], v);
            pivot.api.setColumnsPinned(['current_skew', 'input_skew', 'proposed_skew'], 'left');
            pivot.api.moveColumns(['current_skew', 'input_skew', 'proposed_skew'], pivot.api.getColumnDefs().length - 3);

            pivot?.api.setColumnsVisible(['isLocked'], v);
            pivot.api.moveColumns(['isLocked'], pivot.api.getColumnDefs().length - 4);
            pivot.api.setColumnsPinned(['isLocked'], 'left');

            requestAnimationFrame(async ()=>{
                if (v && realign) await this.alignWidthIfNeeded();
                requestAnimationFrame(()=> {
                    this.topApply.classList.toggle('disabled', !v);
                    this.topDiscard.classList.toggle('disabled', !v);
                    this.topLock.classList.toggle('disabled', !v);
                    this._scheduleAlignSkewButton();
                    pivot.api.setColumnsPinned(['isLocked'], 'left');
                });
            });
        }

    async onPresetLoaded() {
        const page$ = this.context.page.page$;
        this.context.page.linkStoreToInput(this.bulkSkewMode, 'bulkSkewMode', page$, {persist: true, storageKey: 'bulkSkewMode', cb: this.setBulkBars.bind(this)});
        // this.alignWidth();
    }

    async writeVenueToClipboard(venue) {
        let trueVenue = this.context.page.portfolioMeta$.get("venue") || 'ib';
        if (trueVenue !== 'ib') {
            trueVenue = mapVenueFromRfq(trueVenue)
        }
        const clean_venue = mapVenueFromRfq(venue)
        const cols_needed = {...getVenueCopyColumns(clean_venue, false)};
        if (cols_needed) {
            if (this.context.page.page$.get('activeQuoteType') === 'client') {
                if ('newLevel' in cols_needed) {
                    cols_needed['newLevelDisplay'] = cols_needed.newLevel;
                    delete cols_needed.newLevel
                }
            }
            const fields = Object.keys(cols_needed);
            fields.push('isReal', 'assignedTrader');
            if (!fields.includes('grossSize')) {
                fields.push('grossSize')
            }

            const data = this.adapter.engine.getColumns(fields)
            const headers = {};

            Object.entries(cols_needed).forEach(([k, v]) => {
                if (!Array.isArray(v)) {
                    headers[k] = v;
                } else {
                    headers[k] = v[0];
                }
            });
            const reformat = data.filter(row => {
                return !(
                    (row.isReal == null) ||
                    (row.isReal === 0) ||
                    (row.grossSize == null) ||
                    (row.grossSize === 0) ||
                    (row.assignedTrader === "REMOVED")
                )
            }).map(row => {
                const clean = {};
                Object.entries(row).forEach(([k, v]) => {
                    if (!(k in cols_needed)) return;
                    if (!Array.isArray(cols_needed[k])) {
                        clean[k] = v;
                    } else {
                        clean[k] = cols_needed[k][1](v) || '';
                    }
                })
                return clean
            });
            const removedCount = data.length - reformat.length;
            await writeObjectToClipboard(reformat, {headerOverride: headers, addCommaToNumerics: clean_venue === 'ib'});

            const mismatchVenue = (clean_venue !== trueVenue) && (clean_venue !== "ib")
            let msg = `Copied data for: ${clean_venue.toUpperCase()}`;
            if (mismatchVenue) {
                msg = msg + `<br>Venue Expected: ${trueVenue.toUpperCase()}`
            }
            if (removedCount > 0) {
                msg = msg + `<br>**REMOVED ${removedCount} bonds**`
            }
            if (!mismatchVenue) {
                this.context.page.toastManager().success('Submission Copy', msg)
            } else {
                this.context.page.toastManager().warning('Submission Copy - Mismatched Venue', msg)
            }
        } else {
            this.context.page.toastManager().error('Submission Copy', `Failed to write to clipboard.`)
        }
    }

    _setupSkewModeControls(pivot) {
        if (!this.skewModeGroup) return;
        const widget = this;

        // Populate percent market select with available markets
        this._populatePctMarketOptions();

        // Radio change: outright vs percent
        this.context.page.addEventListener(this.skewModeGroup, 'change', (e) => {
            const mode = e.target.value;
            pivot.setSkewMode(mode);
            // Clear pending skews when switching mode to avoid confusion
            pivot.resetAllBuckets();
            widget._updatePendingBar(0);
        });

        // Market select for percent width source
        if (this.pctMarketSelect) {
            this.context.page.addEventListener(this.pctMarketSelect, 'change', (e) => {
                pivot.setSkewPctMarket(e.target.value || null);
            });
        }
    }

    _populatePctMarketOptions() {
        if (!this.pctMarketSelect) return;
        const page = this.context.page;
        const marketMap = page.marketDataMap;
        if (!marketMap || !marketMap.size) return;
        // Keep the default "(Ref Mkt)" option, then add available markets
        const frag = document.createDocumentFragment();
        const def = document.createElement('option');
        def.value = '';
        def.textContent = '(Ref Mkt)';
        frag.appendChild(def);
        for (const [key, meta] of marketMap.entries()) {
            const opt = document.createElement('option');
            opt.value = key;
            opt.textContent = meta?.abbr || meta?.label || key;
            frag.appendChild(opt);
        }
        this.pctMarketSelect.innerHTML = '';
        this.pctMarketSelect.appendChild(frag);
    }

    _setupHotkeys() {
        const page$ = this.context.page.page$;
        const widget = this;
        this.context.page.addEventListener(document, 'keydown', async (event) => {

            // Ctrl+Shift+f to toggle grid link
            if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'f') {
                event.preventDefault();
                if (this.filterToggle) {
                    const newValue = !this.filterToggle.checked;
                    this.filterToggle.checked = newValue;
                    page$.set('linkedPivotFilters', newValue)
                }
            } else if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'b') {
                event.preventDefault();
                if (this.bulkSkewMode) {
                    const newValue = !this.bulkSkewMode.checked;
                    this.bulkSkewMode.checked = newValue;
                    page$.set('bulkSkewMode', newValue)
                }
            }

            // Ctrl+shift+c to copy
            else if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'c') {
                event.preventDefault();
                event.stopPropagation();
                if (widget._applyInFlight) return;
                widget._applyInFlight = true;
                try {
                    const venue = widget.context.page.portfolioMeta$.get("venue") || 'ib';
                    await widget.writeVenueToClipboard(venue);
                    await widget.context.page.send_full_push("Submission", true);
                } finally {
                    widget._applyInFlight = false;
                }
            }

        });
    }

    async onActivate() {
        this.ptPivot._locked = false;
        this.ptPivot.isActive = true;
        await this._scheduleAlignSkewButton();
    }

    _scheduleAlignSkewButton() {
        if (this._alignRafId) return;
        this._alignRafId = requestAnimationFrame(async () => {
            this._alignRafId = 0;
            await this.alignSkewButton();
        });
    }

    async alignSkewButton() {

        if (!this.ptPivot?.api) return;
        const current_skew = this.ptPivot.api.getColumn('current_skew');
        const input_skew = this.ptPivot.api.getColumn('input_skew');
        const prop_skew = this.ptPivot.api.getColumn('proposed_skew');
        if (!current_skew || !input_skew || !prop_skew) return;

        const barrier = this.bulkBar.getBoundingClientRect().width || 0
        const cost = this.topBtnWrapper.getBoundingClientRect().width || 0;

        const max_skew = Math.max(0, (barrier || 0) - (cost || 0));

        const minLeft = Math.max(0, Math.min(current_skew.left, input_skew.left, prop_skew.left));
        this.topBtnWrapper.style.transform = `translateX(${Math.min(max_skew, minLeft - 10)}px)`;

        const w = current_skew.getActualWidth() + input_skew.getActualWidth() + prop_skew.getActualWidth();
        this.topBtnWrapper.style.width = `${w - 65}px`;

    }

    onResumeSubscriptions() {
        this.ptPivot.hardRefresh({force: true});
        if (!this._first_activation) {
            this._first_activation = true;
            this.setBulkBars(this.page.page$.get("bulkSkewMode"));
        }
    }

    onDeactivate() {
        // console.log('[PivotWidget] deactivated');
        // this.ptPivot._locked = true;        // this.ptPivot.isActive = false;
    }

    async onCleanup() {
        // Drain subscription disposers
        for (const dispose of this._disposers) {
            try {
                if (typeof dispose === 'function') dispose();
            } catch (_) {
            }
        }
        this._disposers.length = 0;

        if (this._alignRafId) {
            cancelAnimationFrame(this._alignRafId);
            this._alignRafId = 0;
        }
        if (this._presetTimeoutId) {
            clearTimeout(this._presetTimeoutId);
            this._presetTimeoutId = 0;
        }
        if (this._resizeObserver) {
            try { this._resizeObserver.disconnect(); } catch(_) {}
            this._resizeObserver = null;
        }

        try {
            this.api?.destroy();
        } catch (_) {
        }
        try {
            this.ptPivot.dispose();
        } catch (_) {
        }


        if (this.interactable) {
            try {
                this.interactable.unset();
            } catch (_) {
            }
            this.interactable = null;
        }
        if (this.tt) {
            try {
                this.context.page.tooltipManager()?.remove?.('proceed-lock-tooltip');
            } catch (_) {
            }
            this.tt = null;
        }
        try {
            if (this._applyRafId) cancelAnimationFrame(this._applyRafId);
            this._applyRafId = 0;
            this._applyTries = 0;
        } finally {
            if (this.dynamicController) this.dynamicController.abort();
        }
        if (this.widgetDom) {
            this.widgetDom.innerHTML = '';
        }
    }

    // --- UI & Controls ---

    onRender() {
        this.widgetDiv.innerHTML = `
        <div class="pivot-controls">
            <div class="pivot-controls-top">
                <div class="pivot-control-group">
                    <label>
                        <div class="piv-left">
                            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"><path fill="currentColor" d="M10 8V3h9q.825 0 1.413.588T21 5v3zM5 21q-.825 0-1.412-.587T3 19v-9h5v11zM3 8V5q0-.825.588-1.412T5 3h3v5zm9.85 11l.875.9q.275.275.275.688t-.3.712q-.275.275-.7.275t-.7-.275l-2.6-2.6q-.3-.3-.3-.7t.3-.7l2.6-2.6q.275-.275.688-.287t.712.287q.275.275.288.688t-.263.712l-.875.9H15q.825 0 1.413-.587T17 15v-2.2l-.9.9q-.275.275-.7.275t-.7-.275t-.275-.7t.275-.7l2.6-2.6q.3-.3.7-.3t.7.3l2.6 2.6q.275.275.275.7t-.275.7t-.7.275t-.7-.275l-.9-.9V15q0 1.65-1.175 2.825T15 19z"/></svg>
                            <p>Pivot Portfolio</p>
                        </div>
                        <div class="piv-right">
                            <button class="tooltip-left" id="pivotSettings" data-tooltip="Settings">
                                <lord-icon src="/assets/lottie/cog-hover-2.json" trigger="hover" class="current-color" stroke="currentColor" style="width: 16px; height: 16px;"></lord-icon>
                            </button>
                        </div>
                    </label>
                </div>
                <div id="pivot-quick-presets">
                    <div id="pivot-quick-main">
                        <div class="colored-pill indigo-pill pill-button btn" data-column="assignedTrader">Assigned</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="desigName">Desig</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="lastEditUser">LastEdit</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="isDnt">DNT</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="bvalSubAssetClass">Asset</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="hasPriced">Priced</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="liqScoreCombinedGroup">Liq</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="firmPositionBucket">Firm</div>
                        <div class="colored-pill indigo-pill pill-button btn" data-column="algoPositionBucket">Algo</div>
                    </div>
                    <div id="pivot-quick-clear">
                        <div class="colored-pill amber-pill pill-button btn" data-column="CLEAR">Clear Groups</div>
                        <div class="pivot-quick-groups-title">Quick Groups</div>
                    </div>
                </div>
            </div>
            <div class="pivot-controls-middle">
                <div class="pivot-control-group pivot-skew-mode-group">
                    <span class="label-text skew-mode-label">Skew Mode</span>
                    <div id="pivot-skew-mode" class="pivot-skew-mode-radios">
                        <label class="skew-mode-option">
                            <input type="radio" name="pivotSkewMode" value="outright" checked />
                            <span>Outright</span>
                        </label>
                        <label class="skew-mode-option">
                            <input type="radio" name="pivotSkewMode" value="percent" />
                            <span>%&nbsp;Width</span>
                            <div id="pivot-pct-market-wrap" class="pivot-pct-market-wrap">
                                <select id="pivot-pct-market" class="pivot-pct-market-select">
                                    <option value="">(Ref Mkt)</option>
                                </select>
                            </div>
                        </label>
                    </div>
                </div>
            </div>
            <div class="pivot-controls-bottom">
                <div class="pivot-control-group middle-wrapper">
                                        
                    <div class="pivot-control-group pivot-toggle">
                        <label class="label">
                            <span class="label-text">Use grid filters?</span>
                            <input id="filter-pivot-by-grid" type="checkbox" class="checkbox"/>
                        </label>
                        <label class="label">
                            <span class="label-text">Colorize</span>
                            <input id="pivot-colorize-toggle" type="checkbox" class="checkbox"/>
                        </label>
                        <label class="label">
                            <span class="label-text tooltip tooltip-top" data-tooltip="Grand totals ignore filters; top rows still follow them.">Guard grid totals?</span>
                            <input id="pivot-locktotals-toggle" type="checkbox" class="checkbox"/>
                        </label>
                    </div>
                </div>
                <div class="pivot-control-group bottom-wrapper">
                    <div class="pivot-control-group bulkskew-wrapper">
                        <div class="pivot-control-group pivot-toggle pivot-switch" id="pivot-bulkskew-toggle-outer">
                            <label class="label">
                                <input id="pivot-bulkskew-toggle" type="checkbox" class="toggle"/>
                                <span class="label-text">Bulk Skew Mode</span>
                            </label>
                        </div>
                        <div id="bulk-skew-apply-btn" style="display:none;">
                            <button class="btn btn-primary btn-sm" id="pivot-bulk-skew-apply">APPLY</button>
                            <button class="btn btn-primary btn-sm" id="pivot-bulk-skew-clear">CLEAR</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div id="pivot-grid-container" class="ag-theme-alpine fill-parent">
            <div id="pivot-topbar" class="pivot-topbar">
                <div class="pt-left">
                    <div id="pivot-top-btn-wrapper">
                        <button id="pivot-top-discard" class="btn btn-xs disabled">Discard</button>
                        <button id="pivot-top-apply" class="btn btn-xs disabled" disabled>Apply Pending Skews</button>
                        <button id="pivot-top-lock" class="btn btn-xs">
                             <span class="icon-wrap">
                                 <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 20 20"><path fill="currentColor" d="M8.693 2.058c.404-.058.77-.065 1.13.006c.28.057.529.157.765.282a1.42 1.42 0 0 0-.566 1.401l-.008-.004a1.5 1.5 0 0 0-.487-.207c-.14-.028-.322-.036-.612.005a1.25 1.25 0 0 0-.861.537c-.21.301-.359.75-.405 1.357c-.003.16-.028.647-.065 1.315H9.3a.75.75 0 0 1 0 1.5H7.498a506 506 0 0 1-.35 5.607v.005l-.01.12c-.091 1.115-.226 2.772-1.76 3.666l-.006.003c-.997.57-2.18.384-3.107-.08a.751.751 0 0 1 .67-1.341c.671.335 1.287.349 1.69.12c.807-.472.92-1.326 1.027-2.61c.05-.645.174-2.635.286-4.522l.058-.968H4.801a.75.75 0 0 1 0-1.5h1.28l.006-.093a169 169 0 0 0 .062-1.224v-.025L6.15 5.4l.001-.053c.056-.785.257-1.529.67-2.124A2.75 2.75 0 0 1 8.677 2.06zm1.264 7.101c.31-.132.635-.173.957-.1c.312.07.555.235.736.402c.277.256.505.622.657.867l.074.116a36 36 0 0 0 .76 1.354l1.985-1.985l.53.174a.6.6 0 0 1 .219.14c.07.06.11.14.14.22l.17.527l-2.147 2.15a46 46 0 0 1 1.02 1.798l.061.103c.053.09.102.171.158.258q.116.176.202.257q.043.04.064.05q.019.011.02.01h.006l.033-.006c.018-.01.026-.019.033-.025a1 1 0 0 0 .127-.176a.75.75 0 0 1 1.279.783c-.227.37-.448.601-.804.78a1.6 1.6 0 0 1-1.056.096a1.7 1.7 0 0 1-.733-.423c-.29-.275-.51-.65-.64-.872l-.05-.087l-.028-.05a38 38 0 0 0-.788-1.4l-2.662 2.66a.75.75 0 0 1-1.06-1.06l2.825-2.825a46 46 0 0 1-.947-1.671l-.087-.14c-.058-.091-.108-.172-.167-.259a1.6 1.6 0 0 0-.212-.262l-.042-.035l-.012.01l-.066.076l-.022.025l-.174.202a.75.75 0 1 1-1.133-.983l.164-.19l.022-.026a1.8 1.8 0 0 1 .415-.38l.032-.024a1 1 0 0 1 .14-.079M17.484 6a.3.3 0 0 1 .285.201l.25.766a1.58 1.58 0 0 0 .999.998l.765.248l.016.004a.302.302 0 0 1 0 .57l-.766.248a1.58 1.58 0 0 0-.999.998l-.249.766a.3.3 0 0 1-.46.145a.3.3 0 0 1-.11-.145l-.25-.766a1.58 1.58 0 0 0-.997-1.002l-.766-.248A.3.3 0 0 1 15 8.498a.3.3 0 0 1 .202-.285l.766-.248a1.58 1.58 0 0 0 .983-.998l.248-.766A.3.3 0 0 1 17.484 6m-3.006-6a.43.43 0 0 1 .4.282l.348 1.072a2.2 2.2 0 0 0 1.399 1.396l1.071.349l.022.005a.426.426 0 0 1 .205.643a.42.42 0 0 1-.205.154l-1.072.349a2.21 2.21 0 0 0-1.398 1.396l-.349 1.072a.424.424 0 0 1-.643.204l-.02-.015a.43.43 0 0 1-.136-.19l-.347-1.07a2.2 2.2 0 0 0-1.398-1.402l-1.073-.349a.424.424 0 0 1 0-.797l1.072-.349a2.21 2.21 0 0 0 1.377-1.396L14.08.282a.42.42 0 0 1 .4-.282"/></svg>
                            </span>
                           
                        </button>
                    </div>
                    <span id="pivot-pending-count" class="pending-indicator"></span>
                </div>
            </div>
        </div>`;
    }


    _initControls() {
        const {defs} = this._validGroupFields();
        const groupByOptions = defs.map(cd => ({
            label: cd?.context?.aggregationName ?? cd.headerName ?? cd.field,
            value: cd.field,
            group: cd.context?.customColumnGroup || 'General'
        }));

        const t2 = document.getElementById('portfolio-top-two-rows'); // z-index shim if present
        const pivot = this.ptPivot;
        const widget = this;

        const vc = document.getElementById('venue-caret');
        const gg = document.getElementById('portfolio-pricing-grid');
        if (vc && this.context?.page?.addEventListener) {
            this.context.page.addEventListener(vc, 'mousedown', () => {
                if (t2) t2.style.zIndex = "2";
                if (gg) gg.style.zIndex = "0";
            });
            const vcb = vc.querySelector('*');
            if (vcb) {
                this.context.page.addEventListener(vcb, 'blur', () => {
                    if (t2) t2.style.zIndex = "0";
                    if (gg) gg.style.zIndex = "auto";
                });
            }
        }
    }
}
