// pt/js/widgets/heatmapWidget.js

import { BaseWidget } from './baseWidget.js';
import '../../css/pivot.css';
import { ArrowAgPivotAdapter } from '@/grids/js/arrow/arrowPivotEngine.js';
import * as recursiveMerge from 'deepmerge';
import {heatmapColumns} from "@/pt/js/grids/portfolio/portfolioColumns.js";
import {asyncForEach} from 'modern-async';
import {std, mean, median} from 'mathjs';
import * as utils from '@stdlib/utils';
import {asArray, zipArray} from "@/utils/helpers.js";
import {coerceToNumber} from '@/utils/typeHelpers.js';

export class HeatmapWidget extends BaseWidget {
    constructor(context, widgetId, adapter, selector, config = {}) {
        super(context, widgetId, adapter, selector, config);

        this.tableId = config.tableId || 'heatmapWidget-table';
        this.heatmapSelector = config.heatmapSelector || '#heatmap-grid-container';
        window.ht = this;
        this.ptHeatmap = null;
        this.api = null;

        this.colorize = false;
        this.respectFilters = false;
        this.outliers = false;

        this.buckets = [
            "BVALSkew",
            "CBBTSkew",
            "MACPSkew",
            "IDCSkew",
            "MARKITSkew",
            "MARKSkew",
            "MLCRSkew",
            "ALLQSkew",
            "TRACESkew",
            "AMSkew",
            "STATSSkew",
            "refSkew",
            "BVAL_Level",
            "MACP_Level",
            "CBBT_Level",
            "AM_Level",
            "IDC_Level",
            "MARKIT_Level",
            "MARK_Level",
            "MLCR_Level",
            "TRACE_Level",
            "ALLQ_Level",
            "STATS_Level",
            "newLevelDisplay"
        ]

        this.domains = new Map();
        this.paletteSteps = 64;

        this._onModelUpdated = this._onModelUpdated.bind(this);
        this._onFirstDataRendered = this._onFirstDataRendered.bind(this);

    }

    async onInit() {
        const required_groups = ['QT', 'userSide', 'description'];
        this.required_groups = required_groups;
        const required_aggs = [
            { refSkew: { func: 'wavg', name: 'refSkew',  weight: 'grossSize' } },
        ];

        this.ptHeatmap = new ArrowAgPivotAdapter(this.context, this.adapter, {
            requiredGroups: required_groups,
            initialGroups: required_groups,
            suppressTree: true,
            customDefs: heatmapColumns,
            requiredAggregations: required_aggs,
            enableGrandTotal: true,
            refreshDebounceMs: 300,
        });

        this.ptHeatmap._buildPivotColumnDefs = this._buildPivotColumnDefs.bind(this);
        this.ptHeatmap._transformPivotResult = this._transformPivotResult.bind(this);
        this.ptHeatmap._color.paletteSize = this.paletteSteps;
        this.ptHeatmap._setupRowTintBuckets();
        this.ptHeatmap._ensurePalette()
        this.ptHeatmap.hardRefresh();

        this.ptHeatmap._locked = true;
        this.ptHeatmap.mount(this.heatmapSelector);

        this._renderControls();
        this._cacheDom();
        this._setupReactions();
        this._setupHotkeys();

        const widget = this;
        this.ptHeatmap.grid$.pick('pivotInitialized').onChanges(() => {
            this.api = this.ptHeatmap.api;
            this.api.setGridOption('onRowDoubleClicked', (params)=> {
                if (params?.rowPinned) return;
                if (!this.respectFilters) return;

                const d = params?.data?.description;
                if (d != null) {
                    widget.context.page.setQuickSearch(d);
                    widget.setFilterLink(true);
                }
            });
            if (!this.api) return;
            this.api.addEventListener('firstDataRendered', async (e) => await this._onFirstDataRendered(e));
            this.api.addEventListener('modelUpdated', async (e) => await this._onModelUpdated(e));
        });
    }

    async onActivate() {
        this.ptHeatmap._locked = false;
    }

    async onResumeSubscriptions() {
        this.ptHeatmap.hardRefresh({force:true})
    }

    async onDeactivate() {
        this.ptHeatmap._locked = true;
    }

    _buildPivotColumnDefs() {
        const heatmap = this;
        const fm  = this.ptHeatmap.source.filterManager;
        let srcDefs = this.ptHeatmap.source.getAllColumnDefs() || [];
        srcDefs = srcDefs.concat(asArray(this.ptHeatmap.customDefs));
        this.srcById = new Map();

        for (let i = 0; i < srcDefs.length; i++) {
            let d = srcDefs[i] || {};
            const id = d.colId || d.field;
            const o = this.ptHeatmap.opts.columnOverrides?.[id];
            if (o) {
                d = recursiveMerge.all([d, o]);
            }
            if (id) this.srcById.set(id, d);
        }

        this.ptHeatmap._rebuildSets();
        const adapter = this.ptHeatmap;
        const req = this.required_groups;

        const groupDefs = [];
        // const g = this.ptHeatmap.getAllValidGroupFields();
        const g = this.ptHeatmap.source.getAllFields();
        for (let i = 0; i < g.length; i++) {
            const id = g[i];
            const base = Object.assign({}, this.srcById.get(id) || { colId: id, field: id, headerName: id });
            base.colId = id;
            base.field = id;
            base.hide = !req.includes(id);
            base.sortable = true;
            base.pinned = 'left';
            base.context = base?.context || {};
            groupDefs.push(base);
        }

        const aggDefs = [];
        for (const { input, spec } of this.ptHeatmap._iterAggs()) {
            const outName = spec.name;
            const inName = spec.inField || spec.name;
            const base = this.srcById.get(outName) || this.srcById.get(inName) || {};
            let col = {
                ...base,
                editable: false,
                width: 75,
                colId: outName,
                field: outName,
                headerName: spec.headerName || outName,
                hide: true,
                sortable: true,
                headerClass: ["text-center", "small-header"],
                cellClass: ['center-cell'],
                cellStyle: (params) => {
                    return heatmap._heatCellStyle(params?.data?.__pid, params?.value);
                }
            };
            col.context = col?.context || {};
            col.context.DROP_NULLS = true;
            col.context.ZERO_AS_NULL = true;
            aggDefs.push(col);
        }
        return groupDefs.concat(aggDefs);
    }

    _transformPivotResult(rows, cols) {
        if (!cols || !rows || !cols.length || !rows.length) return []
        rows = rows.map(row => row.map(cell => {
            if (cell == null || cell === 0) return null
            return cell;
        }))
        return zipArray(rows, cols);
    }

    onCleanup() {
        try {
            this.api?.removeEventListener?.('firstDataRendered', this._onFirstDataRendered);
            this.api?.removeEventListener?.('modelUpdated', this._onModelUpdated);
        } catch (_) {}
        try {
            this.api?.destroy?.();
        } catch (_) {}
        if (this.widgetDiv) this.widgetDiv.innerHTML = '';
    }

    // --- UI ---

    onRender() {
        this.widgetDiv.innerHTML = `
            <div id="heatmap-grid-container" class="ag-theme-alpine fill-parent"></div>
            <div class="pivot-controls">
                <div class="pivot-controls-top">
                    <div class="pivot-control-group">
                        <label>
                            <div class="piv-left">
                                <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24"><path fill="currentColor" d="M3 5q0-.825.588-1.412T5 3h14q.825 0 1.413.588T21 5v4H3zm0 6h18v8q0 .825-.587 1.413T19 21H5q-.825 0-1.412-.587T3 19z"/></svg>
                                <p>Heatmap</p>
                            </div>
                            <div class="piv-right">
                                <svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"><path fill="currentColor" d="M12 17q.425 0 .713-.288T13 16v-4q0-.425-.288-.712T12 11t-.712.288T11 12v4q0 .425.288.713T12 17m0-8q.425 0 .713-.288T13 8t-.288-.712T12 7t-.712.288T11 8t.288.713T12 9m0 13q-2.075 0-3.9-.788t-3.175-2.137T2.788 15.9T2 12t.788-3.9t2.137-3.175T8.1 2.788T12 2t3.9.788t3.175 2.137T21.213 8.1T22 12t-.788 3.9t-2.137 3.175t-3.175 2.138T12 22m0-2q3.35 0 5.675-2.325T20 12t-2.325-5.675T12 4T6.325 6.325T4 12t2.325 5.675T12 20m0-8"/></svg>
                            </div>
                        </label>
                    </div>
                </div>                        
                <div class="pivot-controls-bottom">
                    <div class="pivot-control-group pivot-toggle pivot-switch" id="heatmap-mode-toggle-outer">
                        <label class="label">
                            <input id="heatmap-mode-toggle" type="checkbox" class="toggle"/>
                            <span class="label-text">Skew Mode</span>
                        </label>
                    </div>
                    <div class="pivot-control-group pivot-toggle">
                        <label class="label">
                            <span class="label-text">Use grid filters?</span>
                            <input id="heatmap-filter-by-grid" type="checkbox" class="checkbox"/>
                        </label>
                        <label class="label">
                            <span class="label-text">Flag Outliers?</span>
                            <input id="heatmap-outliers" type="checkbox" class="checkbox"/>
                        </label>
                    </div>
                </div>
            </div>`

    }

    _renderControls() {
        // no-op; onRender writes full UI
    }

    _cacheDom() {
        this.filterToggle = document.getElementById('heatmap-filter-by-grid');
        this.outlierToggle = document.getElementById('heatmap-outliers');
        // this.colorizeToggle = document.getElementById('heatmap-colorize-toggle');
        this.gridDiv = document.querySelector(this.heatmapSelector);
        this.modeToggle = document.querySelector('#heatmap-mode-toggle')
    }

    setFilterLink(v) {
        this.filterToggle.checked = v;
        this.respectFilters = v;
        this.context.page.page$.set('linkedPivotFilters', v);
        this.api?.refreshClientSideRowModel?.('filter');
    }

    setOutlier(v) {
        this.outlierToggle.checked = v;
        this.outliers = v;
        this.api?.redrawRows();
    }

    _setupReactions() {
        const widget = this;

        this.context.page.addEventListener(this.filterToggle, 'input', (e) => {
            const v = !!e?.target?.checked;
            widget.setFilterLink(v);
        }, { passive: true });

        this.context.page.addEventListener(this.outlierToggle, 'input', (e) => {
            const v = !!e?.target?.checked;
            widget.setOutlier(v);
        }, { passive: true });


        const adapter = this.ptHeatmap;
        this.context.page.addEventListener(this.modeToggle, 'change', (e) => {
            const v = !!e?.target?.checked;

            if (v) {
                adapter?.api.setColumnsVisible([
                    "description",
                    'BVALSkew',
                    "CBBTSkew",
                    "MACPSkew",
                    "MARKSkew",
                    "IDCSkew",
                    "MARKITSkew",
                    "MLCRSkew",
                    "ALLQSkew",
                    "TRACESkew",
                    "AMSkew",
                    "STATSSkew",
                ], true);
                adapter?.api.setColumnsVisible([
                    'newLevelDisplay',
                    'BVAL_Level',
                    "CBBT_Level",
                    "MACP_Level",
                    "MARK_Level",
                    "IDC_Level",
                    "MARKIT_Level",
                    "MLCR_Level",
                    "ALLQ_Level",
                    "TRACE_Level",
                    "AM_Level",
                    "STATS_Level",
                ], false);
                adapter?.api.setFilterModel({'refSkew':{
                        "filterType": "number",
                        "type": "notBlank"
                    }})
            } else {
                adapter?.api.setColumnsVisible([
                    'BVALSkew',
                    "CBBTSkew",
                    "MACPSkew",
                    "MARKSkew",
                    "IDCSkew",
                    "MARKITSkew",
                    "MLCRSkew",
                    "ALLQSkew",
                    "TRACESkew",
                    "AMSkew",
                    "STATSSkew",
                ], false);
                adapter?.api.setColumnsVisible([
                    "description",
                    'newLevelDisplay',
                    'BVAL_Level',
                    "CBBT_Level",
                    "MACP_Level",
                    "MARK_Level",
                    "IDC_Level",
                    "MARKIT_Level",
                    "MLCR_Level",
                    "ALLQ_Level",
                    "TRACE_Level",
                    "AM_Level",
                    "STATS_Level",
                ], true);
                adapter?.api.setFilterModel({});
            }

        }, { passive: true });

    }

    _setupHotkeys() {
        const page$ = this.context.page.page$;
        this.context.page.addEventListener(document, 'keydown', (event) => {
            if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === 'f') {
                event.preventDefault();
                if (!this.filterToggle) return;
                const newValue = !this.filterToggle.checked;
                this.filterToggle.checked = newValue;
                this.respectFilters = newValue;
                page$.set('linkedPivotFilters', newValue);
                this.api?.refreshClientSideRowModel?.('filter');
            }
        });
    }

    // --- Heatmap logic ---

    async _onFirstDataRendered(e) {
        e?.api.setColumnsVisible([
            'newLevelDisplay',
            'BVAL_Level',
            "CBBT_Level",
            "MACP_Level",
            "MARK_Level",
            "IDC_Level",
            "MARKIT_Level",
            "MLCR_Level",
            "ALLQ_Level",
            "TRACE_Level",
            "AM_Level",
            "STATS_Level",
            "description"
        ], true);
        await this._recomputeDomains();
        this._refreshHeatStyling();
    }

    async _onModelUpdated() {
        await this._recomputeDomains();
        this._refreshHeatStyling();
    }

    async _recomputeDomains() {
        if (!this.api || !this.ptHeatmap?._pivotResult) return;

        const cols = ['__pid'].concat(this.buckets);
        const rows = this.ptHeatmap._pivotResult
            .map(row => Object.values(utils.pick(row, cols))
                .filter(cell => (cell != null) && (cell !== 0)));

        this.domains.clear();
        const domains = this.domains;
        const default_stats = { max: NaN, min: NaN, std: NaN, mean: NaN, };
        await asyncForEach(rows, async (_row) => {
            const idx = _row.shift();
            const row = _row.map(cell => coerceToNumber(cell, {onNaN:null}));
            if (row.length <= 1) {domains.set(idx, default_stats); return}

            try {
                // IQR
                const n = median(row);
                const s = std(row);
                const q1 = n-1.5*s;
                const q3 = n+1.5*s;

                const win = row.filter(v => ((q1 <= v) && (v <= q3)));
                if (win.length <= 1) {domains.set(idx, default_stats); return}

                domains.set(idx, {
                    max: Math.max(...win),
                    min: Math.min(...win),
                    std: std(win),
                    mean: mean(win),
                    q1: q1,
                    q3: q3,
                    _median: n,
                    _std: s,
                    win: win
                });

            } catch {
                domains.set(idx, default_stats);
            }

        });
        this.ptHeatmap._setupRowTintBuckets();
    }

    _refreshHeatStyling() {
        try {
            this.api?.refreshCells?.({ force: true });
        } catch (_) {}
    }

    _heatCellStyle(rowId, value) {
        value = coerceToNumber(value, {onNaN: null});
        if (!value) return {background: 'inherit'};

        const d = this.domains.get(rowId);
        if (!d) return {background: 'inherit'};
        const min = (d.mean - 1.5*d?.std) ?? d.min;
        const max = (d.mean + 1.5*d?.std) ?? d.max;
        if (min === max) return {background: 'inherit'};
        return this._colorFor(value, min, max);
    }

    _colorFor(value, min, max) {
        if (max <= min) return {'background-color': 'inherit'};
        const steps = this.paletteSteps;

        let r = {};
        if (this.outliers && (value > max)) r = {...r, ...this.ptHeatmap._color.posOutlier};
        if (this.outliers && (value < min)) r = {...r, ...this.ptHeatmap._color.negOutlier};

        const t = (value - min) / (max - min);
        const idx = Math.max(0, Math.min(steps - 1, Math.round(t * (steps - 1))));
        const palette = this.ptHeatmap._color.palette;
        if (palette && palette[idx]) return {...palette[idx], ...r};
        return {'background-color': 'inherit'};
    }

    _buildPalette(min, max, steps) {
        if (max <= min) return [];
        const out = new Array(steps);
        for (let i = 0; i < steps; i++) {
            const t = i / (steps - 1);
            // 0 -> red (0deg), 1 -> green (120deg)
            out[i] = this._hslToRgbCss(120 * t, 0.65, 0.52);
        }
        return out;
    }

    _hslToRgbCss(h, s, l) {
        const c = (1 - Math.abs(2 * l - 1)) * s;
        const hp = (h % 360) / 60;
        const x = c * (1 - Math.abs((hp % 2) - 1));
        let r = 0, g = 0, b = 0;

        if (hp >= 0 && hp < 1) { r = c; g = x; b = 0; }
        else if (hp < 2) { r = x; g = c; b = 0; }
        else if (hp < 3) { r = 0; g = c; b = x; }
        else if (hp < 4) { r = 0; g = x; b = c; }
        else if (hp < 5) { r = x; g = 0; b = c; }
        else { r = c; g = 0; b = x; }

        const m = l - c / 2;
        const R = Math.round((r + m) * 255);
        const G = Math.round((g + m) * 255);
        const B = Math.round((b + m) * 255);
        return `rgb(${R}, ${G}, ${B})`;
    }

    _idealTextColor(rgbCss) {
        // rgb(r, g, b)
        const m = rgbCss.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
        if (!m) return '#111';
        const r = parseInt(m[1], 10) / 255;
        const g = parseInt(m[2], 10) / 255;
        const b = parseInt(m[3], 10) / 255;

        const toLin = (u) => (u <= 0.03928 ? u / 12.92 : Math.pow((u + 0.055) / 1.055, 2.4));
        const R = toLin(r), G = toLin(g), B = toLin(b);
        const L = 0.2126 * R + 0.7152 * G + 0.0722 * B;
        return L > 0.55 ? '#111' : '#fff';
    }
}
