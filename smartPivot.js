const S = String.fromCharCode(31)
const N = n => n === null || n === undefined || Number.isNaN(n)
const Z = (v, z) => N(v) || (z && v === 0)
const T = (v, z) => !Z(v, z) && !!v
const K = v => v === null || v === undefined ? '' : (typeof v === 'string' ? v : v + '')

function H(){
    let h=2166136261>>>0;
    return function(x){
        for(let i=0;i<x.length;i++){
            h^=x.charCodeAt(i)
            h=(h+((h<<1)>>>0)+((h<<4)>>>0)+((h<<7)>>>0)+((h<<8)>>>0)+((h<<24)>>>0))>>>0
        }
        return h.toString(36)
    }
}

function MHeap(cmp) {
    const a = [];
    return {
        push(v) {
            a.push(v);
            let i = a.length - 1;
            while (i > 0) {
                let p = (i - 1) >> 1;
                if (cmp(a[i], a[p]) < 0) {
                    let t = a[i];
                    a[i] = a[p];
                    a[p] = t;
                    i = p
                } else break
            }
        }, pop() {
            if (a.length === 0) return;
            let r = a[0], v = a.pop();
            if (a.length) {
                a[0] = v;
                let i = 0;
                for (; ;) {
                    let l = 2 * i + 1, rn = l + 1, m = i;
                    if (l < a.length && cmp(a[l], a[m]) < 0) m = l;
                    if (rn < a.length && cmp(a[rn], a[m]) < 0) m = rn;
                    if (m !== i) {
                        let t = a[i];
                        a[i] = a[m];
                        a[m] = t;
                        i = m
                    } else break
                }
            }
            return r
        }, peek() {
            return a[0]
        }, size() {
            return a.length
        }
    }
}

function MedianAgg() {
    const lo = MHeap((x, y) => y - x), hi = MHeap((x, y) => x - y);
    return {
        add(x) {
            if (hi.size() === 0 || x >= hi.peek()) {
                hi.push(x)
            } else {
                lo.push(x)
            }
            if (hi.size() > lo.size() + 1) {
                lo.push(hi.pop())
            } else if (lo.size() > hi.size()) {
                hi.push(lo.pop())
            }
        }, val() {
            if (hi.size() === 0 && lo.size() === 0) return null;
            if (hi.size() === lo.size()) return (hi.peek() + lo.peek()) / 2;
            return hi.peek()
        }
    }
}

function toCols(input, needed) {
    if (!input) return {}
    if (Array.isArray(input)) {
        const rows = input;
        const out = {};
        for (const k of needed) out[k] = new Array(rows.length);
        for (let i = 0; i < rows.length; i++) {
            const r = rows[i];
            for (const k of needed) out[k][i] = r[k]
        }
        return out
    }
    if (input && typeof input === 'object') {
        if (typeof input.getChild === 'function' && typeof input.numRows === 'number') {
            const out = {};
            for (const k of needed) {
                const col = input.getChild(k) || input.getColumn?.(k) || input.get?.(k);
                if (col && col.toArray) {
                    out[k] = col.toArray()
                } else if (col && typeof col.get === 'function') {
                    const n = input.numRows;
                    const arr = new Array(n);
                    for (let i = 0; i < n; i++) arr[i] = col.get(i);
                    out[k] = arr
                } else if (input.data?.childData) {
                    const idx = input.schema?.fields?.findIndex(f => f.name === k);
                    if (idx >= 0) {
                        const vec = input.getChildAt(idx);
                        out[k] = vec?.toArray?.() || []
                    }
                } else {
                    out[k] = []
                }
            }
            return out
        }
        const keys = Object.keys(input);
        if (keys.length && Array.isArray(input[keys[0]])) {
            const out = {};
            for (const k of needed) out[k] = input[k] || [];
            return out
        }
    }
    return {}
}

function buildSpec(aggs) {
    const out = []
    for (let i = 0; i < aggs.length; i++) {
        const o = aggs[i];
        const k = Object.keys(o)[0];
        const def = o[k]
        if (typeof def === 'string') {
            out.push({
                col: k,
                func: def.toUpperCase(),
                name: k + '_' + def.toUpperCase(),
                dropNulls: false,
                zeroAsNull: false,
                fillNull: undefined,
                abs: false,
                weight: undefined,
                cache: false
            })
        } else {
            out.push({
                col: k,
                func: (def.func || 'SUM').toUpperCase(),
                name: def.name || k + '_' + (def.func || 'SUM').toUpperCase(),
                dropNulls: def.DROP_NULLS !== undefined ? def.DROP_NULLS : true,
                zeroAsNull:  def.ZERO_AS_NULL !== undefined ? def.ZERO_AS_NULL : true,
                fillNull: def.FILL_NULL !== undefined ? def.FILL_NULL : def.fillNull,
                abs: !!def.ABS || !!def.abs,
                weight: def.weight,
                cache: !!def.CACHE || !!def.cache,
                otherCol: def.otherCol || def.OTHER_COL
            })
        }
    }
    return out
}

function preTotals(cols, rows, spec) {
    const res = new Array(spec.length)
    for (let j = 0; j < spec.length; j++) {
        const s = {...spec[j]};
        const ab = s.abs;
        if (s.func === 'PERCENT_OF_COL_COUNT') {
            let t = 0;
            const a = cols[s.col] || [];
            const z = s.zeroAsNull;
            for (let i = 0; i < rows; i++) {
                const v = a[i];
                if (T(v, z)) t++
            }
            res[j] = {count: t}
        } else if (s.func === 'PERCENT_OF_COL_SUM') {
            let t = 0;
            const a = cols[s.col] || [];
            const z = s.zeroAsNull;
            const d = s.dropNulls;
            const f = s.fillNull;
            for (let i = 0; i < rows; i++) {
                let v = Number(a[i]);
                if (N(v) && f !== undefined) v = f;
                if (d && (Z(v, z))) continue;
                let n = ab ? Math.abs(+v) : +v;
                if (!Number.isFinite(n)) continue;
                t += n
            }
            res[j] = {sum: t}
        } else if (s.func === 'PERCENT_OF_OTHER_COL_COUNT') {
            let t = 0;
            const a = cols[s.otherCol] || [];
            const z = s.zeroAsNull;
            for (let i = 0; i < rows; i++) {
                const v = a[i];
                if (T(v, z)) t++
            }
            res[j] = {count: t}
        } else if (s.func === 'PERCENT_OF_OTHER_COL_SUM') {
            let t = 0;
            const a = cols[s.otherCol] || [];
            const z = s.zeroAsNull;
            const d = s.dropNulls;
            const f = s.fillNull;
            const ab = s.abs;
            for (let i = 0; i < rows; i++) {
                let v = Number(a[i]);
                if (N(v) && f !== undefined) v = f;
                if (d && (Z(v, z))) continue;
                let n = ab ? Math.abs(+v) : +v;
                if (!Number.isFinite(n)) continue;
                t += n
            }
            res[j] = {sum: t}
        } else res[j] = null
    }
    return res
}

function initState(ss) {
    const s = {...ss};
    if (s.func === 'SUM') return {sum: 0}
    if (s.func === 'MEAN') return {sum: 0, c: 0}
    if (s.func === 'MEDIAN') return {m: MedianAgg()}
    if (s.func === 'COUNT') return {c: 0}
    if (s.func === 'PERCENT_OF_ROW_COUNT') return {t: 0}
    if (s.func === 'PERCENT_OF_COL_COUNT') return {t: 0}
    if (s.func === 'PERCENT_OF_COL_SUM') return {sum: 0}
    if (s.func === 'PERCENT_OF_COL_WEIGHT') return {sp: 0, sw: 0}
    if (s.func === 'PERCENT_OF_OTHER_COL_COUNT') return {t: 0}
    if (s.func === 'PERCENT_OF_OTHER_COL_SUM') return {sum: 0}
    if (s.func === 'COUNT_DISTINCT') return {set: new Set()}
    if (s.func === 'COUNT_NON_NULL') return {c: 0}
    if (s.func === 'MIN') return {v: undefined}
    if (s.func === 'MAX') return {v: undefined}
    if (s.func === 'FIRST') return {v: undefined, seen: false}
    if (s.func === 'LAST') return {v: undefined}
    if (s.func === 'WAVG') return {sp: 0, sw: 0}
    return {}
}

function updateState(st, s, vv, ww, opts) {
    const z = s.zeroAsNull;
    const d = s.dropNulls;
    const f = s.fillNull
    const ab = s.abs;

    if (s.func === 'COUNT') {
        st.c++;
        return
    }
    let v = ab ? Math.abs(Number(vv)) : Number(vv);
    if (N(v) && f !== undefined) v = f
    if (d && (Z(v, z) || (s.weight !== undefined && Z(ww, z)))) return

    if (s.func === 'SUM') {
        const n = +v;
        if (Number.isFinite(n)) st.sum += n;
        return
    }
    if (s.func === 'MEAN') {
        const n = +v;
        if (Number.isFinite(n)) {
            st.sum += n;
            st.c++
        }
        return
    }
    if (s.func === 'MEDIAN') {
        const n = +v;
        if (Number.isFinite(n)) st.m.add(n);
        return
    }
    if (s.func === 'PERCENT_OF_ROW_COUNT') {
        if (T(v, z)) st.t++;
        return
    }
    if (s.func === 'PERCENT_OF_COL_COUNT') {
        if (T(v, z)) st.t++;
        return
    }
    if (s.func === 'PERCENT_OF_COL_SUM') {
        const n = +v;
        if (Number.isFinite(n)) st.sum += n;
        return
    }
    if (s.func === 'PERCENT_OF_OTHER_COL_COUNT') {
        if (T(v, z)) st.t++;
        return
    }
    if (s.func === 'PERCENT_OF_OTHER_COL_SUM') {
        const n = +v;
        if (Number.isFinite(n)) st.sum += n;
        return
    }
    if (s.func === 'COUNT_DISTINCT') {
        st.set.add(v === undefined ? null : v);
        return
    }
    if (s.func === 'COUNT_NON_NULL') {
        if (T(v, z)) st.c++;
        return
    }
    if (s.func === 'MIN') {
        if (!Z(v, z)) {
            if (st.v === undefined || v < st.v) st.v = v
        }
        return
    }
    if (s.func === 'MAX') {
        if (!Z(v, z)) {
            if (st.v === undefined || v > st.v) st.v = v
        }
        return
    }
    if (s.func === 'FIRST') {
        if (!st.seen && !Z(v, z)) {
            st.v = v;
            st.seen = true
        }
        return
    }
    if (s.func === 'LAST') {
        if (!Z(v, z)) {
            st.v = v
        }
        return
    }
    let w = Number(ww);
    if (s.func === 'WAVG') {
        let nv = +v, nw = +w;
        if (!d) {
            nv = nv || (f ?? 0);
            nw = nw || 0;
        }
        if (Number.isFinite(nv) && Number.isFinite(nw)) {
            st.sp += nv * nw;
            st.sw += nw
        }
        return
    }
    if (s.func === 'PERCENT_OF_COL_WEIGHT') {
        // console.log(v, w, opts)
        let nv = +w, nw = +v;
        if (!d) {
            nv = nv || (f ?? 0);
            nw = nw || 0;
        }
        if (Number.isFinite(nv) && Number.isFinite(nw)) {
            st.sp += nv * nw;
            st.sw += nw
        }
    }
}

function finalizeState(st, s, ctx) {
    if (s.func === 'SUM') return st.sum
    if (s.func === 'MEAN') return st.c ? st.sum / st.c : null
    if (s.func === 'MEDIAN') {
        const v = st.m.val();
        return v === undefined ? null : v
    }
    if (s.func === 'COUNT') return ctx.groupCount
    if (s.func === 'PERCENT_OF_ROW_COUNT') return ctx.groupCount ? (st.t / ctx.groupCount) : 0
    if (s.func === 'PERCENT_OF_COL_COUNT') {
        const tot = ctx.colTotals[s.index]?.count || 0;
        return tot ? (st.t / tot) : 0
    }
    if (s.func === 'PERCENT_OF_COL_SUM') {
        const tot = ctx.colTotals[s.index]?.sum || 0;
        return tot ? (st.sum / tot) : 0
    }
    if (s.func === 'PERCENT_OF_COL_WEIGHT') {
        // console.log(st, s)
        return st.sw ? st.sp / st.sw : null
    }
    if (s.func === 'PERCENT_OF_OTHER_COL_COUNT') {
        const tot = ctx.colTotals[s.index]?.count || 0;
        return tot ? (st.t / tot) : 0
    }
    if (s.func === 'PERCENT_OF_OTHER_COL_SUM') {
        const tot = ctx.colTotals[s.index]?.sum || 0;
        return tot ? (st.sum / tot) : 0
    }
    if (s.func === 'COUNT_DISTINCT') return st.set.size
    if (s.func === 'COUNT_NON_NULL') return st.c
    if (s.func === 'MIN') return st.v === undefined ? null : st.v
    if (s.func === 'MAX') return st.v === undefined ? null : st.v
    if (s.func === 'FIRST') return st.seen ? st.v : null
    if (s.func === 'LAST') return st.v === undefined ? null : st.v
    if (s.func === 'WAVG') return st.sw ? st.sp / st.sw : null
    return null
}

export class PivotEngine {
    constructor(engine) {
        this.engine = engine;
        this.cache = new Map()
        this.colCache = null;
    }

    computePivotWithCache(pivotConfig, rowIndexArray) {
        const cached = this._getPivotCacheEntry(pivotConfig, rowIndexArray);
        if (cached) return cached;
        const cols = this._collectAllColumnsPivotUses(pivotConfig);
        const result = this.compute(this.engine.asTable().select(cols), pivotConfig);
        this._setPivotCacheEntry(pivotConfig, rowIndexArray, result);
        return result;
    }

    _collectAllColumnsPivotUses(pivotConfig) {
        pivotConfig.calculatedCols = undefined;
        const cols = {};
        const push = function(list) {
            if (!list) return;
            for (let i = 0; i < list.length; i++) { cols[list[i]] = 1; }
        };
        push(pivotConfig && pivotConfig.groupBy);
        push(pivotConfig && pivotConfig.valueCols);
        push(pivotConfig && pivotConfig.weightCols);
        push(pivotConfig && pivotConfig.calculatedCols);

        const out = [];
        for (let k in cols) {
            if (cols.hasOwnProperty(k)) out.push(k);
        }
        return out;
    }

    _hash64_any(x) {
        let s;
        if (x == null) {
            s = 'null';
        } else if (typeof x === 'string') {
            s = x;
        } else if (Array.isArray(x)) {
            s = '[';
            for (let i = 0; i < x.length; i++) {
                if (i) s += ',';
                s += String(x[i]);
            }
            s += ']';
        } else if (typeof x === 'object') {
            // stable key order
            const keys = Object.keys(x).sort();
            s = '{';
            for (let i = 0; i < keys.length; i++) {
                if (i) s += ',';
                const k = keys[i];
                s += k + ':' + String(x[k]);
            }
            s += '}';
        } else {
            s = String(x);
        }

        let h = 0xcbf29ce484222325n;
        const p = 0x100000001b3n;
        for (let i = 0; i < s.length; i++) {
            const c = s.charCodeAt(i) & 0xffff;
            const b0 = BigInt(c & 0xff);
            const b1 = BigInt((c >> 8) & 0xff);
            h ^= b0; h = (h * p) & 0xffffffffffffffffn;
            h ^= b1; h = (h * p) & 0xffffffffffffffffn;
        }
        return h;
    }

    _buildPivotKey(pivotConfig, rowIndexArray) {
        // 1) Config hash (structure + options that affect results)
        const cfgHash = this._hash64_any(pivotConfig);

        // 2) Row signature from engine
        const rowSig = this.engine.hashRows(rowIndexArray);
        // console.log('rowSig:', rowSig);

        // 3) Column dependency signature
        const cols = this._collectAllColumnsPivotUses(pivotConfig);
        const colSig = (this.engine && this.engine.hashDepsFor) ? this.engine.hashDepsFor(cols) : 0n;

        // 4) Global version for table swaps/materialization
        const gv = (this.engine && this.engine.globalVersion) ? BigInt(this.engine.globalVersion()) : 0n;

        const key = (cfgHash ^ (rowSig << 1n) ^ (colSig << 2n) ^ (gv << 3n)) & 0xffffffffffffffffn;
        return {
            key, cfgHash, rowSig, colSig, gv,
            colsUsed: cols
        };
    }

    _setPivotCacheEntry(pivotConfig, rowIndexArray, pivotResult) {
        if (!this.cache) this.cache = new Map();
        // Evict oldest entries when cache exceeds capacity to prevent unbounded growth
        const PIVOT_CACHE_MAX = 256;
        if (this.cache.size >= PIVOT_CACHE_MAX) {
            const it = this.cache.keys();
            for (let i = 0, drop = this.cache.size - PIVOT_CACHE_MAX + 1; i < drop; i++) {
                this.cache.delete(it.next().value);
            }
        }
        const meta = this._buildPivotKey(pivotConfig, rowIndexArray);
        this.cache.set(meta.key.toString(), {
            result: pivotResult,
            cfgHash: meta.cfgHash,
            rowSig: meta.rowSig,
            colSig: meta.colSig,
            gv: meta.gv,
            colsUsed: meta.colsUsed
        });
        return meta.key;
    }

    _getPivotCacheEntry(pivotConfig, rowIndexArray) {
        if (!this.cache) return null;
        const probe = this._buildPivotKey(pivotConfig, rowIndexArray);
        const entry = this.cache.get(probe.key.toString());
        if (!entry) return null;
        if (entry.gv !== probe.gv) return null;
        if (entry.rowSig !== probe.rowSig) return null;
        if (entry.colSig !== probe.colSig) return null;
        return entry.result || null;
    }

    collectColumns(input, groupBy, aggs) {
        const spec = buildSpec(aggs)
        // console.log(spec);
        const dep = new Set(groupBy)
        for (const s of spec) {
            dep.add(s.col);
            if (s.weight) dep.add(s.weight)
            if (s.otherCol) dep.add(s.otherCol)
        }
        const need = [...dep]
        const cols = toCols(input, need);
        return {cols, spec}
    }

    compute(input, cfg = {}) {
        // console.log('BEGIN:', input, cfg);
        const groupBy = cfg.group_by || cfg.groupBy || []
        const aggs = cfg.aggregations || []
        const force = !!cfg.FORCE || !!cfg.force
        const grand = !!cfg.GRAND_TOTAL || !!cfg.grandTotal
        const gGroups = cfg.GRAND_TOTAL_GROUPS || cfg.grandTotalGroups || []

        // const cacheKey = cfg.cacheKey

        const {cols, spec} = this.collectColumns(input, groupBy, aggs);

        for (let i = 0; i < spec.length; i++) spec[i].index = i
        const rows = (cols[groupBy[0]] || Object.values(cols)[0] || []).length;

        // const ck = this.keyOf(cfg, rows, cols, cacheKey)
        // console.log(`${cols[groupBy[0]].length} -> ${ck}`);
        //
        // if (!force && this.cache.has(ck)) {
        //     const v = this.cache.get(ck);
        //     return {
        //         columns: [...v.columns],
        //         rows: v.rows.map(r => r.slice()),
        //         totals: v.totals.map(r => r.slice()),
        //         meta: {cached: true}
        //     }
        // }

        const totals = preTotals(cols, rows, spec);

        const groupMap = new Map()
        const grandMap = grand ? new Map() : null
        const gb = groupBy
        const outCols = [...gb.map(k => k)];

        for (const s of spec) outCols.push(s.name)
        for (let i = 0; i < rows; i++) {
            let key = ''
            if (gb.length) {
                const vs = new Array(gb.length);
                for (let j = 0; j < gb.length; j++) {
                    const k = gb[j];
                    vs[j] = K(cols[k][i])
                }
                key = vs.join(S)
            }
            let g = groupMap.get(key)
            if (!g) {
                const st = new Array(spec.length);
                for (let j = 0; j < spec.length; j++) st[j] = initState(spec[j]);
                const vgb = new Array(gb.length);
                if (gb.length) {
                    const parts = key.split(S);
                    for (let j = 0; j < gb.length; j++) vgb[j] = parts[j]
                }
                g = {key, vgb, states: st, count: 0};
                groupMap.set(key, g)
            }
            g.count++
            for (let j = 0; j < spec.length; j++) {
                const s = spec[j]
                const v = cols[s.col]?.[i]
                const w = s.weight ? s.weight && cols[s.weight]?.[i] : undefined
                updateState(g.states[j], s, v, w)
            }
            if (grand) {
                let gkey = ''
                if (gGroups.length) {
                    const vs = new Array(gGroups.length);
                    for (let j = 0; j < gGroups.length; j++) {
                        const k = gGroups[j];
                        vs[j] = K(cols[k][i])
                    }
                    gkey = vs.join(S)
                }
                let gg = grandMap.get(gkey)
                if (!gg) {
                    const st = new Array(spec.length);
                    for (let j = 0; j < spec.length; j++) st[j] = initState(spec[j]);
                    const vgb = new Array(gGroups.length);
                    if (gGroups.length) {
                        const parts = gkey.split(S);
                        for (let j = 0; j < gGroups.length; j++) vgb[j] = parts[j]
                    }
                    gg = {key: gkey, vgb, states: st, count: 0};
                    grandMap.set(gkey, gg)
                }
                gg.count++
                for (let j = 0; j < spec.length; j++) {
                    const s = spec[j]
                    const v = cols[s.col]?.[i]
                    const w = s.weight ? s.weight && cols[s.weight]?.[i] : undefined
                    updateState(gg.states[j], s, v, w)
                }
            }
        }
        const rowsOut = [];
        const grandOut = [];
        for (const g of groupMap.values()) {
            const row = new Array(outCols.length)
            for (let j = 0; j < gb.length; j++) row[j] = g.vgb[j] ?? ''
            const ctx = {groupCount: g.count, colTotals: totals}
            for (let j = 0; j < spec.length; j++) {
                row[gb.length + j] = finalizeState(g.states[j], spec[j], ctx)
            }
            rowsOut.push(row)
        }
        rowsOut.sort((a, b) => {
            for (let i = 0; i < gb.length; i++) {
                if (a[i] === b[i]) continue;
                return a[i] < b[i] ? -1 : 1
            }
            return 0
        })
        if (grand) {
            for (const g of grandMap.values()) {
                const row = new Array(outCols.length)
                const present = new Set(gGroups)
                let p = 0;
                let f = false;
                for (let j = 0; j < gb.length; j++) {
                    const name = gb[j];
                    if (present.has(name)) {
                        const idx = gGroups.indexOf(name);
                        row[j] = g.vgb[idx] ?? ''
                    } else {
                        row[j] = !f ? 'Grand Total' : '';
                        f = true;
                    }
                }
                const ctx = {groupCount: g.count, colTotals: totals}
                for (let j = 0; j < spec.length; j++) {
                    row[gb.length + j] = finalizeState(g.states[j], spec[j], ctx)
                }
                grandOut.push(row)
            }
        }
        // const result = {columns: outCols, rows: rowsOut, totals:grandOut}
        // this.cache.set(ck, {columns: [...outCols], rows: rowsOut.map(r => r.slice()), totals: grandOut.map(r => r.slice())})
        return {columns: outCols, rows: rowsOut, totals: grandOut, meta: {cached: false}}
    }

    clearCache() {
        if (this.cache) this.cache.clear();
        if (this.colCache) this.colCache.clear()
    }

    dropCacheKey(k) {
        if (this.cache) this.cache.delete(k)
    }
}

export class PivotWorker {
    constructor(engine) {
        this.engine = engine;
        this.w = null;  // Worker
        this.pe = null; // Pivot Engine
    }

    normAggs(aggs) {
        const out = [];
        for (let i = 0; i < aggs.length; i++) {
            const o = aggs[i];
            const k = Object.keys(o)[0];
            const v = o[k];
            if (typeof v === 'string') {
                out.push([k, v.toUpperCase(), k + '_' + v.toUpperCase(), undefined, false, false, undefined, false])
            } else {
                out.push([
                    k,
                    (v.func || 'SUM').toUpperCase(), v.name || k + '_' + (v.func || 'SUM').toUpperCase(),
                    v.weight === undefined ? undefined : String(v.weight),
                    !!(v.DROP_NULLS || v.dropNulls),
                    !!(v.ZERO_AS_NULL || v.zeroAsNull),
                    v.FILL_NULL !== undefined ? v.FILL_NULL : v.fillNull,
                    !!(v.ABS || v.abs),
                    !!(v.CACHE || v.cache),
                    v.otherCol || v.OTHER_COL
                ])
            }
        }
        return out
    }

    isTyped(x) {
        return ArrayBuffer.isView(x) && !(x instanceof DataView)
    }

    estBytesForArray(a) {
        let n = a.length;
        if (n === 0) return 0;
        let nums = 0, bools = 0, strs = 0, other = 0, chars = 0;
        const m = n > 64 ? 64 : n;
        for (let i = 0; i < m; i++) {
            const v = a[i];
            const t = typeof v;
            if (t === 'number') {
                nums++
            } else if (t === 'boolean') {
                bools++
            } else if (t === 'string') {
                strs++;
                chars += v.length
            } else other++
        }
        if (nums >= bools && nums >= strs && nums >= other) return n * 8;
        if (bools >= nums && bools >= strs && bools >= other) return n;
        if (strs >= nums && strs >= bools && strs >= other) {
            const avg = (chars / (strs || 1));
            return Math.ceil(n * (avg * 2 + 8))
        }
        return n * 16
    }

    toCols(input, needed) {
        if (!input) return {}
        if (Array.isArray(input)) {
            const out = {};
            for (const k of needed) out[k] = new Array(input.length);
            for (let i = 0; i < input.length; i++) {
                const r = input[i];
                for (const k of needed) out[k][i] = r[k]
            }
            return out
        }
        if (input && typeof input === 'object') {
            if (typeof input.getChild === 'function' && typeof input.numRows === 'number') {
                const out = {};
                for (const k of needed) {
                    const col = input.getChild(k) || input.getColumn?.(k) || input.get?.(k);
                    if (col && col.toArray) {
                        out[k] = col.toArray()
                    } else {
                        const n = input.numRows;
                        const arr = new Array(n);
                        for (let i = 0; i < n; i++) arr[i] = col?.get ? col.get(i) : undefined;
                        out[k] = arr
                    }
                }
                return out
            }
            const keys = Object.keys(input);
            if (keys.length && Array.isArray(input[keys[0]])) {
                const out = {};
                for (const k of needed) out[k] = input[k] || [];
                return out
            }
        }
        return {}
    }

    ckForCols(cols, cfg, userKey) {
        if (userKey) return String(userKey)
        const h = H();
        const g = cfg.group_by || cfg.groupBy || [];
        const a = this.normAggs(cfg.aggregations || []);
        h(JSON.stringify({g, a}))
        const names = Object.keys(cols).sort();
        for (let i = 0; i < names.length; i++) {
            const k = names[i], v = cols[k] || [];
            h('|' + k + '#' + v.length);
            const m = v.length < 32 ? v.length : 32;
            for (let j = 0; j < m; j++) {
                const x = v[j];
                h('|' + (x === null || x === undefined ? '' : String(x)))
            }
        }
        return h('')
    }

    pack(input, cfg) {
        const g = cfg.group_by || cfg.groupBy || []
        const a = cfg.aggregations || []
        const need = new Set(g);
        for (let i = 0; i < a.length; i++) {
            const k = Object.keys(a[i])[0];
            const v = a[i][k];
            need.add(k);
            const w = typeof v === 'string' ? undefined : v && v.weight;
            if (w) need.add(w)
            const oc = typeof v === 'string' ? undefined : v && (v.otherCol || v.OTHER_COL);
            if (oc) need.add(oc)
        }
        const cols = this.toCols(input, [...need])
        const transfers = [];
        let bytes = 0;

        const keys = cols && typeof cols === "object" ? Object.keys(cols) : [];
        const firstKey = keys.length ? keys[0] : null;
        let rows = 0;
        if (firstKey != null) {
            const v = cols[firstKey];
            rows = Array.isArray(v) ? v.length : 0;
        }

        for (const k of keys) {
            const v = cols[k];
            if (this.isTyped(v)) {
                bytes += v.byteLength;
                transfers.push(v.buffer)
            } else if (Array.isArray(v)) {
                bytes += this.estBytesForArray(v)
            }
        }

        const ck = this.ckForCols(cols, cfg, cfg.cacheKey)
        return {cols, bytes, rows, transfers, ck}
    }

    hasHeavy(aggs) {
        for (let i = 0; i < aggs.length; i++) {
            const o = aggs[i];
            const k = Object.keys(o)[0];
            const d = o[k];
            const f = (typeof d === 'string' ? d : d.func || 'SUM').toUpperCase();
            if (f === 'MEDIAN' || f === 'COUNT_DISTINCT') return true
        }
        return false
    }

    getPivotEngine() {
        if (this.pe) return this.pe;
        this.pe = new PivotEngine(this.engine);
        return this.pe;
    }

    compute(input, cfg) {
        const mode = cfg.workerMode || 'auto'
        if (mode === 'never') return Promise.resolve(this.getPivotEngine().compute(input, cfg))
        const p = this.pack(input, cfg)
        const heavy = mode === 'always' || p.bytes > 8_000_000 || p.rows > 300_000 || this.hasHeavy(cfg.aggregations || [])
        if (!heavy) return Promise.resolve(this.getPivotEngine().compute(input, cfg))
        const ww = this.ensureWorker()
        return new Promise((res, rej) => {
            ww.onmessage = e => res(e.data);
            ww.onerror = rej;
            ww.postMessage({type: 'compute', cols: p.cols, cfg, ck: p.ck}, p.transfers)
        })
    }

    clearCache() {
        if (!this.w) return
        return new Promise(r => {
            this.w.onmessage = () => r(true);
            this.w.postMessage({type: 'clear'})
        })
    }

    ensureWorker() {
        if (this.w) return this.w
        const src = `
var WCACHE=new Map();var S=String.fromCharCode(31)
function H(){let h=2166136261>>>0;return function(x){for(let i=0;i<x.length;i++){h^=x.charCodeAt(i);h=(h+((h<<1)>>>0)+((h<<4)>>>0)+((h<<7)>>>0)+((h<<8)>>>0)+((h<<24)>>>0))>>>0}return h.toString(36)}}
function N(n){return n===null||n===undefined||Number.isNaN(n)}
function Z(v,z){return N(v)||(z&&v===0)}
function T(v,z){return !Z(v,z)&&!!v}
function K(v){return v===null||v===undefined?'':(typeof v==='string'?v:v+'')}
function MHeap(cmp){const a=[];return{push(v){a.push(v);let i=a.length-1;while(i>0){let p=(i-1)>>1;if(cmp(a[i],a[p])<0){let t=a[i];a[i]=a[p];a[p]=t;i=p}else break}},pop(){if(a.length===0)return;let r=a[0],v=a.pop();if(a.length){a[0]=v;let i=0;for(;;){let l=2*i+1,rn=l+1,m=i;if(l<a.length&&cmp(a[l],a[m])<0)m=l;if(rn<a.length&&cmp(a[rn],a[m])<0)m=rn;if(m!==i){let t=a[i];a[i]=a[m];a[m]=t;i=m}else break}}return r},peek(){return a[0]},size(){return a.length}}}
function MedianAgg(){const lo=MHeap((x,y)=>y-x),hi=MHeap((x,y)=>x-y);return{add(x){if(hi.size()===0||x>=hi.peek()){hi.push(x)}else{lo.push(x)}if(hi.size()>lo.size()+1){lo.push(hi.pop())}else if(lo.size()>hi.size()){hi.push(lo.pop())}},val(){if(hi.size()===0&&lo.size()===0)return null;if(hi.size()===lo.size())return (hi.peek()+lo.peek())/2;return hi.peek()}}}
function buildSpec(aggs){const out=[];for(let i=0;i<aggs.length;i++){const o=aggs[i];const k=Object.keys(o)[0];const d=o[k];if(typeof d==='string'){out.push({col:k,func:d.toUpperCase(),name:k+'_'+d.toUpperCase(),dropNulls:false,zeroAsNull:false,fillNull:undefined,weight:undefined,otherCol:undefined,index:i})}else{out.push({col:k,func:(d.func||'SUM').toUpperCase(),name:d.name||k+'_'+(d.func||'SUM').toUpperCase(),dropNulls:!!(d.DROP_NULLS||d.dropNulls),zeroAsNull:!!(d.ZERO_AS_NULL||d.zeroAsNull),fillNull:d.FILL_NULL!==undefined?d.FILL_NULL:d.fillNull,weight:d.weight,otherCol:d.otherCol||d.OTHER_COL,index:i})}}return out}
function preTotals(cols,rows,spec){const res=new Array(spec.length);for(let j=0;j<spec.length;j++){const s=spec[j];if(s.func==='PERCENT_OF_COL_COUNT'){let t=0;const a=cols[s.col]||[];const z=s.zeroAsNull;for(let i=0;i<rows;i++){const v=a[i];if(T(v,z))t++}res[j]={count:t}}else if(s.func==='PERCENT_OF_COL_SUM'){let t=0;const a=cols[s.col]||[];const z=s.zeroAsNull;const d=s.dropNulls;const f=s.fillNull;for(let i=0;i<rows;i++){let v=a[i];if(N(v)&&f!==undefined)v=f;if(d&&(Z(v,z)))continue;let n=+v;if(!Number.isFinite(n))continue;t+=n}res[j]={sum:t}}else if(s.func==='PERCENT_OF_OTHER_COL_COUNT'){let t=0;const a=cols[s.otherCol]||[];const z=s.zeroAsNull;for(let i=0;i<rows;i++){const v=a[i];if(T(v,z))t++}res[j]={count:t}}else if(s.func==='PERCENT_OF_OTHER_COL_SUM'){let t=0;const a=cols[s.otherCol]||[];const z=s.zeroAsNull;const d=s.dropNulls;const f=s.fillNull;for(let i=0;i<rows;i++){let v=a[i];if(N(v)&&f!==undefined)v=f;if(d&&(Z(v,z)))continue;let n=+v;if(!Number.isFinite(n))continue;t+=n}res[j]={sum:t}}else res[j]=null}return res}
function initState(s){if(s.func==='SUM')return{sum:0};if(s.func==='MEAN')return{sum:0,c:0};if(s.func==='MEDIAN')return{m:MedianAgg()};if(s.func==='COUNT')return{c:0};if(s.func==='PERCENT_OF_ROW_COUNT')return{t:0};if(s.func==='PERCENT_OF_COL_COUNT')return{t:0};if(s.func==='PERCENT_OF_COL_SUM')return{sum:0};if(s.func==='PERCENT_OF_COL_WEIGHT')return{sp:0,sw:0};if(s.func==='PERCENT_OF_OTHER_COL_COUNT')return{t:0};if(s.func==='PERCENT_OF_OTHER_COL_SUM')return{sum:0};if(s.func==='COUNT_DISTINCT')return{set:new Set()};if(s.func==='COUNT_NON_NULL')return{c:0};if(s.func==='MIN')return{v:undefined};if(s.func==='MAX')return{v:undefined};if(s.func==='FIRST')return{v:undefined,seen:false};if(s.func==='LAST')return{v:undefined};if(s.func==='WAVG')return{sp:0,sw:0};return{}}
function updateState(st,s,v,w){const z=s.zeroAsNull;const d=s.dropNulls;const f=s.fillNull;if(s.func==='COUNT'){st.c++;return}if(N(v)&&f!==undefined)v=f;if(d&&(Z(v,z)||(s.weight!==undefined&&Z(w,z))))return;v=Number(v);if(s.func==='SUM'){const n=+v;if(Number.isFinite(n))st.sum+=n;return}if(s.func==='MEAN'){const n=+v;if(Number.isFinite(n)){st.sum+=n;st.c++}return}if(s.func==='MEDIAN'){const n=+v;if(Number.isFinite(n))st.m.add(n);return}if(s.func==='PERCENT_OF_ROW_COUNT'){if(T(v,z))st.t++;return}if(s.func==='PERCENT_OF_COL_COUNT'){if(T(v,z))st.t++;return}if(s.func==='PERCENT_OF_COL_SUM'){const n=+v;if(Number.isFinite(n))st.sum+=n;return}if(s.func==='PERCENT_OF_OTHER_COL_COUNT'){if(T(v,z))st.t++;return}if(s.func==='PERCENT_OF_OTHER_COL_SUM'){const n=+v;if(Number.isFinite(n))st.sum+=n;return}if(s.func==='COUNT_DISTINCT'){st.set.add(v===undefined?null:v);return}if(s.func==='COUNT_NON_NULL'){if(T(v,z))st.c++;return}if(s.func==='MIN'){if(!Z(v,z)){if(st.v===undefined||v<st.v)st.v=v}return}if(s.func==='MAX'){if(!Z(v,z)){if(st.v===undefined||v>st.v)st.v=v}return}if(s.func==='FIRST'){if(!st.seen&&!Z(v,z)){st.v=v;st.seen=true}return}if(s.func==='LAST'){if(!Z(v,z)){st.v=v}return}w=Number(w);if(s.func==='WAVG'){const nv=+v,nw=+w;if(Number.isFinite(nv)&&Number.isFinite(nw)){st.sp+=nv*nw;st.sw+=nw}return};if(s.func==='PERCENT_OF_COL_WEIGHT'){const nv=+w,nw=+v;if(Number.isFinite(nv)&&Number.isFinite(nw)){st.sp+=nv*nw;st.sw+=nw}return}}
function finalizeState(st,s,ctx){if(s.func==='SUM')return st.sum;if(s.func==='MEAN')return st.c?st.sum/st.c:null;if(s.func==='MEDIAN'){const v=st.m.val();return v===undefined?null:v}if(s.func==='COUNT')return ctx.groupCount;if(s.func==='PERCENT_OF_ROW_COUNT')return ctx.groupCount?(st.t/ctx.groupCount):0;if(s.func==='PERCENT_OF_COL_COUNT'){const tot=ctx.colTotals[s.index]?.count||0;return tot?(st.t/tot):0}if(s.func==='PERCENT_OF_COL_SUM'){const tot=ctx.colTotals[s.index]?.sum||0;return tot?(st.sum/tot):0}if(s.func==='PERCENT_OF_COL_WEIGHT'){return st.sw?st.sp/st.sw:null}if(s.func==='PERCENT_OF_OTHER_COL_COUNT'){const tot=ctx.colTotals[s.index]?.count||0;return tot?(st.t/tot):0}if(s.func==='PERCENT_OF_OTHER_COL_SUM'){const tot=ctx.colTotals[s.index]?.sum||0;return tot?(st.sum/tot):0}if(s.func==='COUNT_DISTINCT')return st.set.size;if(s.func==='COUNT_NON_NULL')return st.c;if(s.func==='MIN')return st.v===undefined?null:st.v;if(s.func==='MAX')return st.v===undefined?null:st.v;if(s.func==='FIRST')return st.seen?st.v:null;if(s.func==='LAST')return st.v===undefined?null:st.v;if(s.func==='WAVG')return st.sw?st.sp/st.sw:null;return null}
function computeWithCols(cols,cfg){
const gb=cfg.group_by||cfg.groupBy||[]
const spec=(function(aggs){const out=[];for(let i=0;i<aggs.length;i++){const o=aggs[i];const k=Object.keys(o)[0];const d=o[k];if(typeof d==='string'){out.push({col:k,func:d.toUpperCase(),name:k+'_'+d.toUpperCase(),dropNulls:false,zeroAsNull:false,fillNull:undefined,weight:undefined,otherCol:undefined,index:i})}else{out.push({col:k,func:(d.func||'SUM').toUpperCase(),name:d.name||k+'_'+(d.func||'SUM').toUpperCase(),dropNulls:!!(d.DROP_NULLS||d.dropNulls),zeroAsNull:!!(d.ZERO_AS_NULL||d.zeroAsNull),fillNull:d.FILL_NULL!==undefined?d.FILL_NULL:d.fillNull,weight:d.weight,otherCol:d.otherCol||d.OTHER_COL,index:i})}}return out})(cfg.aggregations||[])
const rows=(cols[gb[0]]||Object.values(cols)[0]||[]).length
const totals=preTotals(cols,rows,spec)
const groupMap=new Map()
const grand=!!(cfg.GRAND_TOTAL||cfg.grandTotal)
const gGroups=cfg.GRAND_TOTAL_GROUPS||cfg.grandTotalGroups||[]
const gIndexMap={};for(let i=0;i<gGroups.length;i++)gIndexMap[gGroups[i]]=i
const grandMap=grand?new Map():null
const outCols=[...gb.map(k=>k)];for(let i=0;i<spec.length;i++)outCols.push(spec[i].name)
for(let i=0;i<rows;i++){
let key=''
if(gb.length){const vs=new Array(gb.length);for(let j=0;j<gb.length;j++){const k=gb[j];vs[j]=K(cols[k][i])}key=vs.join(S)}
let g=groupMap.get(key)
if(!g){const st=new Array(spec.length);for(let j=0;j<spec.length;j++)st[j]=initState(spec[j]);const vgb=new Array(gb.length);if(gb.length){const parts=key.split(S);for(let j=0;j<gb.length;j++)vgb[j]=parts[j]}g={vgb,states:st,count:0};groupMap.set(key,g)}
g.count++
for(let j=0;j<spec.length;j++){const s=spec[j];const v=cols[s.col]?.[i];const w=s.weight?cols[s.weight]?.[i]:undefined;updateState(g.states[j],s,v,w)}
if(grand){
let gkey=''
if(gGroups.length){const vs=new Array(gGroups.length);for(let j=0;j<gGroups.length;j++){const k=gGroups[j];vs[j]=K(cols[k][i])}gkey=vs.join(S)}
let gg=grandMap.get(gkey)
if(!gg){const st=new Array(spec.length);for(let j=0;j<spec.length;j++)st[j]=initState(spec[j]);const vgb=new Array(gGroups.length);if(gGroups.length){const parts=gkey.split(S);for(let j=0;j<gGroups.length;j++)vgb[j]=parts[j]}gg={vgb,states:st,count:0};grandMap.set(gkey,gg)}
gg.count++
for(let j=0;j<spec.length;j++){const s=spec[j];const v=cols[s.col]?.[i];const w=s.weight?cols[s.weight]?.[i]:undefined;updateState(gg.states[j],s,v,w)}
}
}
const rowsOut=[];
const grandOut=[];
for(const g of groupMap.values()){
const row=new Array(outCols.length)
for(let j=0;j<gb.length;j++)row[j]=g.vgb[j]??''
const ctx={groupCount:g.count,colTotals:totals}
for(let j=0;j<spec.length;j++)row[gb.length+j]=finalizeState(g.states[j],spec[j],ctx)
rowsOut.push(row)
}
if(grand){
for(const g of grandMap.values()){
const row=new Array(outCols.length);
let f=false;
for(let j=0;j<gb.length;j++){
    const name=gb[j];
    const idx=gIndexMap[name];
    if (idx === undefined) {
        if (!f) {
            f = true;
            row[j] = 'Grand Total'
        } else {
            row[j] = ''
        }
    } else {
        row[j] = g.vgb[idx]
    }
}
const ctx={groupCount:g.count,colTotals:totals}
for(let j=0;j<spec.length;j++)row[gb.length+j]=finalizeState(g.states[j],spec[j],ctx)
grandOut.push(row)
}
}
rowsOut.sort((a,b)=>{for(let i=0;i<gb.length;i++){if(a[i]===b[i])continue;return a[i]<b[i]?-1:1}return 0})
return {columns:outCols,rows:rowsOut,totals:grandOut,meta:{cached:false,worker:true}}
}
onmessage=e=>{
const d=e.data
if(d.type==='clear'){WCACHE.clear();postMessage({ok:true});return}
if(d.type==='compute'){
const ck=d.ck||'';const force=!!(d.cfg&& (d.cfg.FORCE||d.cfg.force))
if(!force&&ck&&WCACHE.has(ck)){const r=WCACHE.get(ck);postMessage({columns:r.columns,rows:r.rows,totals:r.totals,meta:{cached:true,worker:true}});return}
const r=computeWithCols(d.cols,d.cfg||{})
if(ck)WCACHE.set(ck,{columns:r.columns,rows:r.rows,totals:r.totals})
postMessage(r)
}
}
`
        const b = new Blob([src], {type: 'application/javascript'})
        const u = URL.createObjectURL(b)
        this.w = new Worker(u)
        return this.w
    }
}

export default {PivotWorker};
