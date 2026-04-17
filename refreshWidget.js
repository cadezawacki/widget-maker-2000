import '@/pt/css/refresh.css';

import {BaseWidget} from "@/pt/js/widgets/baseWidget.js";
import interact from 'interactjs';
import {asArray} from '@/utils/helpers.js';
import {ACTION_MAP} from "@/global/actionMap.js";
import { formatInTimeZone } from 'date-fns-tz';
import { TZDate } from "@date-fns/tz";
import {ExecutePanel} from "@/global/js/executePanel.js";


const QUICK_ACTIONS = [
    { label: "Signals", names: ["refresh_signals"] },
    { label: "Positions", names: ["refresh_positions", "refresh_risk"] },
    { label: "All", action: 'selectAll' },
];

export class RefreshWidget extends BaseWidget {
    constructor(context, widgetId, managerId, selector, config={}) {
        super(context, widgetId, managerId, selector, config);

        this.MKTS_STORAGE_KEY = "mkts_market_order_v1";
        this.MKTS_COLOR_CACHE = null;
        this.RELATIVE_TIMER_ID = null;
        this.timer_interval = (parseInt(getComputedStyle(document.documentElement)
            .getPropertyValue("--mkts-interval-relative-ms"), 10) || 60000);

        this.containerElement = document.querySelector(selector);
        this.markets = null;

        this.initialAnimated = false;
        this.enabled = true;
    }

    async onInit() {

    }

    initMarkets() {
        const engine = this.engine;
        const mm = new Set([...Object.values(this.context.page.refDropdown.config.metricsData)])
        this.markets = Array.from(mm).map(m => {
            m.coverage = (m.coverage || 0) * 100;
            // m.usage = (m.usage || 0) * 100;
            m.lastUpdate = null;
            m.lastRefresh = null;
            let mkt = m.value.toString();
            const timeCol = mkt + 'RefreshTime';
            if (engine.columnDefs.has(timeCol)) {
                let vals = engine.getColumnValues(timeCol).filter(x => x != null);
                if (vals.length) {
                    vals = vals.map(x=>x.split(".")[0] +'+00:00').map(x=> new TZDate(x))
                    m.lastUpdate = new Date(Math.max(...vals))
                }
            }
            const rals = engine.getColumnValues('refSyncTime').filter(x=> x != null);

            if (rals.length) {
                m.lastRefresh = new Date(Math.max(...rals.map(x => new Date(x))));
            }

            return m
        });

    }

    build() {
        this.buildRows(true);
        this.updateGlobalRefreshState();
    }

    rebuildMarkets() {
        this.context.page.refDropdown._updateAndCheckDataAvailability();
        this.context.page.refDropdownLower._updateAndCheckDataAvailability();
        this.initMarkets();
        this.buildRows(false);
        this.updateGlobalRefreshState();
    }

    afterMount() {
        this.cacheDom();
        this.bindEvents();
        this.setupReactions();
        this.initMarkets();
        this.initExecutePanel();
        this.build();
        this.setupRowDragging();
        if (!this.RELATIVE_TIMER_ID) {
            this.updateAllRelativeTimes();
            const table = this;
            this.RELATIVE_TIMER_ID = setInterval(() => {
                table.updateAllRelativeTimes();
            }, this.timer_interval);
        }
    }

    initExecutePanel() {
        this.executePanel = new ExecutePanel(document.getElementById("fixedExecute"), {
            context: this.context,
            portfolioKey: this.context.portfolio_key,
            send: this.simulatedSend.bind(this),
            mode: "fixed",
            onPayloadSent: (p) => console.log("[Fixed] sent:", p),
            quickActions: QUICK_ACTIONS,
        });
    }

    simulatedSend(p) {
        const traceId = p.trace;
        const names = Array.isArray(p.data.funcName) ? p.data.funcName : [p.data.funcName];
        const label = names.length <= 3 ? names.join(", ") : `${names.length} functions`;
        const delay = 800 + Math.random() * 1200;
        setTimeout(() => {
            const rows = names.length * (Math.floor(Math.random() * 200) + 50);
            this.executePanel.handleTraceEvent(traceId, "success", `${rows} rows`);
        }, delay);
    }

    cacheDom() {
        this.tbody = this.containerElement.querySelector("#mkts-tbody");
        this.lastActionElement = this.containerElement.querySelector("#mkts-last-action");
        this.refreshAllButton = this.containerElement.querySelector("#mkts-refresh-all");
        this.container = document.querySelector(".mkts-container");
        this.fixedExecute = document.querySelector("#fixedExecutePanel");
    }


    bindEvents() {
        let self = this;

        this.context.page.addEventListener(this.refreshAllButton, "click", async () => {
            if (!self.enabled) return;
            await self.refreshAllMarkets();
        });

        this.context.page.addEventListener(this.tbody, "click", (e) => {
            if (!self.enabled) return;
            let target = e.target;
            let button = target.closest("button[data-refresh='single']");
            if (!button) return;

            let marketId = button.getAttribute("data-market-id");
            if (!marketId) return;

            self.refreshSingleMarketById(marketId);
        });

    }

    onRender() {
        this.widgetDiv.innerHTML = `
        <div class="refresh-widget-body">
            <div id="fixedExecute">
            
            </div>
            <div class="mkts-container">
                <div class="mkts-inner">
                    <div class="mkts-header">
                        <div class="mkts-title-block">
                            <div class="mkts-title">
                                <div class="mkts-title-main">Update Portfolio Data</div>
                            </div>
                            <div class="mkts-title-sub">
                                <div class="mkts-refresh-sub">
                                    Last action: <strong id="mkts-last-action">No refresh yet</strong>
                                </div>
                            </div>
                        </div>
            
                        <div class="mkts-header-actions">
                            <button class="mkts-refresh-all" id="mkts-refresh-all">
                                <span class="mkts-refresh-icon">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"><g fill="none"><path d="m12.594 23.258l-.012.002l-.071.035l-.02.004l-.014-.004l-.071-.036q-.016-.004-.024.006l-.004.01l-.017.428l.005.02l.01.013l.104.074l.015.004l.012-.004l.104-.074l.012-.016l.004-.017l-.017-.427q-.004-.016-.016-.018m.264-.113l-.014.002l-.184.093l-.01.01l-.003.011l.018.43l.005.012l.008.008l.201.092q.019.005.029-.008l.004-.014l-.034-.614q-.005-.019-.02-.022m-.715.002a.02.02 0 0 0-.027.006l-.006.014l-.034.614q.001.018.017.024l.015-.002l.201-.093l.01-.008l.003-.011l.018-.43l-.003-.012l-.01-.01z"/><path fill="currentColor" d="M20 9.5a1.5 1.5 0 0 1 1.5 1.5a8.5 8.5 0 0 1-8.5 8.5h-2.382a1.5 1.5 0 0 1-2.179 2.06l-2.494-2.494a1.5 1.5 0 0 1-.445-1.052v-.028c.003-.371.142-.71.368-.97l.071-.077l2.5-2.5a1.5 1.5 0 0 1 2.18 2.061H13a5.5 5.5 0 0 0 5.5-5.5A1.5 1.5 0 0 1 20 9.5m-4.44-7.06l2.5 2.5a1.5 1.5 0 0 1 0 2.12l-2.5 2.5a1.5 1.5 0 0 1-2.178-2.06H11A5.5 5.5 0 0 0 5.5 13a1.5 1.5 0 1 1-3 0A8.5 8.5 0 0 1 11 4.5h2.382a1.5 1.5 0 0 1 2.179-2.06Z"/></g></svg>
                                </span>
                                <span class="mkts-refresh-label">Refresh all Markets</span>
                            </button>
                        </div>
                    </div>
            
                    <div class="mkts-table-wrapper">
                        <table class="mkts-table">
                            <thead>
                                <tr class="mkts-fake-header">
                                    <th class="mkts-col-drag"> </th>
                                    <th class="mkts-col-market">Waterfall</th>
                                    <th class="mkts-col-coverage">Cov.</th>
<!--                                    <th class="mkts-col-usage">Grid Usage</th>-->
                                    <th class="mkts-col-time">Last Mkt</th>
                                    <th class="mkts-col-time">Last Ref</th>
                                    <th class="mkts-col-refresh"></th>
                                </tr>
<!--                                <tr class="mkts-real-header">-->
<!--                                    <th class="mkts-col-drag"> </th>-->
<!--                                    <th class="mkts-col-market">Market Waterfall</th>-->
<!--                                    <th class="mkts-col-coverage">Coverage</th>-->
<!--                                    <th class="mkts-col-usage">Grid Usage</th>-->
<!--                                    <th class="mkts-col-time">Last Market Update</th>-->
<!--                                    <th class="mkts-col-time">Last Refresh</th>-->
<!--                                    <th class="mkts-col-refresh"></th>-->
<!--                                </tr>-->
                            </thead>
                            <tbody id="mkts-tbody"></tbody>
                        </table>
                    </div>
            
                    <div class="mkts-foot">
                        <div class="mkts-foot-left">
                            <div class="mkts-foot-pill">
                                Drag the handle to reorder markets.
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        `;
    }

    onResumeSubscriptions() {
        this.rebuildMarkets();
    }


    onActivate() {
        this.animateInitialCoverage();
    }

    setupReactions() {


    }

    async onCleanup() {

    }
    
    // --- heleprs ---
     ensureCoverageColorCache() {
        if (this.MKTS_COLOR_CACHE) return;
        let styles = window.getComputedStyle(this.container);
        this.MKTS_COLOR_CACHE = {
            coverageLow: styles.getPropertyValue("--mkts-color-coverage-low").trim(),
            coverageMid: styles.getPropertyValue("--mkts-color-coverage-mid").trim(),
            coverageHigh: styles.getPropertyValue("--mkts-color-coverage-high").trim()
        };
    }

     coverageColor(percent) {
        this.ensureCoverageColorCache();
        if (percent < 50) return this.MKTS_COLOR_CACHE.coverageLow;
        if (percent < 80) return this.MKTS_COLOR_CACHE.coverageMid;
        return this.MKTS_COLOR_CACHE.coverageHigh;
    }

     isSameDay(date, reference) {
        return (
            date.getFullYear() === reference.getFullYear() &&
            date.getMonth() === reference.getMonth() &&
            date.getDate() === reference.getDate()
        );
    }

     formatDisplayDateTime(date) {
        if (!(date instanceof Date)) return "-";
        let now = new Date();
        if (this.isSameDay(date, now)) {
            return date.toLocaleTimeString([], {
                hour: "numeric",
                minute: "2-digit",
                second: "2-digit",
                hour12: true
            });
        }
        return date.toLocaleString([], {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "numeric",
            minute: "2-digit",
            second: "2-digit",
            hour12: true
        });
    }

     computeRelativeTimeLabel(date, now) {
        let result = { text: "Never", isJustNow: false, isError: false };
        if (!date) return result;
        let diffMs = now.getTime() - date.getTime();
        if (diffMs < 0) diffMs = 0;
        let diffSec = Math.floor(diffMs / 1000);

        if (diffSec < 60) {
            result.text = "Just Now";
            result.isJustNow = true;
            return result;
        }

        let diffMin = Math.floor(diffSec / 60);
        if (diffMin < 60) {
            result.text = diffMin + "m ago";
            return result;
        }

        let diffHour = Math.floor(diffMin / 60);
        if (diffHour < 24) {
            result.text = ">1hr ago";
            return result;
        }

        result.text = ">1 day ago";
        return result;
    }

    loadOrderFromStorage() {
        try {
            if (!window.localStorage) return;
            let raw = window.localStorage.getItem(this.MKTS_STORAGE_KEY);
            if (!raw) return;
            let ids = JSON.parse(raw);
            if (!Array.isArray(ids) || !ids.length) return;

            let idToMarket = {};
            let i;
            for (i = 0; i < this.markets.length; i++) {
                idToMarket[this.markets[i].value] = this.markets[i];
            }
            let ordered = [];
            for (i = 0; i < ids.length; i++) {
                let m = idToMarket[ids[i]];
                if (m) {
                    ordered.push(m);
                    delete idToMarket[ids[i]];
                }
            }
            for (let key in idToMarket) {
                if (Object.prototype.hasOwnProperty.call(idToMarket, key)) {
                    ordered.push(idToMarket[key]);
                }
            }
            this.markets = ordered;
        } catch (e) {
            /* ignore */
        }
    };

    saveOrderToStorage() {
        try {
            if (!window.localStorage) return;
            let ids = this.getOrder();
            window.localStorage.setItem(this.MKTS_STORAGE_KEY, JSON.stringify(ids));
        } catch (e) {
            /* ignore */
        }
    };

    buildRows(initial) {
        this.tbody.innerHTML = "";
        for (let i = 0; i < this.markets.length; i++) {
            let row = this.createRow(this.markets[i], i, initial);
            this.tbody.appendChild(row);
        }
    };

    createRow(market, index, initial) {
        let now = new Date();
        let tr = document.createElement("tr");
        tr.setAttribute("data-index", String(index));
        tr.setAttribute("data-market-id", market.value);
        tr.className = "mkts-row";

        let tdDrag = document.createElement("td");
        tdDrag.className = "mkts-col-drag";
        let dragHandle = document.createElement("div");
        dragHandle.className = "mkts-drag-handle";
        dragHandle.setAttribute("data-handle", "true");
        let dragIcon = document.createElement("span");
        dragIcon.className = "mkts-drag-handle-icon";
        dragIcon.textContent = "⋮⋮";
        dragHandle.appendChild(dragIcon);
        tdDrag.appendChild(dragHandle);
        tr.appendChild(tdDrag);

        let tdMarket = document.createElement("td");
        tdMarket.className = "mkts-col-market";
        let nameDiv = document.createElement("div");
        nameDiv.className = "mkts-market-name";
        nameDiv.textContent = market.abbr || market.label;
        let subDiv = document.createElement("div");
        subDiv.className = "mkts-market-sub";
        subDiv.textContent = ''
        tdMarket.appendChild(nameDiv);
        tdMarket.appendChild(subDiv);
        tr.appendChild(tdMarket);

        let tdCoverage = document.createElement("td");
        tdCoverage.className = "mkts-col-coverage";
        let covWrap = document.createElement("div");
        covWrap.className = "mkts-coverage-wrap";
        let bar = document.createElement("div");
        bar.className = "mkts-coverage-bar";
        let fill = document.createElement("div");
        fill.className = "mkts-coverage-fill";
        let clampedCoverage = Math.max(0, Math.min(100, market.coverage));
        fill.style.backgroundColor = this.coverageColor(clampedCoverage);
        if (initial) {
            fill.style.width = "0%";
            fill.setAttribute("data-target-width", String(clampedCoverage));
        } else {
            fill.style.width = clampedCoverage + "%";
        }
        bar.appendChild(fill);
        let covText = document.createElement("div");
        covText.className = "mkts-coverage-text";
        covText.textContent = market.coverage.toFixed(0) + "%";
        covWrap.appendChild(covText);
        covWrap.appendChild(bar);
        tdCoverage.appendChild(covWrap);
        tr.appendChild(tdCoverage);

        // let tdUsage = document.createElement("td");
        // tdUsage.className = "mkts-col-usage";
        // let usageDiv = document.createElement("div");
        // usageDiv.className = "mkts-usage-text";
        // if (market.value === 'Dynamic') {
        //     usageDiv.textContent = "---"
        // } else {
        //     usageDiv.textContent = market.usage.toFixed(0) + "%";
        // }
        // tdUsage.appendChild(usageDiv);
        // tr.appendChild(tdUsage);

        let tdUpdate = document.createElement("td");
        tdUpdate.className = "mkts-col-time";
        let updateLabel = document.createElement("span");
        updateLabel.className = "mkts-time-label";
        // updateLabel.textContent = "UPDATE";
        let updateTime = document.createElement("span");
        updateTime.className = "mkts-time";
        updateTime.setAttribute("data-time-kind", "update");
        updateTime.textContent = this.formatDisplayDateTime(market.lastUpdate);
        let updateRel = document.createElement("span");
        updateRel.className = "mkts-time-relative";
        updateRel.setAttribute("data-time-kind", "update-relative");
        let updateRelData = this.computeRelativeTimeLabel(market.lastUpdate, now);
        updateRel.textContent = updateRelData.text;
        if (updateRelData.isJustNow) {
            updateRel.classList.add("mkts-time-relative--just-now");
        }
        tdUpdate.appendChild(updateLabel);
        tdUpdate.appendChild(updateTime);
        tdUpdate.appendChild(updateRel);
        tr.appendChild(tdUpdate);

        let tdRefresh = document.createElement("td");
        tdRefresh.className = "mkts-col-time";
        let refreshLabel = document.createElement("span");
        refreshLabel.className = "mkts-time-label";
        // refreshLabel.textContent = "REFRESH";
        let refreshTime = document.createElement("span");
        refreshTime.className = "mkts-time";
        refreshTime.setAttribute("data-time-kind", "refresh");
        refreshTime.textContent = this.formatDisplayDateTime(market.lastRefresh);
        let refreshRel = document.createElement("span");
        refreshRel.className = "mkts-time-relative";
        refreshRel.setAttribute("data-time-kind", "refresh-relative");
        let refreshRelData = this.computeRelativeTimeLabel(market.lastRefresh, now);
        refreshRel.textContent = refreshRelData.text;
        if (refreshRelData.isJustNow) {
            refreshRel.classList.add("mkts-time-relative--just-now");
        }else if (refreshRelData.isError) {
            refreshRel.classList.add("mkts-time-relative--error");
        }
        tdRefresh.appendChild(refreshLabel);
        tdRefresh.appendChild(refreshTime);
        tdRefresh.appendChild(refreshRel);
        tr.appendChild(tdRefresh);

        let tdAction = document.createElement("td");
        tdAction.className = "mkts-col-refresh";

        let btn = document.createElement("button");
        btn.className = "mkts-refresh-btn";
        btn.setAttribute("data-refresh", "single");

        btn.disabled = true;
        btn.classList.add('mkts-refresh-disabled')

        btn.setAttribute("data-market-id", market.value);
        let dot = document.createElement("span");
        dot.className = "mkts-refresh-dot";
        let label = document.createElement("span");
        label.className = "mkts-refresh-label-row";
        label.textContent = "Refresh";
        btn.appendChild(dot);
        btn.appendChild(label);
        tdAction.appendChild(btn);
        tr.appendChild(tdAction);

        return tr;
    };

    animateInitialCoverage() {
        let self = this;
        if (this.initialAnimated) return;
        this.initialAnimated = true;

        requestAnimationFrame(function () {
            let fills = self.tbody.querySelectorAll(".mkts-coverage-fill");
            for (let i = 0; i < fills.length; i++) {
                let fill = fills[i];
                let target = fill.getAttribute("data-target-width");
                if (target !== null) {
                    fill.style.width = target + "%";
                    fill.removeAttribute("data-target-width");
                }
            }
        });
    };

    updateGlobalRefreshState() {
        let anyRefreshing = false;
        for (let i = 0; i < this.markets.length; i++) {
            if (this.markets[i].isRefreshing) {
                anyRefreshing = true;
                break;
            }
        }
        const btn = this.refreshAllButton
        if (!btn) return;

        btn.disabled = !this.enabled;

        if (anyRefreshing) {
            btn.classList.add("mkts-refresh-all--loading");
        } else {
            btn.classList.remove("mkts-refresh-all--loading");
        }
    };

    findMarketById(id) {
        for (let i = 0; i < this.markets.length; i++) {
            if (this.markets[i].value === id) return this.markets[i];
        }
        return null;
    };

    refreshSingleMarketById(marketId) {
        let market = this.findMarketById(marketId);
        if (!market) return;
        this.scheduleRefresh(market.value, "single");
    };

    async refreshAllMarkets() {
        // for (let i = 0; i < this.markets.length; i++) {
        //     const mkt = this.markets[i];
        //     if (!mkt.isRefreshing) {
        //         this.scheduleRefresh(this.markets[i].value, "bulk");
        //     }
        // }

        const roomContext = this.context.page._ptRaw.context;
        const sm = this.context.page.socketManager();
        await sm._sendWebSocketMessage({
            action: ACTION_MAP.get("refresh"),
            context: roomContext,
            options: { broadcast: false, log: false }
        });
        this.logAction("Started bulk refresh");
    };

    scheduleRefresh(marketId, mode) {
        let self = this;
        let market = this.findMarketById(marketId);
        if (!market) return;
        if (market.isRefreshing) return;

        market.isRefreshing = true;
        this.updateRowForMarket(market);
        this.updateGlobalRefreshState();
    };

    updateRowForMarket(market) {
        let row = this.tbody.querySelector("tr[data-market-id='" + market.value + "']");
        if (!row) return;

        if (market.isRefreshing) {
            row.classList.add("mkts-row-refreshing");
        } else {
            row.classList.remove("mkts-row-refreshing");
        }

        let coverageFill = row.querySelector(".mkts-coverage-fill");
        let coverageText = row.querySelector(".mkts-coverage-text");
        // let usageText = row.querySelector(".mkts-usage-text");
        let updateTimeEl = row.querySelector(".mkts-time[data-time-kind='update']");
        let updateRelEl = row.querySelector(".mkts-time-relative[data-time-kind='update-relative']");
        let refreshTimeEl = row.querySelector(".mkts-time[data-time-kind='refresh']");
        let refreshRelEl = row.querySelector(".mkts-time-relative[data-time-kind='refresh-relative']");
        let btn = row.querySelector(".mkts-refresh-btn");

        let clampedCoverage = Math.max(0, Math.min(100, market.coverage));

        if (market.isRefreshing) {
            if (btn) {
                btn.disabled = true;
                btn.classList.add("mkts-refresh-btn--loading");
                btn.textContent = "Loading...";
            }
        } else {
            if (btn) {
                btn.disabled = false;
                btn.classList.remove("mkts-refresh-btn--loading");
                btn.textContent = "Refresh";
            }
        }

        if (!market.isRefreshing) {
            let now = new Date();

            if (coverageFill) {
                let previousWidth = parseFloat(coverageFill.style.width);
                if (isNaN(previousWidth)) {
                    previousWidth = clampedCoverage;
                }
                if (previousWidth !== clampedCoverage) {
                    coverageFill.style.transition = "none";
                    coverageFill.style.width = previousWidth + "%";
                    void coverageFill.offsetHeight;
                    coverageFill.style.transition = "";
                    coverageFill.style.width = clampedCoverage + "%";
                } else {
                    coverageFill.style.width = clampedCoverage + "%";
                }
                coverageFill.style.backgroundColor = this.coverageColor(clampedCoverage);
            }

            if (coverageText) {
                coverageText.textContent = market.coverage.toFixed(0) + "%";
            }
            // if (usageText) {
            //     if (market.value === "Dynamic") {
            //         usageText.textContent = "---"
            //     } else {
            //         usageText.textContent = market.usage.toFixed(0) + "%";
            //     }
            //
            // }
            if (updateTimeEl) {
                updateTimeEl.textContent = this.formatDisplayDateTime(market.lastUpdate);
            }

            if (updateRelEl) {
                let uRel = this.computeRelativeTimeLabel(market.lastUpdate, now);
                updateRelEl.textContent = uRel.text;
                if (uRel.isJustNow) {
                    updateRelEl.classList.add("mkts-time-relative--just-now");
                } else {
                    updateRelEl.classList.remove("mkts-time-relative--just-now");
                }
            }
            if (refreshTimeEl) {
                refreshTimeEl.textContent = market.lastRefresh ? this.formatDisplayDateTime(market.lastRefresh) : "—";
            }
            if (refreshRelEl) {
                let rRel = this.computeRelativeTimeLabel(market.lastRefresh, now);
                refreshRelEl.textContent = rRel.text;
                if (rRel.isJustNow) {
                    refreshRelEl.classList.add("mkts-time-relative--just-now");
                    refreshRelEl.classList.remove("mkts-time-relative--error");
                } else if (rRel.isError) {
                    refreshRelEl.classList.remove("mkts-time-relative--just-now");
                    refreshRelEl.classList.add("mkts-time-relative--error");
                } else {
                    refreshRelEl.classList.remove("mkts-time-relative--just-now");
                    refreshRelEl.classList.remove("mkts-time-relative--error");
                }
            }
        }
    };

    updateAllRelativeTimes() {
        let now = new Date();
        for (let i = 0; i < this.markets.length; i++) {
            let market = this.markets[i];
            let row = this.tbody.querySelector("tr[data-market-id='" + market.value + "']");
            if (!row) continue;

            let updateRelEl = row.querySelector(".mkts-time-relative[data-time-kind='update-relative']");
            let refreshRelEl = row.querySelector(".mkts-time-relative[data-time-kind='refresh-relative']");

            if (updateRelEl) {
                let uRel = this.computeRelativeTimeLabel(market.lastUpdate, now);
                updateRelEl.textContent = uRel.text;
                if (uRel.isJustNow) {
                    updateRelEl.classList.add("mkts-time-relative--just-now");
                } else {
                    updateRelEl.classList.remove("mkts-time-relative--just-now");
                }
            }

            if (refreshRelEl) {
                let rRel = this.computeRelativeTimeLabel(market.lastRefresh, now);
                refreshRelEl.textContent = rRel.text;
                if (rRel.isJustNow) {
                    refreshRelEl.classList.add("mkts-time-relative--just-now");
                    refreshRelEl.classList.remove("mkts-time-relative--error");
                } else if (rRel.isError) {
                    refreshRelEl.classList.add("mkts-time-relative--error");
                    refreshRelEl.classList.remove("mkts-time-relative--just-now");
                } else {
                    refreshRelEl.classList.remove("mkts-time-relative--error");
                    refreshRelEl.classList.remove("mkts-time-relative--just-now");
                }
            }
        }
    };

    reorderMarkets(fromIndex, toIndex) {
        if (fromIndex < 0 || toIndex < 0 ||
            fromIndex >= this.markets.length || toIndex >= this.markets.length) {
            return;
        }
        if (fromIndex === toIndex) return;
        let moving = this.markets.splice(fromIndex, 1)[0];
        this.markets.splice(toIndex, 0, moving);
    };

    rebuildRowsAfterOrderChange() {
        this.buildRows(false);
        for (let i = 0; i < this.markets.length; i++) {
            this.updateRowForMarket(this.markets[i]);
        }
        this.updateGlobalRefreshState();
    };

    getOrder() {
        let ids = [];
        for (let i = 0; i < this.markets.length; i++) {
            ids.push(this.markets[i].id);
        }
        return ids;
    };

    setOrder(ids) {
        if (!ids || !ids.length) return;

        let idToMarket = {};
        let i;
        for (i = 0; i < this.markets.length; i++) {
            idToMarket[this.markets[i].id] = this.markets[i];
        }

        let ordered = [];
        for (i = 0; i < ids.length; i++) {
            let m = idToMarket[ids[i]];
            if (m) {
                ordered.push(m);
                delete idToMarket[ids[i]];
            }
        }

        for (let key in idToMarket) {
            if (Object.prototype.hasOwnProperty.call(idToMarket, key)) {
                ordered.push(idToMarket[key]);
            }
        }

        this.markets = ordered;
        this.saveOrderToStorage();
        this.rebuildRowsAfterOrderChange();
    };

    addMarket(data) {
        let now = new Date();
        data = data || {};

        let market = {
            value: data.value ? String(data.value) : ("mkt-" + (this.markets.length + 1) + "-" + now.getTime()),
            abbr: data.abbr ? String(data.abbr) : "New Market",
            coverage: typeof data.coverage === "number" ? Math.max(0, Math.min(100, data.coverage)) : 0,
            // usage: typeof data.usage === "number" ? Math.max(0, Math.min(100, data.usage)) : 0,
            lastUpdate: data.lastUpdate instanceof Date
                ? data.lastUpdate
                : (data.lastUpdate ? new Date(data.lastUpdate) : now),
            lastRefresh: data.lastRefresh instanceof Date
                ? data.lastRefresh
                : (data.lastRefresh ? new Date(data.lastRefresh) : null),
            isRefreshing: false
        };

        this.markets.push(market);
        // this.saveOrderToStorage();
        this.rebuildRowsAfterOrderChange();
        return market.value;
    };

    deleteMarket(id) {
        for (let i = 0; i < this.markets.length; i++) {
            if (this.markets[i].value === id) {
                this.markets.splice(i, 1);
                this.saveOrderToStorage();
                this.rebuildRowsAfterOrderChange();
                return;
            }
        }
    };

    getMarketTimes(id) {
        let market = this.findMarketById(id);
        if (!market) return null;
        let now = new Date();
        let upd = this.computeRelativeTimeLabel(market.lastUpdate, now);
        let ref = this.computeRelativeTimeLabel(market.lastRefresh, now);
        return {
            lastUpdate: market.lastUpdate,
            lastRefresh: market.lastRefresh,
            relativeUpdate: upd.text,
            relativeRefresh: ref.text
        };
    };

    setEnabled(enabled) {
        this.enabled = !!enabled;
        if (this.enabled) {
            this.containerElement.classList.remove("mkts-disabled");
        } else {
            this.containerElement.classList.add("mkts-disabled");
        }
        this.updateGlobalRefreshState();
    };

    logAction(message) {
        let now = new Date();
        this.lastActionElement.textContent = message + " @ " + this.formatDisplayDateTime(now);
    };


    setupRowDragging() {
        if (typeof interact === "undefined") {
            return;
        }

        const tableInstance = this;
        interact("#mkts-tbody tr.mkts-row").draggable({
            allowFrom: ".mkts-drag-handle",
            listeners: {
                start: function (event) {
                    let target = event.target.closest("tr.mkts-row");
                    if (!target) return;
                    target.classList.add("mkts-row-dragging");
                    target.dataset.y = "0";
                    target.dataset.startIndex = target.getAttribute("data-index") || "0";
                    target.dataset.dropIndex = target.dataset.startIndex;
                },
                move: function (event) {
                    let target = event.target.closest("tr.mkts-row");
                    if (!target) return;

                    let currentY = parseFloat(target.dataset.y || "0");
                    currentY += event.dy;
                    target.dataset.y = String(currentY);
                    target.style.transform = "translateY(" + currentY + "px)";

                    let rows = Array.prototype.slice.call(
                        tableInstance.tbody.querySelectorAll("tr.mkts-row")
                    );
                    let rect = target.getBoundingClientRect();
                    let midY = rect.top + rect.height / 2;

                    let closestRow = null;
                    let closestDistance = Infinity;

                    for (let i = 0; i < rows.length; i++) {
                        let row = rows[i];
                        if (row === target) continue;
                        let r = row.getBoundingClientRect();
                        let center = r.top + r.height / 2;
                        let distance = Math.abs(midY - center);
                        if (distance < closestDistance) {
                            closestDistance = distance;
                            closestRow = row;
                        }
                    }

                    rows.forEach(function (row) {
                        row.classList.remove("mkts-row-drop-target");
                    });

                    if (closestRow) {
                        closestRow.classList.add("mkts-row-drop-target");
                        target.dataset.dropIndex = closestRow.getAttribute("data-index") || "0";
                    } else {
                        target.dataset.dropIndex = target.dataset.startIndex || "0";
                    }
                },
                end: function (event) {
                    let target = event.target.closest("tr.mkts-row");
                    if (!target) return;

                    let fromIndex = parseInt(target.dataset.startIndex || "0", 10);
                    let toIndex = parseInt(target.dataset.dropIndex || String(fromIndex), 10);

                    target.classList.remove("mkts-row-dragging");
                    target.style.transform = "";
                    target.dataset.y = "0";

                    let rows = Array.prototype.slice.call(
                        tableInstance.tbody.querySelectorAll("tr.mkts-row")
                    );
                    rows.forEach(function (row) {
                        row.classList.remove("mkts-row-drop-target");
                    });

                    if (!isNaN(fromIndex) && !isNaN(toIndex) && fromIndex !== toIndex) {
                        tableInstance.reorderMarkets(fromIndex, toIndex);
                        //tableInstance.saveOrderToStorage();
                        tableInstance.rebuildRowsAfterOrderChange();
                        tableInstance.logAction("Row order updated");
                    }
                }
            },
            inertia: false
        });
    }


    // -- API ---
    api() {
        const table = this;
        return {
            getOrder: function () {
                return table.getOrder();
            },
            setOrder: function (order) {
                table.setOrder(order);
            },
            addMarket: function (market) {
                return table.addMarket(market);
            },
            deleteMarket: function (id) {
                table.deleteMarket(id);
            },
            getTimes: function (id) {
                return table.getMarketTimes(id);
            },
            triggerRefresh: function (id) {
                table.refreshSingleMarketById(id);
            },
            triggerRefreshAll: function () {
                table.refreshAllMarkets();
            },
            enable: function () {
                table.setEnabled(true);
            },
            disable: function () {
                table.setEnabled(false);
            }
        }
    }
}

