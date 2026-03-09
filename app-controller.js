const DashboardState = {
    weekly: { data: [], loaded: false },
    monthly: { data: [], loaded: false }
};

// EXPLICITLY MAKE GLOBAL
window.DashboardState = DashboardState;

async function startDataPipeline() {
    try {
        // Load Weekly
        DashboardState.weekly.data = await parseCSV('weekly_aggregated.csv');
        DashboardState.weekly.loaded = true;
        window.dispatchEvent(new CustomEvent('weeklyDataReady'));

        // Load Monthly in background
        const files = ['2026-01_aggregated.csv', '2026-02_aggregated.csv', '2026-03_aggregated.csv', '2026-04_aggregated.csv'];
        const results = await Promise.all(files.map(f => parseCSV(f)));
        DashboardState.monthly.data = results.flat();
        DashboardState.monthly.loaded = true;
        window.dispatchEvent(new CustomEvent('monthlyDataReady'));
    } catch (e) { console.error("Pipeline Failed", e); }
}

async function parseCSV(url) {
    const res = await fetch(url);
    const text = await res.text();
    const rows = text.split('\n').filter(r => r.trim());
    const h = rows[0].toLowerCase().split(',');

    // Map columns by name because Weekly and Monthly differ
    const map = {
        time: h.findIndex(x => x.includes('week') || x.includes('month')),
        bu: h.indexOf('bu'),
        tier: h.indexOf('tier'),
        site: h.indexOf('site'),
        handled: h.indexOf('calls_handled'),
        pres: h.indexOf('offers_present'),
        ext: h.indexOf('offers_extended'),
        acc: h.indexOf('offers_accepted'),
        count: h.indexOf('calls')
    };

    return rows.slice(1).map(r => {
        const c = r.split(',');
        return {
            time: c[map.time],
            bu: c[map.bu],
            tier: c[map.tier],
            site: c[map.site],
            handled: parseInt(c[map.handled]) || 0,
            pres: c[map.pres] || "",
            ext: c[map.ext] || "",
            acc: c[map.acc] || "",
            count: parseInt(c[map.count]) || 0
        };
    });
}
document.addEventListener('DOMContentLoaded', startDataPipeline);