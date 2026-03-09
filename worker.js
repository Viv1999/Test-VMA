/** worker.js - v2.7 Scaling & Attribution **/
const MAX_ROWS = 1000000;
const timeCol = new Uint32Array(MAX_ROWS), buCol = new Uint8Array(MAX_ROWS), 
      tierCol = new Uint8Array(MAX_ROWS), siteCol = new Uint8Array(MAX_ROWS);
const handledCol = new Uint32Array(MAX_ROWS), countCol = new Uint32Array(MAX_ROWS);
const presData = new Array(MAX_ROWS), extData = new Array(MAX_ROWS), accData = new Array(MAX_ROWS);

let timeMap = [], buMap = [], tierMap = [], siteMap = [], rowCount = 0;

self.onmessage = (e) => {
    const { type, data, filters, exclOff, metricKey } = e.data;

    if (type === 'INIT_DATA') {
        rowCount = 0; timeMap = []; buMap = []; tierMap = []; siteMap = [];
        const getIdx = (v, m) => { let i = m.indexOf(v); if (i === -1) { m.push(v); return m.length - 1; } return i; };
        data.forEach(row => {
            timeCol[rowCount] = getIdx(row.time, timeMap);
            buCol[rowCount] = getIdx(row.bu, buMap);
            tierCol[rowCount] = getIdx(row.tier, tierMap);
            siteCol[rowCount] = getIdx(row.site, siteMap);
            handledCol[rowCount] = row.handled; countCol[rowCount] = row.count;
            presData[rowCount] = row.pres; extData[rowCount] = row.ext; accData[rowCount] = row.acc;
            rowCount++;
        });
        const offers = new Set();
        data.forEach(r => r.pres && r.pres.split('|').forEach(o => offers.add(o.trim())));
        self.postMessage({ type: 'READY', timeMap, filters: { bus: buMap, tiers: tierMap, sites: siteMap, offers: Array.from(offers).sort() } });
    }

    if (type === 'PROCESS') {
        const baseTrend = calculate( [], filters, metricKey);
        const simTrend = calculate(exclOff, filters, metricKey);
        self.postMessage({ type: 'DONE', base: baseTrend, impact: simTrend });
    }
};

function calculate(exclOff, f, metricKey) {
    const periods = timeMap.map(() => ({ 
        h: 0, elig: 0, ext: 0, acc: 0, 
        handledPool: new Map(), offerStats: {} 
    }));
    
    const buIdx = buMap.indexOf(f.bu), sIdx = new Set(f.sites.map(s => siteMap.indexOf(s))), tIdx = new Set(f.tiers.map(t => tierMap.indexOf(t)));

    for (let i = 0; i < rowCount; i++) {
        if (buCol[i] !== buIdx || !sIdx.has(siteCol[i]) || !tIdx.has(tierCol[i])) continue;
        
        const p = periods[timeCol[i]];
        const poolKey = `${buCol[i]}-${tierCol[i]}-${siteCol[i]}`;
        if (!p.handledPool.has(poolKey)) p.handledPool.set(poolKey, handledCol[i]);

        const v = countCol[i];
        const rowPres = presData[i].split('|').filter(o => o && !exclOff.includes(o));
        const rowExt = extData[i].split('|').filter(o => o && !exclOff.includes(o));
        const rowAcc = accData[i].split('|').filter(o => o && !exclOff.includes(o));

        if (rowPres.length > 0) p.elig += v;
        if (rowExt.length > 0)  p.ext += v;
        if (rowAcc.length > 0)  p.acc += v;

        // Attribution Logic for Mix-Rate Table
        rowPres.forEach(off => {
            if (!p.offerStats[off]) p.offerStats[off] = { n: 0, d: 0 };
            p.offerStats[off].d += v / rowPres.length; // Weighted Denominator
            
            let success = false;
            if (metricKey === 'eligRate') success = true;
            else if (metricKey === 'offRate') success = rowExt.includes(off);
            else if (metricKey === 'accRate') success = rowAcc.includes(off);
            else if (metricKey === 'convRate') success = rowExt.includes(off) && rowAcc.includes(off);
            
            if (success) p.offerStats[off].n += v / rowPres.length;
        });
    }

    return periods.map(p => {
        const hTotal = Array.from(p.handledPool.values()).reduce((a, b) => a + b, 0);
        return {
            eligRate: (p.elig/hTotal)*100||0, offRate: (p.ext/p.elig)*100||0,
            accRate: (p.acc/p.elig)*100||0, convRate: (p.acc/p.ext)*100||0,
            h: hTotal, elig: p.elig, ext: p.ext, acc: p.acc, offerStats: p.offerStats
        };
    });
}