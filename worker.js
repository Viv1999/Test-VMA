/** worker.js - v2.6 Perfect Attribution (Pool-Based Denominators) **/
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
            handledCol[rowCount] = row.handled;
            countCol[rowCount] = row.count;
            presData[rowCount] = row.pres;
            extData[rowCount] = row.ext;
            accData[rowCount] = row.acc;
            rowCount++;
        });

        const offers = new Set();
        data.forEach(r => r.pres && r.pres.split('|').forEach(o => offers.add(o.trim())));

        self.postMessage({ type: 'READY', timeMap, filters: { bus: buMap, tiers: tierMap, sites: siteMap, offers: Array.from(offers).sort() } });
    }

    if (type === 'RUN_CALC') {
        const results = calculateTrend(exclOff, filters);
        self.postMessage({ type: 'CALC_DONE', results });
    }
};

function calculateTrend(exclOff, f) {
    const periods = timeMap.map(() => ({ handledPool: new Map(), pres: 0, ext: 0, acc: 0 }));
    const buIdx = buMap.indexOf(f.bu), sIdx = new Set(f.sites.map(s => siteMap.indexOf(s))), tIdx = new Set(f.tiers.map(t => tierMap.indexOf(t)));

    for (let i = 0; i < rowCount; i++) {
        if (buCol[i] !== buIdx || !sIdx.has(siteCol[i]) || !tIdx.has(tierCol[i])) continue;
        const p = periods[timeCol[i]];
        const poolKey = `${buCol[i]}-${tierCol[i]}-${siteCol[i]}`;
        
        if (!p.handledPool.has(poolKey)) p.handledPool.set(poolKey, handledCol[i]);

        const rowPres = presData[i].split('|').filter(o => o && !exclOff.includes(o));
        const rowExt = extData[i].split('|').filter(o => o && !exclOff.includes(o));
        const rowAcc = accData[i].split('|').filter(o => o && !exclOff.includes(o));

        if (rowPres.length > 0) p.pres += countCol[i];
        if (rowExt.length > 0)  p.ext += countCol[i];
        if (rowAcc.length > 0)  p.acc += countCol[i];
    }

    return periods.map(p => {
        const handled = Array.from(p.handledPool.values()).reduce((a, b) => a + b, 0);
        return { 
            elig: (p.pres/handled)*100||0, off: (p.ext/p.pres)*100||0, 
            acc: (p.acc/p.pres)*100||0, conv: (p.acc/p.ext)*100||0,
            raw: { handled, pres: p.pres, ext: p.ext, acc: p.acc }
        };
    });
}