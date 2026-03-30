/** worker.js - v3.2 Dual Aggregation & Attribution **/
const MAX_ROWS = 1000000;
const timeCol = new Uint32Array(MAX_ROWS), buCol = new Uint8Array(MAX_ROWS), 
      tierCol = new Uint8Array(MAX_ROWS), siteCol = new Uint8Array(MAX_ROWS);
const handledCol = new Uint32Array(MAX_ROWS), countCol = new Uint32Array(MAX_ROWS);
const presData = new Array(MAX_ROWS), extData = new Array(MAX_ROWS), accData = new Array(MAX_ROWS);

let timeMap = [], buMap = [], tierMap = [], siteMap = [], rowCount = 0;

self.onmessage = (e) => {
    try {
        const { type, data, filters, exclOff, exclTypes, metricKey } = e.data;

        if (type === 'INIT_DATA') {
            rowCount = 0; timeMap = []; buMap = []; tierMap = []; siteMap = [];
            const getIdx = (v, m) => { 
                let clean = String(v || "Unknown").trim(); 
                let i = m.indexOf(clean); 
                if (i === -1) { m.push(clean); return m.length - 1; } 
                return i; 
            };

            data.forEach(row => {
                timeCol[rowCount] = getIdx(row.time, timeMap);
                buCol[rowCount] = getIdx(row.bu, buMap);
                tierCol[rowCount] = getIdx(row.tier, tierMap);
                siteCol[rowCount] = getIdx(row.site, siteMap);
                handledCol[rowCount] = row.handled;
                countCol[rowCount] = row.count;
                presData[rowCount] = String(row.pres || "");
                extData[rowCount] = String(row.ext || "");
                accData[rowCount] = String(row.acc || "");
                rowCount++;
            });

            const offers = new Set();
            for(let i=0; i<rowCount; i++) {
                if(presData[i]) presData[i].split('|').forEach(o => offers.add(o.trim()));
            }

            self.postMessage({ 
                type: 'READY', 
                timeMap, 
                filters: { bus: buMap, tiers: tierMap, sites: siteMap, offers: Array.from(offers).sort() } 
            });
        }

        if (type === 'PROCESS') {
            const base = calculate([], [], filters, metricKey);
            const impact = calculate(exclOff, exclTypes, filters, metricKey);
            self.postMessage({ type: 'DONE', base, impact });
        }
    } catch (err) { console.error("Worker Error:", err); }
};

function calculate(exclOff, exclTypes, f, metricKey) {
    const periods = timeMap.map(() => ({ 
        hPool: new Map(), 
        presVol: 0, 
        extVol: 0, 
        accVol: 0, 
        offerStats: {}, 
        typeStats: {} 
    }));

    const buIdx = buMap.indexOf(f.bu);
    const sIdx = new Set(f.sites.map(s => siteMap.indexOf(s)));
    const tIdx = new Set(f.tiers.map(t => tierMap.indexOf(t)));
    const typeExclSet = new Set(exclTypes);

    for (let i = 0; i < rowCount; i++) {
        // 1. Segment Filtering
        if (buCol[i] !== buIdx || !sIdx.has(siteCol[i]) || !tIdx.has(tierCol[i])) continue;
        
        const p = periods[timeCol[i]];
        const rowVol = countCol[i]; // This is your 'calls' column
        
        // 2. Denominator Handling (Unique sum of calls_handled per segment)
        const poolKey = `${buCol[i]}-${tierCol[i]}-${siteCol[i]}`;
        if (!p.hPool.has(poolKey)) {
            p.hPool.set(poolKey, handledCol[i]);
        }

        // 3. Exclusion Filter
        const isExcl = (o) => exclOff.includes(o) || typeExclSet.has(o.split('-')[0].trim());
        
        // Check if row has at least one valid offer after exclusions
        const hasValidP = presData[i].split('|').some(o => o && !isExcl(o));
        const hasValidE = extData[i].split('|').some(o => o && !isExcl(o));
        const hasValidA = accData[i].split('|').some(o => o && !isExcl(o));

        // 4. Numerator Aggregation
        if (hasValidP) p.presVol += rowVol;
        if (hasValidE) p.extVol += rowVol;
        if (hasValidA) p.accVol += rowVol;

        // 5. Contribution / Driver Logic (Calculated on valid offers only)
        if (hasValidP) {
            const validOffersInRow = presData[i].split('|').filter(o => o && !isExcl(o));
            validOffersInRow.forEach(off => {
                const type = off.split('-')[0].trim();
                const weight = rowVol / validOffersInRow.length; // Spread row volume across valid offers
                
                if (!p.offerStats[off]) p.offerStats[off] = { n: 0 };
                if (!p.typeStats[type]) p.typeStats[type] = { n: 0 };

                // Success check based on active metric
                let success = (metricKey === 'eligRate') || 
                              (metricKey === 'offRate' && extData[i].includes(off)) || 
                              (metricKey === 'accRate' && accData[i].includes(off)) || 
                              (metricKey === 'convRate' && extData[i].includes(off) && accData[i].includes(off));
                
                if (success) {
                    p.offerStats[off].n += weight;
                    p.typeStats[type].n += weight;
                }
            });
        }
    }

    return periods.map(p => {
        const h = Array.from(p.hPool.values()).reduce((a, b) => a + b, 0) || 1;
        return { 
            eligRate: (p.presVol / h) * 100 || 0, 
            offRate: (p.extVol / p.presVol) * 100 || 0, 
            accRate: (p.accVol / p.presVol) * 100 || 0, 
            convRate: (p.accVol / p.extVol) * 100 || 0, 
            pres: p.presVol, 
            ext: p.extVol, 
            h: h 
        };
    });
}