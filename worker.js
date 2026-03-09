/** worker.js - v2.7 Scaling & Attribution **/
const MAX_ROWS = 1000000;
const timeCol = new Uint32Array(MAX_ROWS), buCol = new Uint8Array(MAX_ROWS), 
      tierCol = new Uint8Array(MAX_ROWS), siteCol = new Uint8Array(MAX_ROWS);
const handledCol = new Uint32Array(MAX_ROWS), countCol = new Uint32Array(MAX_ROWS);
const presData = new Array(MAX_ROWS), extData = new Array(MAX_ROWS), accData = new Array(MAX_ROWS);

let timeMap = [], buMap = [], tierMap = [], siteMap = [], rowCount = 0;

// Inside worker.js
self.onmessage = (e) => {
    try {
        const { type, data } = e.data;
        if (type === 'INIT_DATA') {
            console.log("Worker: Starting to process " + data.length + " rows");
            
            // RESET rowCount to avoid overflow on re-loads
            rowCount = 0; 

            for (let i = 0; i < data.length; i++) {
                const row = data[i];
                if (!row) continue;

                // PROTECTIVE MAPPING
                timeCol[rowCount] = getIdx(row.time || "Unknown", timeMap);
                buCol[rowCount] = getIdx(row.bu || "Unknown", buMap);
                tierCol[rowCount] = getIdx(row.tier || "Unknown", tierMap);
                siteCol[rowCount] = getIdx(row.site || "Unknown", siteMap);
                
                handledCol[rowCount] = Number(row.handled) || 0;
                countCol[rowCount] = Number(row.count) || 0;
                
                // Ensure strings exist before splitting
                presData[rowCount] = String(row.pres || "");
                extData[rowCount] = String(row.ext || "");
                accData[rowCount] = String(row.acc || "");
                
                rowCount++;
            }
            
            console.log("Worker: Mapping Complete. RowCount: " + rowCount);

            // Signal the UI to hide the loader
            self.postMessage({ 
                type: 'READY', 
                timeMap: timeMap, 
                filters: { bus: buMap, tiers: tierMap, sites: siteMap, offers: extractUniqueOffers() } 
            });
        }
    } catch (err) {
        // Send the error back to the main console
        self.postMessage({ type: 'ERROR', message: err.message });
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

    // Inside worker.js calculateTrend function:
return periods.map(p => {
    // Total handled pool is the sum of unique calls_handled per category
    const handled = Array.from(p.handledPool.values()).reduce((a, b) => a + b, 0);
    
    return { 
        eligRate: (p.pres / handled) * 100 || 0,        // Eligibility
        offRate:  (p.ext / p.pres) * 100 || 0,         // Offer Rate
        accRate:  (p.acc / p.pres) * 100 || 0,         // Accept Rate
        convRate: (p.acc / p.ext) * 100 || 0,          // Conversion
        offerStats: p.offerStats,
        h: handled
    };
});
}