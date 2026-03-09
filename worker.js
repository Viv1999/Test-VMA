/** worker.js - v3.1 Perfect Additive Attribution **/
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
    const periods = timeMap.map(() => ({ hPool: new Map(), pres: 0, ext: 0, acc: 0, offerStats: {}, typeStats: {} }));
    const buIdx = buMap.indexOf(f.bu), sIdx = new Set(f.sites.map(s => siteMap.indexOf(s))), tIdx = new Set(f.tiers.map(t => tierMap.indexOf(t)));
    const typeExclSet = new Set(exclTypes);

    for (let i = 0; i < rowCount; i++) {
        if (buCol[i] !== buIdx || !sIdx.has(siteCol[i]) || !tIdx.has(tierCol[i])) continue;
        
        const p = periods[timeCol[i]], v = countCol[i];
        const poolKey = `${buCol[i]}-${tierCol[i]}-${siteCol[i]}`;
        if (!p.hPool.has(poolKey)) p.hPool.set(poolKey, handledCol[i]);

        const isExcl = (o) => exclOff.includes(o) || typeExclSet.has(o.split('-')[0].trim());

        const rP = presData[i].split('|').filter(o => o && !isExcl(o));
        const rE = extData[i].split('|').filter(o => o && !isExcl(o));
        const rA = accData[i].split('|').filter(o => o && !isExcl(o));

        if (rP.length > 0) p.pres += v;
        if (rE.length > 0) p.ext += v;
        if (rA.length > 0) p.acc += v;

        rP.forEach(off => {
            const type = off.split('-')[0].trim();
            const weight = v / rP.length;

            if (!p.offerStats[off]) p.offerStats[off] = { n: 0 };
            if (!p.typeStats[type]) p.typeStats[type] = { n: 0 };

            let success = (metricKey === 'eligRate') || 
                          (metricKey === 'offRate' && rE.includes(off)) || 
                          (metricKey === 'accRate' && rA.includes(off)) || 
                          (metricKey === 'convRate' && rE.includes(off) && rA.includes(off));
            
            if (success) {
                p.offerStats[off].n += weight;
                p.typeStats[type].n += weight;
            }
        });
    }

    return periods.map(p => {
        const h = Array.from(p.hPool.values()).reduce((a, b) => a + b, 0) || 1;
        return { 
            eligRate: (p.pres/h)*100||0, offRate: (p.ext/p.pres)*100||0, 
            accRate: (p.acc/p.pres)*100||0, convRate: (p.acc/p.ext)*100||0, 
            pres: p.pres, ext: p.ext, h, 
            offerStats: p.offerStats, typeStats: p.typeStats 
        };
    });
}