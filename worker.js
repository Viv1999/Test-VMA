/** worker.js **/
const MAX_ROWS = 800000;
const mthCol = new Uint8Array(MAX_ROWS), buCol = new Uint8Array(MAX_ROWS), 
      tierCol = new Uint8Array(MAX_ROWS), siteCol = new Uint8Array(MAX_ROWS);
const handledCol = new Uint32Array(MAX_ROWS), countCol = new Uint32Array(MAX_ROWS);
const presData = new Array(MAX_ROWS), extData = new Array(MAX_ROWS), accData = new Array(MAX_ROWS);

let dateMap = [], buMap = [], tierMap = [], siteMap = [], rowCount = 0;

function generateFixedDummyData() {
    const bus = ["Mobile", "Broadband", "TV"];
    const tiers = ["Gold", "Silver", "Bronze"];
    const sites = ["Mumbai", "Delhi", "Bangalore"];
    const months = ["202501", "202502", "202503", "202504", "202505", "202506"];
    const offerTypes = ["Discount", "Cashback", "Loyalty", "Bundle"];
    
    // Step 1: Create a master volume map for segments
    // This ensures every row in 'Jan-Mobile-Gold-Mumbai' sees the same denominator
    const segmentVolumes = {};

    for (let i = 0; i < 100000; i++) {
        const m = months[Math.floor(Math.random() * months.length)];
        const b = bus[Math.floor(Math.random() * bus.length)];
        const t = tiers[Math.floor(Math.random() * tiers.length)];
        const s = sites[Math.floor(Math.random() * sites.length)];
        const segKey = `${m}-${b}-${t}-${s}`;

        if (!segmentVolumes[segKey]) {
            segmentVolumes[segKey] = 500 + Math.floor(Math.random() * 1000);
        }

        const getIdx = (v, map) => { 
            let idx = map.indexOf(v); 
            if (idx === -1) { map.push(v); return map.length - 1; } 
            return idx; 
        };

        mthCol[rowCount] = getIdx(m, dateMap);
        buCol[rowCount] = getIdx(b, buMap);
        tierCol[rowCount] = getIdx(t, tierMap);
        siteCol[rowCount] = getIdx(s, siteMap);
        
        // Use the consistent volume for this segment
        handledCol[rowCount] = segmentVolumes[segKey];

        // Logic: Total counts in a segment cannot exceed handled volume
        const type1 = offerTypes[Math.floor(Math.random() * offerTypes.length)];
        const type2 = offerTypes[Math.floor(Math.random() * offerTypes.length)];
        
        presData[rowCount] = `${type1}-Promo | ${type2}-Special`;
        extData[rowCount] = Math.random() > 0.4 ? `${type1}-Promo` : "";
        accData[rowCount] = Math.random() > 0.7 ? `${type1}-Promo` : "";
        
        countCol[rowCount] = 10 + Math.floor(Math.random() * 40);
        rowCount++;

        if (rowCount % 25000 === 0) self.postMessage({ type: 'PROGRESS', loaded: rowCount });
    }

    const mNames = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const dDates = dateMap.sort().map(v => `${mNames[parseInt(v.substring(4,6))]} ${v.substring(2,4)}`);
    
    let relations = {};
    for (let i = 0; i < rowCount; i++) {
        const bu = buMap[buCol[i]];
        if (!relations[bu]) relations[bu] = { tiers: new Set(), sites: new Set(), types: new Set(), offers: new Set() };
        relations[bu].tiers.add(tierMap[tierCol[i]]);
        relations[bu].sites.add(siteMap[siteCol[i]]);
        if (presData[i]) {
            presData[i].split('|').forEach(o => { 
                const cl = o.trim(); 
                if(cl) { relations[bu].offers.add(cl); relations[bu].types.add(cl.split('-')[0]); }
            });
        }
    }
    for (let k in relations) for (let s in relations[k]) relations[k][s] = Array.from(relations[k][s]).sort();
    
    self.postMessage({ type: 'READY', displayDates: dDates, bus: buMap, relations });
}

function calculate(exclOff, exclTyp, f, metricKey, isSimulation) {
    const res = dateMap.map(() => ({ h: 0, elig: 0, ext: 0, acc: 0, typeStatsNum: {}, comboStatsNum: {} }));
    const tIdx = new Set(f.tiers.map(x => tierMap.indexOf(x)));
    const sIdx = new Set(f.sites.map(x => siteMap.indexOf(x)));
    const buIdx = buMap.indexOf(f.bu);
    const seen = new Set();

    for (let i = 0; i < rowCount; i++) {
        if (buCol[i] !== buIdx || !tIdx.has(tierCol[i]) || !sIdx.has(siteCol[i])) continue;
        const d = mthCol[i], r = res[d];
        
        // UNIQUE SEGMENT DEDUPLICATION (The key to correct eligibility)
        const segKey = `${d}-${buCol[i]}-${tierCol[i]}-${siteCol[i]}`;
        if (!seen.has(segKey)) { 
            r.h += handledCol[i]; 
            seen.add(segKey); 
        }

        const rowOff = presData[i].split('|').map(o => o.trim()).filter(o => o);
        const act = isSimulation ? rowOff.filter(o => !exclOff.includes(o) && !exclTyp.includes(o.split('-')[0])) : rowOff;
        
        if (act.length > 0) {
            const v = countCol[i]; 
            r.elig += v;
            const extAct = extData[i].split('|').some(o => act.includes(o.trim()));
            const accAct = accData[i].split('|').some(o => act.includes(o.trim()));
            if (extAct) r.ext += v;
            if (accAct) r.acc += v;

            const isSuccess = (metricKey === 'eligRate') ? true : (metricKey === 'convRate') ? accAct : extAct;
            if (isSuccess) {
                const ck = act.sort().join(' | ');
                r.comboStatsNum[ck] = (r.comboStatsNum[ck] || 0) + v;
                new Set(act.map(o => o.split('-')[0])).forEach(t => r.typeStatsNum[t] = (r.typeStatsNum[t] || 0) + v);
            }
        }
    }
    return res.map(r => ({ 
        eligRate: (r.elig/r.h)*100||0, offRate: (r.ext/r.elig)*100||0, accRate: (r.acc/r.elig)*100||0, convRate: (r.acc/r.ext)*100||0, 
        h: r.h, elig: r.elig, ext: r.ext, acc: r.acc, typeStatsNum: r.typeStatsNum, comboStatsNum: r.comboStatsNum 
    }));
}

self.onmessage = (e) => {
    if (e.data.type === 'LOAD') generateFixedDummyData();
    if (e.data.type === 'PROCESS') {
        const base = calculate([], [], e.data.filters, e.data.metricKey, false);
        const impact = calculate(e.data.exclOff, e.data.exclTyp, e.data.filters, e.data.metricKey, true);
        self.postMessage({ type: 'DONE', base, impact, periodIdx: e.data.pIdx });
    }
};