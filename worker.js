/** worker.js - Version 1 Stable **/
const MAX_ROWS = 1000000;
const mthCol = new Uint8Array(MAX_ROWS), buCol = new Uint8Array(MAX_ROWS), 
      tierCol = new Uint8Array(MAX_ROWS), siteCol = new Uint8Array(MAX_ROWS);
const handledCol = new Uint32Array(MAX_ROWS), countCol = new Uint32Array(MAX_ROWS);
const presData = new Array(MAX_ROWS), extData = new Array(MAX_ROWS), accData = new Array(MAX_ROWS);

let dateMap = [], buMap = [], tierMap = [], siteMap = [], rowCount = 0;

async function streamCSV(url) {
    const resp = await fetch(url);
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let partial = '', isH = true;

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const lines = (partial + decoder.decode(value)).split('\n');
        partial = lines.pop();

        for (let line of lines) {
            const c = line.split(',');
            if (!line.trim() || isH) { isH = false; continue; }
            if (c.length < 9) continue;
            
            const getIdx = (v, m) => { 
                let clean = v.trim(); 
                let i = m.indexOf(clean); 
                if (i === -1) { m.push(clean); return m.length - 1; } 
                return i; 
            };
            
            mthCol[rowCount] = getIdx(c[0], dateMap);
            buCol[rowCount] = getIdx(c[1], buMap);
            tierCol[rowCount] = getIdx(c[2], tierMap);
            siteCol[rowCount] = getIdx(c[3], siteMap);
            handledCol[rowCount] = parseInt(c[4]) || 0;
            presData[rowCount] = c[5] || ""; 
            extData[rowCount] = c[6] || ""; 
            accData[rowCount] = c[7] || ""; 
            countCol[rowCount] = parseInt(c[8]) || 0;
            rowCount++;
            
            if (rowCount % 100000 === 0) self.postMessage({ type: 'PROGRESS', loaded: rowCount });
        }
    }

    dateMap.sort();
    const mNames = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const dDates = dateMap.map(v => `${mNames[parseInt(v.substring(4,6))] || '??'} ${v.substring(2,4)}`);
    
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
        
        // UNIQUE SEGMENT DEDUPLICATION - The core of Eligibility accuracy
        const segKey = `${d}-${buCol[i]}-${tierCol[i]}-${siteCol[i]}`;
        if (!seen.has(segKey)) { r.h += handledCol[i]; seen.add(segKey); }

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
    if (e.data.type === 'LOAD') streamCSV(e.data.url);
    if (e.data.type === 'PROCESS') {
        const base = calculate([], [], e.data.filters, e.data.metricKey, false);
        const impact = calculate(e.data.exclOff, e.data.exclTyp, e.data.filters, e.data.metricKey, true);
        self.postMessage({ type: 'DONE', base, impact, periodIdx: e.data.pIdx });
    }
};