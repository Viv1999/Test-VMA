/** worker.js **/
const MAX_ROWS = 800000;
const mthCol = new Uint8Array(MAX_ROWS), buCol = new Uint8Array(MAX_ROWS), tierCol = new Uint8Array(MAX_ROWS), siteCol = new Uint8Array(MAX_ROWS);
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
    // ... Metadata & READY message follows ...
}