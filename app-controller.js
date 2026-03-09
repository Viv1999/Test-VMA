/**
 * VG Metric Analyzer - Global Data Controller
 * Manages the Progressive Loading Pipeline for 100k+ records
 */

const DashboardState = {
    weekly: { data: [], loaded: false, error: false },
    monthly: { data: [], loaded: false, error: false },
    activeView: 'weekly'
};

// Configuration: Column names/indices based on your aggregated files
const CONFIG = {
    weeklyFile: 'weekly_aggregated.csv',
    monthlyFiles: [
        '2026-01_aggregated.csv',
        '2026-02_aggregated.csv',
        '2026-03_aggregated.csv',
        '2026-04_aggregated.csv'
    ]
};

/**
 * Robust CSV Parser
 * Automatically detects column indices based on header names
 */
async function parseCSV(fileUrl) {
    try {
        const response = await fetch(fileUrl);
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        const text = await response.text();
        const rows = text.split('\n').filter(line => line.trim() !== "");
        if (rows.length < 2) return [];

        const headers = rows[0].toLowerCase().split(',');
        
        // Dynamic Index Mapping
        const map = {
            time:    headers.findIndex(h => h.includes('week') || h.includes('month')),
            bu:      headers.indexOf('bu'),
            tier:    headers.indexOf('tier'),
            site:    headers.indexOf('site'),
            handled: headers.indexOf('calls_handled'),
            pres:    headers.indexOf('offers_present'),
            ext:     headers.indexOf('offers_extended'),
            acc:     headers.indexOf('offers_accepted'),
            count:   headers.indexOf('calls')
        };

        return rows.slice(1).map(row => {
            const cols = row.split(',');
            return {
                time:    cols[map.time],
                bu:      cols[map.bu],
                tier:    cols[map.tier],
                site:    cols[map.site],
                handled: parseInt(cols[map.handled]) || 0,
                pres:    cols[map.pres] || "",
                ext:     cols[map.ext] || "",
                acc:     cols[map.acc] || "",
                count:   parseInt(cols[map.count]) || 0
            };
        });
    } catch (error) {
        console.error(`Failed to parse ${fileUrl}:`, error);
        throw error;
    }
}

/**
 * The Loading Pipeline
 * Step 1: Load Weekly (High Priority)
 * Step 2: Load Monthly (Background Parallel)
 */
async function startDataPipeline() {
    try {
        // STEP 1: Weekly Data
        console.log("Pipeline: Fetching Weekly Data...");
        DashboardState.weekly.data = await parseCSV(CONFIG.weeklyFile);
        DashboardState.weekly.loaded = true;
        
        // Custom Event to notify UI that Weekly is ready
        window.dispatchEvent(new CustomEvent('weeklyDataReady'));

        // STEP 2: Monthly Data (Background)
        console.log("Pipeline: Fetching Monthly Data in background...");
        const monthlyPromises = CONFIG.monthlyFiles.map(file => parseCSV(file));
        const monthlyResults = await Promise.all(monthlyPromises);
        
        DashboardState.monthly.data = monthlyResults.flat();
        DashboardState.monthly.loaded = true;
        
        // Custom Event to notify UI that Monthly is ready
        window.dispatchEvent(new CustomEvent('monthlyDataReady'));
        console.log("Pipeline: All data cached in memory.");

    } catch (err) {
        alert("Critical Data Load Error. Please ensure CSV files are present and reload the page.");
        DashboardState.weekly.error = true;
        DashboardState.monthly.error = true;
    }
}

// Start the pipeline as soon as the portal loads
document.addEventListener('DOMContentLoaded', startDataPipeline);