document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const elNodeId = document.getElementById('node-id');
    const elNodeRole = document.getElementById('node-role');
    const elNodeTerm = document.getElementById('node-term');
    const elLeaderId = document.getElementById('leader-id');
    const elTotalKeys = document.getElementById('total-keys');
    const tbody = document.getElementById('kv-tbody');
    const eventLog = document.getElementById('event-log');
    
    // Form Elements
    const form = document.getElementById('kv-form');
    const keyInput = document.getElementById('key-input');
    const valInput = document.getElementById('val-input');
    const btnDel = document.getElementById('btn-del');
    const btnClearLogs = document.getElementById('btn-clear-logs');
    const warningNotLeader = document.getElementById('not-leader-warning');
    const hintLeaderId = document.getElementById('hint-leader-id');

    // State
    let kvState = new Map(); // Local replica of KV state
    let currentNodeRole = "Unknown";
    let isInitialLoad = true;

    // ------------------------------------------------------------------------
    // API Interactions
    // ------------------------------------------------------------------------
    
    // Fetch all existing keys on initial load
    async function fetchInitialKeys() {
        try {
            const res = await fetch('/v1/keys');
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            const data = await res.json();
            
            if (data && Object.keys(data).length > 0) {
                for (const [k, v] of Object.entries(data)) {
                    kvState.set(k, v);
                }
                renderTable();
            }
        } catch (e) {
            console.error("Initial keys fetch failed:", e);
        }
    }
    
    // Poll node status every 2 seconds
    async function fetchStatus() {
        try {
            const res = await fetch('/v1/status');
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            const data = await res.json();
            
            elNodeId.textContent = data.node_id;
            elNodeRole.textContent = data.role;
            elNodeRole.setAttribute('data-role', data.role);
            elNodeTerm.textContent = data.term;
            elLeaderId.textContent = data.leader;
            
            currentNodeRole = data.role;
            
            // Toggle form warning if follower
            if (data.role === "Follower" || data.role === "Candidate") {
                warningNotLeader.style.display = 'block';
                hintLeaderId.textContent = data.leader !== "-1" ? data.leader : "Unknown";
            } else {
                warningNotLeader.style.display = 'none';
            }
        } catch (e) {
            console.error("Status fetch failed:", e);
            elNodeRole.textContent = "Offline";
            elNodeRole.setAttribute('data-role', "Offline");
        }
    }

    // Propose PUT
    async function putKey(key, value) {
        try {
            const res = await fetch(`/v1/${encodeURIComponent(key)}`, {
                method: 'PUT',
                body: value,
                headers: { 'Content-Type': 'text/plain' }
            });
            handleMutationResponse(res, key, 'PUT');
        } catch (e) {
            showToast('Error connecting to node', 'error');
        }
    }

    // Propose DELETE
    async function deleteKey(key) {
        try {
            const res = await fetch(`/v1/${encodeURIComponent(key)}`, {
                method: 'DELETE'
            });
            handleMutationResponse(res, key, 'DELETE');
        } catch (e) {
            showToast('Error connecting to node', 'error');
        }
    }

    async function handleMutationResponse(res, key, opStr) {
        if (res.status === 307) {
            const data = await res.json();
            showToast(`Cannot ${opStr}: Node is not the Leader (Leader ID: ${data.leader_id})`, 'warning');
        } else if (!res.ok) {
            const raw = await res.text();
            showToast(`Error: ${raw}`, 'error');
        } else {
            // Success — the SSE stream will update the UI, but we clear form
            form.reset();
            keyInput.focus();
            showToast(`Proposed ${opStr} for '${key}' successfully`, 'success');
        }
    }

    // ------------------------------------------------------------------------
    // SSE Stream (Watcher)
    // ------------------------------------------------------------------------
    
    function setupSSE() {
        const source = new EventSource('/v1/watch');
        
        source.onopen = () => {
            logEvent('Connected to event stream', 'system');
        };

        source.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                processWatcherEvent(data);
            } catch (e) {
                console.error("Error parsing SSE data:", e);
            }
        };

        source.onerror = (err) => {
            console.error("SSE Error:", err);
            source.close();
            logEvent('Connection lost. Reconnecting in 3s...', 'system');
            setTimeout(setupSSE, 3000);
        };
    }

    function processWatcherEvent(ev) {
        const timeStr = new Date().toLocaleTimeString('en-US', {hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit', fractionalSecondDigits:3});
        
        if (ev.type === 'PUT') {
            kvState.set(ev.key, ev.value);
            logEvent(`<span class="type">PUT</span> <span class="key-hl">${ev.key}</span> \u2192 <span class="val-hl">${ev.value}</span>`, 'put', timeStr);
        } else if (ev.type === 'DELETE') {
            kvState.delete(ev.key);
            logEvent(`<span class="type">DEL</span> <span class="key-hl">${ev.key}</span>`, 'delete', timeStr);
        }
        
        renderTable(ev.key); // Pass key to highlight
    }

    // ------------------------------------------------------------------------
    // UI Rendering
    // ------------------------------------------------------------------------

    function renderTable(highlightKey = null) {
        elTotalKeys.textContent = kvState.size;
        
        if (kvState.size === 0) {
            tbody.innerHTML = `<tr id="empty-row"><td colspan="3" class="empty-state">Store is empty. Put a key to begin.</td></tr>`;
            return;
        }

        // Sort keys alphabetically
        const sortedKeys = Array.from(kvState.keys()).sort();
        
        let htmlChunks = [];
        for (const k of sortedKeys) {
            const v = kvState.get(k);
            const highClass = (k === highlightKey && !isInitialLoad) ? 'class="row-new"' : '';
            
            // Note: In real production, encode HTML entities to prevent XSS. 
            // We do a basic replacement here.
            const safeK = escapeHtml(k);
            const safeV = escapeHtml(v);
            
            htmlChunks.push(`
                <tr ${highClass}>
                    <td><strong>${safeK}</strong></td>
                    <td class="val-cell">${safeV}</td>
                    <td style="text-align:right;">
                        <button class="btn-icon" onclick="window.delBtnClick('${safeK}')" title="Delete">
                            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path><line x1="10" y1="11" x2="10" y2="17"></line><line x1="14" y1="11" x2="14" y2="17"></line></svg>
                        </button>
                    </td>
                </tr>
            `);
        }
        
        tbody.innerHTML = htmlChunks.join('');
    }

    function logEvent(htmlContent, typeClass = '', timeStr = null) {
        if (!timeStr) {
            timeStr = new Date().toLocaleTimeString('en-US', {hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit', fractionalSecondDigits:3});
        }
        
        const entry = document.createElement('div');
        entry.className = `log-entry ${typeClass}`;
        entry.innerHTML = `<span class="timestamp">[${timeStr}]</span><span class="message">${htmlContent}</span>`;
        
        eventLog.appendChild(entry);
        
        // Auto-scroll to bottom
        eventLog.scrollTop = eventLog.scrollHeight;
        
        // Keep only last 100 logs
        while (eventLog.children.length > 100) {
            eventLog.removeChild(eventLog.firstChild);
        }
    }

    function showToast(message, type = 'info') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        let icon = '';
        if (type === 'error') icon = '🔴';
        else if (type === 'success') icon = '🟢';
        else if (type === 'warning') icon = '🟡';
        
        toast.innerHTML = `<span>${icon}</span> <span>${message}</span>`;
        container.appendChild(toast);
        
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(120%)';
            setTimeout(() => toast.remove(), 300);
        }, 4000);
    }

    function escapeHtml(unsafe) {
        return (unsafe || "").toString()
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }

    // ------------------------------------------------------------------------
    // Event Listeners
    // ------------------------------------------------------------------------

    form.addEventListener('submit', (e) => {
        e.preventDefault();
        const key = keyInput.value.trim();
        const val = valInput.value.trim();
        if (!key || !val) {
            showToast('Both Key and Value are required for PUT', 'warning');
            return;
        }
        putKey(key, val);
    });

    btnDel.addEventListener('click', () => {
        const key = keyInput.value.trim();
        if (!key) {
            showToast('Key is required for DELETE', 'warning');
            return;
        }
        deleteKey(key);
    });

    btnClearLogs.addEventListener('click', () => {
        eventLog.innerHTML = '';
        logEvent('Logs cleared.', 'system');
    });

    // Global hook for inline delete buttons in the table
    window.delBtnClick = function(key) {
        if (confirm(`Are you sure you want to delete '${key}'?`)) {
            keyInput.value = key; // Pre-fill
            deleteKey(key);
        }
    };

    // ------------------------------------------------------------------------
    // Initialization
    // ------------------------------------------------------------------------
    
    // Initial fetch to populate UI
    fetchInitialKeys();
    fetchStatus();
    setInterval(fetchStatus, 2000); // Poll status every 2s
    setupSSE();
    
    setTimeout(() => { isInitialLoad = false; }, 1000);
});
