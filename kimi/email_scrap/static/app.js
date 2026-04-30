// 🍞 Toast Notification Helper
function showToast(message, type = 'success') {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        document.body.appendChild(container);
    }

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <i class="fa-solid fa-bell"></i>
        <span>${message}</span>
    `;

    container.appendChild(toast);

    // Auto remove with fade animation
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(-20px)';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

// 📋 Copy To Clipboard Helper
window.copyToClipboard = function(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(() => {
            showToast("Email copied to clipboard!", "success");
        }).catch(err => {
            console.error('Failed to copy: ', err);
            showToast("Failed to copy email", "error");
        });
    } else {
        const tempInput = document.createElement("input");
        tempInput.value = text;
        document.body.appendChild(tempInput);
        tempInput.select();
        document.execCommand("copy");
        document.body.removeChild(tempInput);
        showToast("Email copied to clipboard!", "success");
    }
}


// Auth management
async function checkAuth() {
    try {
        const resp = await fetch('/api/check-auth');
        const data = await resp.json();
        if (data.authenticated) {
            document.getElementById('login-overlay').style.display = 'none';
            document.getElementById('dashboard-wrapper').style.display = 'flex';
            
            // 🧑‍💼 Populate User Profile
            const footer = document.getElementById('sidebar-footer-profile');
            if (footer) {
                footer.style.display = 'block';
                document.getElementById('user-name').innerText = data.name || 'User';
                const companySubLine = (data.role && data.role.toLowerCase() === 'admin') 
                    ? '<span style="display:block; font-size: 0.65rem; color: #64748b; margin-top: 2px;"><i class="fa-solid fa-earth-americas" style="font-size: 0.6rem; margin-right: 4px;"></i>Universal</span>'
                    : `<span style="display:block; font-size: 0.65rem; color: #64748b; margin-top: 2px;"><i class="fa-solid fa-building" style="font-size: 0.6rem; margin-right: 4px;"></i>${data.company_name || 'N/A'}</span>`;

                document.getElementById('user-role').innerHTML = `
                    <span>${data.role || 'Member'}</span>
                    ${companySubLine}
                `;



                const avatar = document.getElementById('user-avatar');
                avatar.innerText = (data.name || 'U').charAt(0).toUpperCase();
                
                    if (data.profile_picture) {
                        avatar.innerHTML = `<img src="${data.profile_picture}" style="width:100%; height:100%; border-radius:50%; object-fit:cover;">`;
                        avatar.style.background = 'transparent';
                    }
                    
                    // 🚪 Logout Trigger
                    const logoutBtn = document.getElementById('logout-btn');
                    if (logoutBtn) {
                        logoutBtn.addEventListener('click', async () => {
                            try {
                                const resp = await fetch('/api/logout', { method: 'POST' });
                                const ans = await resp.json();
                                if (ans.success) {
                                    location.reload();
                                } else {
                                    alert("Logout failed: " + (ans.detail || "Error"));
                                }
                            } catch(e) {
                                alert("Error connecting to logout server");
                            }
                        });
                    }
                }

                if (data.role === 'admin') {
                    document.getElementById('nav-company').style.display = 'inline-block';
                    document.getElementById('nav-distribute-sidebar').style.display = 'inline-block';
                    const clearBtn = document.getElementById('clear-leads-btn');
                    if (clearBtn) clearBtn.style.display = 'inline-flex';
                } else {
                    // 🛡️ Hide Administration/Pipeline tabs for Members
                    document.getElementById('nav-pipeline').style.display = 'none';
                    
                    // Force navigation defaults to Emails view to avoid restoring broken views cache
                    localStorage.setItem('active_view', 'nav-emails');
                }
            fetchLeads();
            loadCompanies();

        // 🔄 Restore Last Saved View and Scroll Position on Refresh
        const savedView = localStorage.getItem('active_view');
        if (savedView) {
            const el = document.getElementById(savedView);
            if (el) {
                el.click();
                
                // 📜 Restore Scroll Position for this Specific Tab
                setTimeout(() => {
                    const mainContent = document.querySelector('.main-content');
                    if (mainContent) {
                        const savedScroll = localStorage.getItem(`scroll_${savedView}`);
                        if (savedScroll) {
                            mainContent.scrollTop = parseInt(savedScroll);
                        }
                    }
                }, 100); // ⏱ Delay ensures DOM updates completed before scrolling
            }
        }

        } else {
            document.getElementById('login-overlay').style.display = 'flex';
            document.getElementById('dashboard-wrapper').style.display = 'none';
            initGoogleSignIn();
            loadCompanies();
        }
    } catch (e) {
        console.error("Auth check failed", e);
    }
}

async function loadCompanies() {
    const select = document.getElementById('login-company-select');
    if (!select) return;
    const companies = [
        { id: 1, name: "DP" },
        { id: 2, name: "MCC" },
        { id: 3, name: "VC" },
        { id: 4, name: "ST" }
    ];
    select.innerHTML = companies.map(c => `<option value="${c.id}">${c.name}</option>`).join('');
}


function initGoogleSignIn() {
    if (typeof google === 'undefined') {
        setTimeout(initGoogleSignIn, 500);
        return;
    }
    
    google.accounts.id.initialize({
        client_id: "YOUR_GOOGLE_CLIENT_ID.apps.googleusercontent.com", // TODO: Replace
        callback: handleGoogleCredentialResponse
    });
    
    google.accounts.id.renderButton(
        document.getElementById("google-signin-btn"),
        { theme: "outline", size: "large", width: "100%" }
    );
}

async function handleGoogleCredentialResponse(response) {
    const company_id = document.getElementById('login-company-select').value;
    try {
        const resp = await fetch('/api/login/google', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token: response.credential, company_id: parseInt(company_id) })
        });
        const data = await resp.json();
        if (data.success) {
            location.reload();
        } else {
            alert("Google Sign-in failed: " + data.detail);
        }
    } catch (e) {
        alert("Error logging in");
    }
}



// State management
let leadsData = [];
let filteredData = [];
let rowsPerPage = parseInt(localStorage.getItem('rows_per_page')) || 15;
let currentPage = parseInt(localStorage.getItem('current_page')) || 1;

// DOM Elements
const tableBody = document.getElementById('table-body');
const totalCountEl = document.getElementById('total-count');
const verifiedCountEl = document.getElementById('verified-count');
const tickedCountEl = document.getElementById('ticked-count');
const searchInput = document.getElementById('search-input');
const statusFilter = document.getElementById('status-filter');
const refreshBtn = document.getElementById('refresh-btn');
const prevPageBtn = document.getElementById('prev-page');
const nextPageBtn = document.getElementById('next-page');
const firstPageBtn = document.getElementById('first-page');
const lastPageBtn = document.getElementById('last-page');

const currentPageEl = document.getElementById('current-page');
const startIdxEl = document.getElementById('start-idx');
const endIdxEl = document.getElementById('end-idx');
const filteredCountEl = document.getElementById('filtered-count');
const detailsModal = document.getElementById('details-modal');
const modalBody = document.getElementById('modal-body');
const closeModalBtn = document.querySelector('.close-modal');

// API call to fetch emails
async function fetchLeads() {
    try {
        const response = await fetch('/api/emails');
        if (!response.ok) throw new Error('Network response was not ok');
        leadsData = await response.json();

        // Update Stats
        totalCountEl.textContent = leadsData.length;
        verifiedCountEl.textContent = leadsData.filter(l => l.is_verified).length;
        tickedCountEl.textContent = leadsData.filter(l => l.is_ticked).length;

        applyFilters();
    } catch (error) {
        console.error('Fetch error:', error);
        tableBody.innerHTML = `<tr><td colspan="6" class="table-loader" style="color:red">Failed to load data. Please check backend.</td></tr>`;
    }
}

// Function to Toggle Tickmark state in Backend
async function toggleTickmark(email, currentStatus) {
    const newStatus = !currentStatus;
    try {
        const response = await fetch('/api/emails/toggle', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email: email, is_ticked: newStatus })
        });

        if (response.ok) {
            // Update local state directly to be fast
            const lead = leadsData.find(l => l.email === email);
            if (lead) {
                lead.is_ticked = newStatus;
                tickedCountEl.textContent = leadsData.filter(l => l.is_ticked).length;
            }
            return true;
        }
    } catch (error) {
        console.error('Toggle error:', error);
    }
    return false;
}

// Filter and Search Logic
function applyFilters() {
    const searchTerm = searchInput.value.toLowerCase();
    const filterStatus = statusFilter.value;

    // 💾 Save Filter State
    localStorage.setItem('search_term', searchTerm);
    localStorage.setItem('status_filter', filterStatus);

    // Update active class on stat cards
    document.querySelectorAll('.stat-card').forEach(card => card.classList.remove('active'));
    if (filterStatus === 'all') document.getElementById('total-card').classList.add('active');
    else if (filterStatus === 'verified') document.getElementById('verified-card').classList.add('active');
    else if (filterStatus === 'ticked') document.getElementById('ticked-card').classList.add('active');

    filteredData = leadsData.filter(lead => {
        const matchesSearch = lead.company_name.toLowerCase().includes(searchTerm) ||
            lead.email.toLowerCase().includes(searchTerm);

        let matchesStatus = true;
        if (filterStatus === 'verified') matchesStatus = lead.is_verified;
        else if (filterStatus === 'unverified') matchesStatus = !lead.is_verified;
        else if (filterStatus === 'ticked') matchesStatus = lead.is_ticked;

        return matchesSearch && matchesStatus;
    });

    // Reset to page 1 ONLY if search/filter actually changed (to prevent jumping on load)
    const oldSearch = localStorage.getItem('last_search_context');
    const newContext = searchTerm + '|' + filterStatus;
    if (oldSearch !== newContext) {
        currentPage = 1;
        localStorage.setItem('current_page', currentPage);
        localStorage.setItem('last_search_context', newContext);
    }

    const mainContent = document.querySelector('.main-content');
    if (mainContent) mainContent.scrollTop = 0; // 📜 Reset scroll
    renderTable();
}

// Render Table Rows with Pagination
function renderTable() {
    tableBody.innerHTML = '';



    if (filteredData.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="6" class="table-loader">No leads found.</td></tr>`;
        updatePaginationInfo(0, 0, 0);
        return;
    }

    const startIdx = (currentPage - 1) * rowsPerPage;
    const endIdx = Math.min(startIdx + rowsPerPage, filteredData.length);
    const paginatedItems = filteredData.slice(startIdx, endIdx);

    paginatedItems.forEach((lead, index) => {
        const tr = document.createElement('tr');

        const tickClass = lead.is_ticked ? 'tick-checkbox active' : 'tick-checkbox';
        const verifyBadge = lead.is_verified
            ? `<span class="badge success"><i class="fa-solid fa-check"></i> Verified</span>`
            : `<span class="badge unverified">No status</span>`;

        tr.innerHTML = `
            <td class="col-tick">
                <i class="fa-solid fa-bookmark ${tickClass}" data-email="${lead.email}"></i>
            </td>
            <td class="col-company">${lead.company_name}</td>
            <td class="col-email">
                <div style="display:flex; align-items:center; gap:8px;">
                    <span style="flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${lead.email}</span>
                    <button class="btn btn-sm" onclick="copyToClipboard('${lead.email}')" style="padding:4px 6px; background:#ffffff; border:1px solid #e2e8f0; border-radius:4px; font-size:0.75rem; color:#1e293b; cursor:pointer;" title="Copy email address">

                        <i class="fa-regular fa-copy"></i>
                    </button>
                </div>
            </td>
            <td class="col-phone">${lead.phone}</td>
            <td>${verifyBadge}</td>
            <td>
                <button class="btn btn-sm view-btn" data-id="${lead.id}">View</button>
            </td>
        `;

        tableBody.appendChild(tr);
    });

    // Add Toggle Event listeners
    document.querySelectorAll('.tick-checkbox').forEach(icon => {
        icon.addEventListener('click', async (e) => {
            const email = e.target.getAttribute('data-email');
            const isActive = e.target.classList.contains('active');

            // Optimistic Update UI
            e.target.classList.toggle('active');
            const success = await toggleTickmark(email, isActive);
            if (!success) {
                // roll back if failed
                e.target.classList.toggle('active');
            }
        });
    });

    // Add Modal Event listeners
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const id = parseInt(e.target.getAttribute('data-id'));
            const lead = filteredData.find(l => l.id === id);
            if (lead) showModal(lead);
        });
    });

    updatePaginationInfo(startIdx + 1, endIdx, filteredData.length);
}

function updatePaginationInfo(start, end, total) {
    if (startIdxEl) startIdxEl.textContent = start;
    if (endIdxEl) endIdxEl.textContent = end;
    if (filteredCountEl) filteredCountEl.textContent = total;
    if (currentPageEl) currentPageEl.textContent = currentPage;
    
    // 💾 Save Page State
    localStorage.setItem('current_page', currentPage);

    const totalPages = Math.ceil(total / rowsPerPage) || 1;
    const totalPagesEl = document.getElementById('total-pages');
    const jumpPageEl = document.getElementById('jump-page');
    
    if (totalPagesEl) totalPagesEl.textContent = totalPages;
    if (jumpPageEl) {
        jumpPageEl.value = currentPage;
        jumpPageEl.max = totalPages;
    }

    if (prevPageBtn) prevPageBtn.disabled = currentPage === 1;
    if (firstPageBtn) firstPageBtn.disabled = currentPage === 1;
    if (nextPageBtn) nextPageBtn.disabled = end >= total;
    if (lastPageBtn) lastPageBtn.disabled = end >= total;
}



function showModal(lead) {
    modalBody.innerHTML = `
        <div class="detail-row">
            <div class="detail-label">Company Name</div>
            <div class="detail-value">${lead.company_name}</div>
        </div>
        <div class="detail-row">
            <div class="detail-label">Email Address</div>
            <div class="detail-value">
                <div style="display:flex; align-items:center; gap:8px;">
                    <span>${lead.email}</span>
                    <button class="btn btn-sm" onclick="copyToClipboard('${lead.email}')" style="padding:4px 6px; background:#ffffff; border:1px solid #e2e8f0; border-radius:4px; font-size:0.75rem; color:#1e293b; cursor:pointer;" title="Copy email address">
                        <i class="fa-regular fa-copy"></i>
                    </button>

                </div>
            </div>
        </div>

        <div class="detail-row">
            <div class="detail-label">Contact Phone</div>
            <div class="detail-value">${lead.phone}</div>
        </div>
        <div class="detail-row">
            <div class="detail-label">Mitigation Contact</div>
            <div class="detail-value">${lead.contact_name || 'N/A'}</div>
        </div>
        <div class="detail-row">
            <div class="detail-label">Verified Status</div>
            <div class="detail-value">
                ${lead.is_verified ? '<span style="color:green">✓ Verified</span>' : 'Unverified'}
            </div>
        </div>
        <div class="detail-row">
            <div class="detail-label">Address</div>
            <div class="detail-value">${lead.address || 'N/A'}</div>
        </div>
    `;
    detailsModal.style.display = 'flex';
}

// Event Listeners
searchInput.addEventListener('input', applyFilters);
statusFilter.addEventListener('change', applyFilters);
refreshBtn.addEventListener('click', fetchLeads);

prevPageBtn.addEventListener('click', () => {
    if (currentPage > 1) {
        currentPage--;
        renderTable();
    }
});

nextPageBtn.addEventListener('click', () => {
    if (currentPage * rowsPerPage < filteredData.length) {
        currentPage++;
        renderTable();
    }
});

if (firstPageBtn) {
    firstPageBtn.addEventListener('click', () => {
        currentPage = 1;
        renderTable();
    });
}

if (lastPageBtn) {
    lastPageBtn.addEventListener('click', () => {
        const totalPages = Math.ceil(filteredData.length / rowsPerPage) || 1;
        currentPage = totalPages;
        renderTable();
    });
}


closeModalBtn.addEventListener('click', () => detailsModal.style.display = 'none');
window.addEventListener('click', (e) => { if (e.target === detailsModal) detailsModal.style.display = 'none'; });

// Initial Load
document.addEventListener('DOMContentLoaded', () => {
    checkAuth();

    // 📊 Rows Per Page Selection
    const rowsSelect = document.getElementById('rows-per-page');
    if (rowsSelect) {
        rowsSelect.addEventListener('change', (e) => {
            rowsPerPage = parseInt(e.target.value);
            localStorage.setItem('rows_per_page', rowsPerPage);
            currentPage = 1; // Reset to page 1 list view
            localStorage.setItem('current_page', currentPage);
            renderTable();
        });
    }

    // 📖 Jump to Page Selection
    const jumpSelect = document.getElementById('jump-page');
    if (jumpSelect) {
        jumpSelect.addEventListener('change', (e) => {
            let page = parseInt(e.target.value);
            const totalPages = Math.ceil(filteredData.length / rowsPerPage) || 1;
            
            if (isNaN(page) || page < 1) page = 1;
            if (page > totalPages) page = totalPages;
            
            currentPage = page;
            renderTable();
        });
    }

    // 📋 Restore DOM State from localStorage
    const savedSearch = localStorage.getItem('search_term');
    if (savedSearch && searchInput) searchInput.value = savedSearch;

    const savedStatus = localStorage.getItem('status_filter');
    if (savedStatus && statusFilter) statusFilter.value = savedStatus;

    if (rowsSelect) {
        const savedRows = localStorage.getItem('rows_per_page');
        if (savedRows) rowsSelect.value = savedRows;
    }

    const pipelineInterval = document.getElementById('pipeline-interval');
    if (pipelineInterval) {
        const savedPipeInt = localStorage.getItem('pipeline_interval');
        if (savedPipeInt) pipelineInterval.value = savedPipeInt;
        pipelineInterval.addEventListener('change', (e) => localStorage.setItem('pipeline_interval', e.target.value));
    }

    // 📦 Distribute Leads Persistence
    const distType = document.getElementById('dist-type');
    const distCount = document.getElementById('dist-count');
    const distInterval = document.getElementById('dist-interval');
    
    if (distType) {
        const savedType = localStorage.getItem('dist_type');
        if (savedType) {
            distType.value = savedType;
            document.getElementById('dist-employee-selector-div').style.display = savedType === 'all' ? 'none' : 'block';
            document.getElementById('dist-company-selector-div').style.display = savedType === 'all' ? 'block' : 'none';
        }
    }
    if (distCount) {
        const savedCount = localStorage.getItem('dist_count');
        if (savedCount) distCount.value = savedCount;
        distCount.addEventListener('input', (e) => localStorage.setItem('dist_count', e.target.value));
    }
    if (distInterval) {
        const savedInt = localStorage.getItem('dist_interval');
        if (savedInt) distInterval.value = savedInt;
        distInterval.addEventListener('change', (e) => localStorage.setItem('dist_interval', e.target.value));
    }
    
    // 💾 Restore Login Email
    const manualEmail = document.getElementById('manual-email');
    if (manualEmail) {
        const savedEmail = localStorage.getItem('login_email');
        if (savedEmail) manualEmail.value = savedEmail;
        manualEmail.addEventListener('input', (e) => localStorage.setItem('login_email', e.target.value));
    }

    // Add Manual Login Listener
    const manualBtn = document.getElementById('manual-login-btn');
    if (manualBtn) {
        manualBtn.addEventListener('click', async () => {
            const email = document.getElementById('manual-email').value;
            const password = document.getElementById('manual-password').value;
            if (!email || !password) return showToast("Please fill credentials", 'error');
            
            try {
                const resp = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email: email, password: password })
                });
                const data = await resp.json();
                if (data.success) {
                    location.reload();
                } else {
                    showToast("Login failed: " + (data.detail || "Invalid credentials"), 'error');
                }
            } catch (e) {
                showToast("Error logging in", 'error');
            }
        });
    }
    // Lead fetching is now managed by checkAuth() on window.onload
    
    // Add Stat Card Click Listeners for quick filter
    document.getElementById('total-card').addEventListener('click', () => {
        statusFilter.value = 'all';
        applyFilters();
    });

    document.getElementById('verified-card').addEventListener('click', () => {
        statusFilter.value = 'verified';
        applyFilters();
    });

    document.getElementById('ticked-card').addEventListener('click', () => {
        statusFilter.value = 'ticked';
        applyFilters();
    });

    // Replaced Infinite scroll with traditional page jump controls listeners
});




setInterval(fetchLeads, 5000); // Auto-refresh data every 5 seconds for live updates

// -------------------
// Employees View Logic
// -------------------

// -------------------
// Navigation Logic
// -------------------
const viewsMap = {
    'nav-emails': 'view-emails',
    'nav-pipeline': 'view-pipeline',
    'nav-company': 'view-employees',
    'nav-distribute-sidebar': 'view-distribute'
};

function showView(navId) {
    const targetView = viewsMap[navId];
    if (!targetView) return;

    // Toggle View Sections
    Object.values(viewsMap).forEach(v => {
        const el = document.getElementById(v);
        if (el) el.style.display = (v === targetView) ? 'block' : 'none';
    });

    // Toggle Active Nav Classes
    Object.keys(viewsMap).forEach(n => {
        const el = document.getElementById(n);
        if (el) {
            if (n === navId) el.classList.add('active');
            else el.classList.remove('active');
        }
    });

    localStorage.setItem('active_view', navId); // 💾 Save View State

    // Post-Load Fetches triggers
    if (navId === 'nav-company') fetchEmployees();
    if (navId === 'nav-distribute-sidebar') loadDistributeOptions();
    if (navId === 'nav-queue-monitor') fetchQueueMonitorStatus();
}

// Bind ALL Links
Object.keys(viewsMap).forEach(navId => {
    const el = document.getElementById(navId);
    if (el) {
        el.addEventListener('click', (e) => {
            e.preventDefault();
            showView(navId);
        });
    }
});

async function fetchEmployees() {
    const tbody = document.getElementById('employee-table-body');
    if (!tbody) return;
    try {
        const resp = await fetch('/api/company/users');
        const data = await resp.json();
        if (data.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="table-loader">No employees found.</td></tr>`;
        } else {
            tbody.innerHTML = '';
            data.forEach(user => {
                const tr = document.createElement('tr');
                const userName = user.name || user.email.split('@')[0];
                const badgeClass = user.role && user.role.toLowerCase() === 'admin' ? 'success' : 'primary';
                const roleLabel = user.role && user.role.toLowerCase() === 'admin' ? 'Admin' : 'Member';
                
                tr.innerHTML = `
                    <td>${userName}</td>
                    <td>${user.email}</td>
                    <td><span class="badge ${badgeClass}">${roleLabel}</span></td>
                    <td>${user.role && user.role.toLowerCase() === 'admin' ? 'Universal' : (user.company_code || 'N/A')}</td>
                    <td><button class="btn btn-sm btn-danger delete-user-btn" onclick="window.kickEmployee(${user.id})" style="padding: 4px 8px; font-size: 0.7rem;"><i class="fa-solid fa-trash"></i> Delete</button></td>
                `;
                tbody.appendChild(tr);
            });
        }
    } catch (e) { }
}

// ⚔️ Global Kick Handler
window.kickEmployee = async function(employeeId) {
    if (!confirm("Are you sure you want to kick this employee out?")) return;
    try {
        const resp = await fetch('/api/company/users/kick', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ employee_id: employeeId })
        });
        const ans = await resp.json();
        
        if (resp.ok && ans.success) {
            showToast(ans.message || "Employee kicked out successfully", 'success');
            if (typeof fetchEmployees === 'function') fetchEmployees();
        } else {
            showToast("Kick failed: " + (ans.detail || "Error"), 'error');
        }
    } catch(e) {
        showToast("Exception triggering kickout: " + e.message, 'error');
    }
}

// Add Employee Listeners
const addEmployeeBtn = document.getElementById('add-employee-btn');
const employeeModal = document.getElementById('employee-modal');

if (addEmployeeBtn) {
    addEmployeeBtn.addEventListener('click', () => {
        const select = document.getElementById('emp-company-select');
        const companies = [
            { id: 1, name: "DP" },
            { id: 2, name: "MCC" },
            { id: 3, name: "VC" },
            { id: 4, name: "ST" }
        ];
        select.innerHTML = companies.map(c => `<option value="${c.id}">${c.name}</option>`).join('');
        
        // 💾 Restore Modal State
        const savedComp = localStorage.getItem('emp_form_company');
        if (savedComp) select.value = savedComp;
        const savedName = localStorage.getItem('emp_form_name');
        if (savedName) document.getElementById('emp-name').value = savedName;
        const savedEmail = localStorage.getItem('emp_form_email');
        if (savedEmail) document.getElementById('emp-email').value = savedEmail;
        const savedRole = localStorage.getItem('emp_form_role');
        if (savedRole) document.getElementById('emp-role').value = savedRole;

        employeeModal.style.display = 'flex';
    });
}

// 💾 Save Modal State on Input
['emp-name', 'emp-email', 'emp-role', 'emp-company-select'].forEach(id => {
    const el = document.getElementById(id);
    if (el) {
        el.addEventListener('input', (e) => {
            const key = id.replace(/-/g, '_');
            localStorage.setItem(key, e.target.value);
        });
    }
});


const submitEmployeeBtn = document.getElementById('submit-employee-btn');
if (submitEmployeeBtn) {
    submitEmployeeBtn.addEventListener('click', async () => {
        const name = document.getElementById('emp-name').value;
        const email = document.getElementById('emp-email').value;
        const password = document.getElementById('emp-password').value;
        const role = document.getElementById('emp-role').value;
        const company_id = parseInt(document.getElementById('emp-company-select').value);

        if (!name || !email || !password || isNaN(company_id)) return alert("All fields are required");

        try {
            const resp = await fetch('/api/company/users', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name, email, password, role, company_id })
            });
            const data = await resp.json();
            if (data.success) {
                alert("Employee created successfully!");
                employeeModal.style.display = 'none';
                // Clear fields and storage
                ['emp-name', 'emp-email', 'emp-password', 'emp_form_name', 'emp_form_email'].forEach(id => {
                    const el = document.getElementById(id);
                    if (el) el.value = '';
                    localStorage.removeItem(id.replace(/-/g, '_'));
                });
                fetchEmployees();
            } else {
                alert("Failed: " + (data.detail || "Error creating employee"));
            }
        } catch(e) {
            alert("Error connecting to server");
        }
    });
}

// -------------------
// -------------------
// Distribute Leads Logic
// -------------------
// 📊 Distribute Leads Options Loader
// -------------------
async function loadDistributeOptions() {
    const empSelect = document.getElementById('dist-employee-select');
    const compSelect = document.getElementById('dist-company-select');
    if (!empSelect || !compSelect) return;

    try {
        // Load Employees
        const respEmp = await fetch('/api/company/users');
        const employees = await respEmp.json();
        empSelect.innerHTML = employees.map(e => `<option value="${e.id}">${e.name || 'N/A'} (${e.email})</option>`).join('');
        
        // 💾 Restore Selection
        const savedEmp = localStorage.getItem('dist_target_employee');
        if (savedEmp) empSelect.value = savedEmp;
        empSelect.addEventListener('change', (e) => localStorage.setItem('dist_target_employee', e.target.value));

        // Hardcode Companies
        const companies = [
            { id: 1, name: "DP" },
            { id: 2, name: "MCC" },
            { id: 3, name: "VC" },
            { id: 4, name: "ST" }
        ];
        compSelect.innerHTML = `<option value="0">All Companies</option>` + companies.map(c => `<option value="${c.id}">${c.name}</option>`).join('');
        
        // 💾 Restore Selection
        const savedComp = localStorage.getItem('dist_target_company');
        if (savedComp) compSelect.value = savedComp;
        compSelect.addEventListener('change', (e) => localStorage.setItem('dist_target_company', e.target.value));

    } catch (e) {
        showToast("Failed to load options for distribution", 'error');
    }
}


// 🔄 Toggle Distributions Layout
const distTypeSelect = document.getElementById('dist-type');
if (distTypeSelect) {
    distTypeSelect.addEventListener('change', (e) => {
        const type = e.target.value;
        localStorage.setItem('dist_type', type);
        document.getElementById('dist-employee-selector-div').style.display = type === 'all' ? 'none' : 'block';
        document.getElementById('dist-company-selector-div').style.display = type === 'all' ? 'block' : 'none';
    });
}

let is_distribution_running = false; 
const statusDistributeTxt = document.getElementById('status-distribute');

async function checkAutonomousStatus() {
    if (!statusDistributeTxt) return;
    try {
        const resp = await fetch('/admin/distribute/status');
        const data = await resp.json();
        
        is_distribution_running = data.is_running;

        statusDistributeTxt.innerText = "Status: " + (data.is_running ? "Running (Batch: " + data.lead_count_per_company + ")" : "Stopped");
        statusDistributeTxt.style.color = data.is_running ? "#10b981" : "#94a3b8";

        const btn = document.getElementById('submit-distribute-btn');
        if (btn) {
            if (data.is_running) {
                btn.innerText = "Stop Distribution Cycle";
                btn.style.background = "#ef4444";
            } else {
                btn.innerText = "Start Distribution Cycle";
                btn.style.background = "#10b981";
            }
        }
    } catch (e) {}
}

const submitDistributeBtn = document.getElementById('submit-distribute-btn');
if (submitDistributeBtn) {
    submitDistributeBtn.addEventListener('click', async () => {
        const count = document.getElementById('dist-count').value || 400;
        const interval = document.getElementById('dist-interval').value || 604800;

        try {
            if (is_distribution_running) {
                const resp = await fetch('/admin/distribute/stop', { method: 'POST' });
                const data = await resp.json();
                if (data.success) {
                    showToast(data.message, 'success');
                    checkAutonomousStatus();
                }
            } else {
                const resp = await fetch('/admin/distribute/start?count=' + count + '&interval=' + interval, { method: 'POST' });
                const data = await resp.json();
                if (data.success) {
                    showToast(data.message, 'success');
                    checkAutonomousStatus();
                } else {
                    showToast(data.detail || "Failed to start", 'error');
                }
            }
        } catch (e) {
            showToast("Error toggling distribution", 'error');
        }
    });
}



// Initial checks for Autonomous Continuous Status
setInterval(checkAutonomousStatus, 15000); // Check every 15s
setTimeout(checkAutonomousStatus, 2000);    // Check once loaded

// 📱 Mobile Dashboard Sidebar Toggle
document.addEventListener('DOMContentLoaded', () => {
    const sidebar = document.querySelector('.sidebar');
    const toggle = document.getElementById('mobile-toggle-btn');
    const links = document.querySelectorAll('.nav-links a');

    if (toggle && sidebar) {
        toggle.addEventListener('click', (e) => {
            e.stopPropagation();
            sidebar.classList.toggle('active');
        });

        // Close when clicking outside
        document.addEventListener('click', (e) => {
            if (sidebar.classList.contains('active') && !sidebar.contains(e.target) && e.target !== toggle) {
                sidebar.classList.remove('active');
            }
        });

        // Close when clicking nav links on mobile
        links.forEach(link => {
            link.addEventListener('click', () => {
                if (window.innerWidth <= 768) sidebar.classList.remove('active');
            });
        });

        // 📜 Scroll Position Tracker Memory continuous
        const mainContent = document.querySelector('.main-content');
        if (mainContent) {
            mainContent.addEventListener('scroll', () => {
                const currentView = localStorage.getItem('active_view') || 'nav-emails';
                localStorage.setItem(`scroll_${currentView}`, mainContent.scrollTop);
            });
        }
    }
});

// -------------------
// Pipeline Automation Logic
// -------------------
let is_pipeline_running = false; 
let is_pipeline_active = false;
let was_pipeline_active = false; // Tracks previous execution state for auto-refresh
const statusPipelineTxt = document.getElementById('pipeline-status-text');

async function checkPipelineStatus() {
    if (!statusPipelineTxt) return;
    try {
        const resp = await fetch('/admin/pipeline/status');
        const data = await resp.json();
        
        // 🔄 Auto-update Leads when script finishes executing
        if (was_pipeline_active && !data.is_active) {
            console.log("Pipeline script execution completed. Updating leads list...");
            if (typeof fetchLeads === 'function') fetchLeads();
        }
        was_pipeline_active = data.is_active;

        is_pipeline_running = data.is_running;
        is_pipeline_active = data.is_active;


        let statusText = "Status: " + (data.is_running ? "Schedule Running" : "Schedule Stopped");
        if (data.is_active) {
            statusText += " | 🏃 Script Executing Now...";
        }

        statusPipelineTxt.innerText = statusText;
        statusPipelineTxt.style.color = data.is_active ? "#38bdf8" : (data.is_running ? "#10b981" : "#94a3b8");

        const btn = document.getElementById('start-pipeline-btn');
        if (btn) {
            if (data.is_running) {
                btn.innerHTML = '<i class="fa-solid fa-stop"></i> Stop Schedule';
                btn.style.background = "#ef4444";
            } else {
                btn.innerHTML = '<i class="fa-solid fa-play"></i> Start Schedule';
                btn.style.background = "#10b981";
            }
        }

        const triggerBtn = document.getElementById('trigger-pipeline-btn');
        if (triggerBtn) {
             if (data.is_active) {
                  triggerBtn.innerText = "Executing...";
                  triggerBtn.disabled = true;
                  triggerBtn.style.opacity = "0.7";
             } else {
                  triggerBtn.innerHTML = '<i class="fa-solid fa-bolt"></i> Run Now';
                  triggerBtn.disabled = false;
                  triggerBtn.style.opacity = "1";
             }
        }

        const killBtn = document.getElementById('kill-pipeline-btn');
        if (killBtn) {
            killBtn.style.display = data.is_active ? 'flex' : 'none';
        }

        // 🖥️ Update Terminal Logs Wrapper (Standard View)
        const logsContainer = document.getElementById('pipeline-logs-wrapper');
        const logsEl = document.getElementById('pipeline-logs');
        if (logsContainer && logsEl) {
            if (data.logs && data.logs.length > 0) {
                logsContainer.style.display = 'block';
                let currentText = data.logs.join('\n');
                if (logsEl.innerText !== currentText) {
                    logsEl.innerText = currentText;
                    logsEl.scrollTop = logsEl.scrollHeight;
                }
            } else if (!data.is_active) {
                logsContainer.style.display = 'none';
            }
        }

        // 🖥️ Update Diagnostics Logs Wrapper (Test View)
        const diagLogsContainer = document.getElementById('test-pipeline-logs-wrapper');
        const diagLogsEl = document.getElementById('test-pipeline-logs');
        if (diagLogsContainer && diagLogsEl) {
            if (data.logs && data.logs.length > 0) {
                diagLogsContainer.style.display = 'block';
                let currentText = data.logs.join('\n');
                if (diagLogsEl.innerText !== currentText) {
                    diagLogsEl.innerText = currentText;
                    diagLogsEl.scrollTop = diagLogsEl.scrollHeight;
                }
            } else if (!data.is_active) {
                diagLogsContainer.style.display = 'none';
            }
        }

    } catch (e) {}
}

const startPipelineBtn = document.getElementById('start-pipeline-btn');
if (startPipelineBtn) {
    startPipelineBtn.addEventListener('click', async () => {
        const interval = document.getElementById('pipeline-interval').value || 3600;

        try {
            if (is_pipeline_running) {
                const resp = await fetch('/admin/pipeline/stop', { method: 'POST' });
                const data = await resp.json();
                if (data.success) {
                    showToast(data.message, 'success');
                    checkPipelineStatus();
                }
            } else {
                const resp = await fetch('/admin/pipeline/start?interval=' + interval, { method: 'POST' });
                const data = await resp.json();
                if (data.success) {
                    showToast(data.message, 'success');
                    checkPipelineStatus();
                } else {
                    showToast(data.detail || "Failed to start", 'error');
                }
            }
        } catch (e) {
            showToast("Error toggling pipeline schedule", 'error');
        }
    });
}

const killPipelineBtn = document.getElementById('kill-pipeline-btn');
if (killPipelineBtn) {
    killPipelineBtn.addEventListener('click', async () => {
        if (!confirm("Are you sure you want to STOP the running pipeline? This aborts current execution immediately!")) return;
        try {
            const resp = await fetch('/admin/pipeline/kill', { method: 'POST' });
            const data = await resp.json();
            if (data.success) {
                showToast(data.message, 'success');
                checkPipelineStatus();
            } else {
                showToast(data.message || "Failed to stop", 'error');
            }
        } catch(e) { showToast("Error connecting to server", 'error'); }
    });
}

const triggerPipelineBtn = document.getElementById('trigger-pipeline-btn');
if (triggerPipelineBtn) {
    triggerPipelineBtn.addEventListener('click', async () => {
        const scriptKey = document.getElementById('pipeline-script-select') ? document.getElementById('pipeline-script-select').value : null;
        try {
            const resp = await fetch('/admin/pipeline/trigger' + (scriptKey ? '?script_key=' + scriptKey : ''), { method: 'POST' });
            const data = await resp.json();
            if (data.success) {
                showToast(data.message, 'success');
                checkPipelineStatus();
            } else {
                 showToast(data.detail || data.message || "Failed to trigger", 'error');
            }
        } catch(e) { showToast("Error connecting to server", 'error'); }
    });
}

// 🛠️ Sub-scripts Individual Run Handlers

document.querySelectorAll('.run-script-btn').forEach(btn => {
    btn.addEventListener('click', async (e) => {
         const scriptKey = e.currentTarget.getAttribute('data-script');
         
         // 🏷️ Update active label in Diag Terminal
         const labelEl = document.getElementById('active-diag-script-label');
         if (labelEl) labelEl.innerText = scriptKey + ".py";

         try {
              const resp = await fetch(`/admin/pipeline/trigger?script_key=${scriptKey}`, { method: 'POST' });
              const data = await resp.json();
              if (data.success) {
                   showToast(data.message, 'success');
                   checkPipelineStatus();
              } else {
                   showToast(data.detail || "Failed to trigger.", 'error');
              }
         } catch(e) {
              showToast("Error triggering script", 'error');
         }
    });
});

// 🗑️ Admin Clear Database 
const clearLeadsBtn = document.getElementById('clear-leads-btn');
if (clearLeadsBtn) {
    clearLeadsBtn.addEventListener('click', async () => {
        if (!confirm("🚨 WARNING: Are you sure you want to delete ALL leads, assignments, and verified lists from the database? This CANNOT be undone!")) return;
        try {
            const resp = await fetch('/api/emails/delete-all', { method: 'POST' });
            const data = await resp.json();
            if (data.success) {
                showToast("Database cleared successfully!", 'success');
                if (typeof fetchLeads === 'function') fetchLeads(); // Reload
            } else {
                showToast(data.detail || "Failed to clear data", 'error');
            }
        } catch (e) { showToast("Error connecting to server", 'error'); }
    });
}

// Initial checks for Pipeline Status
setInterval(checkPipelineStatus, 3000); // Check every 3s for live log display (previously 10s)
setTimeout(checkPipelineStatus, 2000);    // Check once loaded

// -------------------
// Queue Monitor Logic
// -------------------
const navQueueMonitor = document.getElementById('nav-queue-monitor');

if (navQueueMonitor) {
    navQueueMonitor.addEventListener('click', (e) => {
        e.preventDefault();
        document.getElementById('view-emails').style.display = 'none';
        document.getElementById('view-employees').style.display = 'none';
        document.getElementById('view-distribute').style.display = 'none';
        document.getElementById('view-pipeline').style.display = 'none';
        if (document.getElementById('view-pipeline-test')) document.getElementById('view-pipeline-test').style.display = 'none';
        document.getElementById('view-queue-monitor').style.display = 'block';

        navEmails.classList.remove('active');
        if (navCompany) navCompany.classList.remove('active');
        if (navDistribute) navDistribute.classList.remove('active');
        if (navPipeline) navPipeline.classList.remove('active');
        if (navPipelineTest) navPipelineTest.classList.remove('active');
        navQueueMonitor.classList.add('active');
        localStorage.setItem('active_view', 'nav-queue-monitor');
        fetchQueueMonitorStatus();
    });
}

// Also hide queue-monitor from other nav clicks
['nav-emails', 'nav-company', 'nav-distribute-sidebar', 'nav-pipeline', 'nav-pipeline-test'].forEach(id => {
    const el = document.getElementById(id);
    if (el) {
        el.addEventListener('click', () => {
            const qmView = document.getElementById('view-queue-monitor');
            if (qmView) qmView.style.display = 'none';
            if (navQueueMonitor) navQueueMonitor.classList.remove('active');
        });
    }
});

async function fetchQueueMonitorStatus() {
    try {
        const resp = await fetch('/admin/queue-monitor/status');
        if (!resp.ok) return;
        const data = await resp.json();

        // Update queue size values
        const queueMap = {
            'normalize': data.queues.normalize_queue,
            'verify': data.queues.verify_queue,
            'enrich': data.queues.enrich_queue,
            'retry': data.queues.retry_queue,
            'dead': data.queues.dead_letter_queue,
        };

        const maxSize = Math.max(...Object.values(queueMap), 1);

        for (const [key, val] of Object.entries(queueMap)) {
            const valueEl = document.getElementById(`qm-${key}`);
            const barEl = document.getElementById(`qm-bar-${key}`);
            if (valueEl) valueEl.textContent = val.toLocaleString();
            if (barEl) barEl.style.width = Math.min((val / maxSize) * 100, 100) + '%';
        }

        // Update worker status pulses
        const workers = ['intake', 'normalize', 'verify', 'enrich', 'retry'];
        for (const w of workers) {
            const pulseEl = document.getElementById(`qm-w-pulse-${w}`);
            const statusEl = document.getElementById(`qm-w-status-${w}`);
            const wData = data.workers[w];
            if (pulseEl && wData) {
                pulseEl.className = 'qm-pulse ' + (wData.status === 'running' ? 'active' : wData.status === 'idle' ? 'idle' : 'stopped');
            }
            if (statusEl && wData) {
                statusEl.textContent = wData.status.charAt(0).toUpperCase() + wData.status.slice(1);
            }
        }

        // System status
        const sysEl = document.getElementById('qm-system-status');
        if (sysEl) {
            if (data.redis_connected && data.system_running) {
                sysEl.innerHTML = '<i class="fa-solid fa-circle" style="color:#22c55e; font-size:0.6rem; margin-right:6px;"></i> System Running';
                sysEl.style.color = '#22c55e';
            } else if (data.redis_connected) {
                sysEl.innerHTML = '<i class="fa-solid fa-circle" style="color:#eab308; font-size:0.6rem; margin-right:6px;"></i> Redis Connected \u2014 Workers Stopped';
                sysEl.style.color = '#eab308';
            } else {
                sysEl.innerHTML = '<i class="fa-solid fa-circle" style="color:#ef4444; font-size:0.6rem; margin-right:6px;"></i> Redis Disconnected';
                sysEl.style.color = '#ef4444';
            }
        }

        // Updated timestamp
        const updateEl = document.getElementById('qm-last-update');
        if (updateEl) updateEl.textContent = 'Updated: ' + new Date().toLocaleTimeString();

    } catch (e) {
        console.error('Queue monitor fetch error:', e);
    }
}

// Start/Stop buttons
const qmStartBtn = document.getElementById('qm-start-btn');
if (qmStartBtn) {
    qmStartBtn.addEventListener('click', async () => {
        try {
            const resp = await fetch('/admin/queue-monitor/start', { method: 'POST' });
            const data = await resp.json();
            showToast(data.message, data.success ? 'success' : 'error');
            setTimeout(fetchQueueMonitorStatus, 1000);
        } catch (e) { showToast('Error starting workers', 'error'); }
    });
}

const qmStopBtn = document.getElementById('qm-stop-btn');
if (qmStopBtn) {
    qmStopBtn.addEventListener('click', async () => {
        if (!confirm('Stop all async pipeline workers?')) return;
        try {
            const resp = await fetch('/admin/queue-monitor/stop', { method: 'POST' });
            const data = await resp.json();
            showToast(data.message, data.success ? 'success' : 'error');
            setTimeout(fetchQueueMonitorStatus, 1000);
        } catch (e) { showToast('Error stopping workers', 'error'); }
    });
}

const qmRefreshBtn = document.getElementById('qm-refresh-btn');
if (qmRefreshBtn) {
    qmRefreshBtn.addEventListener('click', fetchQueueMonitorStatus);
}

// Auto-poll queue monitor every 5s
setInterval(fetchQueueMonitorStatus, 5000);
setTimeout(fetchQueueMonitorStatus, 2500);


