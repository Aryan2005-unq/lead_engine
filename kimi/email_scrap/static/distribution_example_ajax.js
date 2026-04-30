/**
 * Frontend AJAX Example for the Lead Distribution Engine
 **/

// 1. Admin Submitting Lead Allocation counts
async function distributeLeadsToCompanies() {
    // Collect layout values
    const allocations = [
        { company_id: 1, lead_count: 100 },
        { company_id: 2, lead_count: 50 }
    ];

    try {
        const resp = await fetch('/admin/distribute-leads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ company_allocations: allocations })
        });

        const data = await resp.json();
        if (resp.ok && data.success) {
            alert(data.message);
            // Reload summary tables views if applicable
        } else {
            alert("Distribution Failed: " + (data.detail || "Unknown error"));
        }
    } catch (e) {
        alert("Error connecting to distribute leads server triggers");
    }
}

// 2. Member Fetching their assigned Leads list
async function fetchMemberLeads() {
    try {
        const resp = await fetch('/member/leads');
        const leads = await resp.json();

        // Populate items row templates
        const container = document.getElementById('leads-container');
        if (container) {
            container.innerHTML = leads.map(l => `<li>${l.email}</li>`).join('');
        }
    } catch (e) {
        console.error("Failed fetching leads lists", e);
    }
}

// 3. Admin fetching assignments Statistics summaries
async function fetchDistributionSummary() {
    try {
        const resp = await fetch('/admin/distribution-summary');
        const summary = await resp.json();

        const tableBody = document.getElementById('summary-table-body');
        if (tableBody) {
            tableBody.innerHTML = summary.map(item => `
                <tr>
                    <td>${item.name}</td>
                    <td>${item.members}</td>
                    <td>${item.leads_assigned}</td>
                </tr>
            `).join('');
        }
    } catch (e) {
        console.error("Failed fetching summaries counters", e);
    }
}
