import { useMemo, useState } from 'react';
import { initialLeads, jobsSummary, resultsSummary } from './mockData.js';

const NAV_ITEMS = [
  { id: 'dashboard', label: 'Dashboard', icon: '◌' },
  { id: 'leads', label: 'Leads', icon: '◎' },
  { id: 'domains', label: 'Domains', icon: '◇' },
  { id: 'results', label: 'Results', icon: '▣' },
  { id: 'jobs', label: 'Jobs', icon: '△' },
  { id: 'settings', label: 'Settings', icon: '✦' }
];

const STATUS_OPTIONS = ['New', 'In Progress', 'Contacted', 'Replied', 'Closed'];
const OWNER_OPTIONS = ['Unassigned', 'Vipan', 'Aman', 'Team'];
const PLATFORM_OPTIONS = ['All', 'LinkedIn', 'Instagram'];

const sectionMeta = {
  dashboard: {
    title: 'Lead Command Center',
    subtitle: 'A standalone preview UI for managing scraped social leads on the web.'
  },
  leads: {
    title: 'Lead Review',
    subtitle: 'Filter, triage, and update outreach status without touching Telegram.'
  },
  domains: {
    title: 'Domain Coverage',
    subtitle: 'See where lead volume is strongest and jump into any domain quickly.'
  },
  results: {
    title: 'Results Library',
    subtitle: 'Browse generated result files and backups in a cleaner workspace.'
  },
  jobs: {
    title: 'Run Activity',
    subtitle: 'Track active scraping work and recent completed jobs.'
  },
  settings: {
    title: 'System Controls',
    subtitle: 'Preview a future web settings surface for proxies, tracker cadence, and updates.'
  }
};

const badgeClass = (status) => {
  const normalized = String(status).toLowerCase();
  if (normalized === 'new') return 'badge badge-new';
  if (normalized === 'in progress') return 'badge badge-progress';
  if (normalized === 'contacted') return 'badge badge-contacted';
  if (normalized === 'replied') return 'badge badge-replied';
  if (normalized === 'closed') return 'badge badge-closed';
  return 'badge';
};

const formatNow = () => {
  const now = new Date();
  return now.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
};

const App = () => {
  const [activeView, setActiveView] = useState('dashboard');
  const [leads, setLeads] = useState(initialLeads);
  const [query, setQuery] = useState('');
  const [domainFilter, setDomainFilter] = useState('All');
  const [platformFilter, setPlatformFilter] = useState('All');
  const [statusFilter, setStatusFilter] = useState('All');
  const [selectedLeadId, setSelectedLeadId] = useState(initialLeads[0].id);
  const [selectedResultDomain, setSelectedResultDomain] = useState(resultsSummary[0].domain);
  const [proxyMode, setProxyMode] = useState('Auto');
  const [schedule, setSchedule] = useState('Hourly');
  const [notice, setNotice] = useState('UI preview only. Hook this up to your SQLite/CSV/API layer later.');

  const domains = useMemo(
    () => ['All', ...Array.from(new Set(leads.map((lead) => lead.domain))).sort((a, b) => a.localeCompare(b))],
    [leads]
  );

  const filteredLeads = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();
    return leads.filter((lead) => {
      const matchesQuery =
        normalizedQuery.length === 0 ||
        lead.domain.toLowerCase().includes(normalizedQuery) ||
        lead.platform.toLowerCase().includes(normalizedQuery) ||
        lead.socialLink.toLowerCase().includes(normalizedQuery) ||
        lead.sourceUrl.toLowerCase().includes(normalizedQuery) ||
        lead.owner.toLowerCase().includes(normalizedQuery);

      const matchesDomain = domainFilter === 'All' || lead.domain === domainFilter;
      const matchesPlatform = platformFilter === 'All' || lead.platform === platformFilter;
      const matchesStatus = statusFilter === 'All' || lead.status === statusFilter;

      return matchesQuery && matchesDomain && matchesPlatform && matchesStatus;
    });
  }, [leads, query, domainFilter, platformFilter, statusFilter]);

  const selectedLead = leads.find((lead) => lead.id === selectedLeadId) || null;

  const dashboardStats = useMemo(() => {
    const linkedinCount = leads.filter((lead) => lead.platform === 'LinkedIn').length;
    const instagramCount = leads.filter((lead) => lead.platform === 'Instagram').length;
    const contactedCount = leads.filter((lead) => lead.status === 'Contacted' || lead.status === 'Replied').length;
    const newCount = leads.filter((lead) => lead.status === 'New').length;
    return {
      total: leads.length,
      linkedin: linkedinCount,
      instagram: instagramCount,
      domains: new Set(leads.map((lead) => lead.domain)).size,
      newToday: newCount,
      activeJobs: jobsSummary.current.length,
      outreachActive: contactedCount
    };
  }, [leads]);

  const domainCards = useMemo(() => {
    return Array.from(new Set(leads.map((lead) => lead.domain)))
      .sort((a, b) => a.localeCompare(b))
      .map((domain) => {
        const domainLeads = leads.filter((lead) => lead.domain === domain);
        return {
          domain,
          linkedin: domainLeads.filter((lead) => lead.platform === 'LinkedIn').length,
          instagram: domainLeads.filter((lead) => lead.platform === 'Instagram').length,
          total: domainLeads.length,
          open: domainLeads.filter((lead) => lead.status === 'New' || lead.status === 'In Progress').length
        };
      });
  }, [leads]);

  const selectedResultFolder = resultsSummary.find((entry) => entry.domain === selectedResultDomain) || resultsSummary[0];

  const updateLead = (leadId, patch) => {
    setLeads((currentLeads) =>
      currentLeads.map((lead) =>
        lead.id === leadId
          ? {
              ...lead,
              ...patch,
              updatedAt: `Updated ${formatNow()}`
            }
          : lead
      )
    );
  };

  const renderDashboard = () => (
    <div className="page-grid">
      <section className="stats-grid">
        {[
          { label: 'Total Leads', value: dashboardStats.total, accent: 'accent-fire' },
          { label: 'LinkedIn', value: dashboardStats.linkedin, accent: 'accent-sky' },
          { label: 'Instagram', value: dashboardStats.instagram, accent: 'accent-sun' },
          { label: 'New Today', value: dashboardStats.newToday, accent: 'accent-leaf' },
          { label: 'Domains', value: dashboardStats.domains, accent: 'accent-plum' },
          { label: 'Active Jobs', value: dashboardStats.activeJobs, accent: 'accent-bronze' }
        ].map((card) => (
          <article className={`stat-card ${card.accent}`} key={card.label}>
            <span className="eyebrow">{card.label}</span>
            <strong>{card.value}</strong>
          </article>
        ))}
      </section>

      <section className="panel chart-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Lead Momentum</p>
            <h2>Weekly inbound signal</h2>
          </div>
          <span className="ghost-pill">Mock data</span>
        </div>
        <div className="bar-cluster">
          {[18, 26, 22, 31, 28, 36, 41].map((value, index) => (
            <div key={index} className="bar-column">
              <div className="bar-track">
                <div className="bar-fill" style={{ height: `${value * 2}px` }} />
              </div>
              <span>{['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][index]}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="panel split-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Platform Mix</p>
            <h2>Where the best leads are landing</h2>
          </div>
        </div>
        <div className="platform-split">
          <div className="ring">
            <div className="ring-core">
              <strong>{dashboardStats.total}</strong>
              <span>leads</span>
            </div>
          </div>
          <div className="split-legend">
            <div>
              <span className="dot dot-linkedin" />
              LinkedIn
              <strong>{dashboardStats.linkedin}</strong>
            </div>
            <div>
              <span className="dot dot-instagram" />
              Instagram
              <strong>{dashboardStats.instagram}</strong>
            </div>
            <div>
              <span className="dot dot-progress" />
              Outreach Active
              <strong>{dashboardStats.outreachActive}</strong>
            </div>
          </div>
        </div>
      </section>

      <section className="panel activity-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Recent Activity</p>
            <h2>Latest signals across the workspace</h2>
          </div>
        </div>
        <ul className="activity-list">
          <li>
            <span className="activity-dot" />
            <div>
              <strong>Retry run finished for usanews.com</strong>
              <p>84 fresh success rows recovered from failed history.</p>
            </div>
            <time>12m ago</time>
          </li>
          <li>
            <span className="activity-dot activity-dot-warm" />
            <div>
              <strong>Page tracker found 9 new article URLs</strong>
              <p>bossesmag.com queue file created in <code>to scrape</code>.</p>
            </div>
            <time>24m ago</time>
          </li>
          <li>
            <span className="activity-dot activity-dot-cool" />
            <div>
              <strong>Lead status changed to Replied</strong>
              <p>Celeste House account moved into follow-up mode.</p>
            </div>
            <time>35m ago</time>
          </li>
        </ul>
      </section>
    </div>
  );

  const renderLeads = () => (
    <div className="page-grid leads-layout">
      <section className="panel leads-panel">
        <div className="toolbar">
          <input
            className="search-input"
            type="text"
            placeholder="Search domain, profile, owner, source URL..."
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <select value={domainFilter} onChange={(event) => setDomainFilter(event.target.value)}>
            {domains.map((domain) => (
              <option key={domain} value={domain}>
                {domain}
              </option>
            ))}
          </select>
          <select value={platformFilter} onChange={(event) => setPlatformFilter(event.target.value)}>
            {PLATFORM_OPTIONS.map((platform) => (
              <option key={platform} value={platform}>
                {platform}
              </option>
            ))}
          </select>
          <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
            {['All', ...STATUS_OPTIONS].map((status) => (
              <option key={status} value={status}>
                {status}
              </option>
            ))}
          </select>
        </div>

        <div className="table-shell">
          <table className="lead-table">
            <thead>
              <tr>
                <th>Domain</th>
                <th>Platform</th>
                <th>Profile</th>
                <th>Source URL</th>
                <th>Status</th>
                <th>Owner</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {filteredLeads.map((lead) => (
                <tr
                  key={lead.id}
                  className={selectedLeadId === lead.id ? 'is-selected' : ''}
                  onClick={() => setSelectedLeadId(lead.id)}
                >
                  <td>{lead.domain}</td>
                  <td>{lead.platform}</td>
                  <td>
                    <span className="truncate-link">{lead.socialLink}</span>
                  </td>
                  <td>
                    <span className="truncate-link">{lead.sourceUrl}</span>
                  </td>
                  <td>
                    <span className={badgeClass(lead.status)}>{lead.status}</span>
                  </td>
                  <td>{lead.owner}</td>
                  <td>{lead.updatedAt}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <aside className={`detail-drawer ${selectedLead ? 'is-open' : ''}`}>
        {selectedLead ? (
          <>
            <div className="panel-header">
              <div>
                <p className="eyebrow">Lead Detail</p>
                <h2>{selectedLead.domain}</h2>
              </div>
              <span className={badgeClass(selectedLead.status)}>{selectedLead.status}</span>
            </div>

            <div className="detail-list">
              <div>
                <label>Platform</label>
                <strong>{selectedLead.platform}</strong>
              </div>
              <div>
                <label>Profile URL</label>
                <a href={selectedLead.socialLink} onClick={(event) => event.preventDefault()}>
                  {selectedLead.socialLink}
                </a>
              </div>
              <div>
                <label>Source URL</label>
                <a href={selectedLead.sourceUrl} onClick={(event) => event.preventDefault()}>
                  {selectedLead.sourceUrl}
                </a>
              </div>
            </div>

            <div className="form-grid">
              <label>
                Status
                <select
                  value={selectedLead.status}
                  onChange={(event) => updateLead(selectedLead.id, { status: event.target.value })}
                >
                  {STATUS_OPTIONS.map((status) => (
                    <option key={status} value={status}>
                      {status}
                    </option>
                  ))}
                </select>
              </label>

              <label>
                Owner
                <select
                  value={selectedLead.owner}
                  onChange={(event) => updateLead(selectedLead.id, { owner: event.target.value })}
                >
                  {OWNER_OPTIONS.map((owner) => (
                    <option key={owner} value={owner}>
                      {owner}
                    </option>
                  ))}
                </select>
              </label>

              <label className="full-width">
                Note
                <textarea
                  rows="5"
                  value={selectedLead.note}
                  onChange={(event) => updateLead(selectedLead.id, { note: event.target.value })}
                />
              </label>
            </div>

            <div className="quick-actions">
              {['New', 'In Progress', 'Contacted', 'Closed'].map((status) => (
                <button key={status} type="button" onClick={() => updateLead(selectedLead.id, { status })}>
                  Mark {status}
                </button>
              ))}
            </div>
          </>
        ) : (
          <div className="empty-state">Select a lead to inspect and update it.</div>
        )}
      </aside>
    </div>
  );

  const renderDomains = () => (
    <div className="card-grid">
      {domainCards.map((card) => (
        <article className="panel domain-card" key={card.domain}>
          <div className="panel-header">
            <div>
              <p className="eyebrow">Tracked Domain</p>
              <h2>{card.domain}</h2>
            </div>
            <span className="ghost-pill">{card.total} leads</span>
          </div>
          <div className="domain-metrics">
            <div>
              <span>LinkedIn</span>
              <strong>{card.linkedin}</strong>
            </div>
            <div>
              <span>Instagram</span>
              <strong>{card.instagram}</strong>
            </div>
            <div>
              <span>Open</span>
              <strong>{card.open}</strong>
            </div>
          </div>
          <button
            type="button"
            className="text-button"
            onClick={() => {
              setActiveView('leads');
              setDomainFilter(card.domain);
              setNotice(`Showing lead queue for ${card.domain}.`);
            }}
          >
            View leads
          </button>
        </article>
      ))}
    </div>
  );

  const renderResults = () => (
    <div className="page-grid results-layout">
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Pinned Files</p>
            <h2>Fast access</h2>
          </div>
        </div>
        <div className="action-list">
          <button type="button" onClick={() => setNotice('Preview only: connect all_results.csv download later.')}>
            📄 all_results.csv
          </button>
          <button type="button" onClick={() => setNotice('Preview only: connect history.db backup later.')}>
            🗄 history.db backup
          </button>
        </div>
      </section>

      <section className="panel results-domain-list">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Domain Folders</p>
            <h2>Browse result groups</h2>
          </div>
        </div>
        <div className="stack-list">
          {resultsSummary.map((entry) => (
            <button
              key={entry.domain}
              type="button"
              className={`stack-item ${selectedResultDomain === entry.domain ? 'is-active' : ''}`}
              onClick={() => setSelectedResultDomain(entry.domain)}
            >
              <span>{entry.domain}</span>
              <small>{entry.fileCount} files</small>
            </button>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Selected Folder</p>
            <h2>{selectedResultFolder.domain}</h2>
          </div>
          <span className="ghost-pill">{selectedResultFolder.totalLeads} leads</span>
        </div>
        <div className="meta-row">
          <span>Updated {selectedResultFolder.updatedAt}</span>
          <button type="button" className="text-button" onClick={() => setNotice(`Preview only: connect ${selectedResultFolder.domain} export later.`)}>
            Open export actions
          </button>
        </div>
        <div className="file-grid">
          {selectedResultFolder.files.map((file) => (
            <button
              key={file}
              type="button"
              className="file-card"
              onClick={() => setNotice(`Preview only: connect ${selectedResultFolder.domain}/${file} download later.`)}
            >
              <strong>{file}</strong>
              <span>{selectedResultFolder.domain}</span>
            </button>
          ))}
        </div>
      </section>
    </div>
  );

  const renderJobs = () => (
    <div className="page-grid jobs-layout">
      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Current Jobs</p>
            <h2>Live workload</h2>
          </div>
        </div>
        <div className="job-list">
          {jobsSummary.current.map((job) => (
            <article className="job-card" key={job.id}>
              <div className="job-topline">
                <div>
                  <strong>{job.type}</strong>
                  <span>{job.target}</span>
                </div>
                <span className="ghost-pill">{job.status}</span>
              </div>
              <div className="mini-progress">
                <div style={{ width: `${job.progress}%` }} />
              </div>
              <div className="job-meta">
                <span>{job.progress}% complete</span>
                <span>{job.eta}</span>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Recent Runs</p>
            <h2>Completed work</h2>
          </div>
        </div>
        <div className="table-shell">
          <table className="lead-table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Target</th>
                <th>Result</th>
                <th>Started</th>
                <th>Duration</th>
                <th>Found</th>
              </tr>
            </thead>
            <tbody>
              {jobsSummary.recent.map((job) => (
                <tr key={job.id}>
                  <td>{job.type}</td>
                  <td>{job.target}</td>
                  <td>{job.result}</td>
                  <td>{job.started}</td>
                  <td>{job.duration}</td>
                  <td>{job.found}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );

  const renderSettings = () => (
    <div className="card-grid">
      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Proxy Mode</p>
            <h2>Transport profile</h2>
          </div>
        </div>
        <div className="option-grid">
          {['Auto', 'Direct Only', 'Webshare Only', 'Crawlbase Only'].map((mode) => (
            <button
              key={mode}
              type="button"
              className={`mode-card ${proxyMode === mode ? 'is-selected' : ''}`}
              onClick={() => {
                setProxyMode(mode);
                setNotice(`Proxy mode switched to ${mode} in the preview.`);
              }}
            >
              {mode}
            </button>
          ))}
        </div>
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Scheduler</p>
            <h2>Page tracker cadence</h2>
          </div>
        </div>
        <div className="option-grid">
          {['Hourly', 'Every 2 Hours', 'Every 6 Hours'].map((value) => (
            <button
              key={value}
              type="button"
              className={`mode-card ${schedule === value ? 'is-selected' : ''}`}
              onClick={() => {
                setSchedule(value);
                setNotice(`Schedule preview changed to ${value}.`);
              }}
            >
              {value}
            </button>
          ))}
        </div>
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Credentials</p>
            <h2>Provider status</h2>
          </div>
        </div>
        <div className="settings-list">
          <div>
            <span>Webshare</span>
            <strong>Configured</strong>
          </div>
          <div>
            <span>Crawlbase</span>
            <strong>Configured</strong>
          </div>
          <div>
            <span>Last sync</span>
            <strong>Today, 1:04 PM</strong>
          </div>
        </div>
      </article>
    </div>
  );

  const renderActiveView = () => {
    if (activeView === 'dashboard') return renderDashboard();
    if (activeView === 'leads') return renderLeads();
    if (activeView === 'domains') return renderDomains();
    if (activeView === 'results') return renderResults();
    if (activeView === 'jobs') return renderJobs();
    return renderSettings();
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark">LC</span>
          <div>
            <h1>Lead Console</h1>
            <p>Frontend preview</p>
          </div>
        </div>

        <nav className="nav-list">
          {NAV_ITEMS.map((item) => (
            <button
              key={item.id}
              type="button"
              className={`nav-item ${activeView === item.id ? 'is-active' : ''}`}
              onClick={() => setActiveView(item.id)}
            >
              <span>{item.icon}</span>
              {item.label}
            </button>
          ))}
        </nav>

        <div className="sidebar-footnote">
          <span className="eyebrow">Built isolated</span>
          <p>Delete the entire <code>frontend/</code> folder later if you decide not to keep the web UI.</p>
        </div>
      </aside>

      <main className="main-stage">
        <header className="topbar">
          <div>
            <p className="eyebrow">Lead operations</p>
            <h2>{sectionMeta[activeView].title}</h2>
            <p className="topbar-copy">{sectionMeta[activeView].subtitle}</p>
          </div>

          <div className="topbar-actions">
            <div className="status-chip">Mock data only</div>
            <button type="button" className="primary-button" onClick={() => setNotice('Connect these screens to your DB/API layer next.')}>
              Plan backend hookup
            </button>
          </div>
        </header>

        <section className="notice-bar">
          <strong>Workspace note</strong>
          <span>{notice}</span>
        </section>

        {renderActiveView()}
      </main>
    </div>
  );
};

export default App;
