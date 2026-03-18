export const initialLeads = [
  {
    id: 1,
    domain: 'nyweekly.com',
    platform: 'LinkedIn',
    socialLink: 'https://www.linkedin.com/in/alex-harper',
    sourceUrl: 'https://nyweekly.com/business/alex-harper-growth-story',
    status: 'New',
    owner: 'Unassigned',
    note: 'Strong founder profile and recent feature.',
    timestamp: 'Mar 17, 2026',
    updatedAt: '2m ago'
  },
  {
    id: 2,
    domain: 'nyweekly.com',
    platform: 'Instagram',
    socialLink: 'https://www.instagram.com/mila.studio',
    sourceUrl: 'https://nyweekly.com/lifestyle/mila-studio-creative-rhythm',
    status: 'In Progress',
    owner: 'Vipan',
    note: 'Needs warm intro angle.',
    timestamp: 'Mar 17, 2026',
    updatedAt: '8m ago'
  },
  {
    id: 3,
    domain: 'usanews.com',
    platform: 'LinkedIn',
    socialLink: 'https://www.linkedin.com/company/atlas-ventures',
    sourceUrl: 'https://usanews.com/newsroom/atlas-ventures-market-move',
    status: 'Contacted',
    owner: 'Aman',
    note: 'First outreach sent.',
    timestamp: 'Mar 16, 2026',
    updatedAt: '21m ago'
  },
  {
    id: 4,
    domain: 'usawire.com',
    platform: 'Instagram',
    socialLink: 'https://www.instagram.com/ellajamesco',
    sourceUrl: 'https://usawire.com/ella-james-brand-expansion',
    status: 'Replied',
    owner: 'Vipan',
    note: 'Asked for media kit follow-up.',
    timestamp: 'Mar 16, 2026',
    updatedAt: '35m ago'
  },
  {
    id: 5,
    domain: 'wikitia.com',
    platform: 'LinkedIn',
    socialLink: 'https://www.linkedin.com/in/ryan-brooks',
    sourceUrl: 'https://wikitia.com/wiki/Ryan_Brooks',
    status: 'Closed',
    owner: 'Team',
    note: 'Already connected via partner.',
    timestamp: 'Mar 15, 2026',
    updatedAt: '1h ago'
  },
  {
    id: 6,
    domain: 'ceoweekly.com',
    platform: 'Instagram',
    socialLink: 'https://www.instagram.com/founderatlas',
    sourceUrl: 'https://ceoweekly.com/founderatlas-community-builders',
    status: 'New',
    owner: 'Unassigned',
    note: 'Strong visual brand.',
    timestamp: 'Mar 15, 2026',
    updatedAt: '2h ago'
  },
  {
    id: 7,
    domain: 'brainzmagazine.com',
    platform: 'LinkedIn',
    socialLink: 'https://www.linkedin.com/in/nina-frost',
    sourceUrl: 'https://brainzmagazine.com/post/nina-frost-growth-design',
    status: 'In Progress',
    owner: 'Vipan',
    note: 'Hold for April campaign.',
    timestamp: 'Mar 14, 2026',
    updatedAt: '3h ago'
  },
  {
    id: 8,
    domain: 'bossesmag.com',
    platform: 'Instagram',
    socialLink: 'https://www.instagram.com/ardencollective',
    sourceUrl: 'https://bossesmag.com/arden-collective-feature',
    status: 'Contacted',
    owner: 'Aman',
    note: 'Waiting on follow-up window.',
    timestamp: 'Mar 14, 2026',
    updatedAt: '5h ago'
  },
  {
    id: 9,
    domain: 'nyweeklymagazine.com',
    platform: 'LinkedIn',
    socialLink: 'https://www.linkedin.com/company/studio-summit',
    sourceUrl: 'https://nyweeklymagazine.com/interview/studio-summit',
    status: 'New',
    owner: 'Unassigned',
    note: 'Good company page for B2B pitch.',
    timestamp: 'Mar 13, 2026',
    updatedAt: '7h ago'
  },
  {
    id: 10,
    domain: 'hauteliving.com',
    platform: 'Instagram',
    socialLink: 'https://www.instagram.com/celestehouse',
    sourceUrl: 'https://hauteliving.com/celeste-house-interior-story',
    status: 'In Progress',
    owner: 'Team',
    note: 'Luxury design angle.',
    timestamp: 'Mar 13, 2026',
    updatedAt: '9h ago'
  }
];

export const resultsSummary = [
  {
    domain: 'nyweekly.com',
    updatedAt: '5m ago',
    fileCount: 8,
    totalLeads: 608,
    files: ['facebook.csv', 'github.csv', 'instagram.csv', 'linkedin.csv', 'pinterest.csv', 'tiktok.csv', 'twitter.csv', 'youtube.csv']
  },
  {
    domain: 'usanews.com',
    updatedAt: '18m ago',
    fileCount: 8,
    totalLeads: 731,
    files: ['facebook.csv', 'github.csv', 'instagram.csv', 'linkedin.csv', 'pinterest.csv', 'tiktok.csv', 'twitter.csv', 'youtube.csv']
  },
  {
    domain: 'usawire.com',
    updatedAt: '42m ago',
    fileCount: 8,
    totalLeads: 518,
    files: ['facebook.csv', 'github.csv', 'instagram.csv', 'linkedin.csv', 'pinterest.csv', 'tiktok.csv', 'twitter.csv', 'youtube.csv']
  },
  {
    domain: 'wikitia.com',
    updatedAt: '58m ago',
    fileCount: 8,
    totalLeads: 284,
    files: ['facebook.csv', 'github.csv', 'instagram.csv', 'linkedin.csv', 'pinterest.csv', 'tiktok.csv', 'twitter.csv', 'youtube.csv']
  }
];

export const jobsSummary = {
  current: [
    {
      id: 'job-1',
      type: 'Folder Scrape',
      target: 'to scrape',
      progress: 64,
      status: 'Running',
      eta: '06:20 left'
    },
    {
      id: 'job-2',
      type: 'Page Tracker',
      target: 'nyweekly.com',
      progress: 22,
      status: 'Running',
      eta: '01:45 left'
    }
  ],
  recent: [
    {
      id: 'recent-1',
      type: 'Retry',
      target: 'usanews.com',
      result: 'Complete',
      started: '10:12 AM',
      duration: '12m',
      found: 84
    },
    {
      id: 'recent-2',
      type: 'XML Rescan',
      target: 'nyweekly.com',
      result: 'Complete',
      started: '9:42 AM',
      duration: '7m',
      found: 37
    },
    {
      id: 'recent-3',
      type: 'Page Tracker',
      target: 'bossesmag.com',
      result: 'Complete',
      started: '9:00 AM',
      duration: '2m',
      found: 9
    }
  ]
};
