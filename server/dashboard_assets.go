package server

import "embed"

// dashboardAssets is deliberately limited to the three allowlisted files.
// No arbitrary filesystem path is exposed by the HTTP handler.
//
//go:embed dashboard/index.html dashboard/dashboard.js dashboard/dashboard.css
var dashboardAssets embed.FS
