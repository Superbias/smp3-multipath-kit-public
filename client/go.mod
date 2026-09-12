module github.com/Superbias/smp3-multipath-kit-public/client

go 1.22

require (
	github.com/Superbias/smp3-multipath-kit-public/server v0.0.0
	github.com/Superbias/smp3-multipath-kit-public/smp3core v0.0.0
)

replace github.com/Superbias/smp3-multipath-kit-public/smp3core => ../core
replace github.com/Superbias/smp3-multipath-kit-public/server => ../server
