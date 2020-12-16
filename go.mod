module cz

go 1.15

require (
	github.com/brimsec/zq v0.25.0
	github.com/mccanne/z v0.0.0-20201215160356-40945bff422a
	github.com/pierrec/lz4/v4 v4.1.1 // indirect
	golang.org/x/sys v0.0.0-20201214210602-f9fddec55a1e // indirect
)

replace github.com/brimsec/zq => ../zq

replace github.com/mccanne/z => ../z
