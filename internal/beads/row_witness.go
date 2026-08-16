package beads

// RowWitness reports whether this process has already seen a store answer a
// read with at least one row.
//
// It exists for one caller shape: a consumer holding a count of zero that has
// to decide whether that zero is a measurement. Two states produce it — a
// ledger that holds nothing, and a read that could not see the ledger it was
// pointed at — and they are indistinguishable from the number alone. That is
// the whole subject of unread_store_notice.go, which diagnoses the shape at
// the store layer and deliberately lets the empty read succeed, because
// refusing there would fail a merely idle city closed across `gc ready`, `gc
// rig add` and the federation.
//
// A store that has already handed this process a row settles the ambiguity in
// the one direction a read path can act on: the ledger is not empty, so a
// later zero for a whole-ledger read is a failed measurement rather than a
// small one. That lets a consumer for which zero is never a usable answer —
// the store-health denominator is the first — reject the zero without the
// store layer having to refuse anything.
//
// The capability is one-directional on purpose. SawRows reporting true is
// proof the store holds rows. Reporting false proves nothing, because a
// process that has not read the scope yet and a genuinely empty ledger answer
// identically. Callers may use it to reject a zero and never to certify one.
//
// It is optional in the style of Counter and BatchDeleter: a store that cannot
// witness its own rows simply does not implement it, and callers fall back to
// their prior behaviour rather than to a refusal.
type RowWitness interface {
	// SawRows reports whether a read of this store's scope has returned at
	// least one row in this process, counted as the store received it and
	// before any client-side filtering. Filtering can reduce a real answer to
	// nothing, and that reduction says nothing about the ledger.
	SawRows() bool
}
