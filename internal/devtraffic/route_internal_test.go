package devtraffic

import "testing"

func TestCheckRoutesRejectsBadTables(t *testing.T) {
	read := Shape{Name: "r", ReadOnly: true, Route: Split}
	write := Shape{Name: "w"}
	ok := Context{JobTag: "j", SplitReplicaPercent: 30, Shapes: []string{"r", "w"}}
	if err := checkRoutes([]Shape{read, write}, []Context{ok}); err != nil {
		t.Fatalf("a valid table: %v", err)
	}
	for name, tc := range map[string]struct {
		shapes   []Shape
		contexts []Context
	}{
		"write on replica": {[]Shape{{Name: "w", Route: OnReplica}}, nil},
		"write split":      {[]Shape{{Name: "w", Route: Split}}, nil},
		"unknown route":    {[]Shape{{Name: "r", ReadOnly: true, Route: "elsewhere"}}, nil},
		"percent over 100": {[]Shape{read}, []Context{{JobTag: "j", SplitReplicaPercent: 101}}},
		"negative percent": {[]Shape{read}, []Context{{JobTag: "j", SplitReplicaPercent: -1}}},
	} {
		if err := checkRoutes(tc.shapes, tc.contexts); err == nil {
			t.Errorf("%s: checkRoutes accepted it", name)
		}
	}
	if err := checkRoutes(shapes, contexts); err != nil {
		t.Fatalf("the catalog: %v", err)
	}
}
