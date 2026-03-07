package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParsePagination_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/v0/beads", nil)
	pp := parsePagination(r, 50)
	if pp.Offset != 0 {
		t.Errorf("offset = %d, want 0", pp.Offset)
	}
	if pp.Limit != 50 {
		t.Errorf("limit = %d, want 50", pp.Limit)
	}
	if pp.IsPaging {
		t.Error("IsPaging should be false when no cursor/limit params")
	}
}

func TestParsePagination_LimitOnly(t *testing.T) {
	r := httptest.NewRequest("GET", "/v0/beads?limit=10", nil)
	pp := parsePagination(r, 50)
	if pp.Limit != 10 {
		t.Errorf("limit = %d, want 10", pp.Limit)
	}
	if pp.Offset != 0 {
		t.Errorf("offset = %d, want 0", pp.Offset)
	}
	if pp.IsPaging {
		t.Error("IsPaging should be false when only limit is provided (no cursor)")
	}
}

func TestParsePagination_CursorAndLimit(t *testing.T) {
	cursor := encodeCursor(25)
	r := httptest.NewRequest("GET", "/v0/beads?cursor="+cursor+"&limit=10", nil)
	pp := parsePagination(r, 50)
	if pp.Offset != 25 {
		t.Errorf("offset = %d, want 25", pp.Offset)
	}
	if pp.Limit != 10 {
		t.Errorf("limit = %d, want 10", pp.Limit)
	}
	if !pp.IsPaging {
		t.Error("IsPaging should be true when cursor is provided")
	}
}

func TestParsePagination_LimitCapped(t *testing.T) {
	r := httptest.NewRequest("GET", "/v0/beads?limit=999999", nil)
	pp := parsePagination(r, 50)
	if pp.Limit != maxPaginationLimit {
		t.Errorf("limit = %d, want %d (capped)", pp.Limit, maxPaginationLimit)
	}
}

func TestParsePagination_InvalidCursor(t *testing.T) {
	r := httptest.NewRequest("GET", "/v0/beads?cursor=invalid!!!", nil)
	pp := parsePagination(r, 50)
	if pp.Offset != 0 {
		t.Errorf("offset = %d, want 0 for invalid cursor", pp.Offset)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	for _, offset := range []int{0, 1, 50, 100, 9999} {
		cursor := encodeCursor(offset)
		got := decodeCursor(cursor)
		if got != offset {
			t.Errorf("roundtrip(%d): got %d", offset, got)
		}
	}
}

func TestPaginate_FirstPage(t *testing.T) {
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	page, total, next := paginate(items, pageParams{Offset: 0, Limit: 3})
	if total != 10 {
		t.Errorf("total = %d, want 10", total)
	}
	if len(page) != 3 {
		t.Fatalf("len(page) = %d, want 3", len(page))
	}
	if page[0] != 1 || page[2] != 3 {
		t.Errorf("page = %v, want [1,2,3]", page)
	}
	if next == "" {
		t.Error("next cursor should be non-empty")
	}
	// Decode next cursor and verify it's 3.
	if decodeCursor(next) != 3 {
		t.Errorf("next cursor offset = %d, want 3", decodeCursor(next))
	}
}

func TestPaginate_MiddlePage(t *testing.T) {
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	page, total, next := paginate(items, pageParams{Offset: 3, Limit: 3})
	if total != 10 {
		t.Errorf("total = %d, want 10", total)
	}
	if len(page) != 3 {
		t.Fatalf("len(page) = %d, want 3", len(page))
	}
	if page[0] != 4 || page[2] != 6 {
		t.Errorf("page = %v, want [4,5,6]", page)
	}
	if next == "" {
		t.Error("next cursor should be non-empty")
	}
}

func TestPaginate_LastPage(t *testing.T) {
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	page, total, next := paginate(items, pageParams{Offset: 9, Limit: 3})
	if total != 10 {
		t.Errorf("total = %d, want 10", total)
	}
	if len(page) != 1 {
		t.Fatalf("len(page) = %d, want 1", len(page))
	}
	if page[0] != 10 {
		t.Errorf("page = %v, want [10]", page)
	}
	if next != "" {
		t.Errorf("next cursor should be empty on last page, got %q", next)
	}
}

func TestPaginate_BeyondEnd(t *testing.T) {
	items := []int{1, 2, 3}
	page, total, next := paginate(items, pageParams{Offset: 100, Limit: 10})
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if page != nil {
		t.Errorf("page should be nil for offset beyond end, got %v", page)
	}
	if next != "" {
		t.Errorf("next should be empty, got %q", next)
	}
}

func TestPaginate_EmptySlice(t *testing.T) {
	page, total, next := paginate([]int{}, pageParams{Offset: 0, Limit: 10})
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if page != nil {
		t.Errorf("page should be nil for empty slice, got %v", page)
	}
	if next != "" {
		t.Errorf("next should be empty, got %q", next)
	}
}

func TestPaginate_ExactFit(t *testing.T) {
	items := []int{1, 2, 3}
	page, total, next := paginate(items, pageParams{Offset: 0, Limit: 3})
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if len(page) != 3 {
		t.Fatalf("len(page) = %d, want 3", len(page))
	}
	if next != "" {
		t.Errorf("next should be empty when page fits exactly, got %q", next)
	}
}

func TestWritePagedJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writePagedJSON(w, 42, []string{"a", "b"}, 5, encodeCursor(2))

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-GC-Index"); got != "42" {
		t.Errorf("X-GC-Index = %q, want 42", got)
	}
	// Body should include next_cursor.
	body := w.Body.String()
	if !contains(body, "next_cursor") {
		t.Errorf("body should contain next_cursor: %s", body)
	}
}

func TestWritePagedJSON_NoCursor(t *testing.T) {
	w := httptest.NewRecorder()
	writePagedJSON(w, 1, []string{"a"}, 1, "")

	body := w.Body.String()
	if contains(body, "next_cursor") {
		t.Errorf("body should not contain next_cursor when empty: %s", body)
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && indexSubstring(s, substr) >= 0
}

func indexSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
