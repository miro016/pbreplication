package pbreplication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
)

// timeAfterPoll polls cond for up to 5s and reports whether it became true.
func timeAfterPoll(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// makeRelatedCollections builds "authors" and "books" where books has a
// single relation (author) and a multi relation (reviewers) to authors.
func makeRelatedCollections(t *testing.T, app core.App) (*core.Collection, *core.Collection) {
	t.Helper()

	authors := core.NewBaseCollection("authors")
	authors.Fields.Add(&core.TextField{Name: "name"})
	if err := app.Save(authors); err != nil {
		t.Fatal(err)
	}

	books := core.NewBaseCollection("books")
	books.Fields.Add(
		&core.TextField{Name: "title"},
		&core.RelationField{Name: "author", CollectionId: authors.Id, MaxSelect: 1},
		&core.RelationField{Name: "reviewers", CollectionId: authors.Id, MaxSelect: 5},
	)
	if err := app.Save(books); err != nil {
		t.Fatal(err)
	}
	return authors, books
}

func TestIntegrityDetectsDangling(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	authors, books := makeRelatedCollections(t, app)

	author := core.NewRecord(authors)
	author.Set("name", "real author")
	if err := app.Save(author); err != nil {
		t.Fatal(err)
	}

	// book 1: valid single + one dangling multi ref
	b1 := core.NewRecord(books)
	b1.Set("title", "ok-and-dangling")
	b1.Set("author", author.Id)
	b1.Set("reviewers", []string{author.Id, "ghost0000000001"})
	if err := app.SaveNoValidate(b1); err != nil {
		t.Fatal(err)
	}

	// book 2: dangling single ref
	b2 := core.NewRecord(books)
	b2.Set("title", "dangling-single")
	b2.Set("author", "ghost0000000002")
	if err := app.SaveNoValidate(b2); err != nil {
		t.Fatal(err)
	}

	rep, err := r.RunIntegrityCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Converged {
		t.Fatal("report must not converge with dangling refs present")
	}
	if len(rep.Dangling) != 2 {
		t.Fatalf("dangling = %d, want 2: %+v", len(rep.Dangling), rep.Dangling)
	}

	byTarget := map[string]DanglingRef{}
	for _, d := range rep.Dangling {
		byTarget[d.TargetID] = d
	}
	if d, ok := byTarget["ghost0000000001"]; !ok || d.Field != "reviewers" || d.Collection != "books" {
		t.Fatalf("multi-relation dangling ref wrong: %+v", byTarget)
	}
	if d, ok := byTarget["ghost0000000002"]; !ok || d.Field != "author" {
		t.Fatalf("single-relation dangling ref wrong: %+v", byTarget)
	}
	if d := byTarget["ghost0000000001"]; d.TargetCollection != "authors" {
		t.Fatalf("target collection wrong: %+v", d)
	}
}

func TestIntegrityConvergesAfterTargetArrives(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	authors, books := makeRelatedCollections(t, app)

	b := core.NewRecord(books)
	b.Set("title", "early")
	b.Set("author", "lateauthor00001")
	if err := app.SaveNoValidate(b); err != nil {
		t.Fatal(err)
	}

	rep, err := r.RunIntegrityCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Converged || len(rep.Dangling) != 1 {
		t.Fatalf("expected exactly one dangling ref, got %+v", rep.Dangling)
	}

	// the referenced record arrives later (out-of-order replication)
	late := core.NewRecord(authors)
	late.Id = "lateauthor00001"
	late.Set("name", "late author")
	if err := app.Save(late); err != nil {
		t.Fatal(err)
	}

	rep, err = r.RunIntegrityCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Converged || len(rep.Dangling) != 0 {
		t.Fatalf("expected convergence after target arrived: %+v", rep.Dangling)
	}
	if rep.CheckedRefs == 0 || rep.ScannedRecords == 0 {
		t.Fatalf("counters not tracked: %+v", rep)
	}
}

func TestIntegrityCapsReport(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	_, books := makeRelatedCollections(t, app)

	// more dangling refs than the report cap
	const n = maxReportedDangling + 50
	for i := 0; i < n; i++ {
		b := core.NewRecord(books)
		b.Set("title", fmt.Sprintf("b%d", i))
		b.Set("author", fmt.Sprintf("ghost%010d", i))
		if err := app.SaveNoValidate(b); err != nil {
			t.Fatal(err)
		}
	}

	rep, err := r.RunIntegrityCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Dangling) != maxReportedDangling {
		t.Fatalf("report list = %d, want cap %d", len(rep.Dangling), maxReportedDangling)
	}
	if rep.TruncatedAt != n {
		t.Fatalf("TruncatedAt = %d, want %d", rep.TruncatedAt, n)
	}
	if danglingTotal(rep) != n {
		t.Fatalf("danglingTotal = %d, want %d", danglingTotal(rep), n)
	}
}

func TestIntegrityScheduleAndQuiescence(t *testing.T) {
	app, r := newTestNode(t, "nodeA0000000001")
	makeRelatedCollections(t, app)

	r.scheduleIntegrityCheck()
	if !r.integrityPending.Load() {
		t.Fatal("schedule must set the pending flag")
	}

	// not quiescent: parked ops present -> must not start
	r.parkPending(&op{Type: opUpsert, ColID: "x", ColName: "x", RecordID: "y", SrcNode: "z"})
	r.maybeRunIntegrity()
	if !r.integrityPending.Load() {
		t.Fatal("check must not start while ops are parked")
	}
	r.pendingMu.Lock()
	r.pendingOps = nil
	r.pendingMu.Unlock()

	// quiescent -> starts (and completes: tiny DB)
	r.maybeRunIntegrity()
	deadline := timeAfterPoll(t, func() bool { return r.LastIntegrityReport() != nil })
	if !deadline {
		t.Fatal("integrity check did not complete")
	}
	rep := r.LastIntegrityReport()
	if !rep.Converged {
		t.Fatalf("clean DB must converge: %+v", rep.Dangling)
	}

	// event emitted
	found := false
	for _, ev := range r.Events(0) {
		if ev.Type == EventIntegrityReport {
			found = true
		}
	}
	if !found {
		t.Fatal("integrity event missing from the timeline")
	}
}
