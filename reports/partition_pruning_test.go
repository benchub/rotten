package reports_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func partitionsOverlappingRange(t *testing.T, conn *pgx.Conn, parent string, start, end time.Time) map[string]bool {
	t.Helper()
	if !start.Before(end) {
		t.Fatalf("partition pruning range start %s must be before end %s", start, end)
	}

	partitions := map[string]bool{}
	for _, at := range partitionProbeTimes(start, end) {
		partitions[partitionForTime(t, conn, parent, at)] = true
	}
	return partitions
}

func assertPlanTouchesOnlyPartitions(t *testing.T, conn *pgx.Conn, plan, parent string, expected map[string]bool) {
	t.Helper()
	for partition := range expected {
		if !strings.Contains(plan, partition) {
			t.Fatalf("plan does not touch in-range partition %s:\n%s", partition, plan)
		}
	}
	for _, partition := range childPartitions(t, conn, parent) {
		if !expected[partition] && strings.Contains(plan, partition) {
			t.Fatalf("plan touches out-of-range partition %s:\n%s", partition, plan)
		}
	}
}

func partitionForTime(t *testing.T, conn *pgx.Conn, parent string, at time.Time) string {
	t.Helper()
	var partition string
	if err := conn.QueryRow(context.Background(), "select (partition_schema||'.'||partition_table)::regclass::text from public.show_partition_name($1, $2)",
		parent, at.Format(time.RFC3339Nano)).Scan(&partition); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(partition, ".")
	return parts[len(parts)-1]
}

func childPartitions(t *testing.T, conn *pgx.Conn, parent string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "select c.relname from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = $1::regclass", parent)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partition string
		if err := rows.Scan(&partition); err != nil {
			t.Fatal(err)
		}
		partitions = append(partitions, partition)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return partitions
}

func fixedMidnightCrossingRange() (time.Time, time.Time) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	return today.Add(-30 * time.Minute), today.Add(150 * time.Minute)
}

// partitionProbeTimes assumes daily partitions are cut at UTC midnight.
func partitionProbeTimes(start, end time.Time) []time.Time {
	var times []time.Time
	for at := start; at.Before(end); at = nextUTCMidnight(at) {
		times = append(times, at)
	}
	return times
}

func TestNextUTCMidnight(t *testing.T) {
	got := nextUTCMidnight(time.Date(2026, 10, 2, 23, 30, 0, 0, time.FixedZone("offset", -7*60*60)))
	want := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("nextUTCMidnight = %s, want %s", got, want)
	}
}

func TestPartitionProbeTimes(t *testing.T) {
	tests := []struct {
		name  string
		start time.Time
		end   time.Time
		want  []time.Time
	}{
		{
			name:  "start exactly at midnight",
			start: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:  "end exactly at midnight",
			start: time.Date(2026, 10, 2, 23, 30, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 10, 2, 23, 30, 0, 0, time.UTC)},
		},
		{
			name:  "single-day range",
			start: time.Date(2026, 10, 2, 11, 15, 0, 0, time.UTC),
			end:   time.Date(2026, 10, 2, 12, 45, 0, 0, time.UTC),
			want:  []time.Time{time.Date(2026, 10, 2, 11, 15, 0, 0, time.UTC)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := partitionProbeTimes(tt.start, tt.end); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("partitionProbeTimes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func nextUTCMidnight(t time.Time) time.Time {
	utc := t.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day()+1, 0, 0, 0, 0, time.UTC)
}
