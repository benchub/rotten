package pssc

import (
	"reflect"
	"testing"
)

func TestSeesPrepended(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"sqlcommenter, marginalia", false}, // pssc's default: append only
		{"", false},
		{"marginalia(position=any)", true},
		{"sqlcommenter, marginalia(position=prepend)", true},
		{"sqlcommenter(position=any), marginalia(position=any)", true},
		{"marginalia(position=append)", false},
		{" Marginalia ( merge=on , position = ANY ) ", true},
		{"appname(position=any)", false}, // reads application_name, not comments
		{"regex(pattern='x,y', position=prepend)", true},
		{"marginalia(position=appendix)", false},
		{"regex(pattern='x', keys=x)", true}, // regex defaults to position=any
		{"regex(pattern='x', keys=x, position=append)", false},
	}
	for _, c := range cases {
		if got := SeesPrepended(c.in); got != c.want {
			t.Errorf("SeesPrepended(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestMissingMappedTags(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"action, controller, job", nil}, // pssc's default
		{"action,controller,job_tag", nil},
		{"*", nil},
		{" * ", nil},
		{"controller", []string{"action", "job or job_tag"}},
		{"", []string{"controller", "action", "job or job_tag"}},
		{"Controller, action, job", []string{"controller"}}, // keys are case-sensitive
	}
	for _, c := range cases {
		if got := MissingMappedTags(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("MissingMappedTags(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
