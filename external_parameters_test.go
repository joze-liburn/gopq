package gopq

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDetect(t *testing.T) {
	tests := []struct {
		name string
	}{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, _, err := sqlmock.New()
			if err != nil {
				t.Fatalf("%s: an error %s while opening database", test.name, err.Error())
			}
			defer db.Close()

		})
	}
}

func TestFlavour(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		flavour CallConvention
		want    string
	}{
		{name: "no call", query: "select * from table;", want: "select * from table;"},
		{name: "no parameters", query: "call proc();", want: "call proc();"},
		{name: "missing close", query: "call proc(:id;", want: "call proc(:id;"},
		{name: "missing open", query: "call proc :id);", want: "call proc :id);"},
		{name: "single parameter mysql", query: "call proc(:id);", flavour: CallPlaceholder, want: "call proc(?);"},
		{name: "single parameter pgx", query: "call proc(:id);", flavour: CallPositional, want: "call proc($1);"},
		{name: "single parameter oracle", query: "call proc(:id);", flavour: CallNamed, want: "call proc(:id);"},
		{name: "two parameters mysql", query: "call proc(:id, :time);", flavour: CallPlaceholder, want: "call proc(?,?);"},
		{name: "two parameters pgx", query: "call proc(:id, :time);", flavour: CallPositional, want: "call proc($1,$2);"},
		{name: "two parameters oracle", query: "call proc(:id, :time);", flavour: CallNamed, want: "call proc(:id, :time);"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Flavour(test.query, test.flavour)
			if got != test.want {
				t.Errorf("%s: Flavour(%q, %d), got %q, want %q", test.name, test.query, test.flavour, got, test.want)
			}
		})
	}
}
