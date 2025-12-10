package gopq

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

func Detect(db *sql.DB) string {
	vers := []struct {
		test    string
		pattern string
		result  string
	}{
		{test: "select version()", pattern: `^PostgreSQL [[:digit:]]*\.[[:digit:]]*`, result: "postgres"},
		{test: "select @@version", pattern: `^[[:digit:]]*\.[[:digit:]]*\.[[:digit:]]*`, result: "mysql"},
	}

	for _, ver := range vers {
		row, err := db.Query(ver.test)
		if err != nil {
			continue
		}
		var answ string
		err = row.Scan(&answ)
		if err != nil {
			continue
		}
		exp := regexp.MustCompile(ver.pattern)
		if exp.Match([]byte(answ)) {
			return ver.result
		}
	}
	return "unknown"
}

var ProviderAckQueries map[string]AckQueries = map[string]AckQueries{
	"mysql": {
		BaseQueries: BaseQueries{
			Enqueue:    "call gopq_push_ack(?)",
			TryDequeue: "call gopq_pop_ack(?, ?)",
			Len:        "call gopq_len_ack(?)",
		},
		AckUtilsQueries: AckUtilsQueries{
			Details:  "call gopq_selectItemDetails(?)",
			Delete:   "call gopq_deleteItem(?)",
			ForRetry: "call gopq_updateForRetry(?, ?)",
			Expire:   "call gopq_expireAckDeadline(?, ?)",
		},
		Ack: "call gopq_ack_store(?, ?)",
	},
}

func Flavour(sql string, flavor CallConvention) string {
	switch flavor {
	case CallPlaceholder:
	case CallPositional:
	case CallAuto:
		fallthrough
	case CallNamed:
		fallthrough
	default:
		return sql
	}

	parts := strings.Split(sql, "(")
	if len(parts) != 2 {
		return sql
	}
	head := parts[0] + "("

	parts2 := strings.Split(parts[1], ")")
	if len(parts2) != 2 {
		return sql
	}
	tail := ")" + parts2[1]

	argn := len(strings.Split(parts2[0], ","))

	ret := ""
	switch flavor {
	case CallPlaceholder:
		ret = strings.Repeat("?,", argn)
	case CallPositional:
		for i := range argn {
			ret = fmt.Sprintf("%s,$%d", ret, i+1)
		}
	}
	ret = strings.Trim(ret, ",")
	return head + ret + tail
}
