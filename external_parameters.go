package gopq

import (
	"fmt"
	"strings"
)

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
