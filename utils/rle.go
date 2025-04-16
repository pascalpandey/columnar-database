package utils

import "strconv"

// check whether a value is data or run length, negative values are run lengths
func CheckRunLength(v any) (int, bool) {
	switch v := v.(type) {
	case int8:
		if v < 0 {
			return -int(v), true
		}
	case float64:
		if v < 0 {
			return -int(v), true
		}
	case string:
		vi, err := strconv.Atoi(v)
		if err == nil && vi < 0 {
			return -int(vi), true
		}
	}
	return -1, false
}