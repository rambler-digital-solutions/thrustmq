package common

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func Contains(s []uint64, e uint64) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}
