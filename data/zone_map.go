package data

type ZoneMap[T int8 | float64] struct {
	Min T
	Max T
}

// check if a block can be skipped (not loaded to memory) and if the block or part of it qualifies for further filtering
func (zm ZoneMap[T]) Check(queryInclusiveMin, queryInclusiveMax T) (skippable bool, qualified bool) {
	if queryInclusiveMax < zm.Min || queryInclusiveMin > zm.Max { 
		// zone map out of filter range, skippable and block doesnt qualify
		return true, false
	} else if queryInclusiveMin <= zm.Min && queryInclusiveMax >= zm.Max { 
		// zone map completely inside filter range, skippable and block qualifies
		return true, true
	}
	// partial overlap, non-skippable
	return false, true
}