package data

type Bitmap []bool

// check if a block can be skipped (not loaded to memory) and if the block or part of it qualifies for further filtering
func (bm Bitmap) Check(matchVal int8) (skippable bool, qualified bool) {
	if bm[matchVal] {
		for i, otherValExists := range bm {
			if i != int(matchVal) && otherValExists {
				// matchVal exists and other vals are in the block, non skippable
				return false, true
			}
		}
		// only matchVal exists in this block, skippable and block qualifies
		return true, true
	}
	// matchVal doesn't exist in this block, skippable and block doesn't qualify
	return true, false
}