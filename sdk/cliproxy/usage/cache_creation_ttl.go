package usage

// CacheCreationTTLSplit reports the Anthropic cache-write TTL split as the
// (1-hour tokens, known) pair consumed by billing sinks.
//
// The split is known only when the upstream response reported per-TTL counts
// (CacheCreation5mTokens and/or CacheCreation1hTokens), no other TTL bucket is
// non-zero, and the 5-minute plus 1-hour counts cover CacheCreationTokens
// exactly. Known with oneHour == 0 is an explicit all-5-minute result. When the
// split is missing or inconsistent, known is false and oneHour is 0, so sinks
// never guess a TTL.
func (d Detail) CacheCreationTTLSplit() (oneHour int64, known bool) {
	if d.CacheCreation5mTokens == nil && d.CacheCreation1hTokens == nil {
		return 0, false
	}
	for _, other := range []*int64{d.CacheCreation30mTokens, d.CacheCreation12hTokens} {
		if other != nil && *other != 0 {
			return 0, false
		}
	}
	var fiveMinute int64
	if d.CacheCreation5mTokens != nil {
		fiveMinute = *d.CacheCreation5mTokens
	}
	if d.CacheCreation1hTokens != nil {
		oneHour = *d.CacheCreation1hTokens
	}
	if fiveMinute < 0 || oneHour < 0 || oneHour > d.CacheCreationTokens || fiveMinute != d.CacheCreationTokens-oneHour {
		return 0, false
	}
	return oneHour, true
}
