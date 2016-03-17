
SUBDIRS = liblzf murmurhash3 for

if ENABLE_SSE2
SUBDIRS += simdcomp streamvbyte varint
endif

if ENABLE_REMOTE
SUBDIRS += json
endif

DIST_SUBDIRS = liblzf json murmurhash3 simdcomp streamvbyte varint for
