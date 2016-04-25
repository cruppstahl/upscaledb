
SUBDIRS = liblzf murmurhash3 libfor libvbyte

if ENABLE_SSE2
SUBDIRS += simdcomp streamvbyte
endif

if ENABLE_REMOTE
SUBDIRS += json
endif

DIST_SUBDIRS = liblzf json murmurhash3 simdcomp streamvbyte libfor libvbyte
