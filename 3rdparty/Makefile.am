
if ENABLE_ENCRYPTION
SUBDIRS = aes
else
SUBDIRS = 
endif

if ENABLE_COMPRESSION
if SYSTEM_ZLIB
# nop
else
SUBDIRS += zlib
endif
endif

if ENABLE_SERVER
SUBDIRS += json mongoose
endif
