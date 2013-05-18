
if ENABLE_REMOTE
SUBDIRS = protocol
if ENABLE_SERVER
SUBDIRS += server
endif # ENABLE_SERVER
endif # ENABLE_REMOTE

AM_CPPFLAGS = -I../include -I$(top_srcdir)/include $(BOOST_CPPFLAGS)

lib_LTLIBRARIES = libhamsterdb.la

EXTRA_DIST = os_win32.cc

libhamsterdb_la_SOURCES = blob.cc \
	blob.h \
	btree.cc \
	btree_check.cc \
	btree_cursor.cc \
	btree_cursor.h \
	btree_enum.cc \
	btree_erase.cc \
	btree_find.cc \
	btree.h \
	btree_insert.cc \
	btree_key.cc \
	btree_key.h \
	btree_node.h \
	btree_stats.cc \
	btree_stats.h \
	cache.cc \
	cache.h \
	changeset.cc \
	changeset.h \
	config.h \
	cursor.cc \
	cursor.h \
	db.cc \
	db.h \
	device.cc \
	device.h \
	duplicates.cc \
	duplicates.h \
	endianswap.h \
	env.cc \
	env.h \
	error.cc \
	error.h \
	errorinducer.h \
	extkeys.h \
	freelist.cc \
	freelist.h \
	freelist_statistics.cc \
	freelist_statistics.h \
	hamsterdb.cc \
	hash-table.h \
	internal_fwd_decl.h \
	journal.cc \
	journal_entries.h \
	journal.h \
	log.cc \
	log.h \
	mem.cc \
	mem.h \
	os.h \
	os_posix.cc \
	packstart.h \
	packstop.h \
	page.cc \
	page.h \
	rb.h \
	remote.cc \
	serial.h \
	statistics.h \
	txn.cc \
	txn_cursor.cc \
	txn_cursor.h \
	txn.h \
	util.cc \
	util.h \
	version.h

libhamsterdb_la_LDFLAGS = -version-info 4:0:0 

if ENABLE_REMOTE
libhamsterdb_la_LDFLAGS += -lcurl
libhamsterdb_la_LDFLAGS += $(top_builddir)/src/protocol/libprotocol.la
AM_CPPFLAGS += -DHAM_ENABLE_REMOTE
endif

