
SUBDIRS  = 2protoserde
if ENABLE_REMOTE
SUBDIRS += 2protobuf server
endif # ENABLE_REMOTE

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = \
	0root/root.h \
	1base/abi.h \
	1base/byte_array.h \
	1base/error.cc \
	1base/error.h \
	1errorinducer/errorinducer.h \
	1base/mutex.h \
	1base/packstart.h \
	1base/packstop.h \
	1base/pickle.h \
	1base/util.cc \
	1base/util.h \
	1base/version.h \
	1globals/globals.h \
	1globals/globals.cc \
	1mem/mem.cc \
	1mem/mem.h \
	1os/os.h \
	1os/os.cc \
	1os/os_posix.cc \
	1rb/rb.h \
	2page/page.cc \
	2page/page.h \
	2device/device.h \
	2device/device_disk.h \
	2device/device_inmem.h \
	2device/device_factory.h \
	3cache/cache.h \
	3changeset/changeset.cc \
	3changeset/changeset.h \
	3blob_manager/blob_manager.h \
	3blob_manager/blob_manager.cc \
	3blob_manager/blob_manager_inmem.h \
	3blob_manager/blob_manager_inmem.cc \
	3blob_manager/blob_manager_disk.h \
	3blob_manager/blob_manager_disk.cc \
	3blob_manager/blob_manager_factory.h \
	btree_check.cc \
	btree_cursor.cc \
	btree_cursor.h \
	btree_erase.cc \
	btree_find.cc \
	btree_flags.h \
	btree_impl_base.h \
	btree_impl_default.h \
	btree_impl_pax.h \
	btree_index.cc \
	btree_index.h \
	btree_index_factory.h \
	btree_insert.cc \
	btree_keys_base.h \
	btree_keys_binary.h \
	btree_keys_varlen.h \
	btree_keys_pod.h \
	btree_node.h \
	btree_node_proxy.h \
	btree_records_default.h \
	btree_records_inline.h \
	btree_records_internal.h \
	btree_records_duplicate.h \
	btree_stats.cc \
	btree_stats.h \
	btree_visit.cc \
	config.h \
	cursor.cc \
	cursor.h \
	db.cc \
	db.h \
	db_local.cc \
	db_local.h \
	db_remote.cc \
	db_remote.h \
	env.h \
	env_header.h \
	env_local.h \
	env_local.cc \
	env_remote.h \
	env_remote.cc \
	hamsterdb.cc \
	hola.cc \
	journal.cc \
	journal_entries.h \
	journal.h \
	page_manager.cc \
	page_manager.h \
	txn_cursor.cc \
	txn_cursor.h \
	txn_factory.h \
	txn_local.cc \
	txn_local.h \
	txn_remote.cc \
	txn_remote.h \
	txn.h

EXTRA_DIST = 1os/os_win32.cc

AM_CPPFLAGS = -I../include -I$(top_srcdir)/include $(BOOST_CPPFLAGS)
libhamsterdb_la_LDFLAGS = -version-info 5:2:0 $(BOOST_SYSTEM_LDFLAGS)
libhamsterdb_la_LIBADD  = $(BOOST_SYSTEM_LIBS)

if ENABLE_REMOTE
AM_CPPFLAGS += -DHAM_ENABLE_REMOTE
libhamsterdb_la_LIBADD += $(top_builddir)/src/2protobuf/libprotocol.la
endif

