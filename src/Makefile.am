
SUBDIRS  = 2protoserde
if ENABLE_REMOTE
SUBDIRS += 2protobuf 5server
endif # ENABLE_REMOTE

lib_LTLIBRARIES = libhamsterdb.la

libhamsterdb_la_SOURCES = \
	0root/root.h \
	1base/abi.h \
	1base/dynamic_array.h \
	1base/error.cc \
	1base/error.h \
	1base/mutex.h \
	1base/packstart.h \
	1base/packstop.h \
	1base/pickle.h \
	1base/scoped_ptr.h \
	1base/spinlock.h \
	1base/util.cc \
	1base/util.h \
	1errorinducer/errorinducer.h \
	1errorinducer/errorinducer.cc \
	1globals/globals.h \
	1globals/globals.cc \
	1mem/mem.cc \
	1mem/mem.h \
	1os/file.h \
	1os/socket.h \
	1os/os.h \
	1os/os.cc \
	1os/os_posix.cc \
	1rb/rb.h \
	2aes/aes.h \
	2compressor/compressor.h \
	2compressor/compressor_factory.h \
	2compressor/compressor_factory.cc \
	2compressor/compressor_lzf.h \
	2compressor/compressor_lzop.h \
	2compressor/compressor_snappy.h \
	2compressor/compressor_zlib.h \
	2config/db_config.h \
	2config/env_config.h \
	2simd/simd.h \
	2page/page.cc \
	2page/page.h \
	2page/page_collection.h \
	2device/device.h \
	2device/device_disk.h \
	2device/device_inmem.h \
	2device/device_factory.h \
	2lsn_manager/lsn_manager.h \
	2lsn_manager/lsn_manager_test.h \
	2queue/queue.h \
	2worker/worker.h \
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
	3btree/btree_check.cc \
	3btree/btree_cursor.cc \
	3btree/btree_cursor.h \
	3btree/btree_erase.cc \
	3btree/btree_find.cc \
	3btree/btree_flags.h \
	3btree/btree_impl_base.h \
	3btree/btree_impl_default.h \
	3btree/btree_impl_pax.h \
	3btree/btree_index.cc \
	3btree/btree_index.h \
	3btree/btree_index_factory.h \
	3btree/btree_insert.cc \
	3btree/btree_keys_base.h \
	3btree/btree_keys_binary.h \
	3btree/btree_keys_varlen.h \
	3btree/btree_keys_pod.h \
	3btree/btree_keys_zint32.h \
	3btree/btree_keys_simdcomp.h \
	3btree/btree_keys_groupvarint.h \
	3btree/btree_node.h \
	3btree/btree_node_proxy.h \
	3btree/btree_records_base.h \
	3btree/btree_records_default.h \
	3btree/btree_records_inline.h \
	3btree/btree_records_internal.h \
	3btree/btree_records_duplicate.h \
	3btree/btree_stats.cc \
	3btree/btree_stats.h \
	3btree/btree_update.cc \
	3btree/btree_update.h \
	3btree/btree_visit.cc \
	3btree/btree_visitor.h \
	3btree/upfront_index.h \
	3journal/journal.cc \
	3journal/journal.h \
	3journal/journal_entries.h \
	3journal/journal_state.h \
	3journal/journal_test.h \
	3page_manager/page_manager.cc \
	3page_manager/page_manager.h \
	3page_manager/page_manager_state.h \
	3page_manager/page_manager_test.h \
	3page_manager/page_manager_worker.h \
	4context/context.h \
	4cursor/cursor.cc \
	4cursor/cursor.h \
	4db/db.cc \
	4db/db.h \
	4db/db_local.cc \
	4db/db_local.h \
	4db/db_remote.cc \
	4db/db_remote.h \
	4env/env.cc \
	4env/env.h \
	4env/env_test.h \
	4env/env_header.h \
	4env/env_local.h \
	4env/env_local.cc \
	4env/env_local_test.h \
	4env/env_remote.h \
	4env/env_remote.cc \
	4txn/txn_cursor.cc \
	4txn/txn_cursor.h \
	4txn/txn_factory.h \
	4txn/txn_local.cc \
	4txn/txn_local.h \
	4txn/txn_remote.cc \
	4txn/txn_remote.h \
	4txn/txn.h \
	5hamsterdb/hamsterdb.cc \
	5hamsterdb/hola.cc

EXTRA_DIST = 1os/os_win32.cc

AM_CPPFLAGS = -I../include -I$(top_srcdir)/include \
			  $(BOOST_CPPFLAGS)
libhamsterdb_la_LDFLAGS = -version-info 5:2:0 $(BOOST_SYSTEM_LDFLAGS) \
			  $(BOOST_THREAD_LDFLAGS)
libhamsterdb_la_LIBADD  = $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS) \
			  $(top_builddir)/3rdparty/murmurhash3/libmurmurhash3.la

if ENABLE_COMPRESSION
AM_CPPFLAGS += -DHAM_ENABLE_COMPRESSION
libhamsterdb_la_LIBADD  += $(top_builddir)/3rdparty/liblzf/liblzf.la
if WITH_ZLIB
libhamsterdb_la_LIBADD  += -lz
endif
if WITH_SNAPPY
libhamsterdb_la_LIBADD  += -lsnappy
endif
if WITH_LZOP
libhamsterdb_la_LIBADD  += -llzo2
endif
endif

if ENABLE_ENCRYPTION
AM_CPPFLAGS += -DHAM_ENABLE_ENCRYPTION
endif
>>>>>>> 9d7aea4... Pro 2.1.7: commercial fork, heavy-weight compression for records, journal and keys, AES encryption, SIMD

if ENABLE_REMOTE
AM_CPPFLAGS += -DHAM_ENABLE_REMOTE
libhamsterdb_la_LIBADD += $(top_builddir)/src/2protobuf/libprotocol.la
endif

AM_CFLAGS	 =
AM_CXXFLAGS	 =
if ENABLE_SIMD
AM_CPPFLAGS += -DHAM_ENABLE_SIMD
endif
if ENABLE_SSE2
AM_CFLAGS	+= -msse2 -flax-vector-conversions
AM_CXXFLAGS	+= -msse2 -flax-vector-conversions
endif
if ENABLE_SSE4
AM_CFLAGS	+= -msse4 -flax-vector-conversions
AM_CXXFLAGS	+= -msse4 -flax-vector-conversions
endif

