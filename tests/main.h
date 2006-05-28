/**
 * (C) Christoph Rupp, 2005
 * see file COPYING for licence information
 *
 * unit tests - main header file
 *
 */

#ifndef MAIN_H__
#define MAIN_H__

/*
 * command line parameters
 */
extern int argc;
extern char **argv;

/* 
 * declarations of extern test function
 */
extern void test_pageio(void);
extern void test_db(void);
extern void test_errhand(void);
extern void test_cache(void);
extern void test_freelist(void);
extern void test_btree_payload(void);
extern void test_btree_find(void);
extern void test_btree_row(void);
extern void test_btree_berk(void);
extern void test_blob(void);
extern void test_btree_extkeys(void);

#endif /* MAIN_H__ */
