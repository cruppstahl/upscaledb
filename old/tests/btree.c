
#include <string.h>
#include <errno.h>
#include <malloc.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <assert.h>
#include <getopt.h>
#include <unistd.h> // für write()
#include "btree.h"

FILE *g_f=0;
int g_quiet=1;

Page *
allocPage(void)
{
    char temp[PAGESIZE];

    Page *p=(Page *)malloc(sizeof(Page));
    memset(p, 0, sizeof(*p));
    // gleich auf die platte schreiben, damit p->self (offset
    // mit der eigenen startadresse) bestimmt werden kann!
    fseek(g_f, 0, SEEK_END);
    p->self=ftell(g_f);
    TRACE("allocPage(): allocating new %d bytes at page %d\n", 
            PAGESIZE, p->self);
    fwrite(temp, 1, PAGESIZE, g_f);
    return p;
}

Page *
fetchPage(offset_t offset)
{
    Page *p=(Page *)malloc(PAGESIZE);
    fseek(g_f, offset, SEEK_SET);
    if (PAGESIZE!=fread(p, 1, PAGESIZE, g_f)) {
        TRACE("fetchPage(%d) error: %s\n", offset, strerror(errno));
        exit(-1);
    }
    return p;
}

void
storePage(Page *page)
{
    fseek(g_f, page->self, SEEK_SET);
    TRACE("storePage(): writing %d bytes at page %d\n", 
            PAGESIZE, page->self);
    if (PAGESIZE!=fwrite(page, 1, PAGESIZE, g_f)) {
        TRACE("storePage(%d) error: %s\n", page->self, strerror(errno));
        exit(-1);
    }
}

void 
freePage(Page *page)
{
    // nop
}

void
createTree(const char *filename, Btree *tree)
{
    // datei oeffnen
    g_f=fopen(filename, "w+");
    if (!g_f) {
        TRACE("createTree: %s\n", strerror(errno));
        exit(-1);
    }

    char temp[12]; // header-dummy, ist nötig damit root-page
    // keinen offset von 0 hat
    fwrite(temp, 1, sizeof(temp), g_f);

    // root-page allokieren
    tree->root=allocPage();
    storePage(tree->root);
}

void
dump(Page *page)
{
    void dumpRec(Page *);
    printf("---- @@@ dump start @@@ ------------\n");
    dumpRec(page);
    printf("---- @@@ dump end @@@ ------------\n");
}

void 
dumpRec(Page *page)
{
    printf("page %d (%d items, left: %d, right: %d)\n", 
            page->self, page->count, page->left, page->right);
    printf("  (%04d)", page->ptr_left);

    int i;
    for (i=0; i<page->count; i++)
        printf("  (%d/%04d)", page->key[i], page->ptr[i]);
    printf("\n");

    if (page->ptr_left) {
        dumpRec(fetchPage(page->ptr_left));

        for (i=0; i<page->count; i++) 
            dumpRec(fetchPage(page->ptr[i]));
    }
}

void 
verify(Page *page)
{
    int i;
    assert(page->count<=MAXKEYS);
    //assert(page->count>=MINKEYS); 
    if (page->count>1) {
        for (i=0; i<page->count-1; i++)
            assert(page->key[i]<page->key[i+1]);
    }

    // ist die links/rechts-verknüpfung korrekt?
    if (page->left) {
        Page *lsib=fetchPage(page->left);
        assert(lsib->right==page->self);
        assert(lsib->key[lsib->count-1]<page->key[0]);
    }
    if (page->right) {
        Page *rsib=fetchPage(page->right);
        assert(rsib->left==page->self);
        assert(rsib->key[0]>page->key[page->count-1]);
    }
    
    if (page->ptr_left) {
        verify(fetchPage(page->ptr_left));

        for (i=0; i<page->count; i++) 
            verify(fetchPage(page->ptr[i]));
    }
}

int
randomTest(Btree *tree, unsigned count)
{
#define MAXVAL 1000 // 3000000
    int i, errors=0;
    char bools[MAXVAL];
    memset(bools, 0, MAXVAL);

    if (count>MAXVAL) {
        printf("sorry, value too high, max is %d\n", MAXVAL);
        return 0;
    }

    time_t t;
    time(&t);
    srand(t);

    FILE *log=fopen("testreihe.txt", "wt");

    for (i=0; i<count; i++) {
        if (i%100==0) 
            write(fileno(stdout), "+", 1);
        int r=(rand()%(MAXVAL-1))+1;
        if (!bools[r]) {
            bools[r]=1;
            fprintf(log, "%d ", r);
            fflush(log);
            insert(tree, r, r);
        }
    }
    write(fileno(stdout), "\n", 1);

    // alle testen mit find()
    for (i=0; i<MAXVAL; i++) {
        if (bools[i]) {
            if (i%100==0) 
                write(fileno(stdout), ".", 1);
            offset_t ptr=find(tree, i);
            if (!ptr && i!=0) {
                printf("error: %d is %d and not %d\n", i, ptr, i);
                errors++;
                goto err;
            }
        }
    }
    write(fileno(stdout), "\n", 1);

    // alles rauslöschen
    //for (i=0; i<MAXVAL; i++) { // test von links
    //for (i=MAXVAL-1; i>=0; i--) { // test von rechts
    for (i=0; i<MAXVAL*2; i++) { // test von links
        int r=(rand()%(MAXVAL-1))+1;
        if (bools[r]) {
            if (i%100==0) 
                write(fileno(stdout), "-", 1);
            fprintf(log, "-%d ", r);
            fflush(log);
            offset_t ptr=erase(tree, r);
            if (!ptr || ptr!=r) {
                printf("error1: %d is %d and not %d\n", r, ptr, r);
                errors++;
                goto err;
            }
            bools[r]=0;
        }
    }
    fclose(log);
    write(fileno(stdout), "\n", 1);
err:
    dump(fetchPage(tree->root->self));
    printf("---- @@@ results @@@ ------\n");
    printf("errors: %d\n", errors);
    return 0;
}

int
argvTest(Btree *tree, int argc, char **argv)
{
    int del[1024];
    int delidx=0;
    int i, j; 
    int errors=0;

    for (i=1; i<argc; i++) {
        if (argv[i][0]=='-') {
            int val=atoi(argv[i]+1);
            offset_t ptr=erase(tree, val);
            if (ptr!=val) {
                printf("error1: %d is %d and not %d\n", val, ptr, val);
                errors++;
                goto err;
            }
            del[delidx++]=val;
        }
        else {
            int val=atoi(argv[i]);
            insert(tree, val, val);
        }
        verify(tree->root);
    }

    for (i=1; i<argc; i++) {
        if (argv[i][0]=='-') 
            continue;
        int val=atoi(argv[i]);
        for (j=0; j<delidx; j++) {
            if (val==del[j])
                break;
        }
        if (j!=delidx)
            continue;
        unsigned result=find(tree, val);
        if (result!=val) {
            printf("error: %d = %d and not %d\n", val, result, val);
            errors++;
            goto err;
        }
    }
err:
    dump(fetchPage(tree->root->self));
    printf("---- @@@ results @@@ ------\n");
    printf("errors: %d\n", errors);

    return 0;
}

int
fileTest(Btree *tree, const char *filename)
{
    char buffer[1024*1024]; // should be enough for reading the whole file
    FILE *f=fopen(filename, "rt");
    assert(f);
    int r=fread(buffer, 1, 1024*1024, f);
    assert(r<sizeof(buffer));

    int errors=0;
    offset_t res;
    char *tok=strtok(buffer, " ");
    do {
        if (tok[0]=='-') {
            int value=strtoul(tok+1, 0, 0);
            erase(tree, value);
        }
        else {
            int value=strtoul(tok, 0, 0);
            insert(tree, value, value);
            if ((res=find(tree, value))!=value) {
                printf("error: find %d => %d\n", value, res);
                errors++;
                break;
            }
        }
        verify(tree->root);
    } while ((tok=strtok(0, " ")));
    dump(tree->root);
    printf("---- @@@ results @@@ ------\n");
    printf("errors: %d\n", errors);

    fclose(f);
    return 0;
}

void
usage(void)
{
    printf("btree test program - build %d; (C) Christoph Rupp, 2005\n"
           "\n"
           "usage: btree [-h|-q|-b|-r <count>|-i <file>|-a <args>]\n"
           "\n"
           "          -h         prints this message\n"
           "          -q         quiet - no debug output\n"
           "          -b         print build-number\n"
           "          -r <count> insert <count> random values in range [0, 1024[ \n"
           "          -i <file>  insert values from file \n"
           "          -a <args>  insert positive args, delete negative args \n", 
           g_buildno);
}

int
main(int argc, char **argv)
{
    int c;
    Btree tree;
    createTree("btree.dat", &tree);

    while ((c=getopt(argc, argv, "+bhqi:r:a"))!=-1) {
        switch (c) {
            case 'a':
                return argvTest(&tree, argc-1, argv+1);
            case 'b':
                printf("%d", g_buildno);
                return 0;
            case 'q':
                g_quiet=1;
                break;
            case 'i':
                fileTest(&tree, optarg);

                return 0;
            case 'r':
                randomTest(&tree, strtoul(optarg, 0, 0));
                return 0;
            case 'h':
            case '?':
                usage();
                return 0;
        }
    }
    printf("run `btree -h' for help\n");

    return 0;
}

