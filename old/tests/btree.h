
#include <stdio.h>

#define TRACE  if (!g_quiet) printf("%03d: ", __LINE__); \
               if (!g_quiet) printf
//#define TRACE  (void)

#define MINKEYS 2
#define MAXKEYS (2*MINKEYS)
#define PAGESIZE 1024
typedef int      item_t;
typedef unsigned offset_t;

// globale konfigurationsvariablen
//
// wenn g_quiet 1 ist werden TRACEs nicht ausgegeben
extern int g_quiet;
// die buildnumber
extern int g_buildno;

// TODO groesse dynamisch berechnen, abh. von PAGESIZE
typedef struct
{
    unsigned count;
    item_t key[MAXKEYS];
    offset_t self, left, right, ptr[MAXKEYS], ptr_left;
    char padding[PAGESIZE-48];
} Page __attribute__((packed));

typedef struct
{
    Page *root;
} Btree;

extern FILE *g_f;

/** page management */
Page *allocPage(void);
Page *fetchPage(offset_t offset);
void storePage(Page *page);
void freePage(Page *page);
/** create tree */
void createTree(const char *filename, Btree *tree);
/** debugging */
void dump(Page *page);
void verify(Page *page);
/** lookup */
offset_t find(Btree *tree, item_t item);
Page *findNextChild(Page *page, item_t item);
/** insert */
void insert(Btree *tree, item_t item, offset_t offset);
/** erase */
offset_t erase(Btree *tree, item_t item);
