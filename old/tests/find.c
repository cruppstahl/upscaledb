
#include <malloc.h>
#include <assert.h>
#include "btree.h"

offset_t
find(Btree *tree, item_t item)
{
    Page *oldleaf, *leaf=tree->root;
    while (1) {
        if (!leaf)
            return 0;
        if (!leaf->ptr_left)
            break;
        oldleaf=leaf;
        leaf=findNextChild(leaf, item);
        //free(oldleaf);
    }

    int i=0; 
    offset_t ret=0;
    // TODO besserer suchalgorithmus - binaersuche?
    for (i=0; i<leaf->count; i++) {
        if (leaf->key[i]==item) {
            ret=leaf->ptr[i];
            break;
        }
    }
    //free(leaf); -- duerfen nicht die root-page freigeben
    return ret;
}

Page * 
findNextChild(Page *page, item_t item)
{
    assert(page->ptr_left!=0);

    // alle werte < dem ersten Index sind "links" im Teilbaum
    if (item<page->key[0]) 
        return fetchPage(page->ptr_left);

    // ansonsten durchlaufen wir die indices und waehlen den 
    // entsprechenden Teilbaum aus
    // TODO hier koennte man sicher tweaken, evtl durch binaersuche?
    int i;
    for (i=0; i<page->count-1; i++)
        if (page->key[i]<=item && item<page->key[i+1])
            return fetchPage(page->ptr[i]);
    // sonst den ganz rechten teilbaum nehmen
    return fetchPage(page->ptr[page->count-1]);
}
