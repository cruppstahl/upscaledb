
#include <assert.h>
#include <string.h>
#include "btree.h"

typedef struct 
{
    item_t key;
    offset_t ptr;

    // wenn der kleinste key in einem teilbaum sich Ã¤ndert, muss evtl
    // die root-seite angepasst werden. dazu wird hier der alte und
    // der neue kleinste wert gespeichert.
    item_t oldsmallest;
    item_t newsmallest;
    offset_t smallestpage;

    // wenn eine interne seite gesplittet wird, wird hier die adresse
    // der neuen seite gespeichert
    offset_t newpage;

} insert_scratchpad_t;

#define DONE  0
#define SPLIT 1

int insertInLeaf(Page *page, item_t item, offset_t offset, 
                 insert_scratchpad_t *scratchpad);
int insertInInternal(Page *page, item_t item, offset_t offset, 
                 insert_scratchpad_t *scratchpad);
int insertInPage(Page *page, item_t item, offset_t offset, 
                 insert_scratchpad_t *scratchpad);
void insertInPageNosplit(Page *page, item_t item, offset_t offset);
unsigned getPivotElement(Page *page, item_t item);

/** 
 * holt den kleinsten key im teilbaum; nÃ¶tig, um bei einem lÃ¶schen die
 * wurzel-elemente immer korrekt zu halten.
 */
static Page *getSmallestLeafKey(Page *page);

/** 
 * ersetzt einen key in einer seite; schreibt die seite dann zurÃ¼ck
 */
static int replaceKeyInPage(Page *page, 
        item_t oldkey, item_t newkey); 


void
insert(Btree *tree, item_t item, offset_t offset)
{
    Page *newroot;
    insert_scratchpad_t scratchpad;
    memset(&scratchpad, 0, sizeof(scratchpad));

    // start recursion
    int res=insertInPage(tree->root, item, offset, &scratchpad);
    switch (res) {
        case DONE:
            break;
        case SPLIT:
            // wir splitten den root - das ist nichts neues, allerdings 
            // brauchen wir dann einen neuen root
            newroot=allocPage();
            newroot->ptr_left=tree->root->self;
            insertInInternal(newroot, scratchpad.key,
                              scratchpad.ptr, &scratchpad);
            tree->root=newroot;
            break;
        default:
            assert(!"unknown insert result");
            break;
    }

    Page *page=tree->root;
    if (scratchpad.oldsmallest && scratchpad.smallestpage!=page->self) {
        replaceKeyInPage(page, scratchpad.oldsmallest, 
                scratchpad.newsmallest);
    }
}

int
insertInPage(Page *page, item_t item, offset_t offset, 
             insert_scratchpad_t *scratchpad)
{
    int ret;
    Page *child;

    // wir sind im leaf
    if (page->ptr_left==0) 
        return insertInLeaf(page, item, offset, scratchpad);

    // in die naechste ebene hinabsteigen
    child=findNextChild(page, item);
    assert(child);

    // rekursiv aufrufen
    int res=insertInPage(child, item, offset, scratchpad);
    switch (res) {
        case DONE:
            ret=DONE;
            break;
        case SPLIT:
            // eine direkte kindpage wurde gesplittet - 
            // wir fuegen den neuen key bei uns ein; wenn 
            // wir auch gesplittet werden, geben wir den kleinsten
            // key im kompletten teilbaum weiter nach oben - das 
            // ist entweder `scratchpad->key` oder das pivot-element, 
            // das beim split bestimmt wurde.
            ret=insertInInternal(page, scratchpad->key, 
                                    scratchpad->ptr, scratchpad);
            break;
        default:
            assert(!"unknown insert result");
            ret=DONE;
            break;
    }

    if (scratchpad->oldsmallest && scratchpad->smallestpage!=page->self) {
        int i, hit=0;
        Page *p=page;
        for (i=0; i<p->count; i++) {
            if (p->key[i]==scratchpad->oldsmallest) {
                hit=1;
                // child holen
                Page *ch=fetchPage(p->ptr[i]);
                // wenn ch ein leaf ist muss p->key[i]==ch->key[0] sein
                if (ch->ptr_left==0) {
                    if (p->key[i]!=ch->key[0]) {
                        p->key[i]=ch->key[0];
                        storePage(p);
                    }
                }
                // wenn ch eine internal page ist muss sie durch den 
                // kleinsten wert im teilbaum ersetzt werden
                else {
                    Page *smallest=getSmallestLeafKey(ch);
                    p->key[i]=smallest->key[0];
                    storePage(p);
                }
                break;
            }
        }
        // nur weitermachen wenn wir keinen treffer hatten und eine
        // neue seite beim split erzeugt wurde
        if (hit || !scratchpad->newpage)
            return ret;
        // neue seite holen, alles nochmal durcharbeiten
        p=fetchPage(scratchpad->newpage);
        for (i=0; i<p->count; i++) {
            if (p->key[i]==scratchpad->oldsmallest) {
                hit=1;
                // child holen
                Page *ch=fetchPage(p->ptr[i]);
                // wenn ch ein leaf ist muss p->key[i]==ch->key[0] sein
                if (ch->ptr_left==0) {
                    if (p->key[i]!=ch->key[0]) {
                        p->key[i]=ch->key[0];
                        storePage(p);
                    }
                }
                // wenn ch eine internal page ist muss sie durch den 
                // kleinsten wert im teilbaum ersetzt werden
                else {
                    Page *smallest=getSmallestLeafKey(ch);
                    p->key[i]=smallest->key[0];
                    storePage(p);
                }
                break;
            }
        }
    }

    return ret;
}

void 
insertInPageNosplit(Page *page, item_t item, offset_t offset)
{
    assert(page->count<MAXKEYS);

    // TODO das geht schneller mit memmove
    int i, hit=-1;
    for (i=0; i<page->count; i++) {
        if (page->key[i]>item) {
            hit=i;
            for (i=page->count-1; i>=hit; i--) {
                page->key[i+1]=page->key[i];
                page->ptr[i+1]=page->ptr[i];
            }
            break;
        }
    }

    if (hit==-1) 
        hit=page->count;

    page->key[hit]=item;
    page->ptr[hit]=offset;
    page->count++;
}

int 
insertInLeaf(Page *page, item_t item, offset_t offset, 
                 insert_scratchpad_t *scratchpad)
{
    // ist genug platz in der page, sodass wir den Wert noch
    // problemlos einfuegen koennen?
    if (page->count<MAXKEYS) {
        insertInPageNosplit(page, item, offset);
        storePage(page);
        return DONE;
    }

    // wenn nicht genug platz ist, müssen wir die seite splitten.
    // dazu bestimmen wir erst das pivot-element 
    int i, j, pivotidx=getPivotElement(page, item);
    Page *newpage=allocPage();

    // jetzt wird alles rechts vom pivot-key in die neue seite verschoben
    // TODO eher mit memmove?
    for (j=0, i=pivotidx; i<page->count; j++, i++) {
        newpage->key[j]=page->key[i];
        newpage->ptr[j]=page->ptr[i];
    }
    newpage->count=j;
    page->count-=newpage->count;
    // wir achten darauf, dass die leaf-seiten eine korrekt verkettete
    // liste sind
    if (!page->ptr_left) {
        // TODO absolut beschissen - wenn wir jetzt die rechte seite zum 
        // speichern brauchen können wir in ne deadlock-situation kommen. 
        // allerdings haben wir die gesplittete und die neue seite noch
        // nicht zurückgeschrieben - theoretisch können wir also hier 
        // mit LOCK_ERROR abbrechen, falls page->right gelockt ist.
        if (page->right) {
            Page *right=fetchPage(page->right);
            right->left=newpage->self;
            storePage(right);
        }
        newpage->left=page->self;
        newpage->right=page->right;
        page->right=newpage->self;
    }

    // der neue Wert wird eingefuegt
    if (page->count<newpage->count) {
        TRACE("neuer wert %d wird 'links' eingefuegt\n", item);
        insertInPageNosplit(page, item, offset);
        int i;
        for (i=0; i<page->count-1; i++)
            assert(page->key[i]<page->key[i+1]);
    }
    else {
        TRACE("neuer wert %d wird 'rechts' eingefuegt\n", item);
        insertInPageNosplit(newpage, item, offset);
        int i;
        for (i=0; i<page->count-1; i++)
            assert(page->key[i]<page->key[i+1]);
    }
    storePage(page);
    storePage(newpage);

    // im scratchpad speichern wir den kleinsten key der neuen seite - 
    // der muss in einer der parent-seiten eingefuegt werden.
    scratchpad->key=newpage->key[0];
    scratchpad->ptr=newpage->self;
    return SPLIT;
}

int 
insertInInternal(Page *page, item_t item, offset_t ptr, 
                 insert_scratchpad_t *scratchpad)
{
    Page *inspage;
    int retval=DONE;

    // passt der Eintrag noch in die aktuelle seite? dann haben wir kein 
    // problem...
    if (page->count<MAXKEYS) {
        inspage=page;
        retval=DONE;
    }
    // ... ansonsten müssen wir die seite splitten
    else {
        int i, j, pivotidx=getPivotElement(page, item);
        Page *newpage=allocPage();
        scratchpad->newpage=newpage->self;

        // TODO eher mit memmove?
        for (j=0, i=pivotidx+1; i<page->count; j++, i++) {
            newpage->key[j]=page->key[i];
            newpage->ptr[j]=page->ptr[i];
        }
        newpage->ptr_left=page->ptr[pivotidx];
        newpage->count=j;
        page->count-=newpage->count+1;
        if (page->right) {
            Page *oldright=fetchPage(page->right);
            oldright->left=newpage->self;
            storePage(oldright);
        }
        newpage->left=page->self;
        newpage->right=page->right;
        page->right=newpage->self;

        // im scratchpad speichern wir den kleinsten key des kompletten
        // neuen teilbaums, damit wir ihn in einer der parent-seiten
        // einfuegen koennen 
        scratchpad->key=page->key[pivotidx];
        scratchpad->ptr=newpage->self;

        // der neue Wert wird eingefuegt
        if (page->count<newpage->count) {
            TRACE("neuer wert %d wird 'links' eingefuegt\n", item);
            inspage=page;
            storePage(newpage);
        }
        else {
            TRACE("neuer wert %d wird 'rechts' eingefuegt\n", item);
            inspage=newpage;
            storePage(page);
        }

        retval=SPLIT;
    }

    // wenn der einzufuegende schluessel < page->key[0] ist, muessen 
    // wir aufpassen dass ptr_left immer auf die kleinste seite zeigt.
    // d.h. wir mussen vergleichen, ob ptr_left->key[0] kleiner als 
    // ptr->key[0] ist - wenn ja, bleibt ptr_left bestehen, andernseits 
    // wird ptr zum neuen ptr_left.
    // nachteil: wir müssen die seite, auf die ptr_left zeigt, nochmal 
    // einlesen.
    if (item<inspage->key[0]) {
        Page *oldleaf=fetchPage(inspage->ptr_left);
        if (oldleaf->key[0]<item) {
            // die alte seite hat die kleineren indices als die neue seite,
            // also lassen wir den bisherigen ptr_left
            int i; 
            for (i=inspage->count-1; i>=0; i--) {
                inspage->key[i+1]=inspage->key[i];
                inspage->ptr[i+1]=inspage->ptr[i];
            }
            inspage->key[0]=item;
            inspage->ptr[0]=ptr;
        }
        else {
            // die neuen seite hat die kleineren indices, also
            // setzen wir ptr_left auf die neue seite
            Page *smallest=getSmallestLeafKey(inspage);
            item_t oldsmallest=smallest->key[0];
            int i; 
            for (i=inspage->count; i>=0; i--) {
                inspage->key[i+1]=inspage->key[i];
                inspage->ptr[i+1]=inspage->ptr[i];
            }
            inspage->ptr[0]=inspage->ptr_left;
            inspage->ptr_left=ptr;
            smallest=getSmallestLeafKey(oldleaf);
            inspage->key[0]=smallest->key[0];
            // wir müssen leider den allerkleinsten wert im teilbaum nach
            // oben propagieren
            smallest=getSmallestLeafKey(inspage);
            if (oldsmallest!=smallest->key[0]) {
                scratchpad->oldsmallest=oldsmallest;
                scratchpad->newsmallest=smallest->key[0];
                scratchpad->smallestpage=smallest->self;
            }
        }
        inspage->count++;
        storePage(inspage);
    }
    else {
        insertInPageNosplit(inspage, item, ptr);
        storePage(inspage);
    }
    return retval;
}

unsigned 
getPivotElement(Page *page, item_t item)
{
    unsigned pivot=(page->count+1)/2;
    // wenn das neue element in die linke hälfte kommt, verringern wir 
    // pivotidx, damit die rechte seite ausgeglichen ist
    if (item<=page->key[pivot-1])
        pivot--;
    return pivot;
}

static Page *
getSmallestLeafKey(Page *page)
{
    // wenn die seite ein leaf ist, sind wir angekommen
    if (!page->ptr_left) 
        return page;

    // sonst: kleinste child-page suchen, rekursiv absteigen
    Page *child=fetchPage(page->ptr_left);
    assert(child);
    return getSmallestLeafKey(child);
}

static int 
replaceKeyInPage(Page *page, item_t oldkey, item_t newkey)
{
    int i;
    // TODO beschleunigen mit binÃ¤rsuche
    for (i=0; i<page->count; i++) {
        if (page->key[i]==oldkey) {
            page->key[i]=newkey;
            storePage(page);
            return DONE;
        }
        // item ist grÃ¶sser - dann sind wir fertig
        if (page->key[i]>oldkey)
            break;
    }
    return DONE;
}
