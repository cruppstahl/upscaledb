
#include <assert.h>
#include <string.h>
#include "btree.h"

typedef struct erase_scratchpad_t 
{
    // tree, der bearbeitet wird
    Btree *tree;

    // zeiger auf gelöschten datensatz
    offset_t ptr; 

    // wenn key[0] einer seite gelöscht wird, werden hier der alte und 
    // der neue key und die page-adresse gespeichert, damit die 
    // parent-seite die keys entsprechend austauschen kann
    item_t oldkey1; 
    item_t newkey1; 
    offset_t page1;

    // in seltenen faellen wird auch aus einer zweiten page ein element 
    // gelöscht; der alte und neue key und die adresse der seite wird 
    // hier gespeichert
    item_t oldkey2;
    item_t newkey2; 
    offset_t page2;

    // wenn eine seite gelöscht wird, steht hier die adresse der 
    // seite und der key[0]
    item_t delkey;
    offset_t delpage;

    // wenn der kleinste key in einem teilbaum sich ändert, muss evtl
    // die root-seite angepasst werden. dazu wird hier der alte und
    // der neue kleinste wert gespeichert.
    item_t oldsmallest;
    item_t newsmallest;
    offset_t smallestpage;

} erase_scratchpad_t; 

/** der zu löschende key wurde nicht gefunden */
#define NOTFOUND            0 
/** der key wurde erfolgreich gelöscht; der pointer des gelöschten keys 
 * ist in scratchpad.ptr */
#define DONE                1
/** der key wurde gelöscht; er wird in den elternseiten durch einen
 * anderen key ersetzt. der zu ersetzende key ist in scratchpad.oldkey1,
 * der neue key in scratchpad.oldkey2 */
#define FIXKEY              2
/** der key wurde gelöscht; in den elternseiten müssen 2 keys
 * ersetzt werden. die key-paare sind in scratchpad.oldkey1/newkey1 
 * und scratchpad.oldkey2/newkey2 */
#define FIXKEY2             3
/** im zuge des löschens wurde eine ganze seite gelöscht. key und
 * adresse stehen in scratchpad.delkey/delpage */
#define PAGEDELETED         4
/** im zuge des löschens wurde eine seite gelöscht und ein key
 * muss ersetzt werden. key und adresse der gelöschten seite stehen
 * in scratchpad.delkey/delpage, der zu ersetzende key steht in 
 * scratchpad.oldkey1/newkey1 */
#define PAGEDELETED_FIXKEY  5

/** löscht ein Element aus @a page; arbeitet sich rekursiv bis zum
 * zu löschenden Element. das Element wird über den schlüssel (@a item) 
 * oder die seitenadresse (@a page) adressiert, je nachdem welcher 
 * Wert uebergeben wird.
 */
static int eraseRec(Page *page, item_t item, 
        erase_scratchpad_t *scratchpad);

/**
 * löscht ein element aus @a page; führt alle nötigen operationen durch,
 * damit der Baum danach wieder balanciert ist
 */
static int eraseFromPage(Page *page, item_t item, offset_t ptr, 
        erase_scratchpad_t *scratchpad);

/** 
 * löscht ein element aus @a page; führt keine(!) balancing-operationen
 * durch
 */
static int eraseFromPageFinal(Page *page, item_t item, offset_t ptr,
        erase_scratchpad_t *scratchpad);

/** 
 * ersetzt einen key in einer seite; schreibt die seite dann zurück
 */
static int replaceKeyInPage(Page *page, 
        item_t oldkey, item_t newkey); 

/**
 * fügt zwei seiten zusammen; die seite @a src wird nach @a dest 
 * kopiert, @a src wird dann aufgelöst. gleichzeitig wird @a key
 * gelöscht.
 */
static int mergePagesAndEraseKey(Page *src, Page *dest, item_t key, 
        offset_t ptr, erase_scratchpad_t *scratchpad); 

/** 
 * holt den kleinsten key im teilbaum; nötig, um bei einem löschen die
 * wurzel-elemente immer korrekt zu halten.
 */
static Page *getSmallestLeafKey(Page *page);

offset_t 
erase(Btree *tree, item_t item)
{
    erase_scratchpad_t scratchpad;
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.tree=tree;
    Page *oldroot=tree->root;

    int ret;
    switch ((ret=eraseRec(tree->root, item, &scratchpad))) {
    case NOTFOUND:
        return 0;
    case DONE:
        break;
    case FIXKEY:
        replaceKeyInPage(tree->root, scratchpad.oldkey1, scratchpad.newkey1);
        break;
    case PAGEDELETED:
        tree->root=fetchPage(tree->root->ptr_left);
        freePage(oldroot);
        break;
    default:
        printf("SWITCH value is %d\n", ret);
        assert(!"guru meditation error");
    }

    if (tree->root->count==0 && tree->root->ptr_left) {
        tree->root=fetchPage(tree->root->ptr_left);
        freePage(oldroot);
    }
    return scratchpad.ptr;
}

static int
eraseRec(Page *page, item_t item, erase_scratchpad_t *scratchpad)
{
    int ret;

    // wenn die seite ein leaf ist: item aus der seite löschen
    if (!page->ptr_left) 
        return eraseFromPage(page, item, 0, scratchpad);

    // wenn wir (noch) nicht im leaf sind: child-page suchen, 
    // rekursiv absteigen
    else {
        Page *child=findNextChild(page, item);
        assert(child);
        switch ((ret=eraseRec(child, item, scratchpad))) {
        case DONE:
            break;
        case NOTFOUND:
            break;
        case FIXKEY: {
            // schlüssel in der seite austauschen - wenn key[0] ausgetauscht
            // wird, muss der neue key[0] nach oben propagiert werden
            item_t oldkey=page->key[0];
            replaceKeyInPage(page, scratchpad->oldkey1, scratchpad->newkey1);
            if (oldkey!=page->key[0]) {
                scratchpad->oldkey1=oldkey;
                scratchpad->newkey1=page->key[0];
            }
            else
                ret=DONE;
            break;
        }
        case FIXKEY2: {
            // schlüssel austauschen - sowohl oldkey1/newkey1 als auch
            // oldkey2/newkey2; wenn sich key[0] ändert, muss der neue
            // key[0] zum parent propagiert werden
            item_t oldkey=page->key[0];
            replaceKeyInPage(page, scratchpad->oldkey1, scratchpad->newkey1);
            // TODO unnötiges store im ersten replaceKeyInPage
            replaceKeyInPage(page, scratchpad->oldkey2, scratchpad->newkey2);
            if (oldkey!=page->key[0]) {
                scratchpad->oldkey1=oldkey;
                scratchpad->newkey1=page->key[0];
                ret=FIXKEY;
            }
            else
                ret=DONE;
            break;
        }
        case PAGEDELETED:
            // die kindseite wurde gelöscht - wir löschen den key[0] der
            // kindseite aus dieser seite
            ret=eraseFromPage(page, 0, scratchpad->delpage, scratchpad);
            break;
        case PAGEDELETED_FIXKEY: 
            // die kindseite wurde gelöscht UND ein sibling der 
            // kindseite hat einen neuen key[0] bekommen.
            // zuerst kümmern wir uns um den neuen key - rückgabewert
            // ist egal denn der key[0] von page wird nicht geändert
            replaceKeyInPage(page, scratchpad->oldkey1, scratchpad->newkey1);
            // jetzt noch die gelöschte seite entfernen
            ret=eraseFromPage(page, 0, scratchpad->delpage, scratchpad);
            break;
        }
    }

    if (scratchpad->oldsmallest && scratchpad->smallestpage!=page->self) 
        replaceKeyInPage(page, scratchpad->oldsmallest, 
                scratchpad->newsmallest);

    return ret;
}

static int
eraseFromPage(Page *page, item_t item, offset_t ptr, 
        erase_scratchpad_t *scratchpad)
{
    int ret;

    // einfachster fall: wir haben genug elemente in der page und können
    // ein element rauslöschen, ohne konsequenzen befürchten zu müssen
    if (page->count>MINKEYS) {
        // wenn wir das erste element rauslöschen, bekommen wir einen
        // neuen kleinsten index auf dieser seite; diesen index müssen
        // wir wieder zur parent-seite propagieren
        if (item==page->key[0]) {
            ret=eraseFromPageFinal(page, item, ptr, scratchpad); 
            if (ret==NOTFOUND)
                return NOTFOUND;
            scratchpad->oldkey1=item;
            scratchpad->newkey1=page->key[0];
            return FIXKEY;
        }
        // ansonsten löschen wir das element und sind fertig
        return eraseFromPageFinal(page, item, ptr, scratchpad);
    }

    // die seite hat zu wenig elemente und muss mit einer anderen seite
    // zusammengelegt und dann gelöscht werden.
    else if (page->left) {
        // siblings holen
        Page *lsib=fetchPage(page->left);
        assert(lsib);
        // wenn es eine linke sibling-seite gibt, in der noch genug platz
        // ist, dann schieben wir alle elemente nach links
        if (lsib->count+page->count<=MAXKEYS) {
            printf("toleft: deleting %d\n", item);
            Page *ch=getSmallestLeafKey(page);
            item_t oldsmallest=ch->key[0];
            ret=mergePagesAndEraseKey(page, lsib, item, ptr, scratchpad);
            if (ret==NOTFOUND)
                return NOTFOUND;
            ch=getSmallestLeafKey(lsib);
            item_t newsmallest=ch->key[0];
            // verkettung wiederherstellen und seiten zurückschreiben
            lsib->right=page->right;
            Page *rsib=page->right ? fetchPage(page->right) : 0;
            if (rsib) {
                rsib->left=page->left;
                storePage(rsib);
            }
            storePage(lsib);
            // rückgabewerte vorbereiten
            scratchpad->delkey=page->key[0];
            scratchpad->delpage=page->self;
            // seite löschen
            freePage(page);
            if (oldsmallest!=newsmallest) {
                assert(scratchpad->oldsmallest==0);
                scratchpad->oldsmallest =oldsmallest;
                scratchpad->newsmallest =newsmallest;
                scratchpad->smallestpage=page->self;
            }
            return PAGEDELETED;
        }

        // wenn es eine linke sibling-seite gibt, in der mehr als MINKEYS
        // elemente sind, holen wir das grösste element in die aktuelle seite
        else {
            assert(lsib->count>MINKEYS);
            item_t oldkey=page->key[0];
            ret=eraseFromPageFinal(page, item, ptr, scratchpad);
            if (ret==NOTFOUND)
                return NOTFOUND;
            void insertInPageNosplit(Page *page, item_t item, offset_t offset);
            // interne pages muessen anders behandelt werden
            if (lsib->ptr_left!=0) {
                // kleinsten key des teilbaums holen
                Page *child=getSmallestLeafKey(page);
                assert(child);
                item_t oldsmallest=child->key[0];
                // key von page->ptr_left "nach oben" in die page holen
                child=fetchPage(page->ptr_left);
                insertInPageNosplit(page, child->key[0], child->self);
                // höchsten key vom linken sibling entfernen
                // und als ptr_left einfuegen
                page->ptr_left=lsib->ptr[lsib->count-1];
                lsib->count--;
                storePage(lsib);
                storePage(page);
                // nochmal den kleinsten key holen
                child=getSmallestLeafKey(page);
                item_t newsmallest=child->key[0];
                if (oldsmallest!=newsmallest) {
                    assert(scratchpad->oldsmallest==0);
                    scratchpad->oldsmallest =oldsmallest;
                    scratchpad->newsmallest =newsmallest;
                    scratchpad->smallestpage=page->self;
                }
            }
            else {
                item_t   maxlkey=lsib->key[lsib->count-1];
                offset_t maxlptr=lsib->ptr[lsib->count-1];
                lsib->count--;
                storePage(lsib);
                // jetzt die maximum-elemente des linken siblings in die 
                // page einfügen
                insertInPageNosplit(page, maxlkey, maxlptr);
                storePage(page);
            }
            // rückgabewerte vorbereiten - key[0] hat sich geändert, 
            // muss evtl in der parent-page angepasst werden
            scratchpad->oldkey1=oldkey;
            scratchpad->newkey1=page->key[0];
            return FIXKEY;
        }
    }

    // es gibt keine linke sibling-seite, nur eine rechte 
    else if (page->right) {
        Page *rsib=fetchPage(page->right);
        assert(rsib);
        // wenn es eine rechte sibling-seite gibt, in der noch genug platz
        // ist, dann schieben wir alle elemente nach rechts
        if (rsib->count+page->count<=MAXKEYS) {
            scratchpad->oldkey1=rsib->key[0]; 
            ret=mergePagesAndEraseKey(page, rsib, item, ptr, scratchpad);
            if (ret==NOTFOUND)
                return NOTFOUND;
            // verkettung wiederherstellen und seiten zurückschreiben
            rsib->left=0;
            storePage(rsib);
            // rückgabewerte vorbereiten
            scratchpad->delkey =page->key[0];
            scratchpad->delpage=page->self;
            scratchpad->newkey1=rsib->key[0]; 
            // seite löschen
            freePage(page);
            // wir löschen nicht nur die aktuelle seite, sondern ersetzen
            // auch den ersten key im rechten sibling. Wir müssen also ein 
            // PAGEDELETED_FIXKEY zurückgeben, ein einfaches 
            // PAGEDELETED ist nicht genug.
            return PAGEDELETED_FIXKEY;
        }

        // wenn es eine rechte sibling-seite gibt, in der mehr als MINKEYS
        // elemente sind, holen wir das kleinste element in die aktuelle seite
        else {
            assert(rsib->count>MINKEYS);
            // zuerst löschen wir das gesuchte element 
            item_t oldkey=page->key[0];
            // TODO eraseFromPageFinal macht unnötigen store()
            ret=eraseFromPageFinal(page, item, ptr, scratchpad);
            if (ret==NOTFOUND)
                return NOTFOUND;
            item_t   rsiboldkey=rsib->key[0]; 
            offset_t rsiboldptr=rsib->ptr[0];
            // sind wir im leaf? dann nehmen wir das kleinste element der 
            // rechten page, löschen es und fügen es als höchsten key 
            // in die aktuelle seite ein
            if (!page->ptr_left) {
                ret=eraseFromPageFinal(rsib, rsiboldkey, 0, scratchpad); 
                assert(ret!=NOTFOUND);
                page->key[page->count]=rsiboldkey;
                page->ptr[page->count]=rsiboldptr;
                page->count++;
                storePage(page); 
            }
            // wenn wir in einer internen seite sind müssen wir ptr_left
            // beachten!
            else {
                Page *rchild=getSmallestLeafKey(rsib);
                //printf("1smallest key im teilbaum: %d\n", rchild->key[0]);
                assert(rchild);
                item_t oldsmallest=rchild->key[0];
                page->key[page->count]=oldsmallest;
                page->ptr[page->count]=rsib->ptr_left;
                page->count++;
                rsib->ptr_left=rsib->ptr[0];
                ret=eraseFromPageFinal(rsib, rsiboldkey, 0, scratchpad); 
                rchild=getSmallestLeafKey(rsib);
                assert(ret!=NOTFOUND);
                item_t newsmallest=rchild->key[0];
                //printf("1new smallest key im teilbaum: %d\n", rchild->key[0]);
                if (oldsmallest!=newsmallest) {
                    assert(scratchpad->oldsmallest==0);
                    scratchpad->oldsmallest =oldsmallest;
                    scratchpad->newsmallest =newsmallest;
                    scratchpad->smallestpage=page->self;
                }
                storePage(rsib); 
                storePage(page); 
            }
            // der key[0] von rsib hat sich geändert, also geben wir
            // FIXKEY zurück; wenn sich der key[0] von page auch
            // geändert hat, geben wir ein doppeltes FIXKEY zurück
            scratchpad->oldkey1=rsiboldkey;
            scratchpad->newkey1=rsib->key[0]; 
            if (page->key[0]!=oldkey) {
                scratchpad->oldkey2=oldkey;
                scratchpad->newkey2=page->key[0]; 
                return FIXKEY2;
            }
            return FIXKEY;
        }
    }

    // immer noch da? dann müssen wir im root sein
    return eraseFromPageFinal(page, item, ptr, scratchpad); 
}

static int 
eraseFromPageFinal(Page *page, item_t item, offset_t ptr, 
        erase_scratchpad_t *scratchpad)
{
    int i, found=-1;

    // das zu löschende element wird entweder durch den key oder 
    // durch den pointer angegeben. 
    if (ptr!=0) {
        // löschen wir ptr_left?
        if (ptr==page->ptr_left) {
            page->ptr_left=page->ptr[0];
            for (i=1; i<page->count; i++) {
                page->key[i-1]=page->key[i];
                page->ptr[i-1]=page->ptr[i];
            }
            page->count--;
            storePage(page);
            return DONE;
        }
        // nein, wir löschen nicht ptr_left
        else {
            for (i=0; i<page->count; i++) {
                // pointer gefunden 
                if (page->ptr[i]==ptr) {
                    found=i;
                    break;
                }
            }
        }
    }
    else {
        // wir durchlaufen alle elemente, bis wir den gesuchten schlüssel
        // gefunden haben. 
        // TODO schneller machen durch binary search
        for (i=0; i<page->count; i++) {
            // item gefunden 
            if (page->key[i]==item) {
                found=i;
                break;
            }
            // item ist grösser - dann sind wir fertig
            if (page->key[i]>item)
                return NOTFOUND;
        }
    }

    // finden wir ihn nicht, geben wir einen Fehlercode zurück.
    if (found==-1)
        return NOTFOUND;

    // ansonsten speichern wir den gefundenen pointer im scratchpad
    if (!scratchpad->ptr) 
        scratchpad->ptr=page->ptr[found];

    // dann schieben wir alle "grösseren" Elemente um eins nach links
    // TODO schneller machen durch memmove
    for (i=found+1; i<page->count; i++) {
        page->key[i-1]=page->key[i];
        page->ptr[i-1]=page->ptr[i];
    }
    page->count--;
    storePage(page);

    return DONE;
}

static int 
replaceKeyInPage(Page *page, item_t oldkey, item_t newkey)
{
    int i;
    // TODO beschleunigen mit binärsuche
    for (i=0; i<page->count; i++) {
        if (page->key[i]==oldkey) {
            page->key[i]=newkey;
            storePage(page);
            return DONE;
        }
        // item ist grösser - dann sind wir fertig
        if (page->key[i]>oldkey)
            break;
    }
    return NOTFOUND;
}

static int 
mergePagesAndEraseKey(Page *src, Page *dest, item_t item, 
        offset_t ptr, erase_scratchpad_t *scratchpad)
{
    int i;
    assert(src!=0);
    assert(dest!=0);
    void insertInPageNosplit(Page *page, item_t item, offset_t offset);

    // item löschen, wenn nötig
    if (item) {
        // TODO not efficient, storePage wird unnötigerweise aufgerufen
        int ret1=eraseFromPageFinal(src,  item, 0, scratchpad); 
        int ret2=eraseFromPageFinal(dest, item, 0, scratchpad); 
        if (ret1==ret2 && ret1==NOTFOUND)
            return NOTFOUND;
    }
    // sonst pointer löschen
    else {
        assert(ptr);
        if (ptr==src->ptr_left) {
            offset_t newptr=src->ptr[0];
            eraseFromPageFinal(src, 0, src->ptr[0], scratchpad);
            src->ptr_left=newptr;
        }
        else if (ptr==dest->ptr_left) {
            offset_t newptr=dest->ptr[0];
            eraseFromPageFinal(dest, 0, src->ptr[0], scratchpad);
            dest->ptr_left=newptr;
        }
        else {
            // TODO not efficient, storePage wird unnötigerweise aufgerufen
            int ret1=eraseFromPageFinal(src,  0, ptr, scratchpad); 
            int ret2=eraseFromPageFinal(dest, 0, ptr, scratchpad); 
            if (ret1==ret2 && ret1==NOTFOUND)
                return NOTFOUND;
        }
    }

    // TODO schneller machen mit memmove
    //
    // schieben wir von links nach rechts?
    if (src->key[0]<dest->key[0]) {
        // falls wir in einer internen seite sind, wird ptr_left von dest 
        // zu einem regulären ptr, der rest wird nach rechts verschoben, 
        // um platz zu machen
        for (i=dest->count-1; i>=0; i--) {
            dest->key[i+src->count+(dest->ptr_left ? 1 : 0)]=dest->key[i];
            dest->ptr[i+src->count+(dest->ptr_left ? 1 : 0)]=dest->ptr[i];
        }
        if (dest->ptr_left) {
            Page *ch=getSmallestLeafKey(dest);
            //printf("2smallest key im teilbaum: %d\n", ch->key[0]);
            assert(ch);
            dest->key[src->count]=ch->key[0];
            dest->ptr[src->count]=dest->ptr_left;
            dest->count++;
        }
        // jetzt den rest einfügen
        for (i=0; i<src->count; i++) {
            dest->key[i]=src->key[i];
            dest->ptr[i]=src->ptr[i];
            dest->count++;
        }
        dest->ptr_left=src->ptr_left;
        assert(dest->count<=MAXKEYS);
    }
    // nein, wir schieben von rechts nach links
    else {
        // da haben wir das selbe ptr_left-problem
        if (dest->ptr_left) {
            Page *child=fetchPage(src->ptr_left);
            Page *smallest=getSmallestLeafKey(child);
            insertInPageNosplit(dest, smallest->key[0], src->ptr_left);
        }
        for (i=0; i<src->count; i++) {
            // TODO DAS muss schneller gehen
            insertInPageNosplit(dest, src->key[i], src->ptr[i]);
            //dest->key[dest->count]=src->key[i];
            //dest->ptr[dest->count]=src->ptr[i];
            //dest->count++;
        }
    }
    // TODO ist das nötig? wird evtl schon von insertInPageNosplit ausgeführt
    storePage(dest);
    return DONE;
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

