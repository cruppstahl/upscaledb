/*
 * This file Copyright (C) 2008-2009 Mnemosyne LLC
 *
 * This file is licensed by the GPL version 2.  Works owned by the
 * Transmission project are granted a special exemption to clause 2(b)
 * so that the bulk of its code can remain under the MIT license.
 * This exemption does not extend to derived works not owned by
 * the Transmission project.
 *
 * $Id: bencode.c 9671 2009-12-05 02:19:24Z charles $
 */

#include <assert.h>
#include <ctype.h> /* isdigit, isprint, isspace */
#include <errno.h>
#include <math.h> /* fabs */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <locale.h>
#include <unistd.h> /* close() */

#include <event.h> /* evbuffer */

#include "ConvertUTF.h"

#include "transmission.h"
#include "bencode.h"
#include "json.h"
#include "list.h"
#include "ptrarray.h"
#include "utils.h" /* tr_new(), tr_free() */

#ifndef ENODATA
 #define ENODATA EIO
#endif

/**
***
**/

static tr_bool
isContainer( const tr_benc * val )
{
    return tr_bencIsList( val ) || tr_bencIsDict( val );
}

static tr_bool
isSomething( const tr_benc * val )
{
    return isContainer( val ) || tr_bencIsInt( val )
                              || tr_bencIsString( val )
                              || tr_bencIsReal( val )
                              || tr_bencIsBool( val );
}

static void
tr_bencInit( tr_benc * val,
             int       type )
{
    memset( val, 0, sizeof( *val ) );
    val->type = type;
}

/***
****  tr_bencParse()
****  tr_bencLoad()
***/

/**
 * The initial i and trailing e are beginning and ending delimiters.
 * You can have negative numbers such as i-3e. You cannot prefix the
 * number with a zero such as i04e. However, i0e is valid.
 * Example: i3e represents the integer "3"
 * NOTE: The maximum number of bit of this integer is unspecified,
 * but to handle it as a signed 64bit integer is mandatory to handle
 * "large files" aka .torrent for more that 4Gbyte
 */
int
tr_bencParseInt( const uint8_t *  buf,
                 const uint8_t *  bufend,
                 const uint8_t ** setme_end,
                 int64_t *        setme_val )
{
    char *       endptr;
    const void * begin;
    const void * end;
    int64_t      val;

    if( buf >= bufend )
        return EILSEQ;
    if( *buf != 'i' )
        return EILSEQ;

    begin = buf + 1;
    end = memchr( begin, 'e', ( bufend - buf ) - 1 );
    if( end == NULL )
        return EILSEQ;

    errno = 0;
    val = evutil_strtoll( begin, &endptr, 10 );
    if( errno || ( endptr != end ) ) /* incomplete parse */
        return EILSEQ;
    if( val && *(const char*)begin == '0' ) /* no leading zeroes! */
        return EILSEQ;

    *setme_end = (const uint8_t*)end + 1;
    *setme_val = val;
    return 0;
}

/**
 * Byte strings are encoded as follows:
 * <string length encoded in base ten ASCII>:<string data>
 * Note that there is no constant beginning delimiter, and no ending delimiter.
 * Example: 4:spam represents the string "spam"
 */
int
tr_bencParseStr( const uint8_t *  buf,
                 const uint8_t *  bufend,
                 const uint8_t ** setme_end,
                 const uint8_t ** setme_str,
                 size_t *         setme_strlen )
{
    size_t       len;
    const void * end;
    char *       endptr;

    if( buf >= bufend )
        return EILSEQ;

    if( !isdigit( *buf  ) )
        return EILSEQ;

    end = memchr( buf, ':', bufend - buf );
    if( end == NULL )
        return EILSEQ;

    errno = 0;
    len = strtoul( (const char*)buf, &endptr, 10 );
    if( errno || endptr != end )
        return EILSEQ;

    if( (const uint8_t*)end + 1 + len > bufend )
        return EILSEQ;

    *setme_end = (const uint8_t*)end + 1 + len;
    *setme_str = (const uint8_t*)end + 1;
    *setme_strlen = len;
    return 0;
}

/* set to 1 to help expose bugs with tr_bencListAdd and tr_bencDictAdd */
#define LIST_SIZE 4 /* number of items to increment list/dict buffer by */

static int
makeroom( tr_benc * val,
          size_t    count )
{
    assert( TR_TYPE_LIST == val->type || TR_TYPE_DICT == val->type );

    if( val->val.l.count + count > val->val.l.alloc )
    {
        /* We need a bigger boat */
        const int len = val->val.l.alloc + count +
                        ( count % LIST_SIZE ? LIST_SIZE -
                          ( count % LIST_SIZE ) : 0 );
        void * tmp = realloc( val->val.l.vals, len * sizeof( tr_benc ) );
        if( !tmp )
            return 1;

        val->val.l.alloc = len;
        val->val.l.vals  = tmp;
    }

    return 0;
}

static tr_benc*
getNode( tr_benc *     top,
         tr_ptrArray * parentStack,
         int           type )
{
    tr_benc * parent;

    assert( top );
    assert( parentStack );

    if( tr_ptrArrayEmpty( parentStack ) )
        return top;

    parent = tr_ptrArrayBack( parentStack );
    assert( parent );

    /* dictionary keys must be strings */
    if( ( parent->type == TR_TYPE_DICT )
      && ( type != TR_TYPE_STR )
      && ( !( parent->val.l.count % 2 ) ) )
        return NULL;

    makeroom( parent, 1 );
    return parent->val.l.vals + parent->val.l.count++;
}

/**
 * This function's previous recursive implementation was
 * easier to read, but was vulnerable to a smash-stacking
 * attack via maliciously-crafted bencoded data. (#667)
 */
static int
tr_bencParseImpl( const void *     buf_in,
                  const void *     bufend_in,
                  tr_benc *        top,
                  tr_ptrArray *    parentStack,
                  const uint8_t ** setme_end )
{
    int             err;
    const uint8_t * buf = buf_in;
    const uint8_t * bufend = bufend_in;

    tr_bencInit( top, 0 );

    while( buf != bufend )
    {
        if( buf > bufend ) /* no more text to parse... */
            return 1;

        if( *buf == 'i' ) /* int */
        {
            int64_t         val;
            const uint8_t * end;
            tr_benc *       node;

            if( ( err = tr_bencParseInt( buf, bufend, &end, &val ) ) )
                return err;

            node = getNode( top, parentStack, TR_TYPE_INT );
            if( !node )
                return EILSEQ;

            tr_bencInitInt( node, val );
            buf = end;

            if( tr_ptrArrayEmpty( parentStack ) )
                break;
        }
        else if( *buf == 'l' ) /* list */
        {
            tr_benc * node = getNode( top, parentStack, TR_TYPE_LIST );
            if( !node )
                return EILSEQ;
            tr_bencInit( node, TR_TYPE_LIST );
            tr_ptrArrayAppend( parentStack, node );
            ++buf;
        }
        else if( *buf == 'd' ) /* dict */
        {
            tr_benc * node = getNode( top, parentStack, TR_TYPE_DICT );
            if( !node )
                return EILSEQ;
            tr_bencInit( node, TR_TYPE_DICT );
            tr_ptrArrayAppend( parentStack, node );
            ++buf;
        }
        else if( *buf == 'e' ) /* end of list or dict */
        {
            tr_benc * node;
            ++buf;
            if( tr_ptrArrayEmpty( parentStack ) )
                return EILSEQ;

            node = tr_ptrArrayBack( parentStack );
            if( tr_bencIsDict( node ) && ( node->val.l.count % 2 ) )
            {
                /* odd # of children in dict */
                tr_bencFree( &node->val.l.vals[--node->val.l.count] );
                return EILSEQ;
            }

            tr_ptrArrayPop( parentStack );
            if( tr_ptrArrayEmpty( parentStack ) )
                break;
        }
        else if( isdigit( *buf ) ) /* string? */
        {
            const uint8_t * end;
            const uint8_t * str;
            size_t          str_len;
            tr_benc *       node;

            if( ( err = tr_bencParseStr( buf, bufend, &end, &str, &str_len ) ) )
                return err;

            node = getNode( top, parentStack, TR_TYPE_STR );
            if( !node )
                return EILSEQ;

            tr_bencInitStr( node, str, str_len );
            buf = end;

            if( tr_ptrArrayEmpty( parentStack ) )
                break;
        }
        else /* invalid bencoded text... march past it */
        {
            ++buf;
        }
    }

    err = !isSomething( top ) || !tr_ptrArrayEmpty( parentStack );

    if( !err && setme_end )
        *setme_end = buf;

    return err;
}

int
tr_bencParse( const void *     buf,
              const void *     end,
              tr_benc *        top,
              const uint8_t ** setme_end )
{
    int           err;
    tr_ptrArray   parentStack = TR_PTR_ARRAY_INIT;

    top->type = 0; /* set to `uninitialized' */
    err = tr_bencParseImpl( buf, end, top, &parentStack, setme_end );
    if( err )
        tr_bencFree( top );

    tr_ptrArrayDestruct( &parentStack, NULL );
    return err;
}

int
tr_bencLoad( const void * buf_in,
             size_t       buflen,
             tr_benc *    setme_benc,
             char **      setme_end )
{
    const uint8_t * buf = buf_in;
    const uint8_t * end;
    const int       ret = tr_bencParse( buf, buf + buflen, setme_benc, &end );

    if( !ret && setme_end )
        *setme_end = (char*) end;
    return ret;
}

/***
****
***/

/* returns true if the given string length would fit in our string buffer */
static TR_INLINE int
stringFitsInBuffer( const tr_benc * val, int len )
{
    return len < (int)sizeof( val->val.s.str.buf );
}

/* returns true if the benc's string was malloced.
 * this occurs when the string is too long for our string buffer */
static TR_INLINE int
stringIsAlloced( const tr_benc * val )
{
    return !stringFitsInBuffer( val, val->val.s.len );
}

/* returns a const pointer to the benc's string */
static TR_INLINE const char*
getStr( const tr_benc* val )
{
    return stringIsAlloced(val) ? val->val.s.str.ptr : val->val.s.str.buf;
}

static int
dictIndexOf( const tr_benc * val, const char * key )
{
    if( tr_bencIsDict( val ) )
    {
        size_t       i;
        const size_t len = strlen( key );

        for( i = 0; ( i + 1 ) < val->val.l.count; i += 2 )
        {
            const tr_benc * child = val->val.l.vals + i;
            if( ( child->type == TR_TYPE_STR )
              && ( child->val.s.len == len )
              && !memcmp( getStr(child), key, len ) )
                return i;
        }
    }

    return -1;
}

tr_benc *
tr_bencDictFind( tr_benc * val, const char * key )
{
    const int i = dictIndexOf( val, key );

    return i < 0 ? NULL : &val->val.l.vals[i + 1];
}

static tr_bool
tr_bencDictFindType( tr_benc * dict, const char * key, int type, tr_benc ** setme )
{
    return tr_bencIsType( *setme = tr_bencDictFind( dict, key ), type );
}

size_t
tr_bencListSize( const tr_benc * list )
{
    return tr_bencIsList( list ) ? list->val.l.count : 0;
}

tr_benc*
tr_bencListChild( tr_benc * val,
                  size_t    i )
{
    tr_benc * ret = NULL;

    if( tr_bencIsList( val ) && ( i < val->val.l.count ) )
        ret = val->val.l.vals + i;
    return ret;
}

static void
tr_benc_warning( const char * err )
{
    fprintf( stderr, "warning: %s\n", err );
}

tr_bool
tr_bencGetInt( const tr_benc * val,
               int64_t *       setme )
{
    tr_bool success = FALSE;

    if( !success && (( success = tr_bencIsInt( val ))))
        if( setme )
            *setme = val->val.i;

    if( !success && (( success = tr_bencIsBool( val )))) {
        tr_benc_warning( "reading bool as an int" );
        if( setme )
            *setme = val->val.b ? 1 : 0;
    }

    return success;
}

tr_bool
tr_bencGetStr( const tr_benc * val,
               const char **   setme )
{
    const int success = tr_bencIsString( val );

    if( success )
        *setme = getStr( val );

    return success;
}

tr_bool
tr_bencGetBool( const tr_benc * val, tr_bool * setme )
{
    const char * str;
    tr_bool success = FALSE;

    if(( success = tr_bencIsBool( val )))
        *setme = val->val.b;

    if( !success && tr_bencIsInt( val ) )
        if(( success = ( val->val.i==0 || val->val.i==1 ) ))
            *setme = val->val.i!=0;

    if( !success && tr_bencGetStr( val, &str ) )
        if(( success = ( !strcmp(str,"true") || !strcmp(str,"false"))))
            *setme = !strcmp(str,"true");

    return success;
}

tr_bool
tr_bencGetReal( const tr_benc * val, double * setme )
{
    tr_bool success = FALSE;

    if( !success && (( success = tr_bencIsReal( val ))))
        *setme = val->val.d;

    if( !success && (( success = tr_bencIsInt( val ))))
        *setme = val->val.i;

    if( !success && tr_bencIsString(val) )
    {
        char * endptr;
        char locale[128];
        double d;

        /* the json spec requires a '.' decimal point regardless of locale */
        tr_strlcpy( locale, setlocale( LC_NUMERIC, NULL ), sizeof( locale ) );
        setlocale( LC_NUMERIC, "POSIX" );
        d  = strtod( getStr(val), &endptr );
        setlocale( LC_NUMERIC, locale );

        if(( success = ( getStr(val) != endptr ) && !*endptr ))
            *setme = d;
    }


    return success;
}

tr_bool
tr_bencDictFindInt( tr_benc * dict, const char * key, int64_t * setme )
{
    return tr_bencGetInt( tr_bencDictFind( dict, key ), setme );
}

tr_bool
tr_bencDictFindBool( tr_benc * dict, const char * key, tr_bool * setme )
{
    return tr_bencGetBool( tr_bencDictFind( dict, key ), setme );
}

tr_bool
tr_bencDictFindReal( tr_benc * dict, const char * key, double * setme )
{
    return tr_bencGetReal( tr_bencDictFind( dict, key ), setme );
}

tr_bool
tr_bencDictFindStr( tr_benc *  dict, const char *  key, const char ** setme )
{
    return tr_bencGetStr( tr_bencDictFind( dict, key ), setme );
}

tr_bool
tr_bencDictFindList( tr_benc * dict, const char * key, tr_benc ** setme )
{
    return tr_bencDictFindType( dict, key, TR_TYPE_LIST, setme );
}

tr_bool
tr_bencDictFindDict( tr_benc * dict, const char * key, tr_benc ** setme )
{
    return tr_bencDictFindType( dict, key, TR_TYPE_DICT, setme );
}

tr_bool
tr_bencDictFindRaw( tr_benc         * dict,
                    const char      * key,
                    const uint8_t  ** setme_raw,
                    size_t          * setme_len )
{
    tr_benc * child;
    const tr_bool found = tr_bencDictFindType( dict, key, TR_TYPE_STR, &child );

    if( found ) {
        *setme_raw = (uint8_t*) getStr(child);
        *setme_len = child->val.s.len;
    }

    return found;
}

/***
****
***/

void
tr_bencInitRaw( tr_benc * val, const void * src, size_t byteCount )
{
    tr_bencInit( val, TR_TYPE_STR );

    if( stringFitsInBuffer( val, val->val.s.len = byteCount ))
        memcpy( val->val.s.str.buf, src, byteCount );
    else
        val->val.s.str.ptr = tr_memdup( src, byteCount );
}

void
tr_bencInitStr( tr_benc * val, const void * str, int len )
{
    tr_bencInit( val, TR_TYPE_STR );

    if( str == NULL )
        len = 0;
    else if( len < 0 )
        len = strlen( str );

    if( stringFitsInBuffer( val, val->val.s.len = len )) {
        memcpy( val->val.s.str.buf, str, len );
        val->val.s.str.buf[len] = '\0';
    } else
        val->val.s.str.ptr = tr_strndup( str, len );
}

void
tr_bencInitBool( tr_benc * b, int value )
{
    tr_bencInit( b, TR_TYPE_BOOL );
    b->val.b = value != 0;
}

void
tr_bencInitReal( tr_benc * b, double value )
{
    tr_bencInit( b, TR_TYPE_REAL );
    b->val.d = value;
}

void
tr_bencInitInt( tr_benc * b, int64_t value )
{
    tr_bencInit( b, TR_TYPE_INT );
    b->val.i = value;
}

int
tr_bencInitList( tr_benc * b, size_t reserveCount )
{
    tr_bencInit( b, TR_TYPE_LIST );
    return tr_bencListReserve( b, reserveCount );
}

int
tr_bencListReserve( tr_benc * b, size_t count )
{
    assert( tr_bencIsList( b ) );
    return makeroom( b, count );
}

int
tr_bencInitDict( tr_benc * b, size_t reserveCount )
{
    tr_bencInit( b, TR_TYPE_DICT );
    return tr_bencDictReserve( b, reserveCount );
}

int
tr_bencDictReserve( tr_benc * b, size_t reserveCount )
{
    assert( tr_bencIsDict( b ) );
    return makeroom( b, reserveCount * 2 );
}

tr_benc *
tr_bencListAdd( tr_benc * list )
{
    tr_benc * item;

    assert( tr_bencIsList( list ) );

    if( list->val.l.count == list->val.l.alloc )
        tr_bencListReserve( list, LIST_SIZE );

    assert( list->val.l.count < list->val.l.alloc );

    item = &list->val.l.vals[list->val.l.count];
    list->val.l.count++;
    tr_bencInit( item, TR_TYPE_INT );

    return item;
}

tr_benc *
tr_bencListAddInt( tr_benc * list,
                   int64_t   val )
{
    tr_benc * node = tr_bencListAdd( list );

    tr_bencInitInt( node, val );
    return node;
}

tr_benc *
tr_bencListAddStr( tr_benc *    list,
                   const char * val )
{
    tr_benc * node = tr_bencListAdd( list );

    tr_bencInitStr( node, val, -1 );
    return node;
}

tr_benc*
tr_bencListAddList( tr_benc * list,
                    size_t    reserveCount )
{
    tr_benc * child = tr_bencListAdd( list );

    tr_bencInitList( child, reserveCount );
    return child;
}

tr_benc*
tr_bencListAddDict( tr_benc * list,
                    size_t    reserveCount )
{
    tr_benc * child = tr_bencListAdd( list );

    tr_bencInitDict( child, reserveCount );
    return child;
}

tr_benc *
tr_bencDictAdd( tr_benc *    dict,
                const char * key )
{
    tr_benc * keyval, * itemval;

    assert( tr_bencIsDict( dict ) );
    if( dict->val.l.count + 2 > dict->val.l.alloc )
        makeroom( dict, 2 );
    assert( dict->val.l.count + 2 <= dict->val.l.alloc );

    keyval = dict->val.l.vals + dict->val.l.count++;
    tr_bencInitStr( keyval, key, -1 );

    itemval = dict->val.l.vals + dict->val.l.count++;
    tr_bencInit( itemval, TR_TYPE_INT );

    return itemval;
}

static tr_benc*
dictFindOrAdd( tr_benc * dict, const char * key, int type )
{
    tr_benc * child;

    /* see if it already exists, and if so, try to reuse it */
    if(( child = tr_bencDictFind( dict, key ))) {
        if( !tr_bencIsType( child, type ) ) {
            tr_bencDictRemove( dict, key );
            child = NULL;
        }
    }

    /* if it doesn't exist, create it */
    if( child == NULL )
        child = tr_bencDictAdd( dict, key );

    return child;
}

tr_benc*
tr_bencDictAddInt( tr_benc *    dict,
                   const char * key,
                   int64_t      val )
{
    tr_benc * child = dictFindOrAdd( dict, key, TR_TYPE_INT );
    tr_bencInitInt( child, val );
    return child;
}

tr_benc*
tr_bencDictAddBool( tr_benc * dict, const char * key, tr_bool val )
{
    tr_benc * child = dictFindOrAdd( dict, key, TR_TYPE_BOOL );
    tr_bencInitBool( child, val );
    return child;
}

tr_benc*
tr_bencDictAddReal( tr_benc * dict, const char * key, double val )
{
    tr_benc * child = dictFindOrAdd( dict, key, TR_TYPE_REAL );
    tr_bencInitReal( child, val );
    return child;
}

tr_benc*
tr_bencDictAddStr( tr_benc * dict, const char * key, const char * val )
{
    tr_benc * child;

    /* see if it already exists, and if so, try to reuse it */
    if(( child = tr_bencDictFind( dict, key ))) {
        if( tr_bencIsString( child ) ) {
            if( stringIsAlloced( child ) )
                tr_free( child->val.s.str.ptr );
        } else {
            tr_bencDictRemove( dict, key );
            child = NULL;
        }
    }

    /* if it doesn't exist, create it */
    if( child == NULL )
        child = tr_bencDictAdd( dict, key );

    /* set it */
    tr_bencInitStr( child, val, -1 );

    return child;
}

tr_benc*
tr_bencDictAddList( tr_benc *    dict,
                    const char * key,
                    size_t       reserveCount )
{
    tr_benc * child = tr_bencDictAdd( dict, key );

    tr_bencInitList( child, reserveCount );
    return child;
}

tr_benc*
tr_bencDictAddDict( tr_benc *    dict,
                    const char * key,
                    size_t       reserveCount )
{
    tr_benc * child = tr_bencDictAdd( dict, key );

    tr_bencInitDict( child, reserveCount );
    return child;
}

tr_benc*
tr_bencDictAddRaw( tr_benc *    dict,
                   const char * key,
                   const void * src,
                   size_t       len )
{
    tr_benc * child = tr_bencDictAdd( dict, key );

    tr_bencInitRaw( child, src, len );
    return child;
}

int
tr_bencDictRemove( tr_benc *    dict,
                   const char * key )
{
    int i = dictIndexOf( dict, key );

    if( i >= 0 )
    {
        const int n = dict->val.l.count;
        tr_bencFree( &dict->val.l.vals[i] );
        tr_bencFree( &dict->val.l.vals[i + 1] );
        if( i + 2 < n )
        {
            dict->val.l.vals[i]   = dict->val.l.vals[n - 2];
            dict->val.l.vals[i + 1] = dict->val.l.vals[n - 1];
        }
        dict->val.l.count -= 2;
    }
    return i >= 0; /* return true if found */
}

/***
****  BENC WALKING
***/

struct KeyIndex
{
    const char *  key;
    int           index;
};

static int
compareKeyIndex( const void * va,
                 const void * vb )
{
    const struct KeyIndex * a = va;
    const struct KeyIndex * b = vb;

    return strcmp( a->key, b->key );
}

struct SaveNode
{
    const tr_benc *  val;
    int              valIsVisited;
    int              childCount;
    int              childIndex;
    int *            children;
};

static struct SaveNode*
nodeNewDict( const tr_benc * val )
{
    int               i, j;
    int               nKeys;
    struct SaveNode * node;
    struct KeyIndex * indices;

    assert( tr_bencIsDict( val ) );

    nKeys = val->val.l.count / 2;
    node = tr_new0( struct SaveNode, 1 );
    node->val = val;
    node->children = tr_new0( int, nKeys * 2 );

    /* ugh, a dictionary's children have to be sorted by key... */
    indices = tr_new( struct KeyIndex, nKeys );
    for( i = j = 0; i < ( nKeys * 2 ); i += 2, ++j )
    {
        indices[j].key = getStr(&val->val.l.vals[i]);
        indices[j].index = i;
    }
    qsort( indices, j, sizeof( struct KeyIndex ), compareKeyIndex );
    for( i = 0; i < j; ++i )
    {
        const int index = indices[i].index;
        node->children[node->childCount++] = index;
        node->children[node->childCount++] = index + 1;
    }

    assert( node->childCount == nKeys * 2 );
    tr_free( indices );
    return node;
}

static struct SaveNode*
nodeNewList( const tr_benc * val )
{
    int               i, n;
    struct SaveNode * node;

    assert( tr_bencIsList( val ) );

    n = val->val.l.count;
    node = tr_new0( struct SaveNode, 1 );
    node->val = val;
    node->childCount = n;
    node->children = tr_new0( int, n );
    for( i = 0; i < n; ++i ) /* a list's children don't need to be reordered */
        node->children[i] = i;

    return node;
}

static struct SaveNode*
nodeNewLeaf( const tr_benc * val )
{
    struct SaveNode * node;

    assert( !isContainer( val ) );

    node = tr_new0( struct SaveNode, 1 );
    node->val = val;
    return node;
}

static struct SaveNode*
nodeNew( const tr_benc * val )
{
    struct SaveNode * node;

    if( tr_bencIsList( val ) )
        node = nodeNewList( val );
    else if( tr_bencIsDict( val ) )
        node = nodeNewDict( val );
    else
        node = nodeNewLeaf( val );

    return node;
}

typedef void ( *BencWalkFunc )( const tr_benc * val, void * user_data );

struct WalkFuncs
{
    BencWalkFunc    intFunc;
    BencWalkFunc    boolFunc;
    BencWalkFunc    realFunc;
    BencWalkFunc    stringFunc;
    BencWalkFunc    dictBeginFunc;
    BencWalkFunc    listBeginFunc;
    BencWalkFunc    containerEndFunc;
};

/**
 * This function's previous recursive implementation was
 * easier to read, but was vulnerable to a smash-stacking
 * attack via maliciously-crafted bencoded data. (#667)
 */
static void
bencWalk( const tr_benc          * top,
          const struct WalkFuncs * walkFuncs,
          void                   * user_data )
{
    tr_ptrArray stack = TR_PTR_ARRAY_INIT;

    tr_ptrArrayAppend( &stack, nodeNew( top ) );

    while( !tr_ptrArrayEmpty( &stack ) )
    {
        struct SaveNode * node = tr_ptrArrayBack( &stack );
        const tr_benc *   val;

        if( !node->valIsVisited )
        {
            val = node->val;
            node->valIsVisited = TRUE;
        }
        else if( node->childIndex < node->childCount )
        {
            const int index = node->children[node->childIndex++];
            val = node->val->val.l.vals +  index;
        }
        else /* done with this node */
        {
            if( isContainer( node->val ) )
                walkFuncs->containerEndFunc( node->val, user_data );
            tr_ptrArrayPop( &stack );
            tr_free( node->children );
            tr_free( node );
            continue;
        }

        if( val ) switch( val->type )
            {
                case TR_TYPE_INT:
                    walkFuncs->intFunc( val, user_data );
                    break;

                case TR_TYPE_BOOL:
                    walkFuncs->boolFunc( val, user_data );
                    break;

                case TR_TYPE_REAL:
                    walkFuncs->realFunc( val, user_data );
                    break;

                case TR_TYPE_STR:
                    walkFuncs->stringFunc( val, user_data );
                    break;

                case TR_TYPE_LIST:
                    if( val != node->val )
                        tr_ptrArrayAppend( &stack, nodeNew( val ) );
                    else
                        walkFuncs->listBeginFunc( val, user_data );
                    break;

                case TR_TYPE_DICT:
                    if( val != node->val )
                        tr_ptrArrayAppend( &stack, nodeNew( val ) );
                    else
                        walkFuncs->dictBeginFunc( val, user_data );
                    break;

                default:
                    /* did caller give us an uninitialized val? */
                    tr_err( "%s", _( "Invalid metadata" ) );
                    break;
            }
    }

    tr_ptrArrayDestruct( &stack, NULL );
}

/****
*****
****/

static void
saveIntFunc( const tr_benc * val, void * evbuf )
{
    evbuffer_add_printf( evbuf, "i%" PRId64 "e", val->val.i );
}

static void
saveBoolFunc( const tr_benc * val, void * evbuf )
{
    if( val->val.b )
        evbuffer_add( evbuf, "i1e", 3 );
    else
        evbuffer_add( evbuf, "i0e", 3 );
}

static void
saveRealFunc( const tr_benc * val, void * evbuf )
{
    char buf[128];
    char locale[128];
    size_t len;

    /* always use a '.' decimal point s.t. locale-hopping doesn't bite us */
    tr_strlcpy( locale, setlocale( LC_NUMERIC, NULL ), sizeof( locale ) );
    setlocale( LC_NUMERIC, "POSIX" );
    tr_snprintf( buf, sizeof( buf ), JSON_PARSER_FLOAT_SPRINTF_TOKEN, val->val.d );
    setlocale( LC_NUMERIC, locale );

    len = strlen( buf );
    evbuffer_add_printf( evbuf, "%lu:", (unsigned long)len );
    evbuffer_add( evbuf, buf, len );
}

static void
saveStringFunc( const tr_benc * val, void * evbuf )
{
    evbuffer_add_printf( evbuf, "%lu:", (unsigned long)val->val.s.len );
    evbuffer_add( evbuf, getStr(val), val->val.s.len );
}

static void
saveDictBeginFunc( const tr_benc * val UNUSED, void * evbuf )
{
    evbuffer_add( evbuf, "d", 1 );
}

static void
saveListBeginFunc( const tr_benc * val UNUSED, void * evbuf )
{
    evbuffer_add( evbuf, "l", 1 );
}

static void
saveContainerEndFunc( const tr_benc * val UNUSED, void * evbuf )
{
    evbuffer_add( evbuf, "e", 1 );
}

static const struct WalkFuncs saveFuncs = { saveIntFunc,
                                            saveBoolFunc,
                                            saveRealFunc,
                                            saveStringFunc,
                                            saveDictBeginFunc,
                                            saveListBeginFunc,
                                            saveContainerEndFunc };

/***
****
***/

static void
freeDummyFunc( const tr_benc * val UNUSED,
               void * buf          UNUSED  )
{}

static void
freeStringFunc( const tr_benc * val,
                void *          freeme )
{
    if( stringIsAlloced( val ) )
        tr_ptrArrayAppend( freeme, val->val.s.str.ptr );
}

static void
freeContainerBeginFunc( const tr_benc * val,
                        void *          freeme )
{
    tr_ptrArrayAppend( freeme, val->val.l.vals );
}

static const struct WalkFuncs freeWalkFuncs = { freeDummyFunc,
                                                freeDummyFunc,
                                                freeDummyFunc,
                                                freeStringFunc,
                                                freeContainerBeginFunc,
                                                freeContainerBeginFunc,
                                                freeDummyFunc };

void
tr_bencFree( tr_benc * val )
{
    if( isSomething( val ) )
    {
        tr_ptrArray a = TR_PTR_ARRAY_INIT;
        bencWalk( val, &freeWalkFuncs, &a );
        tr_ptrArrayDestruct( &a, tr_free );
    }
}

/***
****
***/

struct ParentState
{
    int    bencType;
    int    childIndex;
    int    childCount;
};

struct jsonWalk
{
    tr_bool doIndent;
    tr_list * parents;
    struct evbuffer *  out;
};

static void
jsonIndent( struct jsonWalk * data )
{
    if( data->doIndent )
    {
        char buf[1024];
        const int width = tr_list_size( data->parents ) * 4;

        buf[0] = '\n';
        memset( buf+1, ' ', width );
        evbuffer_add( data->out, buf, 1+width );
    }
}

static void
jsonChildFunc( struct jsonWalk * data )
{
    if( data->parents )
    {
        struct ParentState * parentState = data->parents->data;

        switch( parentState->bencType )
        {
            case TR_TYPE_DICT:
            {
                const int i = parentState->childIndex++;
                if( !( i % 2 ) )
                    evbuffer_add( data->out, ": ", data->doIndent ? 2 : 1 );
                else {
                    const tr_bool isLast = parentState->childIndex == parentState->childCount;
                    if( !isLast ) {
                        evbuffer_add( data->out, ", ", data->doIndent ? 2 : 1 );
                        jsonIndent( data );
                    }
                }
                break;
            }

            case TR_TYPE_LIST:
            {
                const tr_bool isLast = ++parentState->childIndex == parentState->childCount;
                if( !isLast ) {
                    evbuffer_add( data->out, ", ", data->doIndent ? 2 : 1 );
                    jsonIndent( data );
                }
                break;
            }

            default:
                break;
        }
    }
}

static void
jsonPushParent( struct jsonWalk * data,
                const tr_benc *   benc )
{
    struct ParentState * parentState = tr_new( struct ParentState, 1 );

    parentState->bencType = benc->type;
    parentState->childIndex = 0;
    parentState->childCount = benc->val.l.count;
    tr_list_prepend( &data->parents, parentState );
}

static void
jsonPopParent( struct jsonWalk * data )
{
    tr_free( tr_list_pop_front( &data->parents ) );
}

static void
jsonIntFunc( const tr_benc * val,
             void *          vdata )
{
    struct jsonWalk * data = vdata;

    evbuffer_add_printf( data->out, "%" PRId64, val->val.i );
    jsonChildFunc( data );
}

static void
jsonBoolFunc( const tr_benc * val, void * vdata )
{
    struct jsonWalk * data = vdata;

    if( val->val.b )
        evbuffer_add( data->out, "true", 4 );
    else
        evbuffer_add( data->out, "false", 5 );

    jsonChildFunc( data );
}

static void
jsonRealFunc( const tr_benc * val, void * vdata )
{
    struct jsonWalk * data = vdata;
    char locale[128];

    if( fabs( val->val.d ) < 0.00001 )
        evbuffer_add( data->out, "0", 1 );
    else {
        /* json requires a '.' decimal point regardless of locale */
        tr_strlcpy( locale, setlocale( LC_NUMERIC, NULL ), sizeof( locale ) );
        setlocale( LC_NUMERIC, "POSIX" );
        evbuffer_add_printf( data->out, "%.4f", tr_truncd( val->val.d, 4 ) );
        setlocale( LC_NUMERIC, locale );
    }

    jsonChildFunc( data );
}

static void
jsonStringFunc( const tr_benc * val, void * vdata )
{
    struct jsonWalk * data = vdata;
    const unsigned char * it = (const unsigned char *) getStr(val);
    const unsigned char * end = it + val->val.s.len;

    evbuffer_expand( data->out, val->val.s.len + 2 );
    evbuffer_add( data->out, "\"", 1 );

    for( ; it!=end; ++it )
    {
        switch( *it )
        {
            case '/': evbuffer_add( data->out, "\\/", 2 ); break;
            case '\b': evbuffer_add( data->out, "\\b", 2 ); break;
            case '\f': evbuffer_add( data->out, "\\f", 2 ); break;
            case '\n': evbuffer_add( data->out, "\\n", 2 ); break;
            case '\r': evbuffer_add( data->out, "\\r", 2 ); break;
            case '\t': evbuffer_add( data->out, "\\t", 2 ); break;
            case '"': evbuffer_add( data->out, "\\\"", 2 ); break;
            case '\\': evbuffer_add( data->out, "\\\\", 2 ); break;

            default:
                if( isascii( *it ) )
                    evbuffer_add( data->out, it, 1 );
                else {
                    const UTF8 * tmp = it;
                    UTF32        buf = 0;
                    UTF32 *      u32 = &buf;
                    ConversionResult result = ConvertUTF8toUTF32( &tmp, end, &u32, &buf + 1, 0 );
                    if((( result==conversionOK ) || (result==targetExhausted)) && (tmp!=it)) {
                        evbuffer_add_printf( data->out, "\\u%04x", (unsigned int)buf );
                        it = tmp - 1;
                    }
                }
        }
    }
    evbuffer_add( data->out, "\"", 1 );
    jsonChildFunc( data );
}

static void
jsonDictBeginFunc( const tr_benc * val,
                   void *          vdata )
{
    struct jsonWalk * data = vdata;

    jsonPushParent( data, val );
    evbuffer_add( data->out, "{", 1 );
    if( val->val.l.count )
        jsonIndent( data );
}

static void
jsonListBeginFunc( const tr_benc * val,
                   void *          vdata )
{
    const size_t      nChildren = tr_bencListSize( val );
    struct jsonWalk * data = vdata;

    jsonPushParent( data, val );
    evbuffer_add( data->out, "[", 1 );
    if( nChildren )
        jsonIndent( data );
}

static void
jsonContainerEndFunc( const tr_benc * val,
                      void *          vdata )
{
    struct jsonWalk * data = vdata;
    int               emptyContainer = FALSE;

    jsonPopParent( data );
    if( !emptyContainer )
        jsonIndent( data );
    if( tr_bencIsDict( val ) )
        evbuffer_add( data->out, "}", 1 );
    else /* list */
        evbuffer_add( data->out, "]", 1 );
    jsonChildFunc( data );
}

static const struct WalkFuncs jsonWalkFuncs = { jsonIntFunc,
                                                jsonBoolFunc,
                                                jsonRealFunc,
                                                jsonStringFunc,
                                                jsonDictBeginFunc,
                                                jsonListBeginFunc,
                                                jsonContainerEndFunc };

/***
****
***/

static size_t
tr_bencDictSize( const tr_benc * dict )
{
    size_t count = 0;

    if( tr_bencIsDict( dict ) )
        count = dict->val.l.count / 2;

    return count;
}

tr_bool
tr_bencDictChild( tr_benc * dict, size_t n, const char ** key, tr_benc ** val )
{
    tr_bool success = 0;

    assert( tr_bencIsDict( dict ) );

    if( tr_bencIsDict( dict ) && (n*2)+1 <= dict->val.l.count )
    {
        tr_benc * k = dict->val.l.vals + (n*2);
        tr_benc * v = dict->val.l.vals + (n*2) + 1;
        if(( success = tr_bencGetStr( k, key ) && isSomething( v )))
            *val = v;
    }

    return success;
}

void
tr_bencMergeDicts( tr_benc * target, const tr_benc * source )
{
    size_t i;
    const size_t sourceCount = tr_bencDictSize( source );

    assert( tr_bencIsDict( target ) );
    assert( tr_bencIsDict( source ) );

    for( i=0; i<sourceCount; ++i )
    {
        const char * key;
        tr_benc * val;
        tr_benc * t;

        if( tr_bencDictChild( (tr_benc*)source, i, &key, &val ) )
        {
            if( tr_bencIsBool( val ) )
            {
                tr_bool boolVal;
                tr_bencGetBool( val, &boolVal );
                tr_bencDictAddBool( target, key, boolVal );
            }
            else if( tr_bencIsReal( val ) )
            {
                double realVal = 0;
                tr_bencGetReal( val, &realVal );
                tr_bencDictAddReal( target, key, realVal );
            }
            else if( tr_bencIsInt( val ) )
            {
                int64_t intVal = 0;
                tr_bencGetInt( val, &intVal );
                tr_bencDictAddInt( target, key, intVal );
            }
            else if( tr_bencIsString( val ) )
            {
                const char * strVal = NULL;
                tr_bencGetStr( val, &strVal );
                tr_bencDictAddStr( target, key, strVal );
            }
            else if( tr_bencIsDict( val ) && tr_bencDictFindDict( target, key, &t ) )
            {
                tr_bencMergeDicts( t, val );
            }
            else
            {
                tr_dbg( "tr_bencMergeDicts skipping \"%s\"", key );
            }
        }
    }
}

/***
****
***/

void
tr_bencToBuf( const tr_benc * top, tr_fmt_mode mode, struct evbuffer * buf )
{
    evbuffer_drain( buf, EVBUFFER_LENGTH( buf ) );

    switch( mode )
    {
        case TR_FMT_BENC:
            bencWalk( top, &saveFuncs, buf );
            break;

        case TR_FMT_JSON:
        case TR_FMT_JSON_LEAN: {
            struct jsonWalk data;
            data.doIndent = mode==TR_FMT_JSON;
            data.out = buf;
            data.parents = NULL;
            bencWalk( top, &jsonWalkFuncs, &data );
            if( EVBUFFER_LENGTH( buf ) )
                evbuffer_add_printf( buf, "\n" );
            break;
        }
    }
}

char*
tr_bencToStr( const tr_benc * top, tr_fmt_mode mode, int * len )
{
    char * ret;
    struct evbuffer * buf = evbuffer_new( );
    tr_bencToBuf( top, mode, buf );
    ret = tr_strndup( EVBUFFER_DATA( buf ), EVBUFFER_LENGTH( buf ) );
    if( len != NULL )
        *len = (int) EVBUFFER_LENGTH( buf );
    evbuffer_free( buf );
    return ret;
}

int
tr_bencToFile( const tr_benc * top, tr_fmt_mode mode, const char * filename )
{
    int err = 0;
    FILE * fp = fopen( filename, "wb+" );

    if( fp == NULL )
    {
        err = errno;
        tr_err( _( "Couldn't open \"%1$s\": %2$s" ),
                filename, tr_strerror( errno ) );
    }
    else
    {
        int len;
        char * str = tr_bencToStr( top, mode, &len );

        if( fwrite( str, 1, len, fp ) == (size_t)len )
            tr_dbg( "tr_bencToFile saved \"%s\"", filename );
        else {
            err = errno;
            tr_err( _( "Couldn't save file \"%1$s\": %2$s" ), filename, tr_strerror( errno ) );
        }

        tr_free( str );
        fclose( fp );
    }

    return err;
}

/***
****
***/

int
tr_bencLoadFile( tr_benc * setme, tr_fmt_mode mode, const char * filename )
{
    int err;
    size_t contentLen;
    uint8_t * content;

    content = tr_loadFile( filename, &contentLen );
    if( !content && errno )
        err = errno;
    else if( !content )
        err = ENODATA;
    else {
        if( mode == TR_FMT_BENC )
            err = tr_bencLoad( content, contentLen, setme, NULL );
        else
            err = tr_jsonParse( filename, content, contentLen, setme, NULL );
    }

    tr_free( content );
    return err;
}
