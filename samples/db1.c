#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/times.h>
#include <pthread.h>
#include <ham/hamsterdb.h>


/* WARNING :
 * This implementation is using 32 bits long values for sizes
 */
typedef unsigned int md5_size;

/* MD5 context */
struct md5_ctx {
	struct {
		unsigned int A, B, C, D; /* registers */
	} regs;
	unsigned char *buf;
	md5_size size;
	md5_size bits;
};

/* Size of the MD5 buffer */
#define MD5_BUFFER 1024

/* Basic md5 functions */
#define F(x,y,z) ((x & y) | (~x & z))
#define G(x,y,z) ((x & z) | (~z & y))
#define H(x,y,z) (x ^ y ^ z)
#define I(x,y,z) (y ^ (x | ~z))

/* Rotate left 32 bits values (words) */
#define ROTATE_LEFT(w,s) ((w << s) | ((w & 0xFFFFFFFF) >> (32 - s)))

#define FF(a,b,c,d,x,s,t) (a = b + ROTATE_LEFT((a + F(b,c,d) + x + t), s))
#define GG(a,b,c,d,x,s,t) (a = b + ROTATE_LEFT((a + G(b,c,d) + x + t), s))
#define HH(a,b,c,d,x,s,t) (a = b + ROTATE_LEFT((a + H(b,c,d) + x + t), s))
#define II(a,b,c,d,x,s,t) (a = b + ROTATE_LEFT((a + I(b,c,d) + x + t), s))

unsigned char *md5 (unsigned char *, md5_size, unsigned char *);
void md5_init (struct md5_ctx *);
void md5_update (struct md5_ctx *context);
void md5_final (unsigned char *digest, struct md5_ctx *context);


#define S11 7
#define S12 12
#define S13 17
#define S14 22
#define S21 5
#define S22 9
#define S23 14
#define S24 20
#define S31 4
#define S32 11
#define S33 16
#define S34 23
#define S41 6
#define S42 10
#define S43 15
#define S44 21


#define memcopy(a,b,c) md5_memcopy ((a), (b), (c))
#define memset(a,b,c) md5_memset ((a), (b), (c))

#define GET_UINT32(a,b,i)				\
{							\
	(a) = ( (unsigned int) (b)[(i)	]      )	\
	    | ( (unsigned int) (b)[(i)+1] << 8 )	\
	    | ( (unsigned int) (b)[(i)+2] << 16)	\
	    | ( (unsigned int) (b)[(i)+3] << 24);	\
}

/* local functions */
static void md5_memcopy (unsigned char *, unsigned char *, const unsigned int);
static void md5_memset (unsigned char *, const unsigned char, const unsigned int);
static void md5_addsize (unsigned char *, md5_size , md5_size);
static void md5_encode (unsigned char *, struct md5_ctx *);

static unsigned char MD5_PADDING [64] = { /* 512 Bits */
	0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

/*
 * An easy way to do the md5 sum of a short memory space
 */
unsigned char *md5 (unsigned char *M, md5_size len, unsigned char *_digest)
{
	int buflen = (len > MD5_BUFFER) ? MD5_BUFFER: len;
	struct md5_ctx *context;

	context = malloc (sizeof (struct md5_ctx));
	context->buf = malloc (buflen);
	context->size = 0;
	context->bits = 0;

	/* Init registries */
	context->regs.A = 0x67452301;
	context->regs.B = 0xefcdab89;
	context->regs.C = 0x98badcfe;
	context->regs.D = 0x10325476;

	do {
		memcopy (context->buf + context->size, M + context->bits, buflen - context->size);
		context->size += buflen - context->size;
		md5_update (context);
	} while (len - context->bits > 64);

	md5_final (_digest, context);

        free (context->buf);
	free (context);

	return _digest;
}

void md5_init (struct md5_ctx *context)
{
	context->buf = malloc (MD5_BUFFER);
	memset (context->buf, '\0', MD5_BUFFER);
	context->size = 0;
	context->bits = 0;

	/* Init registries */
	context->regs.A = 0x67452301;
	context->regs.B = 0xefcdab89;
	context->regs.C = 0x98badcfe;
	context->regs.D = 0x10325476;
}

/* md5_size is bytes while the size at the end of the message is in bits ... */
static void md5_addsize (unsigned char *M, md5_size index, md5_size oldlen)
{
	assert (((index * 8) % 512) == 448); /* If padding is not done then exit */

	M[index++] = (unsigned char) ((oldlen << 3) & 0xFF);
	M[index++] = (unsigned char) ((oldlen >> 5) & 0xFF);
	M[index++] = (unsigned char) ((oldlen >> 13) & 0xFF);
	M[index++] = (unsigned char) ((oldlen >> 21) & 0xFF);
	/* Fill with 0 because md5_size is 32 bits long */
	M[index++] = 0; M[index++] = 0;
	M[index++] = 0; M[index++] = 0;
}

/*
 * Update a context by concatenating a new block
 */
void md5_update (struct md5_ctx *context)
{
	unsigned char buffer [64]; /* 512 bits */
	int i;

	for (i = 0; context->size - i > 63; i += 64) {
		memcopy (buffer, context->buf + i, 64);
		md5_encode (buffer, context);
		context->bits += 64;
	}
	memcopy (buffer, context->buf + i, context->size - i);
	memcopy (context->buf, buffer, context->size - i);
	context->size -= i;
}

void md5_final (unsigned char *digest, struct md5_ctx *context)
{
	unsigned char buffer [64]; /* 512 bits */
	int i;

	assert (context->size < 64);

	if (context->size + 1 > 56) { /* We have to create another block */
		memcopy (buffer, context->buf, context->size);
		memcopy (buffer + context->size, MD5_PADDING, 64 - context->size);
		md5_encode (buffer, context);
		context->bits += context->size;
		context->size = 0;
		/* Proceed final block */
		memset (buffer, '\0', 56);
		/*memcopy (buffer, MD5_PADDING + 1, 56);*/
		md5_addsize (buffer, 56, context->bits);
		md5_encode (buffer, context);
	} else {
		memcopy (buffer, context->buf, context->size);
		context->bits += context->size;

		memcopy (buffer + context->size, MD5_PADDING, 56 - context->size);
		md5_addsize (buffer, 56, context->bits);
		md5_encode (buffer, context);
	}
	/* update digest */
	for (i = 0; i < 4; i++)
		digest [i] = (unsigned char) ((context->regs.A >> (i*8)) & 0xFF);
	for (; i < 8; i++)
		digest [i] = (unsigned char) ((context->regs.B >> ((i-4)*8)) & 0xFF);
	for (; i < 12; i++)
		digest [i] = (unsigned char) ((context->regs.C >> ((i-8)*8)) & 0xFF);
	for (; i < 16; i++)
		digest [i] = (unsigned char) ((context->regs.D >> ((i-12)*8)) & 0xFF);
}

static void md5_encode (unsigned char *buffer, struct md5_ctx *context)
{
	unsigned int a = context->regs.A, b = context->regs.B, c = context->regs.C, d = context->regs.D;
	unsigned int x[16];

	GET_UINT32 (x[ 0],buffer, 0);
	GET_UINT32 (x[ 1],buffer, 4);
	GET_UINT32 (x[ 2],buffer, 8);
	GET_UINT32 (x[ 3],buffer,12);
	GET_UINT32 (x[ 4],buffer,16);
	GET_UINT32 (x[ 5],buffer,20);
	GET_UINT32 (x[ 6],buffer,24);
	GET_UINT32 (x[ 7],buffer,28);
	GET_UINT32 (x[ 8],buffer,32);
	GET_UINT32 (x[ 9],buffer,36);
	GET_UINT32 (x[10],buffer,40);
	GET_UINT32 (x[11],buffer,44);
	GET_UINT32 (x[12],buffer,48);
	GET_UINT32 (x[13],buffer,52);
	GET_UINT32 (x[14],buffer,56);
	GET_UINT32 (x[15],buffer,60);

	/* Round 1 */
	FF (a, b, c, d, x[ 0], S11, 0xd76aa478); /* 1 */
	FF (d, a, b, c, x[ 1], S12, 0xe8c7b756); /* 2 */
	FF (c, d, a, b, x[ 2], S13, 0x242070db); /* 3 */
	FF (b, c, d, a, x[ 3], S14, 0xc1bdceee); /* 4 */
	FF (a, b, c, d, x[ 4], S11, 0xf57c0faf); /* 5 */
	FF (d, a, b, c, x[ 5], S12, 0x4787c62a); /* 6 */
	FF (c, d, a, b, x[ 6], S13, 0xa8304613); /* 7 */
	FF (b, c, d, a, x[ 7], S14, 0xfd469501); /* 8 */
	FF (a, b, c, d, x[ 8], S11, 0x698098d8); /* 9 */
	FF (d, a, b, c, x[ 9], S12, 0x8b44f7af); /* 10 */
	FF (c, d, a, b, x[10], S13, 0xffff5bb1); /* 11 */
	FF (b, c, d, a, x[11], S14, 0x895cd7be); /* 12 */
	FF (a, b, c, d, x[12], S11, 0x6b901122); /* 13 */
	FF (d, a, b, c, x[13], S12, 0xfd987193); /* 14 */
	FF (c, d, a, b, x[14], S13, 0xa679438e); /* 15 */
	FF (b, c, d, a, x[15], S14, 0x49b40821); /* 16 */

	/* Round 2 */
	GG (a, b, c, d, x[ 1], S21, 0xf61e2562); /* 17 */
	GG (d, a, b, c, x[ 6], S22, 0xc040b340); /* 18 */
	GG (c, d, a, b, x[11], S23, 0x265e5a51); /* 19 */
	GG (b, c, d, a, x[ 0], S24, 0xe9b6c7aa); /* 20 */
	GG (a, b, c, d, x[ 5], S21, 0xd62f105d); /* 21 */
	GG (d, a, b, c, x[10], S22,  0x2441453); /* 22 */
	GG (c, d, a, b, x[15], S23, 0xd8a1e681); /* 23 */
	GG (b, c, d, a, x[ 4], S24, 0xe7d3fbc8); /* 24 */
	GG (a, b, c, d, x[ 9], S21, 0x21e1cde6); /* 25 */
	GG (d, a, b, c, x[14], S22, 0xc33707d6); /* 26 */
	GG (c, d, a, b, x[ 3], S23, 0xf4d50d87); /* 27 */

	GG (b, c, d, a, x[ 8], S24, 0x455a14ed); /* 28 */
	GG (a, b, c, d, x[13], S21, 0xa9e3e905); /* 29 */
	GG (d, a, b, c, x[ 2], S22, 0xfcefa3f8); /* 30 */
	GG (c, d, a, b, x[ 7], S23, 0x676f02d9); /* 31 */
	GG (b, c, d, a, x[12], S24, 0x8d2a4c8a); /* 32 */

	/* Round 3 */
	HH (a, b, c, d, x[ 5], S31, 0xfffa3942); /* 33 */
	HH (d, a, b, c, x[ 8], S32, 0x8771f681); /* 34 */
	HH (c, d, a, b, x[11], S33, 0x6d9d6122); /* 35 */
	HH (b, c, d, a, x[14], S34, 0xfde5380c); /* 36 */
	HH (a, b, c, d, x[ 1], S31, 0xa4beea44); /* 37 */
	HH (d, a, b, c, x[ 4], S32, 0x4bdecfa9); /* 38 */
	HH (c, d, a, b, x[ 7], S33, 0xf6bb4b60); /* 39 */
	HH (b, c, d, a, x[10], S34, 0xbebfbc70); /* 40 */
	HH (a, b, c, d, x[13], S31, 0x289b7ec6); /* 41 */
	HH (d, a, b, c, x[ 0], S32, 0xeaa127fa); /* 42 */
	HH (c, d, a, b, x[ 3], S33, 0xd4ef3085); /* 43 */
	HH (b, c, d, a, x[ 6], S34,  0x4881d05); /* 44 */
	HH (a, b, c, d, x[ 9], S31, 0xd9d4d039); /* 45 */
	HH (d, a, b, c, x[12], S32, 0xe6db99e5); /* 46 */
	HH (c, d, a, b, x[15], S33, 0x1fa27cf8); /* 47 */
	HH (b, c, d, a, x[ 2], S34, 0xc4ac5665); /* 48 */

	/* Round 4 */
	II (a, b, c, d, x[ 0], S41, 0xf4292244); /* 49 */
	II (d, a, b, c, x[ 7], S42, 0x432aff97); /* 50 */
	II (c, d, a, b, x[14], S43, 0xab9423a7); /* 51 */
	II (b, c, d, a, x[ 5], S44, 0xfc93a039); /* 52 */
	II (a, b, c, d, x[12], S41, 0x655b59c3); /* 53 */
	II (d, a, b, c, x[ 3], S42, 0x8f0ccc92); /* 54 */
	II (c, d, a, b, x[10], S43, 0xffeff47d); /* 55 */
	II (b, c, d, a, x[ 1], S44, 0x85845dd1); /* 56 */
	II (a, b, c, d, x[ 8], S41, 0x6fa87e4f); /* 57 */
	II (d, a, b, c, x[15], S42, 0xfe2ce6e0); /* 58 */
	II (c, d, a, b, x[ 6], S43, 0xa3014314); /* 59 */
	II (b, c, d, a, x[13], S44, 0x4e0811a1); /* 60 */
	II (a, b, c, d, x[ 4], S41, 0xf7537e82); /* 61 */
	II (d, a, b, c, x[11], S42, 0xbd3af235); /* 62 */
	II (c, d, a, b, x[ 2], S43, 0x2ad7d2bb); /* 63 */
	II (b, c, d, a, x[ 9], S44, 0xeb86d391); /* 64 */

	context->regs.A += a;
	context->regs.B += b;
	context->regs.C += c;
	context->regs.D += d;
}

/* OBSOLETE */
static void md5_memcopy (unsigned char *dest, unsigned char *src, unsigned int count)
{
	unsigned int i;

	for (i = 0; i < count; i++) {
		dest [i] = src [i];
	}
}

static void md5_memset (unsigned char *p, const unsigned char c, const unsigned int count)
{
	unsigned int i;

	for (i = 0; i < count; i++) {
		p [i] = c;
	}
}



struct test_struct {
	pthread_mutex_t    db_lock;
	/** Pad the cache line around the mutex */
	char               _pad[64 - sizeof(pthread_mutex_t)];
	uint64_t		  keyvalue;
	ham_db_t          *db;
    ham_env_t         *env;
    const char* db_file;
};

struct test_key {
	char data[20];
};

struct test_record {
	char data[8];
};


void encode(void* data,int data_size,char* output){

	md5(data,data_size,output);
#if 0
    sprintf(output, "%u", *(uint32_t*)data);
#endif
}





clock_t start_test(const char* msg,struct tms* start_timer){
	    fprintf(stderr, "Starting %s\n", msg);
	    return times(start_timer);
}

clock_t end_test(const char* msg,struct tms* end_timer){
	    fprintf(stderr, "Ending %s\n", msg);
	    return times(end_timer);
}


int
populate(struct test_struct *test_struct)
{
    int ret = 0;
    ham_cursor_t *cursor = NULL;
    ham_status_t ret_t;
    struct test_key *test_key = NULL;
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */
    int cursor_state = 0;


    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));
    assert(pthread_mutex_lock(&test_struct->db_lock) == 0);
    ret_t = ham_cursor_create(test_struct->db, NULL, 0, &cursor);
    if (ret_t != HAM_SUCCESS) {
        fprintf(stderr,
              "Could not get DB cursor, [%s].", ham_strerror(ret_t));
        ret = -1;
        goto err;
    }

    cursor_state = HAM_CURSOR_FIRST;
    for (;;) {
        ret_t = ham_cursor_move(cursor, &key, &record, cursor_state);
        if (ret_t != HAM_SUCCESS) {
            if (ret_t == HAM_KEY_NOT_FOUND) {
                fprintf(stderr, "Reached end of database.");
                break;          // reached the end of the database
            } else {
                fprintf(stderr,
                      "DB cursor move error, [%s].", ham_strerror(ret_t));
                ret = -1;
                goto err;
            }
        }
        test_struct->keyvalue++;
        if ((test_struct->keyvalue%100000)==0){
                fprintf(stderr,"read [%d] records\n",test_struct->keyvalue);

		}
        cursor_state = HAM_CURSOR_NEXT;
    }

err:
    if (cursor != NULL) {
        ret_t = ham_cursor_close(cursor);
        if (ret_t != HAM_SUCCESS) {
            fprintf(stderr, "DB cursor close error, [%s].",
                  ham_strerror(ret_t));
            ret = -1;
        }
    }

    assert(pthread_mutex_unlock(&test_struct->db_lock) == 0);
    return ret;
}



int32_t add(struct test_struct * test_struct,
                           struct test_key * test_key,
                           struct test_record* test_record
                           )
{
    int ret = 0;
    ham_status_t st;            /* status variable */
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    // setup the key and record
    key.data = test_key;
    key.size = sizeof(struct test_key);
    record.size = sizeof(struct test_record);
    record.data = test_record;
    assert(pthread_mutex_lock(&test_struct->db_lock) == 0);
    // ham_insert creates its own temporary transaction
    st = ham_insert(test_struct->db, 0, &key, &record,
                    HAM_OVERWRITE);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"DB insert failed, [%s].",
              ham_strerror(st));
        ret = -1;
    }
    assert(pthread_mutex_unlock(&test_struct->db_lock) == 0);

    return ret;
}



uint32_t find(struct test_struct * test_struct,
                             struct test_key * test_key,
                             struct test_record * test_record)
{
    int ret = 0;
    ham_status_t st;            /* status variable */
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    // Setup the key and Record - record points to test_struct_enty
    key.data = test_key;
    key.size = sizeof(struct test_key);
    record.data = test_record;
    record.flags = HAM_RECORD_USER_ALLOC;
    record.size = sizeof(struct test_record);;

    assert(pthread_mutex_lock(&test_struct->db_lock) == 0);
    st = ham_find(test_struct->db, 0, &key, &record, 0);
	if (st == HAM_KEY_NOT_FOUND) {
		ret =-1;
    }
    assert(pthread_mutex_unlock(&test_struct->db_lock) == 0);

    return ret;
}



uint32_t _delete(struct test_struct * test_struct,
                               struct test_key * test_key)
{
    int ret = 0;
    ham_status_t st;            /* status variable */
    ham_key_t key;              /* the structure for a key */

    // Set up the key
    memset(&key, 0, sizeof(key));
    key.data = test_key;
    key.size = sizeof(struct test_key);

    assert(pthread_mutex_lock(&test_struct->db_lock) == 0);
    // ham_erase creates its own temporary transaction if called without
    // a transaction handle
    st = ham_erase(test_struct->db, 0, &key, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"DB erase failed, [%s].",
              ham_strerror(st));
        ret = -1;
    }
    assert(pthread_mutex_unlock(&test_struct->db_lock) == 0);

    return ret;
}



void release(struct test_struct *test_struct)
{
    if (test_struct->db != NULL) {
        ham_delete(test_struct->db);
    }

    return;
}

void
timer_print_times(const char *label,
                               clock_t real, struct tms *start, struct tms *end)
{
    long clock_tick = sysconf(_SC_CLK_TCK);
    assert(clock_tick > 0);

    fprintf(stderr, "%s, ", label);
    fprintf(stderr, "Real: %7.2fs, ", real / (double) clock_tick);
    fprintf(stderr, "User: %7.2fs, ",
            (end->tms_utime - start->tms_utime) / (double) clock_tick);
    fprintf(stderr, "Sys: %7.2fs\n",
            (end->tms_stime - start->tms_stime) / (double) clock_tick);
}


int close_env(struct test_struct *test_struct);
int create_env(struct test_struct *test_struct,
                                uint32_t db_cache_size)
{
    int ret = 0;
    ham_status_t st;

    ham_parameter_t env_parameters[] = {
        {HAM_PARAM_CACHESIZE, db_cache_size * 1024 * 1024},
        // get an error if we go above this limit
        {HAM_PARAM_MAX_ENV_DATABASES, 256},
        {0, 0}
    };


    st = ham_env_new(&test_struct->env);
    if (st != HAM_SUCCESS) {
        test_struct->env = 0;
        fprintf(stderr,"Error allocating hamster environment 0x%x", st);
        ret = -1;
        goto err;
    }

    st = ham_env_create_ex(test_struct->env,
                           test_struct->db_file,
                           HAM_ENABLE_TRANSACTIONS |
                           HAM_DISABLE_MMAP, 0664, env_parameters);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"Error creating hamster environment 0x%x", st);
        ret = -1;
        goto err;
    }

    return ret;
err:
    close_env(test_struct);
    return ret;
}


int open_env(struct test_struct *test_struct,
                                uint32_t db_cache_size)
{
    int ret = 0;
    ham_status_t st;

    ham_parameter_t env_parameters[] = {
        {HAM_PARAM_CACHESIZE, db_cache_size * 1024 * 1024},
        {0, 0}
    };


    st = ham_env_new(&test_struct->env);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"Error allocating hamster environment %d\n", st);
        ret = -1;
        goto err;
    }


    st = ham_env_open_ex(test_struct->env,
                         test_struct->db_file,
                         HAM_ENABLE_TRANSACTIONS |
                         HAM_DISABLE_MMAP |
                         HAM_AUTO_RECOVERY |
                         HAM_ENABLE_RECOVERY, env_parameters);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"Error opening hamster environment %d\n", st);
        ham_env_delete(test_struct->env);
        ret = -1;
        goto err;
    }

err:
    return ret;
}


int close_env(struct test_struct *test_struct)
{
    int ret = 0;
    ham_status_t st;

    if (test_struct->env == NULL) {
        goto out;
    }
    // This will close all the open databases
    st = ham_env_close(test_struct->env,
                       HAM_AUTO_CLEANUP | HAM_TXN_AUTO_COMMIT);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"Error closing hamster environment 0x%x", st);
        ret = -1;
    }

    if (test_struct->db != NULL) {
        ham_delete(test_struct->db);
        test_struct->db = NULL;
    }


    ham_env_delete(test_struct->env);
    test_struct->env = NULL;
out:
    return ret;
}


int
create_db(struct test_struct *test_struct,
                              uint16_t db_id)
{
    int ret = 0;
    ham_status_t st;

    assert(db_id != 0);

    ham_parameter_t create_params[] = {
        {HAM_PARAM_KEYSIZE, 20},
        {0, 0}
    };


    st = ham_new(&test_struct->db);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"ham_new error 0x%x", st);
        ret = -1;
        goto out;
    }

    st = ham_env_create_db(test_struct->env, test_struct->db,
                           db_id, HAM_DISABLE_VAR_KEYLEN, create_params);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"ham_env_create_db error 0x%x", st);
        ret = -1;
        goto err;
    }


out:
    return ret;
err:
    ham_delete(test_struct->db);
    return ret;
}


int
open_db(struct test_struct *test_struct,
                              uint16_t db_id)
{
    int ret = 0;
    ham_status_t st;

    st = ham_new(&test_struct->db);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"ham_new error %d", st);
        ret = -1;
        goto out;
    }

    st = ham_env_open_db(test_struct->env, test_struct->db,
                         db_id, HAM_DISABLE_VAR_KEYLEN, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr,"ham_env_open_db error %d", st);
        ret = -1;
        goto err;
    }

out:
    return ret;
err:
    ham_delete(test_struct->db);
    return ret;
}


void add_record_test(struct test_struct* test_struct){
    struct tms start_timer, end_timer;
	struct test_key test_key;
	struct test_record test_record;
	int t=0,u=0,v=0,counter=test_struct->keyvalue;
	clock_t start, end;
	memset(&test_key,0,sizeof(struct test_key));
	memset(&test_record,0,sizeof(struct test_record));
    // insert 50 000 keys
	for (u=0;u<50;u++){
		start = start_test(__func__,&start_timer);
		for (t=0;t<1000;t++){
			counter++;
			encode(&counter,sizeof(int),(char*)&test_key.data);
			add(test_struct,&test_key,&test_record);
		}
		end = end_test(__func__,&end_timer);
		fprintf(stderr, "count [%d] (thousands)", u);
		timer_print_times(__func__, end - start, &start_timer,&end_timer);
	}
}

void find_record_test(struct test_struct* test_struct){
    struct tms start_timer, end_timer;
	struct test_key test_key;
	struct test_record test_record;
	int t=0;
	clock_t start, end;
	memset(&test_key,0,sizeof(struct test_key));
	memset(&test_record,0,sizeof(struct test_record));
	start = start_test(__func__,&start_timer);
	for (t=0;t<1024*256;t++){
		encode(&t,sizeof(int),(char*)&test_key.data);
		find(test_struct,&test_key,&test_record);
	}
    end = end_test(__func__,&end_timer);
    timer_print_times(__func__, end - start, &start_timer,&end_timer);
}


void delete_record_test(struct test_struct* test_struct){
    struct tms start_timer, end_timer;
	struct test_key test_key;
	struct test_record test_record;
	int t=0;
	clock_t start, end;
	memset(&test_key,0,sizeof(struct test_key));
	memset(&test_record,0,sizeof(struct test_record));
	start = start_test(__func__,&start_timer);
	for (t=0;t<1024*256;t++){
		encode(&t,sizeof(int),(char*)&test_key.data);
		_delete(test_struct,&test_key);
	}
    end = end_test(__func__,&end_timer);
    timer_print_times(__func__, end - start, &start_timer,&end_timer);
}


void combined_record_test(struct test_struct* test_struct){
    struct tms start_timer, end_timer;
	struct test_key test_key;
	struct test_record test_record;
	int t=0;
	clock_t start, end;
	memset(&test_key,0,sizeof(struct test_key));
	memset(&test_record,0,sizeof(struct test_record));
	start = start_test(__func__,&start_timer);
	for (t=0;t<1024*256;t++){
		encode(&t,sizeof(int),(char*)&test_key.data);
		find(test_struct,&test_key,&test_record);
		add(test_struct,&test_key,&test_record);
	}
    end = end_test(__func__,&end_timer);
    timer_print_times(__func__, end - start, &start_timer,&end_timer);
}



void invalid_find_record_test(struct test_struct* test_struct){
    struct tms start_timer, end_timer;
	struct test_key test_key;
	struct test_record test_record;
	int t=0;
	clock_t start, end;
	memset(&test_key,0,sizeof(struct test_key));
	memset(&test_record,0,sizeof(struct test_record));
	start = start_test(__func__,&start_timer);
	for (t=1024*256;t<1024*512;t++){
		encode(&t,sizeof(int),(char*)&test_key.data);
		assert(find(test_struct,&test_key,&test_record)==-1);// Record Exists ?
	}
    end = end_test(__func__,&end_timer);
    timer_print_times(__func__, end - start, &start_timer,&end_timer);
}

int file_exists(char* filepath){
	struct stat sb;
	return (stat(filepath,&sb)==0);
}

void do_flush(struct test_struct* test_struct){
	ham_env_flush(test_struct->env,0);
}


int main()
{
    int dummy=0;
	struct test_struct test_struct;
    struct tms start_timer, end_timer;
	clock_t start, end;
    int res = 0;
	memset(&test_struct,0,sizeof(test_struct));
	test_struct.db_file="/tmp/testdb";
	pthread_mutex_init(&test_struct.db_lock, NULL);
	if (file_exists(test_struct.db_file)==0){
		fprintf(stderr,"Creating [%s]\n",test_struct.db_file);
		create_env(&test_struct,4000);
		create_db(&test_struct,1);
	}else{
		fprintf(stderr,"[%s] Already exists\n",test_struct.db_file);
		open_env(&test_struct,4000);
		open_db(&test_struct,1);
		populate(&test_struct);
	}
    getc(stdin);
	start = times(&start_timer);
	add_record_test(&test_struct);
	close_env(&test_struct);
    end = times(&end_timer);
    timer_print_times("all tests" , end - start, &start_timer,&end_timer);
	pthread_mutex_destroy(&test_struct.db_lock);
    return res;
}
