/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * The java wrapper routines
 *
 */

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <string.h>

#include <ham/hamsterdb_int.h>

#include "de_crupp_hamsterdb_Error.h"

static JavaVM *javavm=0;

#define jni_log(x) printf(x)

typedef struct jnipriv
{
    JNIEnv *jenv;
    jobject jobj;
} jnipriv;

#define SET_DB_CONTEXT(db, jenv, jobj) jnipriv p; p.jenv=jenv; p.jobj=jobj; \
                                       ham_set_context_data(db, &p);

static jint
jni_set_cursor_env(JNIEnv *jenv, jobject jobj, jlong jhandle)
{
    jclass jcls;
    jfieldID jfid;
    jobject jdbobject;
    ham_cursor_t *c=(ham_cursor_t *)jhandle;

    /* get the callback method */
    jcls=(*jenv)->GetObjectClass(jenv, jobj);
    if (!jcls) {
        jni_log(("GetObjectClass failed\n"));
        return (HAM_INTERNAL_ERROR);
    }

    jfid=(*jenv)->GetFieldID(jenv, jcls, "m_db", 
            "Lde/crupp/hamsterdb/Database;");
    if (!jfid) {
        jni_log(("GetFieldID failed\n"));
        return (HAM_INTERNAL_ERROR);
    }

    jdbobj=(*jenv)->GetObjectField(jenv, jobj, jfid);
    if (!jdbobj) {
        jni_log(("GetObjectFieldID failed\n"));
        return (HAM_INTERNAL_ERROR);
    }

    SET_DB_CONTEXT(cursor_get_db(c), jenv, jdbobj);
    return (0);
}

static void
jni_throw_error(JNIEnv *jenv, ham_status_t st)
{
    jclass jcls=(*jenv)->FindClass(jenv, "de/crupp/hamsterdb/Error");
    if (!jcls) {
        jni_log(("Cannot find class de.crupp.hamsterdb.Error\n"));
        return;
    }

    jmethodID ctor=(*jenv)->GetMethodID(jenv, jcls, "", "(I)V");
    if (!ctor) {
        jni_log(("Cannot find constructor of Error class\n"));
        return;
    }

    jobject jobj=(*jenv)->NewObject(jenv, jcls, ctor, st);
    if (!jobj) {
        jni_log(("Cannot create new Exception\n"));
        return;
    }

    (*jenv)->Throw(jenv, jobj);
}

static void
jni_errhandler(int level, const char *message)
{
    jclass jcls;
    jmethodID jmid;
    jobject jobj;
    jfieldID jfid;
    JNIEnv *jenv;
    jstring str;

    if ((*javavm)->AttachCurrentThread(javavm, (void **)&jenv, 0) != 0) {
        jni_log(("AttachCurrentThread failed\n"));
        return;
    }

    jcls=(*jenv)->FindClass(jenv, "de/crupp/hamsterdb/Database");
    if (!jcls) {
        jni_log(("unable to find class de/crupp/hamsterdb/Database\n"));
        return;
    }

    jfid=(*jenv)->GetStaticFieldID(jenv, jcls, "m_eh", 
            "Lde/crupp/hamsterdb/ErrorHandler;");
    if (!jfid) {
        jni_log(("unable to find ErrorHandler field\n"));
        return;
    }

    jobj=(*jenv)->GetStaticObjectField(jenv, jcls, jfid);
    if (!jobj) {
        jni_log(("unable to get ErrorHandler object\n"));
        return;
    }

    jcls=(*jenv)->GetObjectClass(jenv, jobj);
    if (!jcls) {
        jni_log(("unable to get ErrorHandler class\n"));
        return;
    }

    jmid=(*jenv)->GetMethodID(jenv, jcls, "handleMessage",
            "(ILjava/lang/String;)V");
    if (!jmid) {
        jni_log(("unable to get handleMessage method\n"));
        return;
    }

    str=(*jenv)->NewStringUTF(jenv, message);
    if (!str) {
        jni_log(("unable to create new Java string\n"));
        return;
    }

    /* call the java method */
    (*jenv)->CallNonvirtualVoidMethod(jenv, jobj, jcls,
            jmid, (jint)level, str);
}

static int
jni_compare_func(ham_db_t *db, 
        const ham_u8_t *lhs, ham_size_t lhs_length, 
        const ham_u8_t *rhs, ham_size_t rhs_length)
{
    jobject jcmpobj;
    jclass jcls, jcmpcls;
    jfieldID jfid;
    jmethodID jmid;
    jbyteArray jlhs, jrhs;

    int ret;

    /* get the Java Environment and the Database instance */
    jnipriv *p=(jnipriv *)ham_get_context_data(db);

    /* get the callback method */
    jcls=(*p->jenv)->GetObjectClass(p->jenv, p->jobj);
    if (!jcls) {
        jni_log(("GetObjectClass failed\n"));
        return (-1); /* TODO throw! */
    }

    jfid=(*p->jenv)->GetFieldID(p->jenv, jcls, "m_cmp", 
            "Lde/crupp/hamsterdb/Comparable;");
    if (!jfid) {
        jni_log(("GetFieldID failed\n"));
        return (-1); /* TODO throw! */
    }

    jcmpobj=(*p->jenv)->GetObjectField(p->jenv, p->jobj, jfid);
    if (!jcmpobj) {
        jni_log(("GetObjectFieldID failed\n"));
        return (-1); /* TODO throw! */
    }

    jcmpcls=(*p->jenv)->GetObjectClass(p->jenv, jcmpobj);
    if (!jcmpcls) {
        jni_log(("GetObjectClass failed\n"));
        return (-1); /* TODO throw! */
    }

    jmid=(*p->jenv)->GetMethodID(p->jenv, jcmpcls, "compare",
            "([B[B)I");
    if (!jmid) {
        jni_log(("GetMethodID failed\n"));
        return (-1); /* TODO throw! */
    }

    /* prepare the parameters */
    jlhs=(*p->jenv)->NewByteArray(p->jenv, lhs_length);
    if (!jlhs) {
        jni_log(("NewByteArray failed\n"));
        return (-1); /* TODO throw! */
    }

    if (lhs_length)
        (*p->jenv)->SetByteArrayRegion(p->jenv, jlhs, 0, lhs_length,
                (jbyte *)lhs);

    jrhs=(*p->jenv)->NewByteArray(p->jenv, rhs_length);
    if (!jrhs) {
        jni_log(("NewByteArray failed\n"));
        return (-1); /* TODO throw! */
    }

    if (rhs_length)
        (*p->jenv)->SetByteArrayRegion(p->jenv, jrhs, 0, rhs_length,
                (jbyte *)rhs);

    ret=(*p->jenv)->CallIntMethod(p->jenv, jcmpobj, jmid, jlhs, jrhs);

    return (ret);
}

static int
jni_prefix_compare_func(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length, ham_size_t lhs_real_length,
        const ham_u8_t *rhs, ham_size_t rhs_length, ham_size_t rhs_real_length)
{
    jobject jcmpobj;
    jclass jcls, jcmpcls;
    jfieldID jfid;
    jmethodID jmid;
    jbyteArray jlhs, jrhs;

    int ret;

    /* get the Java Environment and the Database instance */
    jnipriv *p=(jnipriv *)ham_get_context_data(db);

    /* get the callback method */
    jcls=(*p->jenv)->GetObjectClass(p->jenv, p->jobj);
    if (!jcls) {
        jni_log(("GetObjectClass failed\n"));
        return (-1); /* TODO throw! */
    }

    jfid=(*p->jenv)->GetFieldID(p->jenv, jcls, "m_prefix_cmp", 
            "Lde/crupp/hamsterdb/PrefixComparable;");
    if (!jfid) {
        jni_log(("GetFieldID failed\n"));
        return (-1); /* TODO throw! */
    }

    jcmpobj=(*p->jenv)->GetObjectField(p->jenv, p->jobj, jfid);
    if (!jcmpobj) {
        jni_log(("GetObjectFieldID failed\n"));
        return (-1); /* TODO throw! */
    }

    jcmpcls=(*p->jenv)->GetObjectClass(p->jenv, jcmpobj);
    if (!jcmpcls) {
        jni_log(("GetObjectClass failed\n"));
        return (-1); /* TODO throw! */
    }

    jmid=(*p->jenv)->GetMethodID(p->jenv, jcmpcls, "compare",
            "([BI[BI)I");
    if (!jmid) {
        jni_log(("GetMethodID failed\n"));
        return (-1); /* TODO throw! */
    }

    /* prepare the parameters */
    jlhs=(*p->jenv)->NewByteArray(p->jenv, lhs_length);
    if (!jlhs) {
        jni_log(("NewByteArray failed\n"));
        return (-1); /* TODO throw! */
    }

    if (lhs_length)
        (*p->jenv)->SetByteArrayRegion(p->jenv, jlhs, 0, lhs_length,
                (jbyte *)lhs);

    jrhs=(*p->jenv)->NewByteArray(p->jenv, rhs_length);
    if (!jrhs) {
        jni_log(("NewByteArray failed\n"));
        return (-1); /* TODO throw! */
    }

    if (rhs_length)
        (*p->jenv)->SetByteArrayRegion(p->jenv, jrhs, 0, rhs_length,
                (jbyte *)rhs);

    ret=(*p->jenv)->CallIntMethod(p->jenv, jcmpobj, jmid, jlhs, 
            (jint)lhs_real_length, jrhs, (jint)rhs_real_length);

    return (ret);
}

static ham_status_t
jparams_to_native(JNIEnv *jenv, jobjectArray jparams, ham_parameter_t **pparams)
{
    unsigned int i, j=0, size=(*jenv)->GetArrayLength(jenv, jparams);
    ham_parameter_t *params=(ham_parameter_t *)malloc(sizeof(*params)*(size+1));
    if (!params)
        return (HAM_OUT_OF_MEMORY);

    for (i=0; i<size; i++) {
        jobject jobj=(*jenv)->GetObjectArrayElement(jenv, jparams, i);
        if (jobj) {
            jclass jcls=(*jenv)->GetObjectClass(jenv, jobj);
            jfieldID fidname=(*jenv)->GetFieldID(jenv, jcls, "name", "I");
            jfieldID fidvalue=(*jenv)->GetFieldID(jenv, jcls, "value", "J");
            jlong lname=(*jenv)->GetLongField(jenv, jobj, fidname);
            jlong lvalue=(*jenv)->GetLongField(jenv, jobj, fidvalue);
            params[j].name=(ham_u32_t)lname;
            params[j].value=(ham_u64_t)lvalue;
            j++;
        }
    }
    params[j].name=0;
    params[j].value=0;

    *pparams=params;

    return (0);
}

JNIEXPORT jstring JNICALL 
Java_de_crupp_hamsterdb_Error_ham_1strerror(JNIEnv *jenv, jobject jobj, 
        jint jerrno)
{
    return (*jenv)->NewStringUTF(jenv, 
            ham_strerror((ham_status_t)jerrno));
}

JNIEXPORT jint JNICALL 
Java_de_crupp_hamsterdb_Database_ham_1get_1version(JNIEnv *jenv, jclass jcls, 
        jint which)
{
    ham_u32_t v;

    if (which==0)
        ham_get_version(&v, 0, 0);
    else if (which==1)
        ham_get_version(0, &v, 0);
    else
        ham_get_version(0, 0, &v);

    return ((jint)v);
}

JNIEXPORT jstring JNICALL
Java_de_crupp_hamsterdb_Database_ham_1get_1license(JNIEnv *jenv, jclass jcls,
        jint which)
{
    const char *p;

    if (which==0)
        ham_get_license(&p, 0);
    else if (which==1)
        ham_get_license(0, &p);
    
    return ((*jenv)->NewStringUTF(jenv, p));
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ham_1set_1errhandler(JNIEnv *jenv,
        jclass jcls, jobject jeh)
{
    if (!jeh) {
        ham_set_errhandler(0);
        return;
    }

    /* set global javavm pointer, if needed */
    if (!javavm) {
        if ((*jenv)->GetJavaVM(jenv, &javavm) != 0) {
            jni_log(("Cannot get Java VM\n"));
            return;
        }
    }

    ham_set_errhandler(jni_errhandler);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Database_ham_1new(JNIEnv *jenv, jobject jobj)
{
    ham_db_t *db;

    if (ham_new(&db))
        return (0);
    return ((jlong)db);
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ham_1delete(JNIEnv *jenv, jobject jobj,
        jlong jhandle)
{
    ham_delete((ham_db_t *)jhandle);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1create_1ex(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jstring jfilename, jint jflags, jint jmode,
        jobjectArray jparams)
{
    ham_status_t st;
    ham_parameter_t *params=0;
    const char* filename=0;

    if (jparams) {
        st=jparams_to_native(jenv, jparams, &params);
        if (st)
            return (st);
    }

    if (jfilename)
        filename=(*jenv)->GetStringUTFChars(jenv, jfilename, 0); 

    st=ham_create_ex((ham_db_t *)jhandle, filename, (ham_u32_t)jflags,
            (ham_u32_t)jmode, params);

    if (params)
        free(params);
    if (jfilename)
        (*jenv)->ReleaseStringUTFChars(jenv, jfilename, filename); 

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1open_1ex(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jstring jfilename, jint jflags, jobjectArray jparams)
{
    ham_status_t st;
    ham_parameter_t *params=0;
    const char* filename=0;

    if (jparams) {
        st=jparams_to_native(jenv, jparams, &params);
        if (st)
            return (st);
    }

    if (jfilename)
        filename=(*jenv)->GetStringUTFChars(jenv, jfilename, 0); 

    st=ham_open_ex((ham_db_t *)jhandle, filename, (ham_u32_t)jflags, params);

    if (params)
        free(params);
    if (jfilename)
        (*jenv)->ReleaseStringUTFChars(jenv, jfilename, filename); 

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1get_1error(JNIEnv *jenv, jobject jobj, 
        jlong jhandle)
{
    return (ham_get_error((ham_db_t *)jhandle));
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ham_1set_1compare_1func(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jobject jcmp)
{
    /* jcmp==null: set default compare function */
    if (!jcmp) {
        ham_set_compare_func((ham_db_t *)jhandle, 0);
        return;
    }

    ham_set_compare_func((ham_db_t *)jhandle, jni_compare_func);
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ham_1set_1prefix_1compare_1func(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jobject jcmp)
{
    /* jcmp==null: disable prefix compare function */
    if (!jcmp) {
        ham_set_prefix_compare_func((ham_db_t *)jhandle, 0);
        return;
    }

    ham_set_prefix_compare_func((ham_db_t *)jhandle, jni_prefix_compare_func);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1enable_1compression(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jint jlevel, jint jflags)
{
    return (ham_enable_compression((ham_db_t *)jhandle, (ham_u32_t)jlevel,
                (ham_u32_t)jflags));
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_hamsterdb_Database_ham_1find(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jbyteArray jkey, jint jflags)
{
    ham_status_t st;
    ham_key_t hkey;
    ham_record_t hrec;
    jbyteArray jrec;

    SET_DB_CONTEXT((ham_db_t *)jhandle, jenv, jobj);

    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));

    hkey.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    hkey.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jkey);
    
    st=ham_find((ham_db_t *)jhandle, 0, &hkey, &hrec, (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);
    if (st)
        return (0);

    jrec =(*jenv)->NewByteArray(jenv, hrec.size);
    if (hrec.size)
        (*jenv)->SetByteArrayRegion(jenv, jrec, 0, hrec.size, 
                (jbyte *)hrec.data);

    return (jrec);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1insert(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jbyteArray jkey, jbyteArray jrecord, jint jflags)
{
    ham_status_t st;
    ham_key_t hkey;
    ham_record_t hrec;

    SET_DB_CONTEXT((ham_db_t *)jhandle, jenv, jobj);

    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));

    hkey.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    hkey.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jkey);
    hrec.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jrecord, 0);
    hrec.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jrecord);
    
    st=ham_insert((ham_db_t *)jhandle, 0, &hkey, &hrec, (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);
    (*jenv)->ReleaseByteArrayElements(jenv, jrecord, (jbyte *)hrec.data, 0);

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1erase(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jbyteArray jkey, jint jflags)
{
    ham_status_t st;
    ham_key_t hkey;

    SET_DB_CONTEXT((ham_db_t *)jhandle, jenv, jobj);

    memset(&hkey, 0, sizeof(hkey));

    hkey.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    hkey.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jkey);
    
    st=ham_erase((ham_db_t *)jhandle, 0, &hkey, (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1flush(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jint jflags)
{
    return (ham_flush((ham_db_t *)jhandle, (ham_u32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1close(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jint jflags)
{
    return (ham_close((ham_db_t *)jhandle, (ham_u32_t)jflags));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1create(JNIEnv *jenv, jobject jobj,
        jlong jdbhandle)
{
    ham_cursor_t *cursor;
    ham_status_t st;

    /* 
     * in case of an error, return 0; the java library will check for
     * 0 and return ham_get_error(db)
     */
    st=ham_cursor_create((ham_db_t *)jdbhandle, 0, 0, &cursor);
    if (st)
        return (0);
    return ((jlong)cursor);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1clone(JNIEnv *jenv, jobject jobj,
        jlong jhandle)
{
    ham_cursor_t *cursor;
    ham_status_t st;

    /* 
     * in case of an error, return 0; the java library will check for
     * 0 and return ham_get_error(db)
     */
    st=ham_cursor_clone((ham_cursor_t *)jhandle, &cursor);
    if (st)
        return (0);
    return ((jlong)cursor);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1move_1to(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jint jflags)
{
    jint st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    return ((jint)ham_cursor_move((ham_cursor_t *)jhandle, 0, 0, 
                (ham_u32_t)jflags));
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1get_1key(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jint jflags)
{
    ham_status_t st;
    ham_key_t key;
    jbyteArray ret;
    memset(&key, 0, sizeof(key));

    st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    st=ham_cursor_move((ham_cursor_t *)jhandle, &key, 0, (ham_u32_t)jflags);
    if (st)
        return (0);

    ret=(*jenv)->NewByteArray(jenv, key.size);
    if (key.size)
        (*jenv)->SetByteArrayRegion(jenv, ret, 0, key.size, (jbyte *)key.data);
    return (ret);
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1get_1record(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jint jflags)
{
    ham_status_t st;
    ham_record_t rec;
    jbyteArray ret;
    memset(&rec, 0, sizeof(rec));

    st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    st=ham_cursor_move((ham_cursor_t *)jhandle, 0, &rec, (ham_u32_t)jflags);
    if (st)
        return (0);

    ret=(*jenv)->NewByteArray(jenv, rec.size);
    if (rec.size)
        (*jenv)->SetByteArrayRegion(jenv, ret, 0, rec.size, (jbyte *)rec.data);
    return (ret);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1overwrite(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jbyteArray jrec, jint jflags)
{
    ham_status_t st;
    ham_record_t hrec;
    memset(&hrec, 0, sizeof(hrec));

    st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    hrec.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jrec, 0);
    hrec.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jrec);
    
    st=ham_cursor_overwrite((ham_cursor_t *)jhandle, &hrec, (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jrec, (jbyte *)hrec.data, 0);

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1find(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jbyteArray jkey, jint jflags)
{
    ham_status_t st;
    ham_key_t hkey;
    memset(&hkey, 0, sizeof(hkey));

    st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    hkey.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    hkey.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jkey);
    
    st=ham_cursor_find((ham_cursor_t *)jhandle, &hkey, (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1insert(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jbyteArray jkey, jbyteArray jrecord, jint jflags)
{
    ham_status_t st;
    ham_key_t hkey;
    ham_record_t hrec;
    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));

    st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    hkey.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    hkey.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jkey);
    hrec.data=(ham_u8_t *)(*jenv)->GetByteArrayElements(jenv, jrecord, 0);
    hrec.size=(ham_size_t)(*jenv)->GetArrayLength(jenv, jrecord);
    
    st=ham_cursor_insert((ham_cursor_t *)jhandle, &hkey, &hrec, 
            (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);
    (*jenv)->ReleaseByteArrayElements(jenv, jrecord, (jbyte *)hrec.data, 0);

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1erase(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jint jflags)
{
    jint st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    return ((jint)ham_cursor_erase((ham_cursor_t *)jhandle, (ham_u32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1get_1duplicate_1count(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jint jflags)
{
    ham_size_t count;
    ham_status_t st;

    st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    /* 
     * in case of an error, return 0; the java library will check for
     * 0 and return ham_get_error(db)
     */
    st=ham_cursor_get_duplicate_count((ham_cursor_t *)jhandle, &count, 
            (ham_u32_t)jflags);
    if (st)
        return (0);
    return ((jlong)count);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1close(JNIEnv *jenv, jobject jobj,
        jlong jhandle)
{
    jint st=jni_set_cursor_env(jenv, jobj, jhandle);
    if (st)
        return (st);

    return ((jint)ham_cursor_close((ham_cursor_t *)jhandle));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1new(JNIEnv *jenv, jobject jobj)
{
    ham_env_t *env;

    if (ham_env_new(&env))
        return (0);
    return ((jlong)env);
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1delete(JNIEnv *jenv, 
        jobject jobj, jlong jhandle)
{
    ham_env_delete((ham_env_t *)jhandle);
}

JNIEXPORT jint JNICALL 
Java_de_crupp_hamsterdb_Environment_ham_1env_1create_1ex(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jstring jfilename, 
        jint jflags, jint jmode, jobjectArray jparams)
{
    ham_status_t st;
    ham_parameter_t *params=0;
    const char* filename=0;

    if (jparams) {
        st=jparams_to_native(jenv, jparams, &params);
        if (st)
            return (st);
    }

    if (jfilename)
        filename=(*jenv)->GetStringUTFChars(jenv, jfilename, 0); 

    st=ham_env_create_ex((ham_env_t *)jhandle, filename, (ham_u32_t)jflags,
            (ham_u32_t)jmode, params);

    if (params)
        free(params);
    (*jenv)->ReleaseStringUTFChars(jenv, jfilename, filename); 

    return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1open_1ex(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jstring jfilename, 
        jint jflags, jobjectArray jparams)
{
    ham_status_t st;
    ham_parameter_t *params=0;
    const char* filename=0;

    if (jparams) {
        st=jparams_to_native(jenv, jparams, &params);
        if (st)
            return (st);
    }

    if (jfilename)
        filename=(*jenv)->GetStringUTFChars(jenv, jfilename, 0); 

    st=ham_env_open_ex((ham_env_t *)jhandle, filename, (ham_u32_t)jflags, 
            params);

    if (params)
        free(params);
    (*jenv)->ReleaseStringUTFChars(jenv, jfilename, filename); 

    return (st);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1create_1db(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jshort jname, jint jflags, 
        jobjectArray jparams)
{
    ham_status_t st;
    ham_parameter_t *params=0;
    ham_db_t *db;

    if (jparams) {
        st=jparams_to_native(jenv, jparams, &params);
        if (st)
            return (st);
    }

    if ((st=ham_new(&db))) {
        jni_throw_error(jenv, st);
        return (0);
    }

    st=ham_env_create_db((ham_env_t *)jhandle, db, (ham_u16_t)jname,
            (ham_u32_t)jflags, params);

    if (params)
        free(params);
    if (st) {
        ham_delete(db);
        jni_throw_error(jenv, st);
    }

    return ((jlong)db);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1open_1db(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jshort jname, jint jflags, 
        jobjectArray jparams)
{
    ham_status_t st;
    ham_parameter_t *params=0;
    ham_db_t *db;

    if (jparams) {
        st=jparams_to_native(jenv, jparams, &params);
        if (st)
            return (st);
    }

    if ((st=ham_new(&db))) {
        jni_throw_error(jenv, st);
        return (0);
    }

    st=ham_env_open_db((ham_env_t *)jhandle, db, (ham_u16_t)jname,
            (ham_u32_t)jflags, params);

    if (params)
        free(params);
    if (st) {
        ham_delete(db);
        jni_throw_error(jenv, st);
    }

    return ((jlong)db);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1rename_1db(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jshort joldname, 
        jshort jnewname, jint jflags)
{
    return ((jint)ham_env_rename_db((ham_env_t *)jhandle, (ham_u16_t)joldname,
                (ham_u16_t)jnewname, (ham_u32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1erase_1db(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jshort jname, jint jflags)
{
    return ((jint)ham_env_erase_db((ham_env_t *)jhandle, (ham_u16_t)jname,
                (ham_u32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1enable_1encryption(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jbyteArray jkey, jint jflags)
{
    ham_status_t st;
    jbyte *pkey=(*jenv)->GetByteArrayElements(jenv, jkey, 0);
    
    st=ham_env_enable_encryption((ham_env_t *)jhandle, 
            (ham_u8_t *)pkey, (ham_u32_t)jflags);

    (*jenv)->ReleaseByteArrayElements(jenv, jkey, pkey, 0);

    return (st);
}

JNIEXPORT jshortArray JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1get_1database_1names(JNIEnv *jenv,
       jobject jobj, jlong jhandle)
{
    ham_status_t st;
    jshortArray ret;
    ham_size_t num_dbs=128;
    ham_u16_t *dbs=0;
    
    while (1) {
        dbs=(ham_u16_t *)realloc(dbs, sizeof(ham_u16_t)*num_dbs);
        if (!dbs) {
            jni_throw_error(jenv, HAM_OUT_OF_MEMORY);
            return (0);
        }

        st=ham_env_get_database_names((ham_env_t *)jhandle, dbs, &num_dbs);

        /* buffer too small? */
        if (st==HAM_LIMITS_REACHED) {
            num_dbs*=2;
            continue;
        }
        if (st) {
            free(dbs);
            jni_throw_error(jenv, st);
            return (0);
        }
        break;
    }

    ret=(*jenv)->NewShortArray(jenv, (jint)num_dbs);
    (*jenv)->SetShortArrayRegion(jenv, ret, 0, num_dbs, (jshort *)dbs);
    free(dbs);
    return (ret);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ham_1env_1close(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jint jflags)
{
    return (ham_env_close((ham_env_t *)jhandle, (ham_u32_t)jflags));
}
