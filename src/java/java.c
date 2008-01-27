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

#include <ham/hamsterdb.h>

#include "de_crupp_hamsterdb_Error.h"

/* the java error handler */
static jobject g_jobj_eh=0;
static JNIEnv *g_jenv_eh=0;

/* the environent and class of the current call (needed for compare
 * callbacks)
 */
static jobject g_jobj_db=0;
static JNIEnv *g_jenv_db=0;
#define PREPARE_DB_ENV                  g_jenv_db=jenv; g_jobj_db=jobj

static void
jni_throw_error(JNIEnv *jenv, ham_status_t st)
{
    jclass jcls=(*jenv)->FindClass(jenv, "de/crupp/hamsterdb/Error");
    jmethodID ctor=(*jenv)->GetMethodID(jenv, jcls, "", "(I)V");
    jobject jobj=(*jenv)->NewObject(jenv, jcls, ctor, st);
    (*jenv)->Throw(jenv, jobj);
}

static JavaVM *javavm;

static void
jni_errhandler(int level, const char *message)
{
    jclass jcls;
    jmethodID jmid;
    JNIEnv *jenv;

    if ((*javavm)->AttachCurrentThread(javavm, (void **)&jenv, 0) != 0) {
        printf("JNI error: AttachCurrentThread failed\n");
        return;
    }

    printf("local env: 0x%llx\n", (unsigned long long)jenv);
    printf("local obj: 0x%llx\n", (unsigned long long)g_jobj_eh);
    
printf("%d\n", __LINE__);

    /* get callback method */
    jcls=(*jenv)->GetObjectClass(jenv, g_jobj_eh);
printf("%d - jcls is 0x%llx\n", __LINE__, (unsigned long long)jcls);
    jmid=(*jenv)->GetMethodID(jenv, jcls, "handleMessage",
            "(ILjava/lang/String;)V");
printf("%d\n", __LINE__);

    /* call the java method */
    (*jenv)->CallNonvirtualVoidMethod(jenv, g_jobj_eh, jcls,
            jmid, (jint)level, (*jenv)->NewStringUTF(jenv, message));
printf("%d\n", __LINE__);

    /* clean up  - not needed??
    (*jenv)->ReleaseStringUTFChars(jenv, jstr, message); */
}

static int
jni_compare_func(const ham_u8_t *lhs, ham_size_t lhs_length, 
        const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int ret;

    jbyteArray jlhs, jrhs;

    /* get the callback method */
    jclass jcls   =(*g_jenv_db)->GetObjectClass(g_jenv_db, g_jobj_db);
    jfieldID fid  =(*g_jenv_db)->GetFieldID(g_jenv_db, jcls, "m_cmp", 
            "Lde/crupp/hamsterdb/Comparable;");
    jobject jcmp  =(*g_jenv_db)->GetObjectField(g_jenv_db, g_jobj_db, fid);
    jclass jcmpcls=(*g_jenv_db)->GetObjectClass(g_jenv_db, jcmp);
    jmethodID jmid=(*g_jenv_db)->GetMethodID(g_jenv_db, jcmpcls, "comparable",
            "([B[B)I");

    /* prepare the parameters */
    jlhs=(*g_jenv_db)->NewByteArray(g_jenv_db, lhs_length);
    if (lhs_length)
        (*g_jenv_db)->SetByteArrayRegion(g_jenv_db, jlhs, 0, lhs_length,
                (jbyte *)lhs);
    jrhs=(*g_jenv_db)->NewByteArray(g_jenv_db, rhs_length);
    if (rhs_length)
        (*g_jenv_db)->SetByteArrayRegion(g_jenv_db, jrhs, 0, rhs_length,
                (jbyte *)rhs);

    ret=(*g_jenv_db)->CallIntMethod(g_jenv_db, jcmp, jmid, jlhs, jrhs);

    (*g_jenv_db)->ReleaseByteArrayElements(g_jenv_db, jlhs, 
            lhs_length ? (jbyte *)lhs : 0, 0);
    (*g_jenv_db)->ReleaseByteArrayElements(g_jenv_db, jrhs, 
            rhs_length ? (jbyte *)rhs : 0, 0);

    return (ret);
}

static int
jni_prefix_compare_func(const ham_u8_t *lhs, ham_size_t lhs_length, 
        ham_size_t lhs_real_length, const ham_u8_t *rhs, ham_size_t rhs_length,
        ham_size_t rhs_real_length)
{
    int ret;

    jbyteArray jlhs, jrhs;

    /* get the callback method */
    jclass jcls   =(*g_jenv_db)->GetObjectClass(g_jenv_db, g_jobj_db);
    jfieldID fid  =(*g_jenv_db)->GetFieldID(g_jenv_db, jcls, "m_cmp", 
            "Lde/crupp/hamsterdb/Comparable;");
    jobject jcmp  =(*g_jenv_db)->GetObjectField(g_jenv_db, g_jobj_db, fid);
    jclass jcmpcls=(*g_jenv_db)->GetObjectClass(g_jenv_db, jcmp);
    jmethodID jmid=(*g_jenv_db)->GetMethodID(g_jenv_db, jcmpcls, "comparable",
            "([B[B)I");

    /* prepare the parameters */
    jlhs=(*g_jenv_db)->NewByteArray(g_jenv_db, lhs_length);
    if (lhs_length)
        (*g_jenv_db)->SetByteArrayRegion(g_jenv_db, jlhs, 0, lhs_length,
                (jbyte *)lhs);
    jrhs=(*g_jenv_db)->NewByteArray(g_jenv_db, rhs_length);
    if (rhs_length)
        (*g_jenv_db)->SetByteArrayRegion(g_jenv_db, jrhs, 0, rhs_length,
                (jbyte *)rhs);

    ret=(*g_jenv_db)->CallIntMethod(g_jenv_db, jcmp, jmid, jlhs, 
            (jint)lhs_real_length, jrhs, (jint)rhs_real_length);

    (*g_jenv_db)->ReleaseByteArrayElements(g_jenv_db, jlhs, 
            lhs_length ? (jbyte *)lhs : 0, 0);
    (*g_jenv_db)->ReleaseByteArrayElements(g_jenv_db, jrhs, 
            rhs_length ? (jbyte *)rhs : 0, 0);

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
    PREPARE_DB_ENV;

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

    g_jobj_eh=jeh;
    g_jenv_eh=jenv;
    printf("set_errhandler env=0x%llx, obj=0x%llx\n",
            (unsigned long long)jenv, (unsigned long long)jeh);

    if ((*jenv)->GetJavaVM(jenv, &javavm) != 0) {
        printf("Cannot get Java VM\n");
        return;
    }

    ham_set_errhandler(jni_errhandler);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Database_ham_1new(JNIEnv *jenv, jobject jobj)
{
    ham_db_t *db;

    PREPARE_DB_ENV;

    if (ham_new(&db))
        return (0);
    return ((jlong)db);
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ham_1delete(JNIEnv *jenv, jobject jobj,
        jlong jhandle)
{
    PREPARE_DB_ENV;

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

    PREPARE_DB_ENV;

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

    PREPARE_DB_ENV;

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
    PREPARE_DB_ENV;

    return (ham_get_error((ham_db_t *)jhandle));
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ham_1set_1compare_1func(JNIEnv *jenv, 
        jobject jobj, jlong jhandle, jobject jcmp)
{
    PREPARE_DB_ENV;

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
    PREPARE_DB_ENV;

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
    PREPARE_DB_ENV;

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
    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));

    PREPARE_DB_ENV;

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
    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));

    PREPARE_DB_ENV;

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
    memset(&hkey, 0, sizeof(hkey));

    PREPARE_DB_ENV;

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
    PREPARE_DB_ENV;

    return (ham_flush((ham_db_t *)jhandle, (ham_u32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ham_1close(JNIEnv *jenv, jobject jobj,
        jlong jhandle, jint jflags)
{
    PREPARE_DB_ENV;

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

    PREPARE_DB_ENV;

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

    PREPARE_DB_ENV;

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

    PREPARE_DB_ENV;

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
    return ((jint)ham_cursor_erase((ham_cursor_t *)jhandle, (ham_u32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ham_1cursor_1get_1duplicate_1count(JNIEnv *jenv,
        jobject jobj, jlong jhandle, jint jflags)
{
    ham_size_t count;
    ham_status_t st;

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
