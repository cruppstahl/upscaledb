/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <ups/upscaledb_int.h>

#include "de_crupp_hamsterdb_Cursor.h"
#include "de_crupp_hamsterdb_DatabaseException.h"
#include "de_crupp_hamsterdb_Database.h"
#include "de_crupp_hamsterdb_Environment.h"

static JavaVM *javavm = 0;

#define jni_log(x) printf(x)

typedef struct jnipriv {
  JNIEnv *jenv;
  jobject jobj;
} jnipriv;

#define SET_DB_CONTEXT(db, jenv, jobj)                          \
      jnipriv p;                                                \
      p.jenv = jenv;                                            \
      p.jobj = jobj;                                            \
      ups_set_context_data(db, &p);

static jint
jni_set_cursor_env(jnipriv *p, JNIEnv *jenv, jobject jobj, jlong jhandle)
{
  jclass jcls;
  jfieldID jfid;
  jobject jdbobj;
  ups_cursor_t *c = (ups_cursor_t *)jhandle;

  /* get the callback method */
  jcls = (*jenv)->GetObjectClass(jenv, jobj);
  if (!jcls) {
    jni_log(("GetObjectClass failed\n"));
    return (UPS_INTERNAL_ERROR);
  }

  jfid = (*jenv)->GetFieldID(jenv, jcls, "m_db",
      "Lde/crupp/hamsterdb/Database;");
  if (!jfid) {
    jni_log(("GetFieldID failed\n"));
    return (UPS_INTERNAL_ERROR);
  }

  jdbobj = (*jenv)->GetObjectField(jenv, jobj, jfid);
  if (!jdbobj) {
    jni_log(("GetObjectField failed\n"));
    return (UPS_INTERNAL_ERROR);
  }

  p->jenv = jenv;
  p->jobj = jdbobj;
  ups_set_context_data(ups_cursor_get_database(c), p);

  return (0);
}

static void
jni_throw_error(JNIEnv *jenv, ups_status_t st)
{
  jmethodID ctor;
  jobject jobj;
  jclass jcls = (*jenv)->FindClass(jenv,"de/crupp/hamsterdb/DatabaseException");
  if (!jcls) {
    jni_log(("Cannot find class de.crupp.hamsterdb.DatabaseException\n"));
    return;
  }

  ctor = (*jenv)->GetMethodID(jenv, jcls, "<init>", "(I)V");
  if (!ctor) {
    jni_log(("Cannot find constructor of DatabaseException class\n"));
    return;
  }

  jobj = (*jenv)->NewObject(jenv, jcls, ctor, st);
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

  jcls = (*jenv)->FindClass(jenv, "de/crupp/hamsterdb/Database");
  if (!jcls) {
    jni_log(("unable to find class de/crupp/hamsterdb/Database\n"));
    return;
  }

  jfid = (*jenv)->GetStaticFieldID(jenv, jcls, "m_eh",
      "Lde/crupp/hamsterdb/ErrorHandler;");
  if (!jfid) {
    jni_log(("unable to find ErrorHandler field\n"));
    return;
  }

  jobj = (*jenv)->GetStaticObjectField(jenv, jcls, jfid);
  if (!jobj) {
    jni_log(("unable to get ErrorHandler object\n"));
    return;
  }

  jcls = (*jenv)->GetObjectClass(jenv, jobj);
  if (!jcls) {
    jni_log(("unable to get ErrorHandler class\n"));
    return;
  }

  jmid = (*jenv)->GetMethodID(jenv, jcls, "handleMessage",
      "(ILjava/lang/String;)V");
  if (!jmid) {
    jni_log(("unable to get handleMessage method\n"));
    return;
  }

  str = (*jenv)->NewStringUTF(jenv, message);
  if (!str) {
    jni_log(("unable to create new Java string\n"));
    return;
  }

  /* call the java method */
  (*jenv)->CallNonvirtualVoidMethod(jenv, jobj, jcls,
      jmid, (jint)level, str);
}

static int
jni_compare_func(ups_db_t *db,
        const uint8_t *lhs, uint32_t lhs_length,
        const uint8_t *rhs, uint32_t rhs_length)
{
  jobject jcmpobj;
  jclass jcls, jcmpcls;
  jfieldID jfid;
  jmethodID jmid;
  jbyteArray jlhs, jrhs;

  /* get the Java Environment and the Database instance */
  jnipriv *p = (jnipriv *)ups_get_context_data(db, UPS_TRUE);

  /* get the callback method */
  jcls = (*p->jenv)->GetObjectClass(p->jenv, p->jobj);
  if (!jcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jfid = (*p->jenv)->GetFieldID(p->jenv, jcls, "m_cmp",
      "Lde/crupp/hamsterdb/CompareCallback;");
  if (!jfid) {
    jni_log(("GetFieldID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jcmpobj = (*p->jenv)->GetObjectField(p->jenv, p->jobj, jfid);
  if (!jcmpobj) {
    jni_log(("GetObjectFieldID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jcmpcls = (*p->jenv)->GetObjectClass(p->jenv, jcmpobj);
  if (!jcmpcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jmid = (*p->jenv)->GetMethodID(p->jenv, jcmpcls, "compare",
      "([B[B)I");
  if (!jmid) {
    jni_log(("GetMethodID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  /* prepare the parameters */
  jlhs = (*p->jenv)->NewByteArray(p->jenv, lhs_length);
  if (!jlhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (lhs_length)
    (*p->jenv)->SetByteArrayRegion(p->jenv, jlhs, 0, lhs_length,
        (jbyte *)lhs);

  jrhs = (*p->jenv)->NewByteArray(p->jenv, rhs_length);
  if (!jrhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (rhs_length)
    (*p->jenv)->SetByteArrayRegion(p->jenv, jrhs, 0, rhs_length,
        (jbyte *)rhs);

  return (*p->jenv)->CallIntMethod(p->jenv, jcmpobj, jmid, jlhs, jrhs);
}

#if 0 /* unused */
static int
jni_duplicate_compare_func(ups_db_t *db,
        const uint8_t *lhs, uint32_t lhs_length,
        const uint8_t *rhs, uint32_t rhs_length)
{
  jobject jcmpobj;
  jclass jcls, jcmpcls;
  jfieldID jfid;
  jmethodID jmid;
  jbyteArray jlhs, jrhs;

  /* get the Java Environment and the Database instance */
  jnipriv *p = (jnipriv *)ups_get_context_data(db);

  /* get the callback method */
  jcls = (*p->jenv)->GetObjectClass(p->jenv, p->jobj);
  if (!jcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jfid = (*p->jenv)->GetFieldID(p->jenv, jcls, "m_dupe_cmp",
      "Lde/crupp/hamsterdb/DuplicateCompareCallback;");
  if (!jfid) {
    jni_log(("GetFieldID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jcmpobj = (*p->jenv)->GetObjectField(p->jenv, p->jobj, jfid);
  if (!jcmpobj) {
    jni_log(("GetObjectFieldID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jcmpcls = (*p->jenv)->GetObjectClass(p->jenv, jcmpobj);
  if (!jcmpcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jmid = (*p->jenv)->GetMethodID(p->jenv, jcmpcls, "compare",
      "([B[B)I");
  if (!jmid) {
    jni_log(("GetMethodID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  /* prepare the parameters */
  jlhs = (*p->jenv)->NewByteArray(p->jenv, lhs_length);
  if (!jlhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (lhs_length)
    (*p->jenv)->SetByteArrayRegion(p->jenv, jlhs, 0, lhs_length,
        (jbyte *)lhs);

  jrhs = (*p->jenv)->NewByteArray(p->jenv, rhs_length);
  if (!jrhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (rhs_length)
    (*p->jenv)->SetByteArrayRegion(p->jenv, jrhs, 0, rhs_length,
        (jbyte *)rhs);

  return (*p->jenv)->CallIntMethod(p->jenv, jcmpobj, jmid, jlhs, jrhs);
}
#endif

static ups_status_t
jparams_to_native(JNIEnv *jenv, jobjectArray jparams, ups_parameter_t **pparams)
{
  unsigned int i, j = 0, size = (*jenv)->GetArrayLength(jenv, jparams);
  ups_parameter_t *params = (ups_parameter_t *)malloc(sizeof(*params) * (size + 1));
  if (!params)
    return (UPS_OUT_OF_MEMORY);

  for (i = 0; i < size; i++) {
    jobject jobj = (*jenv)->GetObjectArrayElement(jenv, jparams, i);
    if (jobj) {
      jfieldID fidname, fidvalue;
      jclass jcls = (*jenv)->GetObjectClass(jenv, jobj);
      if (!jcls) {
        free(params);
        jni_log(("GetObjectClass failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      fidname = (*jenv)->GetFieldID(jenv, jcls, "name", "I");
      if (!fidname) {
        free(params);
        jni_log(("GetFieldID failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      fidvalue = (*jenv)->GetFieldID(jenv, jcls, "value", "J");
      if (!fidvalue) {
        free(params);
        jni_log(("GetFieldID failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      params[j].name = (uint32_t)(*jenv)->GetLongField(jenv,
          jobj, fidname);
      params[j].value = (uint64_t)(*jenv)->GetLongField(jenv,
          jobj, fidvalue);
      j++;
    }
  }
  params[j].name = 0;
  params[j].value = 0;

  *pparams = params;

  return (0);
}

static ups_status_t
jparams_from_native(JNIEnv *jenv, ups_parameter_t *params, jobjectArray jparams)
{
  unsigned int i, j = 0, size = (*jenv)->GetArrayLength(jenv, jparams);

  for (i = 0; i < size; i++) {
    jobject jobj = (*jenv)->GetObjectArrayElement(jenv, jparams, i);
    if (jobj) {
      jfieldID fidname, fidvalue;
      jclass jcls = (*jenv)->GetObjectClass(jenv, jobj);
      if (!jcls) {
        jni_log(("GetObjectClass failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      fidname = (*jenv)->GetFieldID(jenv, jcls, "name", "I");
      if (!fidname) {
        jni_log(("GetFieldID failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      /* some values have to be stored in a string, not a long; so far
       * this is only the case for UPS_PARAM_FILENAME. */
      if (params[j].name == UPS_PARAM_FILENAME) {
        jstring str;
        fidvalue = (*jenv)->GetFieldID(jenv, jcls, "stringValue",
                  "Ljava/lang/String;");
        if (!fidvalue) {
          jni_log(("GetFieldID (stringValue) failed\n"));
          return (UPS_INTERNAL_ERROR);
        }
        assert(params[j].name == (uint32_t)(*jenv)->GetLongField(jenv,
            jobj, fidname));
        str = (*jenv)->NewStringUTF(jenv, (const char *)params[j].value);
        if (!str) {
          jni_log(("unable to create new Java string\n"));
          return (UPS_INTERNAL_ERROR);
        }
        (*jenv)->SetObjectField(jenv, jobj, fidvalue, str);
      }
      else {
        fidvalue = (*jenv)->GetFieldID(jenv, jcls, "value", "J");
        if (!fidvalue) {
          jni_log(("GetFieldID (value) failed\n"));
          return (UPS_INTERNAL_ERROR);
        }
        assert(params[j].name == (uint32_t)(*jenv)->GetLongField(jenv,
            jobj, fidname));
        (*jenv)->SetLongField(jenv, jobj, fidvalue,
            (jlong)params[j].value);
      }
      j++;
    }
  }

  return (0);
}

JNIEXPORT jstring JNICALL
Java_de_crupp_hamsterdb_DatabaseException_ups_1strerror(JNIEnv *jenv,
    jobject jobj, jint jerrno)
{
  return (*jenv)->NewStringUTF(jenv, ups_strerror((ups_status_t)jerrno));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ups_1get_1version(JNIEnv *jenv, jclass jcls,
    jint which)
{
  uint32_t v;

  if (which == 0)
    ups_get_version(&v, 0, 0);
  else if (which == 1)
    ups_get_version(0, &v, 0);
  else
    ups_get_version(0, 0, &v);

  return ((jint)v);
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ups_1set_1errhandler(JNIEnv *jenv,
    jclass jcls, jobject jeh)
{
  if (!jeh) {
    ups_set_errhandler(0);
    return;
  }

  /* set global javavm pointer, if needed */
  if (!javavm) {
    if ((*jenv)->GetJavaVM(jenv, &javavm) != 0) {
      jni_log(("Cannot get Java VM\n"));
      return;
    }
  }

  ups_set_errhandler(jni_errhandler);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1get_1error(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  return (ups_db_get_error((ups_db_t *)jhandle));
}

JNIEXPORT void JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1set_1compare_1func(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jobject jcmp)
{
  /* jcmp==null: set default compare function */
  if (!jcmp) {
    ups_db_set_compare_func((ups_db_t *)jhandle, 0);
    return;
  }

  ups_db_set_compare_func((ups_db_t *)jhandle, jni_compare_func);
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1find(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jlong jtxnhandle, jbyteArray jkey, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  ups_record_t hrec;
  jbyteArray jrec;

  SET_DB_CONTEXT((ups_db_t *)jhandle, jenv, jobj);

  memset(&hkey, 0, sizeof(hkey));
  memset(&hrec, 0, sizeof(hrec));

  hkey.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
  hkey.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jkey);

  st = ups_db_find((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
            &hkey, &hrec, (uint32_t)jflags);

  (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);
  if (st)
    return (0);

  jrec = (*jenv)->NewByteArray(jenv, hrec.size);
  if (hrec.size)
    (*jenv)->SetByteArrayRegion(jenv, jrec, 0, hrec.size,
        (jbyte *)hrec.data);

  return (jrec);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1insert(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jlong jtxnhandle, jbyteArray jkey,
    jbyteArray jrecord, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  ups_record_t hrec;

  SET_DB_CONTEXT((ups_db_t *)jhandle, jenv, jobj);

  memset(&hkey, 0, sizeof(hkey));
  memset(&hrec, 0, sizeof(hrec));

  hkey.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
  hkey.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jkey);
  hrec.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jrecord, 0);
  hrec.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jrecord);

  st = ups_db_insert((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
            &hkey, &hrec, (uint32_t)jflags);

  (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);
  (*jenv)->ReleaseByteArrayElements(jenv, jrecord, (jbyte *)hrec.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1erase(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jlong jtxnhandle, jbyteArray jkey, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;

  SET_DB_CONTEXT((ups_db_t *)jhandle, jenv, jobj);

  memset(&hkey, 0, sizeof(hkey));

  hkey.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
  hkey.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jkey);

  st = ups_db_erase((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
            &hkey, (uint32_t)jflags);

  (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1close(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jint jflags)
{
  return (ups_db_close((ups_db_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1create(JNIEnv *jenv, jobject jobj,
    jlong jdbhandle, jlong jtxnhandle)
{
  ups_cursor_t *cursor;
  ups_status_t st;

  /*
   * in case of an error, return 0; the java library will check for
   * 0 and return ups_get_error(db)
   */
  st = ups_cursor_create(&cursor, (ups_db_t *)jdbhandle,
            (ups_txn_t *)jtxnhandle, 0);
  if (st)
    return (0);
  return ((jlong)cursor);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1clone(JNIEnv *jenv, jobject jobj,
    jlong jhandle)
{
  ups_cursor_t *cursor;
  ups_status_t st;

  /*
   * in case of an error, return 0; the java library will check for
   * 0 and return ups_get_error(db)
   */
  st = ups_cursor_clone((ups_cursor_t *)jhandle, &cursor);
  if (st)
    return (0);
  return ((jlong)cursor);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1move_1to(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  jnipriv p;
  jint st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  return ((jint)ups_cursor_move((ups_cursor_t *)jhandle, 0, 0,
        (uint32_t)jflags));
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1get_1key(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  ups_status_t st;
  ups_key_t key;
  jbyteArray ret;
  jnipriv p;
  memset(&key, 0, sizeof(key));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (0);

  st = ups_cursor_move((ups_cursor_t *)jhandle, &key, 0, (uint32_t)jflags);
  if (st)
    return (0);

  ret = (*jenv)->NewByteArray(jenv, key.size);
  if (key.size)
    (*jenv)->SetByteArrayRegion(jenv, ret, 0, key.size, (jbyte *)key.data);
  return (ret);
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1get_1record(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  ups_status_t st;
  ups_record_t rec;
  jbyteArray ret;
  jnipriv p;
  memset(&rec, 0, sizeof(rec));

  st=jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (0);

  st=ups_cursor_move((ups_cursor_t *)jhandle, 0, &rec, (uint32_t)jflags);
  if (st)
    return (0);

  ret = (*jenv)->NewByteArray(jenv, rec.size);
  if (rec.size)
    (*jenv)->SetByteArrayRegion(jenv, ret, 0, rec.size, (jbyte *)rec.data);
  return (ret);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1overwrite(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jbyteArray jrec, jint jflags)
{
  ups_status_t st;
  ups_record_t hrec;
  jnipriv p;
  memset(&hrec, 0, sizeof(hrec));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  hrec.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jrec, 0);
  hrec.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jrec);

  st=ups_cursor_overwrite((ups_cursor_t *)jhandle, &hrec, (uint32_t)jflags);

  (*jenv)->ReleaseByteArrayElements(jenv, jrec, (jbyte *)hrec.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1find(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jbyteArray jkey, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  jnipriv p;
  memset(&hkey, 0, sizeof(hkey));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  hkey.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
  hkey.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jkey);

  st = ups_cursor_find((ups_cursor_t *)jhandle, &hkey, 0, (uint32_t)jflags);

  (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1insert(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jbyteArray jkey, jbyteArray jrecord, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  ups_record_t hrec;
  jnipriv p;
  memset(&hkey, 0, sizeof(hkey));
  memset(&hrec, 0, sizeof(hrec));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  hkey.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jkey, 0);
  hkey.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jkey);
  hrec.data = (uint8_t *)(*jenv)->GetByteArrayElements(jenv, jrecord, 0);
  hrec.size = (uint32_t)(*jenv)->GetArrayLength(jenv, jrecord);

  st = ups_cursor_insert((ups_cursor_t *)jhandle, &hkey, &hrec,
                (uint32_t)jflags);

  (*jenv)->ReleaseByteArrayElements(jenv, jkey, (jbyte *)hkey.data, 0);
  (*jenv)->ReleaseByteArrayElements(jenv, jrecord, (jbyte *)hrec.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1erase(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jint jflags)
{
  jnipriv p;
  jint st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  return ((jint)ups_cursor_erase((ups_cursor_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1get_1duplicate_1count(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  uint32_t count;
  ups_status_t st;
  jnipriv p;

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  /*
   * in case of an error, return 0; the java library will check for
   * 0 and return ups_get_error(db)
   */
  st = ups_cursor_get_duplicate_count((ups_cursor_t *)jhandle, &count,
      (uint32_t)jflags);
  if (st)
    return (0);
  return ((jint)count);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1get_1record_1size
    (JNIEnv *jenv, jobject jobj, jlong jhandle)
{
  uint64_t size;
  ups_status_t st;
  jnipriv p;

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  /*
   * in case of an error, return 0; the java library will check for
   * 0 and return ups_get_error(db)
   */
  st = ups_cursor_get_record_size((ups_cursor_t *)jhandle, &size);
  if (st)
    return (0);
  return ((jlong)size);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Cursor_ups_1cursor_1close(JNIEnv *jenv, jobject jobj,
    jlong jhandle)
{
  jnipriv p;
  jint st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  return ((jint)ups_cursor_close((ups_cursor_t *)jhandle));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1create(JNIEnv *jenv,
    jobject jobj, jstring jfilename, jint jflags, jint jmode,
    jobjectArray jparams)
{
  ups_status_t st;
  ups_parameter_t *params = 0;
  const char* filename = 0;
  ups_env_t *env;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);
  }

  if (jfilename)
    filename = (*jenv)->GetStringUTFChars(jenv, jfilename, 0);

  st = ups_env_create(&env, filename, (uint32_t)jflags,
                (uint32_t)jmode, params);

  if (params)
    free(params);
  (*jenv)->ReleaseStringUTFChars(jenv, jfilename, filename);

  if (st) {
    jni_throw_error(jenv, st);
    return 0;
  }
  return ((jlong)env);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1open(JNIEnv *jenv,
    jobject jobj, jstring jfilename,
    jint jflags, jobjectArray jparams)
{
  ups_status_t st;
  ups_parameter_t *params = 0;
  const char* filename = 0;
  ups_env_t *env;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);
  }

  if (jfilename)
    filename = (*jenv)->GetStringUTFChars(jenv, jfilename, 0);

  st = ups_env_open(&env, filename, (uint32_t)jflags, params);

  if (params)
    free(params);
  (*jenv)->ReleaseStringUTFChars(jenv, jfilename, filename);

  if (st) {
    jni_throw_error(jenv, st);
    return 0;
  }
  return ((jlong)env);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1create_1db(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jshort jname, jint jflags,
    jobjectArray jparams)
{
  ups_status_t st;
  ups_parameter_t *params = 0;
  ups_db_t *db;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);
  }

  st = ups_env_create_db((ups_env_t *)jhandle, &db, (uint16_t)jname,
                    (uint32_t)jflags, params);

  if (params)
    free(params);
  if (st) {
    jni_throw_error(jenv, st);
    return 0;
  }

  return ((jlong)db);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1open_1db(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jshort jname, jint jflags,
    jobjectArray jparams)
{
  ups_status_t st;
  ups_parameter_t *params = 0;
  ups_db_t *db;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);
  }

  st = ups_env_open_db((ups_env_t *)jhandle, &db, (uint16_t)jname,
                    (uint32_t)jflags, params);

  if (params)
    free(params);
  if (st) {
    jni_throw_error(jenv, st);
    return 0;
  }

  return ((jlong)db);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1rename_1db(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jshort joldname,
    jshort jnewname, jint jflags)
{
  return ((jint)ups_env_rename_db((ups_env_t *)jhandle, (uint16_t)joldname,
        (uint16_t)jnewname, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1erase_1db(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jshort jname, jint jflags)
{
  return ((jint)ups_env_erase_db((ups_env_t *)jhandle, (uint16_t)jname,
        (uint32_t)jflags));
}

JNIEXPORT jshortArray JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1get_1database_1names(JNIEnv *jenv,
     jobject jobj, jlong jhandle)
{
  ups_status_t st;
  jshortArray ret;
  uint32_t num_dbs = 128;
  uint16_t *dbs = 0;

  while (1) {
    dbs = (uint16_t *)realloc(dbs, sizeof(uint16_t) * num_dbs);
    if (!dbs) {
      jni_throw_error(jenv, UPS_OUT_OF_MEMORY);
      return (0);
    }

    st = ups_env_get_database_names((ups_env_t *)jhandle, dbs, &num_dbs);

    /* buffer too small? */
    if (st == UPS_LIMITS_REACHED) {
      num_dbs *= 2;
      continue;
    }
    if (st) {
      free(dbs);
      jni_throw_error(jenv, st);
      return (0);
    }
    break;
  }

  ret = (*jenv)->NewShortArray(jenv, (jint)num_dbs);
  (*jenv)->SetShortArrayRegion(jenv, ret, 0, num_dbs, (jshort *)dbs);
  free(dbs);
  return (ret);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1close(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  return (ups_env_close((ups_env_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1txn_1begin
    (JNIEnv *jenv, jobject jobj, jlong jhandle, jint jflags)
{
  ups_txn_t *txn;
  ups_status_t st = ups_txn_begin(&txn, (ups_env_t *)jhandle, 0, 0,
            (uint32_t)jflags);
  if (st) {
    jni_throw_error(jenv, st);
    return ((jlong)0);
  }
  return ((jlong)txn);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Transaction_ups_1txn_1commit(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  return (ups_txn_commit((ups_txn_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Transaction_ups_1txn_1abort(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  return (ups_txn_abort((ups_txn_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1get_1parameters(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jobjectArray jparams)
{
  ups_status_t st = 0;
  ups_parameter_t *params = 0;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);

    st = ups_db_get_parameters((ups_db_t *)jhandle, params);
    if (st) {
      free(params);
      return (st);
    }
    st = jparams_from_native(jenv, params, jparams);

    free(params);
  }
  return (st);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_hamsterdb_Database_ups_1db_1get_1key_1count(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jlong jtxnhandle, jint jflags)
{
  ups_status_t st;
  uint64_t keycount;
  st = ups_db_get_key_count((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
        (uint32_t)jflags, &keycount);
  if (st) {
    jni_throw_error(jenv, st);
    return (-1);
  }

  return ((jlong)keycount);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1get_1parameters(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jobjectArray jparams)
{
  ups_status_t st = 0;
  ups_parameter_t *params = 0;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);

    st = ups_env_get_parameters((ups_env_t *)jhandle, params);
    if (st) {
      free(params);
      return (st);
    }
    st = jparams_from_native(jenv, params, jparams);

    free(params);
  }
  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_hamsterdb_Environment_ups_1env_1flush(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  return (ups_env_flush((ups_env_t *)jhandle, (uint32_t)0));
}
