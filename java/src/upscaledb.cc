/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <map>
#include <vector>

#include <ups/upscaledb_int.h>
#include <ups/upscaledb_uqi.h>

#include "de_crupp_upscaledb_Cursor.h"
#include "de_crupp_upscaledb_DatabaseException.h"
#include "de_crupp_upscaledb_Database.h"
#include "de_crupp_upscaledb_Environment.h"

extern "C" {

static JavaVM *g_javavm = 0;
static std::map<uint32_t, jobject> g_callbacks;

#define jni_log(x) printf(x)

typedef struct jnipriv {
  JNIEnv *jenv;
  jobject jobj;
  jobject jcmp;
} jnipriv;

#define SET_DB_CONTEXT(db, jenv, jobj)                          \
      jnipriv p;                                                \
      p.jenv = jenv;                                            \
      p.jobj = jobj;                                            \
      p.jcmp = 0;                                               \
      ups_set_context_data(db, &p);

static jint
jni_set_cursor_env(jnipriv *p, JNIEnv *jenv, jobject jobj, jlong jhandle)
{
  jclass jcls;
  jfieldID jfid;
  jobject jdbobj;
  ups_cursor_t *c = (ups_cursor_t *)jhandle;

  /* get the callback method */
  jcls = jenv->GetObjectClass(jobj);
  if (!jcls) {
    jni_log(("GetObjectClass failed\n"));
    return (UPS_INTERNAL_ERROR);
  }

  jfid = jenv->GetFieldID(jcls, "m_db",
      "Lde/crupp/upscaledb/Database;");
  if (!jfid) {
    jni_log(("GetFieldID failed\n"));
    return (UPS_INTERNAL_ERROR);
  }

  jdbobj = jenv->GetObjectField(jobj, jfid);
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
  jclass jcls = jenv->FindClass("de/crupp/upscaledb/DatabaseException");
  if (!jcls) {
    jni_log(("Cannot find class de.crupp.upscaledb.DatabaseException\n"));
    return;
  }

  ctor = jenv->GetMethodID(jcls, "<init>", "(I)V");
  if (!ctor) {
    jni_log(("Cannot find constructor of DatabaseException class\n"));
    return;
  }

  jobj = jenv->NewObject(jcls, ctor, st);
  if (!jobj) {
    jni_log(("Cannot create new Exception\n"));
    return;
  }

  jenv->Throw((jthrowable)jobj);
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

  if (g_javavm->AttachCurrentThread((void **)&jenv, 0) != 0) {
    jni_log(("AttachCurrentThread failed\n"));
    return;
  }

  jcls = jenv->FindClass("de/crupp/upscaledb/Database");
  if (!jcls) {
    jni_log(("unable to find class de/crupp/upscaledb/Database\n"));
    return;
  }

  jfid = jenv->GetStaticFieldID(jcls, "m_eh",
      "Lde/crupp/upscaledb/ErrorHandler;");
  if (!jfid) {
    jni_log(("unable to find ErrorHandler field\n"));
    return;
  }

  jobj = jenv->GetStaticObjectField(jcls, jfid);
  if (!jobj) {
    jni_log(("unable to get ErrorHandler object\n"));
    return;
  }

  jcls = jenv->GetObjectClass(jobj);
  if (!jcls) {
    jni_log(("unable to get ErrorHandler class\n"));
    return;
  }

  jmid = jenv->GetMethodID(jcls, "handleMessage", "(ILjava/lang/String;)V");
  if (!jmid) {
    jni_log(("unable to get handleMessage method\n"));
    return;
  }

  str = jenv->NewStringUTF(message);
  if (!str) {
    jni_log(("unable to create new Java string\n"));
    return;
  }

  /* call the java method */
  jenv->CallNonvirtualVoidMethod(jobj, jcls, jmid, (jint)level, str);
}

static int
jni_compare_func(ups_db_t *db,
        const uint8_t *lhs, uint32_t lhs_length,
        const uint8_t *rhs, uint32_t rhs_length)
{
  /* get the Java Environment and the Database instance */
  jnipriv *p = (jnipriv *)ups_get_context_data(db, UPS_TRUE);

  /* get the callback method from the database object */
  jclass jcls = p->jenv->GetObjectClass(p->jobj);
  if (!jcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jfieldID jfid = p->jenv->GetFieldID(jcls, "m_cmp",
      "Lde/crupp/upscaledb/CompareCallback;");
  if (!jfid) {
    jni_log(("GetFieldID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jobject jcmpobj = p->jenv->GetObjectField(p->jobj, jfid);
  if (!jcmpobj) {
    jni_log(("GetObjectField failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jclass jcmpcls = p->jenv->GetObjectClass(jcmpobj);
  if (!jcmpcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jmethodID jmid = p->jenv->GetMethodID(jcmpcls, "compare", "([B[B)I");
  if (!jmid) {
    jni_log(("GetMethodID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  /* prepare the parameters */
  jbyteArray jlhs = p->jenv->NewByteArray(lhs_length);
  if (!jlhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (lhs_length)
    p->jenv->SetByteArrayRegion(jlhs, 0, lhs_length, (jbyte *)lhs);

  jbyteArray jrhs = p->jenv->NewByteArray(rhs_length);
  if (!jrhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (rhs_length)
    p->jenv->SetByteArrayRegion(jrhs, 0, rhs_length, (jbyte *)rhs);

  return (p->jenv->CallIntMethod(jcmpobj, jmid, jlhs, jrhs));
}

static int
jni_compare_func2(ups_db_t *db,
        const uint8_t *lhs, uint32_t lhs_length,
        const uint8_t *rhs, uint32_t rhs_length)
{
  /* get the Java Environment and the Database instance */
  jnipriv *p = (jnipriv *)ups_get_context_data(db, UPS_TRUE);

  jobject jcmpobj = p->jcmp;
  if (!jcmpobj) {
    /* load the callback object */
    uint32_t hash = ups_db_get_compare_name_hash(db);
    p->jcmp = jcmpobj = g_callbacks[hash]; // TODO lock
  }

  jclass jcmpcls = p->jenv->GetObjectClass(jcmpobj);
  if (!jcmpcls) {
    jni_log(("GetObjectClass failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  jmethodID jmid = p->jenv->GetMethodID(jcmpcls, "compare", "([B[B)I");
  if (!jmid) {
    jni_log(("GetMethodID failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  /* prepare the parameters */
  jbyteArray jlhs = p->jenv->NewByteArray(lhs_length);
  if (!jlhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (lhs_length)
    p->jenv->SetByteArrayRegion(jlhs, 0, lhs_length, (jbyte *)lhs);

  jbyteArray jrhs = p->jenv->NewByteArray(rhs_length);
  if (!jrhs) {
    jni_log(("NewByteArray failed\n"));
    jni_throw_error(p->jenv, UPS_INTERNAL_ERROR);
    return (-1);
  }

  if (rhs_length)
    p->jenv->SetByteArrayRegion(jrhs, 0, rhs_length, (jbyte *)rhs);

  return (p->jenv->CallIntMethod(jcmpobj, jmid, jlhs, jrhs));
}

static ups_status_t
jparams_to_native(JNIEnv *jenv, jobjectArray jparams, ups_parameter_t **pparams)
{
  unsigned int i, j = 0, size = jenv->GetArrayLength(jparams);
  ups_parameter_t *params = (ups_parameter_t *)malloc(sizeof(*params) * (size + 1));
  if (!params)
    return (UPS_OUT_OF_MEMORY);

  for (i = 0; i < size; i++) {
    jobject jobj = jenv->GetObjectArrayElement(jparams, i);
    if (jobj) {
      jfieldID fidname, fidvalue;
      jclass jcls = jenv->GetObjectClass(jobj);
      if (!jcls) {
        free(params);
        jni_log(("GetObjectClass failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      fidname = jenv->GetFieldID(jcls, "name", "I");
      if (!fidname) {
        free(params);
        jni_log(("GetFieldID failed\n"));
        return (UPS_INTERNAL_ERROR);
      }

      params[j].name = (uint32_t)jenv->GetLongField(jobj, fidname);

      // A few parameters are passed as strings
      if (params[j].name == UPS_PARAM_LOG_DIRECTORY
          || params[j].name == UPS_PARAM_ENCRYPTION_KEY
          || params[j].name == UPS_PARAM_CUSTOM_COMPARE_NAME) {
        fidvalue = jenv->GetFieldID(jcls, "stringValue", "Ljava/lang/String;");
        if (!fidvalue) {
          free(params);
          jni_log(("GetFieldID failed\n"));
          return (UPS_INTERNAL_ERROR);
        }

        jstring s = (jstring)jenv->GetObjectField(jobj, fidvalue);
        if (s) {
          const char *zname = jenv->GetStringUTFChars(s, 0);
          char *p = (char *)malloc(strlen(zname) + 1);
          strcpy(p, zname);
          params[j].value = (uint64_t)p; // TODO hack & leaks
          jenv->ReleaseStringUTFChars(s, zname);
        }
      }
      else {
        fidvalue = jenv->GetFieldID(jcls, "value", "J");
        if (!fidvalue) {
          free(params);
          jni_log(("GetFieldID failed\n"));
          return (UPS_INTERNAL_ERROR);
        }

        params[j].value = (uint64_t)jenv->GetLongField(jobj, fidvalue);
      }

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
  unsigned int i, j = 0, size = jenv->GetArrayLength(jparams);

  for (i = 0; i < size; i++) {
    jobject jobj = jenv->GetObjectArrayElement(jparams, i);
    if (jobj) {
      jfieldID fidname, fidvalue;
      jclass jcls = jenv->GetObjectClass(jobj);
      if (!jcls) {
        jni_log(("GetObjectClass failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      fidname = jenv->GetFieldID(jcls, "name", "I");
      if (!fidname) {
        jni_log(("GetFieldID failed\n"));
        return (UPS_INTERNAL_ERROR);
      }
      /* some values have to be stored in a string, not a long; so far
       * this is only the case for UPS_PARAM_FILENAME. */
      if (params[j].name == UPS_PARAM_FILENAME) {
        jstring str;
        fidvalue = jenv->GetFieldID(jcls, "stringValue", "Ljava/lang/String;");
        if (!fidvalue) {
          jni_log(("GetFieldID (stringValue) failed\n"));
          return (UPS_INTERNAL_ERROR);
        }
        assert(params[j].name == (uint32_t)jenv->GetLongField(jobj, fidname));
        str = jenv->NewStringUTF((const char *)params[j].value);
        if (!str) {
          jni_log(("unable to create new Java string\n"));
          return (UPS_INTERNAL_ERROR);
        }
        jenv->SetObjectField(jobj, fidvalue, str);
      }
      else {
        fidvalue = jenv->GetFieldID(jcls, "value", "J");
        if (!fidvalue) {
          jni_log(("GetFieldID (value) failed\n"));
          return (UPS_INTERNAL_ERROR);
        }
        assert(params[j].name == (uint32_t)jenv->GetLongField(jobj, fidname));
        jenv->SetLongField(jobj, fidvalue, (jlong)params[j].value);
      }
      j++;
    }
  }

  return (0);
}

JNIEXPORT jstring JNICALL
Java_de_crupp_upscaledb_DatabaseException_ups_1strerror(JNIEnv *jenv,
    jobject jobj, jint jerrno)
{
  return jenv->NewStringUTF(ups_strerror((ups_status_t)jerrno));
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Database_ups_1get_1version(JNIEnv *jenv, jclass jcls,
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
Java_de_crupp_upscaledb_Database_ups_1set_1errhandler(JNIEnv *jenv,
    jclass jcls, jobject jeh)
{
  if (!jeh) {
    ups_set_error_handler(0);
    return;
  }

  /* set global javavm pointer, if needed */
  if (!g_javavm) {
    if (jenv->GetJavaVM(&g_javavm) != 0) {
      jni_log(("Cannot get Java VM\n"));
      return;
    }
  }

  ups_set_error_handler(jni_errhandler);
}

JNIEXPORT void JNICALL
Java_de_crupp_upscaledb_Database_ups_1register_1compare(JNIEnv *jenv,
    jclass jcls, jstring jname, jobject jcmp)
{
  if (!jname || !jcmp)
    jni_throw_error(jenv, UPS_INV_PARAMETER);

  const char *zname = jenv->GetStringUTFChars(jname, 0);
  ups_register_compare(zname, jni_compare_func2);

  uint32_t hash = ups_calc_compare_name_hash(zname);
  // increase the refcount of the compare object; jcmp is a local ref and
  // will go out of scope when this function returns
  g_callbacks[hash] = jenv->NewGlobalRef(jcmp); // TODO lock

  jenv->ReleaseStringUTFChars(jname, zname);
}

JNIEXPORT void JNICALL
Java_de_crupp_upscaledb_Database_ups_1db_1set_1compare_1func(JNIEnv *jenv,
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
Java_de_crupp_upscaledb_Database_ups_1db_1find(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jlong jtxnhandle, jbyteArray jkey, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  ups_record_t hrec;
  jbyteArray jrec;

  SET_DB_CONTEXT((ups_db_t *)jhandle, jenv, jobj);

  memset(&hkey, 0, sizeof(hkey));
  memset(&hrec, 0, sizeof(hrec));

  hkey.data = (uint8_t *)jenv->GetByteArrayElements(jkey, 0);
  hkey.size = (uint32_t)jenv->GetArrayLength(jkey);

  st = ups_db_find((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
            &hkey, &hrec, (uint32_t)jflags);

  jenv->ReleaseByteArrayElements(jkey, (jbyte *)hkey.data, 0);
  if (st)
    return (0);

  jrec = jenv->NewByteArray(hrec.size);
  if (hrec.size)
    jenv->SetByteArrayRegion(jrec, 0, hrec.size, (jbyte *)hrec.data);

  return (jrec);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Database_ups_1db_1insert(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jlong jtxnhandle, jbyteArray jkey,
    jbyteArray jrecord, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  ups_record_t hrec;

  SET_DB_CONTEXT((ups_db_t *)jhandle, jenv, jobj);

  memset(&hkey, 0, sizeof(hkey));
  memset(&hrec, 0, sizeof(hrec));

  hkey.data = (uint8_t *)jenv->GetByteArrayElements(jkey, 0);
  hkey.size = (uint32_t)jenv->GetArrayLength(jkey);
  hrec.data = (uint8_t *)jenv->GetByteArrayElements(jrecord, 0);
  hrec.size = (uint32_t)jenv->GetArrayLength(jrecord);

  st = ups_db_insert((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
            &hkey, &hrec, (uint32_t)jflags);

  jenv->ReleaseByteArrayElements(jkey, (jbyte *)hkey.data, 0);
  jenv->ReleaseByteArrayElements(jrecord, (jbyte *)hrec.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Database_ups_1db_1erase(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jlong jtxnhandle, jbyteArray jkey, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;

  SET_DB_CONTEXT((ups_db_t *)jhandle, jenv, jobj);

  memset(&hkey, 0, sizeof(hkey));

  hkey.data = (uint8_t *)jenv->GetByteArrayElements(jkey, 0);
  hkey.size = (uint32_t)jenv->GetArrayLength(jkey);

  st = ups_db_erase((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
            &hkey, (uint32_t)jflags);

  jenv->ReleaseByteArrayElements(jkey, (jbyte *)hkey.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Database_ups_1db_1bulk_1operations(JNIEnv *jenv,
                jobject jobj, jlong jhandle, jlong jtxnhandle,
                jobjectArray joperations, jint jflags)
{
  std::vector<ups_operation_t> ops;
  jbyteArray jbyteData;
  jobject jop;
  jfieldID fidname;

  unsigned size = jenv->GetArrayLength(joperations);
  for (unsigned i = 0; i < size; i++) {
    ups_operation_t operation = {0};
    jop = jenv->GetObjectArrayElement(joperations, i);

    jclass jcls = jenv->GetObjectClass(jop);
    if (!jcls) {
      jni_log(("GetObjectClass failed\n"));
      return (UPS_INTERNAL_ERROR);
    }

    // ups_operation_t::type
    fidname = jenv->GetFieldID(jcls, "type", "I");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    operation.type = jenv->GetIntField(jop, fidname);

    // ups_operation_t::flags
    fidname = jenv->GetFieldID(jcls, "flags", "I");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    operation.flags = jenv->GetIntField(jop, fidname);

    // ups_operation_t::key
    fidname = jenv->GetFieldID(jcls, "key", "[B");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    jbyteData = (jbyteArray)jenv->GetObjectField(jop, fidname);
    if (jbyteData) {
      operation.key.size = (uint16_t)jenv->GetArrayLength(jbyteData);
      operation.key.data = (uint8_t *)jenv->GetByteArrayElements(jbyteData, 0);
    }

    // ups_operation_t::record
    fidname = jenv->GetFieldID(jcls, "record", "[B");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    jbyteData = (jbyteArray)jenv->GetObjectField(jop, fidname);
    if (jbyteData) {
      operation.record.size = (uint16_t)jenv->GetArrayLength(jbyteData);
      operation.record.data = (uint8_t *)jenv->GetByteArrayElements(jbyteData, 0);
    }

    ops.push_back(operation);
  }

  ups_status_t st = ups_db_bulk_operations((ups_db_t *)jhandle,
                  (ups_txn_t *)jtxnhandle, ops.data(), ops.size(), 0);
  if (st)
    return (st);

  bool is_record_number_db = (ups_db_get_flags((ups_db_t *)jhandle)
                                & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))
                             != 0;

  ups_operation_t *pop = &ops[0];
  for (unsigned i = 0; i < size; i++, pop++) {
    jop = jenv->GetObjectArrayElement(joperations, i);

    bool copy_key = false;
    bool copy_record = false;

    if (pop->type == UPS_OP_INSERT) {
      // if this a record number database? then we might have to copy the key
      if (is_record_number_db)
        copy_key = true;
    }

    else if (pop->type == UPS_OP_FIND) {
      // copy key if approx. matching was used
      if (ups_key_get_intflags(&pop->key) != 0)
        copy_key = true;
      copy_record = true;
    }

    jclass jcls = jenv->GetObjectClass(jop);
    if (!jcls) {
      jni_log(("GetObjectClass failed\n"));
      return (UPS_INTERNAL_ERROR);
    }

    // ups_operation_t::key
    fidname = jenv->GetFieldID(jcls, "key", "[B");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    jbyteData = (jbyteArray)jenv->GetObjectField(jop, fidname);
    if (copy_key) {
      jbyteData = jenv->NewByteArray(pop->key.size);
      if (pop->key.size)
        jenv->SetByteArrayRegion(jbyteData, 0, pop->key.size,
                        (jbyte *)pop->key.data);
      jenv->SetObjectField(jop, fidname, jbyteData);
    }

    // ups_operation_t::record
    fidname = jenv->GetFieldID(jcls, "record", "[B");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    jbyteData = (jbyteArray)jenv->GetObjectField(jop, fidname);
    if (copy_record) {
      jbyteData = jenv->NewByteArray(pop->record.size);
      if (pop->record.size)
        jenv->SetByteArrayRegion(jbyteData, 0, pop->record.size,
                        (jbyte *)pop->record.data);
      jenv->SetObjectField(jop, fidname, jbyteData);
    }

    // ups_operation_t::result
    fidname = jenv->GetFieldID(jcls, "result", "I");
    if (!fidname) {
      jni_log(("GetFieldID failed\n"));
      return (UPS_INTERNAL_ERROR);
    }
    jenv->SetIntField(jop, fidname, pop->result);
  }

  return 0;
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Database_ups_1db_1close(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jint jflags)
{
  return (ups_db_close((ups_db_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1create(JNIEnv *jenv, jobject jobj,
    jlong jdbhandle, jlong jtxnhandle)
{
  ups_cursor_t *cursor;
  ups_status_t st;

  st = ups_cursor_create(&cursor, (ups_db_t *)jdbhandle,
            (ups_txn_t *)jtxnhandle, 0);
  if (st) {
    jni_throw_error(jenv, st);
    return (0);
  }
  return ((jlong)cursor);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1clone(JNIEnv *jenv, jobject jobj,
    jlong jhandle)
{
  ups_cursor_t *cursor;
  ups_status_t st;

  st = ups_cursor_clone((ups_cursor_t *)jhandle, &cursor);
  if (st) {
    jni_throw_error(jenv, st);
    return (0);
  }
  return ((jlong)cursor);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1move_1to(JNIEnv *jenv,
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
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1get_1key(JNIEnv *jenv,
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

  ret = jenv->NewByteArray(key.size);
  if (key.size)
    jenv->SetByteArrayRegion(ret, 0, key.size, (jbyte *)key.data);
  return (ret);
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1get_1record(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  ups_status_t st;
  ups_record_t rec;
  jbyteArray ret;
  jnipriv p;
  memset(&rec, 0, sizeof(rec));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (0);

  st = ups_cursor_move((ups_cursor_t *)jhandle, 0, &rec, (uint32_t)jflags);
  if (st)
    return (0);

  ret = jenv->NewByteArray(rec.size);
  if (rec.size)
    jenv->SetByteArrayRegion(ret, 0, rec.size, (jbyte *)rec.data);
  return (ret);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1overwrite(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jbyteArray jrec, jint jflags)
{
  ups_status_t st;
  ups_record_t hrec;
  jnipriv p;
  memset(&hrec, 0, sizeof(hrec));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  hrec.data = (uint8_t *)jenv->GetByteArrayElements(jrec, 0);
  hrec.size = (uint32_t)jenv->GetArrayLength(jrec);

  st = ups_cursor_overwrite((ups_cursor_t *)jhandle, &hrec, (uint32_t)jflags);

  jenv->ReleaseByteArrayElements(jrec, (jbyte *)hrec.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1find(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jbyteArray jkey, jint jflags)
{
  ups_status_t st;
  ups_key_t hkey;
  jnipriv p;
  memset(&hkey, 0, sizeof(hkey));

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  hkey.data = (uint8_t *)jenv->GetByteArrayElements(jkey, 0);
  hkey.size = (uint32_t)jenv->GetArrayLength(jkey);

  st = ups_cursor_find((ups_cursor_t *)jhandle, &hkey, 0, (uint32_t)jflags);

  jenv->ReleaseByteArrayElements(jkey, (jbyte *)hkey.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1insert(JNIEnv *jenv, jobject jobj,
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

  hkey.data = (uint8_t *)jenv->GetByteArrayElements(jkey, 0);
  hkey.size = (uint32_t)jenv->GetArrayLength(jkey);
  hrec.data = (uint8_t *)jenv->GetByteArrayElements(jrecord, 0);
  hrec.size = (uint32_t)jenv->GetArrayLength(jrecord);

  st = ups_cursor_insert((ups_cursor_t *)jhandle, &hkey, &hrec,
                (uint32_t)jflags);

  jenv->ReleaseByteArrayElements(jkey, (jbyte *)hkey.data, 0);
  jenv->ReleaseByteArrayElements(jrecord, (jbyte *)hrec.data, 0);

  return (st);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1erase(JNIEnv *jenv, jobject jobj,
    jlong jhandle, jint jflags)
{
  jnipriv p;
  jint st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  return ((jint)ups_cursor_erase((ups_cursor_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1get_1duplicate_1count(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  uint32_t count;
  ups_status_t st;
  jnipriv p;

  st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  st = ups_cursor_get_duplicate_count((ups_cursor_t *)jhandle, &count,
      (uint32_t)jflags);
  if (st) {
    jni_throw_error(jenv, st);
    return (0);
  }
  return ((jint)count);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1get_1record_1size
    (JNIEnv *jenv, jobject jobj, jlong jhandle)
{
  uint32_t size;
  jnipriv p;

  ups_status_t st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  st = ups_cursor_get_record_size((ups_cursor_t *)jhandle, &size);
  if (st) {
    jni_throw_error(jenv, st);
    return (0);
  }
  return ((jlong)size);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Cursor_ups_1cursor_1close(JNIEnv *jenv, jobject jobj,
    jlong jhandle)
{
  jnipriv p;
  jint st = jni_set_cursor_env(&p, jenv, jobj, jhandle);
  if (st)
    return (st);

  return ((jint)ups_cursor_close((ups_cursor_t *)jhandle));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1create(JNIEnv *jenv,
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
    filename = jenv->GetStringUTFChars(jfilename, 0);

  st = ups_env_create(&env, filename, (uint32_t)jflags,
                (uint32_t)jmode, params);

  if (params)
    free(params);
  jenv->ReleaseStringUTFChars(jfilename, filename);

  if (st) {
    jni_throw_error(jenv, st);
    return 0;
  }
  return ((jlong)env);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1open(JNIEnv *jenv,
    jobject jobj, jstring jfilename,
    jint jflags, jobjectArray jparams)
{
  ups_status_t st;
  ups_parameter_t *params = 0;
  const char *filename = 0;
  ups_env_t *env;

  if (jparams) {
    st = jparams_to_native(jenv, jparams, &params);
    if (st)
      return (st);
  }

  if (jfilename)
    filename = jenv->GetStringUTFChars(jfilename, 0);

  st = ups_env_open(&env, filename, (uint32_t)jflags, params);

  if (params)
    free(params);
  jenv->ReleaseStringUTFChars(jfilename, filename);

  if (st) {
    jni_throw_error(jenv, st);
    return 0;
  }
  return ((jlong)env);
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1create_1db(JNIEnv *jenv,
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
Java_de_crupp_upscaledb_Environment_ups_1env_1open_1db(JNIEnv *jenv,
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
Java_de_crupp_upscaledb_Environment_ups_1env_1rename_1db(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jshort joldname,
    jshort jnewname, jint jflags)
{
  return ((jint)ups_env_rename_db((ups_env_t *)jhandle, (uint16_t)joldname,
        (uint16_t)jnewname, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1erase_1db(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jshort jname, jint jflags)
{
  return ((jint)ups_env_erase_db((ups_env_t *)jhandle, (uint16_t)jname,
        (uint32_t)jflags));
}

JNIEXPORT jshortArray JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1get_1database_1names(JNIEnv *jenv,
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

  ret = jenv->NewShortArray((jint)num_dbs);
  jenv->SetShortArrayRegion(ret, 0, num_dbs, (jshort *)dbs);
  free(dbs);
  return (ret);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1close(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  return (ups_env_close((ups_env_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Environment_ups_1txn_1begin
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
Java_de_crupp_upscaledb_Transaction_ups_1txn_1commit(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  return (ups_txn_commit((ups_txn_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Transaction_ups_1txn_1abort(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jflags)
{
  return (ups_txn_abort((ups_txn_t *)jhandle, (uint32_t)jflags));
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Database_ups_1db_1get_1parameters(JNIEnv *jenv,
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
Java_de_crupp_upscaledb_Database_ups_1db_1count(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jlong jtxnhandle, jint jflags)
{
  ups_status_t st;
  uint64_t keycount;
  st = ups_db_count((ups_db_t *)jhandle, (ups_txn_t *)jtxnhandle,
        (uint32_t)jflags, &keycount);
  if (st) {
    jni_throw_error(jenv, st);
    return (-1);
  }

  return ((jlong)keycount);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1get_1parameters(JNIEnv *jenv,
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
Java_de_crupp_upscaledb_Environment_ups_1env_1flush(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  return (ups_env_flush((ups_env_t *)jhandle, (uint32_t)0));
}

JNIEXPORT jlong JNICALL
Java_de_crupp_upscaledb_Environment_ups_1env_1select_1range(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jstring jquery, jlong jbegin, jlong jend)
{
  uqi_result_t *result;
  ups_status_t st;
  const char *query = 0;
  if (jquery)
    query = jenv->GetStringUTFChars(jquery, 0);

  st = uqi_select_range((ups_env_t *)jhandle, query, (ups_cursor_t *)jbegin,
                    (ups_cursor_t *)jend, &result);
  jenv->ReleaseStringUTFChars(jquery, query);

  if (st) {
    jni_throw_error(jenv, st);
    return (0);
  }
  return ((jlong)result);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1row_1count(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  return (uqi_result_get_row_count((uqi_result_t *)jhandle));
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1key_1type(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  return (uqi_result_get_key_type((uqi_result_t *)jhandle));
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1key(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jrow)
{
  ups_key_t ukey = {0};
  uqi_result_get_key((uqi_result_t *)jhandle, jrow, &ukey);

  jbyteArray jkey = jenv->NewByteArray(ukey.size);
  if (ukey.size)
    jenv->SetByteArrayRegion(jkey, 0, ukey.size, (jbyte *)ukey.data);
  return (jkey);
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1key_1data(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  uint32_t size;
  void *data = uqi_result_get_key_data((uqi_result_t *)jhandle, &size);

  jbyteArray jb = jenv->NewByteArray(size);
  if (size)
    jenv->SetByteArrayRegion(jb, 0, size, (jbyte *)data);
  return (jb);
}

JNIEXPORT jint JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1record_1type(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  return (uqi_result_get_record_type((uqi_result_t *)jhandle));
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1record(JNIEnv *jenv,
    jobject jobj, jlong jhandle, jint jrow)
{
  ups_record_t urec = {0};
  uqi_result_get_record((uqi_result_t *)jhandle, jrow, &urec);

  jbyteArray jrec = jenv->NewByteArray(urec.size);
  if (urec.size)
    jenv->SetByteArrayRegion(jrec, 0, urec.size, (jbyte *)urec.data);
  return (jrec);
}

JNIEXPORT jbyteArray JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1get_1record_1data(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  uint32_t size;
  void *data = uqi_result_get_record_data((uqi_result_t *)jhandle, &size);

  jbyteArray jb = jenv->NewByteArray(size);
  if (size)
    jenv->SetByteArrayRegion(jb, 0, size, (jbyte *)data);
  return (jb);
}

JNIEXPORT void JNICALL
Java_de_crupp_upscaledb_Result_uqi_1result_1close(JNIEnv *jenv,
    jobject jobj, jlong jhandle)
{
  uqi_result_close((uqi_result_t *)jhandle);
}

} // extern "C"
