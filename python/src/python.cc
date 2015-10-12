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

#include <Python.h>
#include "structmember.h"
#include <ups/upscaledb_int.h>

static PyObject *g_exception = NULL;
static PyObject *g_errhandler = NULL;

#define THROW(st)  { throw_exception(st); return (0); }

static void
throw_exception(ups_status_t st)
{
  PyObject *tuple = Py_BuildValue("(is)", st, ups_strerror(st));
  PyErr_SetObject(g_exception, tuple);
  Py_DECREF(tuple);
}

/* an Environment Object */
typedef struct {
    PyObject_HEAD
    PyObject *dblist;
    ups_env_t *env;
} HamEnvironment;

/* a Database Object */
typedef struct {
    PyObject_HEAD
    ups_db_t *db;
    uint32_t flags;
    PyObject *comparecb;
    PyObject *cursorlist;
    PyObject *err_type, *err_value, *err_traceback;
} HamDatabase;

/* a Cursor Object */
typedef struct {
    PyObject_HEAD
    HamDatabase *db;
    ups_cursor_t *cursor;
} HamCursor;

/* a Transaction Object */
typedef struct {
    PyObject_HEAD
    ups_txn_t *txn;
} HamTransaction;

static void
db_dealloc(HamDatabase *self)
{
  if (!self->db)
    return;

  /* close the other members */
  Py_XDECREF(self->comparecb);
  Py_XDECREF(self->err_type);
  Py_XDECREF(self->err_value);
  Py_XDECREF(self->err_traceback);
  if (self->cursorlist) {
    Py_ssize_t i, size = PyList_GET_SIZE(self->cursorlist);
    for (i = 0; i < size; i++) {
      HamCursor *c = (HamCursor *)PyList_GET_ITEM(self->cursorlist, i);
      if (c && c->cursor) {
        ups_cursor_close(c->cursor);
        c->cursor = 0;
      }
      Py_XDECREF(c);
    }
    Py_DECREF(self->cursorlist);
    self->cursorlist = 0;
  }

  ups_db_close(self->db, 0);
  self->db = 0;

  PyObject_Del(self);
}

static bool
parse_parameters(PyObject *extargs, ups_parameter_t *params)
{
  int name;
  int i, extsize = PyTuple_Size(extargs);

  memset(params, 0, 64 * sizeof(ups_parameter_t));

  /* sanity check */
  if (extsize > 64)
    return (false);

  for (i = 0; i < extsize; i++) {
    PyObject *n, *v, *o = PyTuple_GetItem(extargs, i);
    if (!o)
      return (false);
    if (o->ob_type != &PyTuple_Type) {
      PyErr_SetString(PyExc_TypeError,
            "Last argument must be a tuple of tuples");
      return (false);
    }
    if (PyTuple_Size(o) != 2) {
      PyErr_SetString(PyExc_TypeError,
            "Last argument must be a tuple of tuples");
      return (false);
    }
    n = PyTuple_GetItem(o, 0);
    v = PyTuple_GetItem(o, 1);
    name = PyInt_AsLong(n);
    if (PyErr_Occurred()) 
      return (false);

    // a few parameters are passed as a string
    params[i].name = name;
    if (name == UPS_PARAM_LOG_DIRECTORY || name == UPS_PARAM_ENCRYPTION_KEY)
      params[i].value = (uint64_t)PyBytes_AsString(v);
    else
      params[i].value = PyInt_AsLong(v);
    if (PyErr_Occurred())
      return (false);
  }

  return (true);
}

static PyObject *
env_create(HamEnvironment *self, PyObject *args)
{
  ups_status_t st;
  const char *filename = 0;
  uint32_t mode = 0;
  uint32_t flags = 0;
  PyObject *extargs = 0;
  ups_parameter_t params[64]; /* should be enough */

  if (!PyArg_ParseTuple(args, "|ziiO!:create", &filename, &flags, &mode,
                        &PyTuple_Type, &extargs))
    return (0);

  if (extargs) {
    if (!parse_parameters(extargs, params))
      return (0);
  }

  st = ups_env_create(&self->env, filename, flags, mode, 
                  extargs ? params : 0);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
env_open(HamEnvironment *self, PyObject *args)
{
  ups_status_t st;
  const char *filename = 0;
  uint32_t flags = 0;
  PyObject *extargs = 0;
  ups_parameter_t params[64]; /* should be enough */

  if (!PyArg_ParseTuple(args, "|ziO!:open", &filename, &flags,
                          &PyTuple_Type, &extargs))
    return (0);

  if (extargs) {
    if (!parse_parameters(extargs, params))
      return (0);
  }

  st = ups_env_open(&self->env, filename, flags, extargs ? params : 0);
  if (st)
    THROW(st);

  return (Py_BuildValue(""));
}

static PyObject *
env_close(HamEnvironment *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":close"))
    return (0);

  ups_status_t st = ups_env_close(self->env, 0);
  if (st)
    THROW(st);
  self->env = 0;
  return (Py_BuildValue(""));
}

static PyObject *
construct_db(PyObject *self, PyObject *args);
static PyObject *
construct_env(PyObject *self, PyObject *args);
static PyObject *
construct_cursor(PyObject *self, PyObject *args);

static PyObject *
env_create_db(HamEnvironment *self, PyObject *args)
{
  ups_status_t st;
  int name = 0;
  uint32_t flags = 0;
  PyObject *extargs = 0;
  ups_parameter_t params[64]; /* should be enough */

  if (!PyArg_ParseTuple(args, "i|iO!:create_db", &name, &flags,
                        &PyTuple_Type, &extargs))
    return (0);

  if (extargs) {
    if (!parse_parameters(extargs, params))
      return (0);
  }

  HamDatabase *db = (HamDatabase *)construct_db(0, 0);
  if (!db)
    return (0);

  st = ups_env_create_db(self->env, &db->db, (uint16_t)name, flags,
                  extargs ? params : 0);
  if (st) {
    db_dealloc(db);
    THROW(st);
  }

  db->flags = flags;
  ups_set_context_data(db->db, db);

  /* add the new database to the environment */
  if (!self->dblist) {
    self->dblist = PyList_New(10);
    if (!self->dblist)
      return (0);
  }
  if (PyList_Append(self->dblist, (PyObject *)db))
    return (0);
  Py_INCREF(db);

  return ((PyObject *)db);
}

static PyObject *
env_open_db(HamEnvironment *self, PyObject *args)
{
  ups_status_t st;
  int name = 0;
  uint32_t flags = 0;
  PyObject *extargs = 0;
  ups_parameter_t params[64]; /* should be enough */

  if (!PyArg_ParseTuple(args, "i|iO!:open_db", &name, &flags,
                        &PyTuple_Type, &extargs))
    return (0);

  if (extargs) {
    if (!parse_parameters(extargs, params))
      return (0);
  }

  HamDatabase *db = (HamDatabase *)construct_db(0, 0);
  if (!db)
    return (0);

  ups_set_context_data(db->db, db);

  st = ups_env_open_db(self->env, &db->db, (uint16_t)name, flags,
                  extargs ? params : 0);
  if (st) {
    db_dealloc(db);
    THROW(st);
  }

  ups_parameter_t flag_params[] = {
    {UPS_PARAM_FLAGS, 0},
    {0, 0}
  };
  ups_db_get_parameters(db->db, &flag_params[0]);
  db->flags = (uint32_t)flag_params[0].value;

  /* add the new database to the environment */
  if (!self->dblist) {
    self->dblist = PyList_New(10);
    if (!self->dblist)
      return (0);
  }
  if (PyList_Append(self->dblist, (PyObject *)db))
    return (0);
  Py_INCREF(db);

  return ((PyObject *)db);
}

static PyObject *
env_rename_db(HamEnvironment *self, PyObject *args)
{
  uint32_t oldname = 0, newname = 0;

  if (!PyArg_ParseTuple(args, "ii|:rename_db", &oldname, &newname))
    return (0);

  ups_status_t st = ups_env_rename_db(self->env,
                  (uint16_t)oldname, (uint16_t)newname, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
env_erase_db(HamEnvironment *self, PyObject *args)
{
  uint32_t name = 0;

  if (!PyArg_ParseTuple(args, "i|:erase_db", &name))
    return (0);

  ups_status_t st = ups_env_erase_db(self->env, (uint16_t)name, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
env_flush(HamEnvironment *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":flush"))
    return (0);

  ups_status_t st = ups_env_flush(self->env, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
env_get_database_names(HamEnvironment *self, PyObject *args)
{
  uint16_t names[1024];
  uint32_t count = 1024;

  if (!PyArg_ParseTuple(args, ":get_database_names"))
    return (0);

  ups_status_t st = ups_env_get_database_names(self->env, names, &count);
  if (st)
    THROW(st);

  PyObject *ret = PyTuple_New(count);
  if (!ret)
    return (0);

  for (uint32_t i = 0; i < count; i++) {
    PyObject *io = PyInt_FromLong(names[i]);
    if (!PyTuple_SET_ITEM(ret, i, io))
      return (0);
  }
  return (ret);
}

static PyObject *
db_find(HamDatabase *self, PyObject *args)
{
  ups_key_t key = {0};
  ups_record_t record = {0};
  uint32_t recno32;
  uint64_t recno64;
  HamTransaction *txn = 0;

  /* recno: first object is an integer */
  if (self->flags & UPS_RECORD_NUMBER32) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "OO!:find", &txn, &PyInt_Type, &temp))
      return (0);
    recno32 = (uint32_t)PyInt_AsLong(temp);
    key.data = &recno32;
    key.size = sizeof(recno32);
  }
  else if (self->flags & UPS_RECORD_NUMBER64) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "OO!:find", &txn, &PyInt_Type, &temp))
      return (0);
    recno64 = PyInt_AsLong(temp);
    key.data = &recno64;
    key.size = sizeof(recno64);
  }
  else if (!PyArg_ParseTuple(args, "Os#:find", &txn, &key.data, &key.size))
    return (0);

  /* check if first object is either a Transaction or None */
  if (txn == (HamTransaction *)Py_None)
    txn = 0;

  ups_status_t st = ups_db_find(self->db, txn ? txn->txn : 0, &key, &record, 0);
  if (st) {
    if (self->err_type || self->err_value) {
      PyErr_Restore(self->err_type, self->err_value, self->err_traceback);
      self->err_type = 0;
      self->err_value = 0;
      self->err_traceback = 0;
      return (0);
    }
    THROW(st);
  }

  return (Py_BuildValue("s#", record.data, record.size));
}

static void
txn_dealloc(HamTransaction *self);
static PyObject *
txn_getattr(HamTransaction *self, char *name);

statichere PyTypeObject HamTransaction_Type = {
    PyObject_HEAD_INIT(NULL)
    0,          /*ob_size*/
    "txn",   /*tp_name*/
    sizeof(HamTransaction),   /*tp_basicsize*/
    0,          /*tp_itemsize*/
    /* methods */
    (destructor)txn_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    (getattrfunc)txn_getattr, /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash*/
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,     /* tp_flags */
    "upscaledb Transaction"  /* tp_doc */
};

static PyObject *
db_insert(HamDatabase *self, PyObject *args)
{
  ups_key_t key = {0};
  ups_record_t record = {0};
  uint32_t flags = 0;
  HamTransaction *txn = 0;

  /* recno: ignore the second object */
  if (self->flags & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "OOs#|i:insert", &txn, &temp,
                            &record.data, &record.size, &flags))
      return (0);
  }
  else if (!PyArg_ParseTuple(args, "Os#s#|i:insert", &txn, &key.data, &key.size,
                          &record.data, &record.size, &flags))
    return (0);

  /* check if first object is either a Transaction or None */
  if (txn == (HamTransaction *)Py_None)
    txn = 0;

  ups_status_t st = ups_db_insert(self->db, txn ? txn->txn : 0,
                  &key, &record, flags);
  if (st) {
    if (self->err_type || self->err_value) {
      PyErr_Restore(self->err_type, self->err_value, self->err_traceback);
      self->err_type = 0;
      self->err_value = 0;
      self->err_traceback = 0;
      return (0);
    }
    THROW(st);
  }
  return (Py_BuildValue(""));
}

static PyObject *
db_erase(HamDatabase *self, PyObject *args)
{
  ups_key_t key = {0};
  uint32_t recno32;
  uint64_t recno64;
  HamTransaction *txn;

  /* recno: first object is an integer */
  if (self->flags & UPS_RECORD_NUMBER32) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "OO!:find", &txn, &PyInt_Type, &temp))
      return (0);
    recno32 = (uint32_t)PyInt_AsLong(temp);
    key.data = &recno32;
    key.size = sizeof(recno32);
  }
  else if (self->flags & UPS_RECORD_NUMBER64) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "OO!:find", &txn, &PyInt_Type, &temp))
      return (0);
    recno64 = PyInt_AsLong(temp);
    key.data = &recno64;
    key.size = sizeof(recno64);
  }
  else if (!PyArg_ParseTuple(args, "Os#:erase", &txn, &key.data, &key.size))
    return (0);

  /* check if first object is either a Transaction or None */
  if (txn == (HamTransaction *)Py_None)
    txn = 0;

  ups_status_t st = ups_db_erase(self->db, txn ? txn->txn : 0, &key, 0);
  if (st) {
    if (self->err_type || self->err_value) {
      PyErr_Restore(self->err_type, self->err_value, self->err_traceback);
      self->err_type = 0;
      self->err_value = 0;
      self->err_traceback = 0;
      return (0);
    }
    THROW(st);
  }
  return (Py_BuildValue(""));
}

static int
compare_func(ups_db_t *db,
                const uint8_t *lhs, uint32_t lhs_length,
                const uint8_t *rhs, uint32_t rhs_length)
{
  PyObject *arglist, *result;
  HamDatabase *self = (HamDatabase *)ups_get_context_data(db, UPS_TRUE);

  arglist = Py_BuildValue("(s#s#)", lhs, lhs_length, rhs, rhs_length);
  result = PyEval_CallObject(self->comparecb, arglist);
  Py_DECREF(arglist);

  if (!result) {
    /* save exception */
    PyErr_Fetch(&self->err_type, &self->err_value, &self->err_traceback);
    return (0);
  }
  int i = PyInt_AsLong(result);
  PyErr_Fetch(&self->err_type, &self->err_value, &self->err_traceback);
  Py_DECREF(result);
  return (i);
}

static PyObject *
db_set_compare_func(HamDatabase *self, PyObject *args)
{
  PyObject *cb = 0;

  if (!PyArg_ParseTuple(args, "O:set_compare_func", &cb))
    return (0);

  if (cb == Py_None)
    return (Py_BuildValue("i", ups_db_set_compare_func(self->db, 0)));

  if (!PyCallable_Check(cb)) {
    PyErr_SetString(PyExc_TypeError, "parameter must be callable");
    return (0);
  }

  Py_XDECREF(self->comparecb);
  self->comparecb = 0;

  Py_XINCREF(cb);
  self->comparecb = cb;

  ups_status_t st = ups_db_set_compare_func(self->db, compare_func);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
db_close(HamDatabase *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":close"))
    return (0);

  ups_status_t st = ups_db_close(self->db, 0);
  if (st)
    THROW(st);
  self->db = 0;
  return (Py_BuildValue(""));
}

static PyMethodDef HamEnvironment_methods[] = {
  {"create", (PyCFunction)env_create, 
      METH_VARARGS},
  {"open", (PyCFunction)env_open,
      METH_VARARGS},
  {"close", (PyCFunction)env_close,
      METH_VARARGS},
  {"create_db", (PyCFunction)env_create_db,
      METH_VARARGS},
  {"open_db", (PyCFunction)env_open_db,
      METH_VARARGS},
  {"rename_db", (PyCFunction)env_rename_db,
      METH_VARARGS},
  {"erase_db", (PyCFunction)env_erase_db,
      METH_VARARGS},
  {"get_database_names", (PyCFunction)env_get_database_names,
      METH_VARARGS},
  {"flush", (PyCFunction)env_flush,
      METH_VARARGS},
  {NULL}  /* Sentinel */
};

static PyMethodDef HamDatabase_methods[] = {
  {"close", (PyCFunction)db_close,
      METH_VARARGS},
  {"find", (PyCFunction)db_find,
      METH_VARARGS},
  {"insert", (PyCFunction)db_insert,
      METH_VARARGS},
  {"erase", (PyCFunction)db_erase,
      METH_VARARGS},
  {"set_compare_func", (PyCFunction)db_set_compare_func, 
      METH_VARARGS},
  {NULL}  /* Sentinel */
};

static PyObject *
db_getattr(HamDatabase *self, char *name)
{
  return (Py_FindMethod(HamDatabase_methods, (PyObject *)self, name));
}

static PyObject *
env_getattr(HamEnvironment *self, char *name)
{
  return (Py_FindMethod(HamEnvironment_methods, (PyObject *)self, name));
}

static void
env_dealloc(HamEnvironment *self)
{
  if (self->dblist) {
    Py_ssize_t i, size = PyList_GET_SIZE(self->dblist);
    for (i = 0; i < size; i++) {
      HamDatabase *db = (HamDatabase *)PyList_GET_ITEM(self->dblist, i);
      if (db && db->db) {
        ups_db_close(db->db, 0);
        db->db = 0;
      }
      Py_XDECREF(db);
    }
    Py_DECREF(self->dblist);
    self->dblist = 0;
  }

  if (self->env) {
    ups_env_close(self->env, 0);
    self->env = 0;
  }

  PyObject_Del(self);
}

static void
cursor_dealloc(HamCursor *self)
{
  if (!self->cursor)
    return;

  ups_cursor_close(self->cursor);
  self->cursor = 0;

  PyObject_Del(self);
}

static PyObject *
cursor_create(HamCursor *self, PyObject *args);
static PyObject *
cursor_clone(HamCursor *self, PyObject *args);
static PyObject *
cursor_close(HamCursor *self, PyObject *args);
static PyObject *
cursor_insert(HamCursor *self, PyObject *args);
static PyObject *
cursor_find(HamCursor *self, PyObject *args);
static PyObject *
cursor_erase(HamCursor *self, PyObject *args);
static PyObject *
cursor_move_to(HamCursor *self, PyObject *args);
static PyObject *
cursor_get_key(HamCursor *self, PyObject *args);
static PyObject *
cursor_get_record(HamCursor *self, PyObject *args);
static PyObject *
cursor_overwrite(HamCursor *self, PyObject *args);
static PyObject *
cursor_get_duplicate_count(HamCursor *self, PyObject *args);
static PyObject *
cursor_get_duplicate_position(HamCursor *self, PyObject *args);
static PyObject *
cursor_get_record_size(HamCursor *self, PyObject *args);

static PyMethodDef HamCursor_methods[] = {
  {"create", (PyCFunction)cursor_create, 
      METH_VARARGS},
  {"clone", (PyCFunction)cursor_clone,
      METH_VARARGS},
  {"close", (PyCFunction)cursor_close,
      METH_VARARGS},
  {"insert", (PyCFunction)cursor_insert,
      METH_VARARGS},
  {"find", (PyCFunction)cursor_find,
      METH_VARARGS},
  {"erase", (PyCFunction)cursor_erase,
      METH_VARARGS},
  {"move_to", (PyCFunction)cursor_move_to,
      METH_VARARGS},
  {"get_key", (PyCFunction)cursor_get_key,
      METH_VARARGS},
  {"get_record", (PyCFunction)cursor_get_record,
      METH_VARARGS},
  {"overwrite", (PyCFunction)cursor_overwrite,
      METH_VARARGS},
  {"get_duplicate_count", (PyCFunction)cursor_get_duplicate_count,
      METH_VARARGS},
  {"get_duplicate_position", (PyCFunction)cursor_get_duplicate_position,
      METH_VARARGS},
  {"get_record_size", (PyCFunction)cursor_get_record_size,
      METH_VARARGS},
  {NULL}  /* Sentinel */
};

static PyObject *
construct_txn(PyObject *self, PyObject *args);
static PyObject *
txn_begin(HamTransaction *self, PyObject *args);
static PyObject *
txn_commit(HamTransaction *self, PyObject *args);
static PyObject *
txn_abort(HamTransaction *self, PyObject *args);

static PyMethodDef HamTransaction_methods[] = {
  {"begin", (PyCFunction)txn_begin, 
      METH_VARARGS},
  {"abort", (PyCFunction)txn_abort,
      METH_VARARGS},
  {"commit", (PyCFunction)txn_commit,
      METH_VARARGS},
  {NULL}  /* Sentinel */
};

static void
add_const(PyObject *dict, const char *name, long value)
{
  PyObject *v = PyInt_FromLong(value);
  if (!v || PyDict_SetItemString(dict, name, v))
    PyErr_Clear();

  Py_XDECREF(v);
}

static PyObject *
strerror(PyObject *self, PyObject *args)
{
  ups_status_t st;

  if (!PyArg_ParseTuple(args, "i:strerror", &st))
    return (0);

  return (Py_BuildValue("s", ups_strerror(st)));
}

static PyObject *
is_debug(PyObject *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":is_debug"))
    return (0);

  return (Py_BuildValue("i", ups_is_debug() == true ? 1 : 0));
}

static PyObject *
get_version(PyObject *self, PyObject *args)
{
  char buffer[64];
  uint32_t major, minor, revision;

  if (!PyArg_ParseTuple(args, ":get_version"))
    return (0);

  ups_get_version(&major, &minor, &revision);
  sprintf(buffer, "%u.%u.%u", major, minor, revision);
  return (Py_BuildValue("s", buffer));
}

static void
errhandler(int level, const char *message)
{
  PyObject *args = Py_BuildValue("(is)", level, message);
  (void)PyEval_CallObject(g_errhandler, args);
  Py_DECREF(args);
}

static PyObject *
set_error_handler(PyObject *self, PyObject *args)
{
  PyObject *cb = 0;

  if (!PyArg_ParseTuple(args, "O:set_error_handler", &cb))
    return (0);

  if (cb == Py_None) {
    Py_XDECREF(g_errhandler);
    ups_set_error_handler(0);
    return (Py_BuildValue(""));
  }

  if (!PyCallable_Check(cb)) {
    PyErr_SetString(PyExc_TypeError, "parameter must be callable");
    return (0);
  }

  Py_XDECREF(g_errhandler);
  g_errhandler = 0;

  Py_XINCREF(cb);
  g_errhandler = cb;
  ups_set_error_handler(errhandler);
  return (Py_BuildValue(""));
}

/* all exported static functions */
static PyMethodDef upscaledb_methods[] = {
  {"get_version", (PyCFunction)get_version, METH_VARARGS, 
      "returns the version of the upscaledb library"},
  {"strerror", (PyCFunction)strerror, METH_VARARGS, 
      "returns a descriptive error string"},
  {"set_error_handler", (PyCFunction)set_error_handler, METH_VARARGS, 
      "sets the global error handler callback function"},
  {"is_debug", (PyCFunction)is_debug, METH_VARARGS, 
      "checks if the library was built for debugging"},
  {"env", (PyCFunction)construct_env, METH_VARARGS, 
      "creates a new Environment object"},
  {"db", (PyCFunction)construct_db, METH_VARARGS, 
      "creates a new Database object"},
  {"cursor", (PyCFunction)construct_cursor, METH_VARARGS, 
      "creates a new Cursor object"},
  {"txn", (PyCFunction)construct_txn, METH_VARARGS, 
      "creates a new Transaction object"},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};

statichere PyTypeObject HamEnvironment_Type = {
    PyObject_HEAD_INIT(NULL)
    0,          /*ob_size*/
    "upscaledb.env",  /*tp_name*/
    sizeof(HamEnvironment),   /*tp_basicsize*/
    0,          /*tp_itemsize*/
    /* methods */
    (destructor)env_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    (getattrfunc)env_getattr, /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash*/
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,     /* tp_flags */
    "upscaledb Environment"  /* tp_doc */
};

static PyObject *
construct_env(PyObject *self, PyObject *args)
{
  HamEnvironment *env = PyObject_New(HamEnvironment, &HamEnvironment_Type);
  if (!env)
    return (0);

  env->env = 0;
  env->dblist = 0;
  return ((PyObject *)env);
}
  
statichere PyTypeObject HamDatabase_Type = {
    PyObject_HEAD_INIT(NULL)
    0,          /*ob_size*/
    "db",       /*tp_name*/
    sizeof(HamDatabase),   /*tp_basicsize*/
    0,          /*tp_itemsize*/
    /* methods */
    (destructor)db_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    (getattrfunc)db_getattr, /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash*/
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,     /* tp_flags */
    "upscaledb Database"  /* tp_doc */
};

static PyObject *
construct_db(PyObject *self, PyObject *args)
{
  HamDatabase *hdb = PyObject_New(HamDatabase, &HamDatabase_Type);
  if (!hdb)
    return (0);

  hdb->db = 0;
  hdb->flags = 0;
  hdb->comparecb = 0;
  hdb->err_type = 0;
  hdb->err_value = 0;
  hdb->err_traceback = 0;
  hdb->cursorlist = 0;

  return ((PyObject *)hdb);
}

static PyObject *
cursor_getattr(HamCursor *self, char *name)
{
  return (Py_FindMethod(HamCursor_methods, (PyObject *)self, name));
}

statichere PyTypeObject HamCursor_Type = {
    PyObject_HEAD_INIT(NULL)
    0,          /*ob_size*/
    "cursor",   /*tp_name*/
    sizeof(HamCursor),   /*tp_basicsize*/
    0,          /*tp_itemsize*/
    /* methods */
    (destructor)cursor_dealloc, /*tp_dealloc*/
    0,          /*tp_print*/
    (getattrfunc)cursor_getattr, /*tp_getattr*/
    0,          /*tp_setattr*/
    0,          /*tp_compare*/
    0,          /*tp_repr*/
    0,          /*tp_as_number*/
    0,          /*tp_as_sequence*/
    0,          /*tp_as_mapping*/
    0,          /*tp_hash*/
    0,          /*tp_call*/
    0,          /*tp_str*/
    0,          /*tp_getattro*/
    0,          /*tp_setattro*/
    0,          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,     /* tp_flags */
    "upscaledb Cursor"  /* tp_doc */
};

static PyObject *
txn_getattr(HamTransaction *self, char *name)
{
  return (Py_FindMethod(HamTransaction_methods, (PyObject *)self, name));
}

static PyObject *
construct_txn(PyObject *self, PyObject *args)
{
  HamTransaction *txn = PyObject_New(HamTransaction, &HamTransaction_Type);
  if (!txn)
    return (0);

  txn->txn = 0;
  return (txn_begin(txn, args));
}

static void
txn_dealloc(HamTransaction *self)
{
  if (!self->txn)
    return;

  ups_txn_abort(self->txn, 0);
  self->txn = 0;

  PyObject_Del(self);
}

static PyObject *
txn_begin(HamTransaction *self, PyObject *args)
{
  HamEnvironment *env = 0;

  if (!PyArg_ParseTuple(args, "O!:begin", &HamEnvironment_Type, &env))
    return (0);

  if (self->txn)
    ups_txn_abort(self->txn, 0);

  ups_status_t st = ups_txn_begin(&self->txn, env->env, 0, 0, 0);
  if (st)
    THROW(st);

  Py_INCREF(self);
  return ((PyObject *)self);
}

static PyObject *
txn_abort(HamTransaction *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":txn_abort"))
    return (0);

  ups_status_t st = ups_txn_abort(self->txn, 0);
  if (st)
    THROW(st);

  self->txn = 0;
  PyObject_Del(self);
  return (Py_BuildValue(""));
}

static PyObject *
txn_commit(HamTransaction *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":txn_commit"))
    return (0);

  ups_status_t st = ups_txn_commit(self->txn, 0);
  if (st)
    THROW(st);

  self->txn = 0;
  PyObject_Del(self);
  return (Py_BuildValue(""));
}

static PyObject *
construct_cursor(PyObject *self, PyObject *args)
{
  HamCursor *c = PyObject_New(HamCursor, &HamCursor_Type);
  if (!c)
    return (0);

  c->cursor = 0;
  return (cursor_create(c, args));
}

static PyObject *
cursor_create(HamCursor *self, PyObject *args)
{
  HamDatabase *db = 0;
  HamTransaction *txn = 0;

  if (!PyArg_ParseTuple(args, "O!|O:create", &HamDatabase_Type, &db, &txn))
    return (0);

  if (self->cursor)
    ups_cursor_close(self->cursor);

  /* check if first object is either a Transaction or None */
  if (txn && txn == (HamTransaction *)Py_None)
    txn = 0;

  ups_status_t st = ups_cursor_create(&self->cursor, db->db,
                  txn ? txn->txn : 0, 0);
  if (st)
    THROW(st);

  self->db = db;

  /* add the new cursor to the database */
  if (!db->cursorlist) {
    db->cursorlist = PyList_New(10);
    if (!db->cursorlist)
      return (0);
  }
  if (PyList_Append(db->cursorlist, (PyObject *)self))
    return (0);
  Py_INCREF(self);

  return ((PyObject *)self);
}

static PyObject *
cursor_clone(HamCursor *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":clone"))
    return (0);

  HamCursor *c = PyObject_New(HamCursor, &HamCursor_Type);
  if (!c)
    return (0);

  ups_status_t st = ups_cursor_clone(self->cursor, &c->cursor);
  if (st)
    THROW(st);

  /* add the new cursor to the database */
  HamDatabase *db = self->db;

  if (!db->cursorlist) {
    db->cursorlist = PyList_New(10);
    if (!db->cursorlist)
      return (0);
  }
  if (PyList_Append(db->cursorlist, (PyObject *)c))
      return (0);
  Py_INCREF(c);

  c->db = db;

  return ((PyObject *)c);
}

static PyObject *
cursor_insert(HamCursor *self, PyObject *args)
{
  ups_key_t key = {0};
  ups_record_t record = {0};
  uint32_t flags = 0;

  /* recno: ignore the first object */
  if (self->db->flags & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "Os#|i:insert", &temp,
              &record.data, &record.size, &flags))
      return (0);
  }
  else if (!PyArg_ParseTuple(args, "s#s#|i:insert", &key.data, &key.size,
              &record.data, &record.size, &flags))
    return (0);

  ups_status_t st = ups_cursor_insert(self->cursor, &key, &record, flags);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
cursor_find(HamCursor *self, PyObject *args)
{
  ups_key_t key = {0};
  ups_record_t record = {0};
  uint32_t recno32;
  uint64_t recno64;

  /* recno: first object is an integer */
  if (self->db->flags & UPS_RECORD_NUMBER32) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "O!:find", &PyInt_Type, &temp))
      return (0);
    recno32 = (uint32_t)PyInt_AsLong(temp);
    key.data = &recno32;
    key.size = sizeof(recno32);
  }
  else if (self->db->flags & UPS_RECORD_NUMBER64) {
    PyObject *temp;
    if (!PyArg_ParseTuple(args, "O!:find", &PyInt_Type, &temp))
      return (0);
    recno64 = PyInt_AsLong(temp);
    key.data = &recno64;
    key.size = sizeof(recno64);
  }
  else if (!PyArg_ParseTuple(args, "s#:find", &key.data, &key.size))
    return (0);

  ups_status_t st = ups_cursor_find(self->cursor, &key, &record, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue("s#", record.data, record.size));
}

static PyObject *
cursor_erase(HamCursor *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":erase"))
    return (0);

  ups_status_t st = ups_cursor_erase(self->cursor, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
cursor_move_to(HamCursor *self, PyObject *args)
{
  uint32_t flags;

  if (!PyArg_ParseTuple(args, "i:move_to", &flags))
    return (0);

  ups_status_t st = ups_cursor_move(self->cursor, 0, 0, flags);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}

static PyObject *
cursor_get_key(HamCursor *self, PyObject *args)
{
  ups_key_t key = {0};
  HamDatabase *db = self->db;

  if (!PyArg_ParseTuple(args, ":get_key"))
    return (0);

  ups_status_t st = ups_cursor_move(self->cursor, &key, 0, 0);
  if (st)
    THROW(st);

  /* recno: return int, otherwise string */
  if (db->flags & UPS_RECORD_NUMBER32) {
    uint32_t recno = *(uint32_t *)key.data;
    return (Py_BuildValue("i", (int)recno));
  }
  if (db->flags & UPS_RECORD_NUMBER64) {
    uint64_t recno = *(uint64_t *)key.data;
    return (Py_BuildValue("i", (int)recno));
  }
  return (Py_BuildValue("s#", key.data, key.size));
}

static PyObject *
cursor_get_record(HamCursor *self, PyObject *args)
{
  ups_record_t record = {0};

  if (!PyArg_ParseTuple(args, ":get_record"))
    return (0);

  ups_status_t st = ups_cursor_move(self->cursor, 0, &record, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue("s#", record.data, record.size));
}

static PyObject *
cursor_overwrite(HamCursor *self, PyObject *args)
{
  ups_record_t record = {0};

  if (!PyArg_ParseTuple(args, "s#:overwrite", &record.data, &record.size))
    return (0);

  ups_status_t st = ups_cursor_overwrite(self->cursor, &record, 0);
  if (st)
    THROW(st);
  return (Py_BuildValue(""));
}


static PyObject *
cursor_get_duplicate_count(HamCursor *self, PyObject *args)
{
  uint32_t count = 0;

  if (!PyArg_ParseTuple(args, ":get_duplicate_count"))
      return (0);

  ups_status_t st = ups_cursor_get_duplicate_count(self->cursor, &count, 0);
  if (st)
    THROW(st);

  return (Py_BuildValue("i", count));
}

static PyObject *
cursor_get_duplicate_position(HamCursor *self, PyObject *args)
{
  uint32_t position = 0;

  if (!PyArg_ParseTuple(args, ":get_duplicate_position"))
      return (0);

  ups_status_t st = ups_cursor_get_duplicate_position(self->cursor, &position);
  if (st)
    THROW(st);

  return (Py_BuildValue("i", position));
}

static PyObject *
cursor_get_record_size(HamCursor *self, PyObject *args)
{
  uint64_t size = 0;

  if (!PyArg_ParseTuple(args, ":get_record_size"))
      return (0);

  ups_status_t st = ups_cursor_get_record_size(self->cursor, &size);
  if (st)
    THROW(st);

  return (Py_BuildValue("i", (uint32_t)size));
}

static PyObject *
cursor_close(HamCursor *self, PyObject *args)
{
  if (!PyArg_ParseTuple(args, ":close"))
    return (0);

  ups_status_t st = ups_cursor_close(self->cursor);
  if (st)
    THROW(st);
  self->cursor = 0;
  return (Py_BuildValue(""));
}

PyMODINIT_FUNC
initupscaledb()
{
  PyObject *m = Py_InitModule3("upscaledb", upscaledb_methods, "upscaledb");
  if (!m)
    return;

  HamDatabase_Type.ob_type = &PyType_Type;
  HamEnvironment_Type.ob_type = &PyType_Type;
  HamCursor_Type.ob_type = &PyType_Type;
  HamTransaction_Type.ob_type = &PyType_Type;

  PyObject *d = PyModule_GetDict(m);
  g_exception = PyErr_NewException((char *)"upscaledb.error", NULL, NULL);
  PyDict_SetItemString(d, "error", g_exception);

  add_const(d, "UPS_TYPE_BINARY", UPS_TYPE_BINARY);
  add_const(d, "UPS_TYPE_CUSTOM", UPS_TYPE_CUSTOM);
  add_const(d, "UPS_TYPE_UINT8", UPS_TYPE_UINT8);
  add_const(d, "UPS_TYPE_UINT16", UPS_TYPE_UINT16);
  add_const(d, "UPS_TYPE_UINT32", UPS_TYPE_UINT32);
  add_const(d, "UPS_TYPE_UINT64", UPS_TYPE_UINT64);
  add_const(d, "UPS_TYPE_REAL32", UPS_TYPE_REAL32);
  add_const(d, "UPS_TYPE_REAL64", UPS_TYPE_REAL64);
  add_const(d, "UPS_SUCCESS", UPS_SUCCESS);
  add_const(d, "UPS_INV_RECORD_SIZE", UPS_INV_RECORD_SIZE);
  add_const(d, "UPS_INV_KEY_SIZE", UPS_INV_KEY_SIZE);
  add_const(d, "UPS_INV_PAGE_SIZE", UPS_INV_PAGE_SIZE);
  add_const(d, "UPS_OUT_OF_MEMORY", UPS_OUT_OF_MEMORY);
  add_const(d, "UPS_INV_PARAMETER", UPS_INV_PARAMETER);
  add_const(d, "UPS_INV_FILE_HEADER", UPS_INV_FILE_HEADER);
  add_const(d, "UPS_INV_FILE_VERSION", UPS_INV_FILE_VERSION);
  add_const(d, "UPS_KEY_NOT_FOUND", UPS_KEY_NOT_FOUND);
  add_const(d, "UPS_DUPLICATE_KEY", UPS_DUPLICATE_KEY);
  add_const(d, "UPS_INTEGRITY_VIOLATED", UPS_INTEGRITY_VIOLATED);
  add_const(d, "UPS_INTERNAL_ERROR", UPS_INTERNAL_ERROR);
  add_const(d, "UPS_WRITE_PROTECTED", UPS_WRITE_PROTECTED);
  add_const(d, "UPS_BLOB_NOT_FOUND", UPS_BLOB_NOT_FOUND);
  add_const(d, "UPS_IO_ERROR", UPS_IO_ERROR);
  add_const(d, "UPS_NOT_IMPLEMENTED", UPS_NOT_IMPLEMENTED);
  add_const(d, "UPS_FILE_NOT_FOUND", UPS_FILE_NOT_FOUND);
  add_const(d, "UPS_WOULD_BLOCK", UPS_WOULD_BLOCK);
  add_const(d, "UPS_NOT_READY", UPS_NOT_READY);
  add_const(d, "UPS_LIMITS_REACHED", UPS_LIMITS_REACHED);
  add_const(d, "UPS_ALREADY_INITIALIZED", UPS_ALREADY_INITIALIZED);
  add_const(d, "UPS_NEED_RECOVERY", UPS_NEED_RECOVERY);
  add_const(d, "UPS_CURSOR_STILL_OPEN", UPS_CURSOR_STILL_OPEN);
  add_const(d, "UPS_FILTER_NOT_FOUND", UPS_FILTER_NOT_FOUND);
  add_const(d, "UPS_TXN_CONFLICT", UPS_TXN_CONFLICT);
  add_const(d, "UPS_KEY_ERASED_IN_TXN", UPS_KEY_ERASED_IN_TXN);
  add_const(d, "UPS_TXN_STILL_OPEN", UPS_TXN_STILL_OPEN);
  add_const(d, "UPS_CURSOR_IS_NIL", UPS_CURSOR_IS_NIL);
  add_const(d, "UPS_DATABASE_NOT_FOUND", UPS_DATABASE_NOT_FOUND);
  add_const(d, "UPS_DATABASE_ALREADY_EXISTS", UPS_DATABASE_ALREADY_EXISTS);
  add_const(d, "UPS_DATABASE_ALREADY_OPEN", UPS_DATABASE_ALREADY_OPEN);
  add_const(d, "UPS_ENVIRONMENT_ALREADY_OPEN", UPS_ENVIRONMENT_ALREADY_OPEN);
  add_const(d, "UPS_LOG_INV_FILE_HEADER", UPS_LOG_INV_FILE_HEADER);
  add_const(d, "UPS_NETWORK_ERROR", UPS_NETWORK_ERROR);
  add_const(d, "UPS_DEBUG_LEVEL_DEBUG", UPS_DEBUG_LEVEL_DEBUG);
  add_const(d, "UPS_DEBUG_LEVEL_NORMAL", UPS_DEBUG_LEVEL_NORMAL);
  add_const(d, "UPS_DEBUG_LEVEL_FATAL", UPS_DEBUG_LEVEL_FATAL);
  add_const(d, "UPS_TXN_READ_ONLY", UPS_TXN_READ_ONLY);
  add_const(d, "UPS_TXN_TEMPORARY", UPS_TXN_TEMPORARY);
  add_const(d, "UPS_ENABLE_FSYNC", UPS_ENABLE_FSYNC);
  add_const(d, "UPS_READ_ONLY", UPS_READ_ONLY);
  add_const(d, "UPS_IN_MEMORY", UPS_IN_MEMORY);
  add_const(d, "UPS_DISABLE_MMAP", UPS_DISABLE_MMAP);
  add_const(d, "UPS_RECORD_NUMBER", UPS_RECORD_NUMBER64); // deprecated
  add_const(d, "UPS_RECORD_NUMBER32", UPS_RECORD_NUMBER32);
  add_const(d, "UPS_RECORD_NUMBER64", UPS_RECORD_NUMBER64);
  add_const(d, "UPS_ENABLE_DUPLICATE_KEYS", UPS_ENABLE_DUPLICATE_KEYS);
  add_const(d, "UPS_ENABLE_RECOVERY", UPS_ENABLE_RECOVERY);
  add_const(d, "UPS_AUTO_RECOVERY", UPS_AUTO_RECOVERY);
  add_const(d, "UPS_ENABLE_TRANSACTIONS", UPS_ENABLE_TRANSACTIONS);
  add_const(d, "UPS_CACHE_UNLIMITED", UPS_CACHE_UNLIMITED);
  add_const(d, "UPS_DISABLE_RECOVERY", UPS_DISABLE_RECOVERY);
  add_const(d, "UPS_IS_REMOTE_INTERNAL", UPS_IS_REMOTE_INTERNAL);
  add_const(d, "UPS_DISABLE_RECLAIM_INTERNAL", UPS_DISABLE_RECLAIM_INTERNAL);
  add_const(d, "UPS_FORCE_RECORDS_INLINE", UPS_FORCE_RECORDS_INLINE);
  add_const(d, "UPS_FLUSH_WHEN_COMMITTED", UPS_FLUSH_WHEN_COMMITTED);
  add_const(d, "UPS_ENABLE_CRC32", UPS_ENABLE_CRC32);
  add_const(d, "UPS_OVERWRITE", UPS_OVERWRITE);
  add_const(d, "UPS_DUPLICATE", UPS_DUPLICATE);
  add_const(d, "UPS_DUPLICATE_INSERT_BEFORE", UPS_DUPLICATE_INSERT_BEFORE);
  add_const(d, "UPS_DUPLICATE_INSERT_AFTER", UPS_DUPLICATE_INSERT_AFTER);
  add_const(d, "UPS_DUPLICATE_INSERT_FIRST", UPS_DUPLICATE_INSERT_FIRST);
  add_const(d, "UPS_DUPLICATE_INSERT_LAST", UPS_DUPLICATE_INSERT_LAST);
  add_const(d, "UPS_DIRECT_ACCESS", UPS_DIRECT_ACCESS);
  add_const(d, "UPS_HINT_APPEND", UPS_HINT_APPEND);
  add_const(d, "UPS_HINT_PREPEND", UPS_HINT_PREPEND);
  add_const(d, "UPS_ERASE_ALL_DUPLICATES", UPS_ERASE_ALL_DUPLICATES);
  add_const(d, "UPS_PARAM_CACHE_SIZE", UPS_PARAM_CACHE_SIZE);
  add_const(d, "UPS_PARAM_CACHESIZE", UPS_PARAM_CACHESIZE);
  add_const(d, "UPS_PARAM_PAGE_SIZE", UPS_PARAM_PAGE_SIZE);
  add_const(d, "UPS_PARAM_PAGESIZE", UPS_PARAM_PAGESIZE);
  add_const(d, "UPS_PARAM_FILE_SIZE_LIMIT", UPS_PARAM_FILE_SIZE_LIMIT);
  add_const(d, "UPS_PARAM_KEY_SIZE", UPS_PARAM_KEY_SIZE);
  add_const(d, "UPS_PARAM_KEYSIZE", UPS_PARAM_KEYSIZE);
  add_const(d, "UPS_PARAM_MAX_DATABASES", UPS_PARAM_MAX_DATABASES);
  add_const(d, "UPS_PARAM_KEY_TYPE", UPS_PARAM_KEY_TYPE);
  add_const(d, "UPS_PARAM_LOG_DIRECTORY", UPS_PARAM_LOG_DIRECTORY);
  add_const(d, "UPS_PARAM_ENCRYPTION_KEY", UPS_PARAM_ENCRYPTION_KEY);
  add_const(d, "UPS_PARAM_NETWORK_TIMEOUT_SEC", UPS_PARAM_NETWORK_TIMEOUT_SEC);
  add_const(d, "UPS_PARAM_RECORD_SIZE", UPS_PARAM_RECORD_SIZE);
  add_const(d, "UPS_RECORD_SIZE_UNLIMITED", UPS_RECORD_SIZE_UNLIMITED);
  add_const(d, "UPS_KEY_SIZE_UNLIMITED", UPS_KEY_SIZE_UNLIMITED);
  add_const(d, "UPS_PARAM_FLAGS", UPS_PARAM_FLAGS);
  add_const(d, "UPS_PARAM_FILEMODE", UPS_PARAM_FILEMODE);
  add_const(d, "UPS_PARAM_FILENAME", UPS_PARAM_FILENAME);
  add_const(d, "UPS_PARAM_DATABASE_NAME", UPS_PARAM_DATABASE_NAME);
  add_const(d, "UPS_PARAM_MAX_KEYS_PER_PAGE", UPS_PARAM_MAX_KEYS_PER_PAGE);
  add_const(d, "UPS_PARAM_JOURNAL_COMPRESSION", UPS_PARAM_JOURNAL_COMPRESSION);
  add_const(d, "UPS_PARAM_RECORD_COMPRESSION", UPS_PARAM_RECORD_COMPRESSION);
  add_const(d, "UPS_PARAM_KEY_COMPRESSION", UPS_PARAM_KEY_COMPRESSION);
  add_const(d, "UPS_COMPRESSOR_NONE", UPS_COMPRESSOR_NONE);
  add_const(d, "UPS_COMPRESSOR_ZLIB", UPS_COMPRESSOR_ZLIB);
  add_const(d, "UPS_COMPRESSOR_SNAPPY", UPS_COMPRESSOR_SNAPPY);
  add_const(d, "UPS_COMPRESSOR_LZF", UPS_COMPRESSOR_LZF);
  add_const(d, "UPS_TXN_AUTO_ABORT", UPS_TXN_AUTO_ABORT);
  add_const(d, "UPS_TXN_AUTO_COMMIT", UPS_TXN_AUTO_COMMIT);
  add_const(d, "UPS_CURSOR_FIRST", UPS_CURSOR_FIRST);
  add_const(d, "UPS_CURSOR_LAST", UPS_CURSOR_LAST);
  add_const(d, "UPS_CURSOR_NEXT", UPS_CURSOR_NEXT);
  add_const(d, "UPS_CURSOR_PREVIOUS", UPS_CURSOR_PREVIOUS);
  add_const(d, "UPS_SKIP_DUPLICATES", UPS_SKIP_DUPLICATES);
  add_const(d, "UPS_ONLY_DUPLICATES", UPS_ONLY_DUPLICATES);
}

