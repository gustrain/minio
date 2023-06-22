/* Author:  Gus Waldspurger

   Python wrapper for MinIO cache.
   */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "../minio/minio.h"


/* Python wrapper for cache_t type. */
typedef struct {
    PyObject_HEAD

    cache_t  cache;          /* MinIO cache. */
    size_t   max_file_size;  /* Max file size we allow in this cache. */
    uint8_t *temp;           /* MAX_FILE_SIZE bytes used for copying. */
} PyCache;

/* PyCache deallocate method. */
static void
PyCache_dealloc(PyObject *self)
{
    PyCache *cache = (PyCache *) self;

    /* Free the memory allocate for the cache region. */
    if (cache->cache.data != NULL) {
        free(cache->cache.data);
    }

    /* Free the cache struct itself. */
    Py_TYPE(cache)->tp_free((PyObject *) cache);
}

/* PyCache initialization method. */
static int
PyCache_init(PyObject *self, PyObject *args, PyObject *kwds)
{
    PyCache *cache = (PyCache *) self;

    /* Parse arguments. */
    int size, max_file_size;
    static char *kwlist[] = {"size", "max_file_size", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ii", kwlist, &size, &max_file_size)) {
        PyErr_SetString(PyExc_Exception, "missing argument");
        return -1;
    }

    /* Set up the copy area. */
    cache->max_file_size = max_file_size;
    if ((cache->temp = malloc(max_file_size)) == NULL) {
        PyErr_SetString(PyExc_MemoryError, "couldn't allocate temp area");
        PyErr_NoMemory();
        return -1;
    }

    /* Initialize the cache. */
    int status = cache_init(&cache->cache, size, POLICY_MINIO);
    if (status < 0) {
        switch (status) {
            case -ENOMEM:
                PyErr_SetString(PyExc_MemoryError, "couldn't allocate cache");
                break;
            case -EPERM:
                PyErr_SetString(PyExc_PermissionError, "couldn't pin cache memory");
                break;
        }

        return -1;
    }

    return 0;
}

/* PyCache creation method. */
static PyObject *
PyCache_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    /* Allocate the PyCache struct. */
    PyCache *self;
    if ((self = (PyCache *) type->tp_alloc(type, 0)) == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    return (PyObject *) self;
}

/* PyCache read/get method. */
static PyObject *
PyCache_read(PyCache *self, PyObject *args, PyObject *kwds)
{
    /* Parse arguments. */
    char *filepath;
    static char *kwlist[] = {"filepath", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s", kwlist, &filepath)) {
        PyErr_SetString(PyExc_Exception, "missing argument");
        return NULL;
    }

    /* Get the file contents. */
    ssize_t size = cache_read(&self->cache,
                             filepath,
                             self->temp,
                             self->max_file_size);
    if (size < 0l) {
        switch (size) {
            case -EINVAL:
                PyErr_SetString(PyExc_MemoryError, "insufficient buffer size");
                break;
            case -ENOMEM:
                PyErr_SetString(PyExc_MemoryError, "unable to allocate hash table entry");
                break;
            case -ENOENT:
                PyErr_SetString(PyExc_FileNotFoundError, filepath);
                break;
            default:
                PyErr_SetString(PyExc_Exception, "unknown exception");
                break;
        }

        return NULL;
    }
    
    return PyBytes_FromStringAndSize((char *) self->temp, size);
}

/* PyCache method to flush the cache. */
static PyObject *
PyCache_flush(PyCache *self, PyObject *args, PyObject *kwds)
{
    cache_flush(&self->cache);

    return PyLong_FromLong(0);
}

/* PyCache method to get the cache's "size" field. */
static PyObject *
PyCache_get_size(PyCache *self, PyObject *args, PyObject *kwds)
{
    return PyLong_FromLong(self->cache.size);
}

/* PyCache method to get the cache's "used" field. */
static PyObject *
PyCache_get_used(PyCache *self, PyObject *args, PyObject *kwds)
{
    return PyLong_FromLong(self->cache.used);
}

/* PyCache methods. */
static PyMethodDef PyCache_methods[] = {
    {"read_file", (PyCFunction) PyCache_read, METH_VARARGS | METH_KEYWORDS, "Read a file through the cache."},
    {"flush", (PyCFunction) PyCache_flush, METH_NOARGS, "Flush the cache."},
    {"get_size", (PyCFunction) PyCache_get_size, METH_NOARGS, "Get size of cache in bytes."},
    {"get_used", (PyCFunction) PyCache_get_used, METH_NOARGS, "Get number of bytes used in cache."},
    {NULL} /* Sentinel. */
};

static PyTypeObject PythonCacheType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "minio.PythonCache",
    .tp_doc = PyDoc_STR("MinIO Python cache"),
    .tp_basicsize = sizeof(PyCache),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,

    /* Methods. */
    .tp_dealloc = PyCache_dealloc,
    .tp_new = PyCache_new,
    .tp_init = PyCache_init,
    .tp_methods = PyCache_methods,
};


static PyMethodDef CacheMethods[] = {
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef miniomodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "minio",
    .m_doc = "Python module to implement a MinIO file cache.",
    .m_size = -1,
    .m_methods = CacheMethods,
};

PyMODINIT_FUNC
PyInit_minio(void)
{
    PyObject *module;
    
    /* Check PythonCacheType is ready. */
    if (PyType_Ready(&PythonCacheType) < 0) {
        return NULL;
    }

    /* Create Python module. */
    if ((module = PyModule_Create(&miniomodule)) == NULL) {
        return NULL;
    }

    /* Add the PythonCacheType type. */
    Py_INCREF(&PythonCacheType);
    if (PyModule_AddObject(module, "PyCache", (PyObject *) &PythonCacheType) < 0) {
        Py_DECREF(&PythonCacheType);
        Py_DECREF(module);
        return NULL;
    }

    return module;
}