from distutils.core import setup, Extension

setup(name = 'MinIO',
      version = '1.0',
      description = 'Python MinIO file cache module.',
      author = 'Gus Waldspurger',
      author_email = 'gus@waldspurger.com',
      ext_modules = [
            Extension('minio', sources = ['csrc/miniomodule/miniomodule.c',
                                          'csrc/minio/minio.c'])
      ])