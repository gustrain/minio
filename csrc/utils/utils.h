/* MIT License

   Copyright (c) 2023 Gus Waldspurger

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in all
   copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
   */

#ifndef __UTILS_H__

#include <stdio.h>

#define DEBUG 0
#define DEBUG_LOG(fmt, ...) \
    do { if (DEBUG) fprintf(stderr, "[%8s:%-5d] " fmt, __FILE__, \
                            __LINE__, ## __VA_ARGS__); } while (0)

#define ALT_DEBUG 1
#define ALT_DEBUG_LOG(fmt, ...) \
    do { if (ALT_DEBUG) fprintf(stderr, "[%8s:%-5d] " fmt, __FILE__, \
                                __LINE__, ## __VA_ARGS__); } while (0)


void *mmap_alloc(size_t size);
void mmap_free(void *ptr, size_t size)

#endif
