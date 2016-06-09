/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFS3_WRAPPER_H
#define LIBHDFS3_WRAPPER_H

#include "Adaptor.hh"

DIAGNOSTIC_IGNORE("-Wconversion")

DIAGNOSTIC_PUSH

DIAGNOSTIC_IGNORE("-Wdocumentation-deprecated-sync")
DIAGNOSTIC_IGNORE("-Wdocumentation")

#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Wshorten-64-to-32")
  DIAGNOSTIC_IGNORE("-Wreserved-id-macro")
#endif

#include "hdfs/hdfs.h"

DIAGNOSTIC_POP

#endif
