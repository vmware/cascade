// Copyright 2017-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-2-Clause
//
// The BSD-2 license (the License) set forth below applies to all parts of the
// Cascade project.  You may not use this file except in compliance with the
// License.
//
// BSD-2 License
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
// list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS AS IS AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef CASCADE_SRC_TARGET_CORE_AVMM_AVMM_COMPILER_H
#define CASCADE_SRC_TARGET_CORE_AVMM_AVMM_COMPILER_H

#include <condition_variable>
#include <mutex>
#include <stdint.h>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include "cascade/cascade.h"
#include "target/core_compiler.h"
#include "target/core/avmm/avmm_logic.h"
#include "target/core/avmm/syncbuf.h"

namespace cascade {

class sockstream;

namespace avmm {

class AvmmCompiler : public CoreCompiler {
  public:
    AvmmCompiler();
    ~AvmmCompiler() override;

    void release(size_t slot);
    void stop_compile(Engine::Id id) override;

  private:
    // Compilation States:
    enum class State : uint8_t {
      FREE = 0,
      COMPILING,
      WAITING,
      STOPPED,
      CURRENT
    };
    // Slot Information:
    struct Slot {
      Engine::Id id;
      State state;
      std::string text;
    };

    // State:
    Cascade* caslib_;
    syncbuf requests_;
    syncbuf responses_;

    // Program Management:
    std::mutex lock_;
    std::condition_variable cv_;
    std::vector<Slot> slots_;

    // Compiler Interface:
    AvmmLogic* compile_logic(Engine::Id id, ModuleDeclaration* md, Interface* interface) override;

    // Compilation Helpers:
    bool id_in_use(Engine::Id id) const;
    int get_free_slot() const;
    void compile();
    void reprogram();
    void kill_all();
};

} // namespace avmm
} // namespace cascade

#endif