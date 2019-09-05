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

#include "target/core/de10/de10_compiler.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <map>
#include <unistd.h>
#include "common/sockstream.h"
#include "target/compiler.h"
#include "target/core/de10/de10_rewrite.h"
#include "verilog/analyze/evaluate.h"
#include "verilog/analyze/module_info.h"
#include "verilog/ast/ast.h"

using namespace std;

namespace cascade {

constexpr auto HW_REGS_BASE = 0xfc000000u;
constexpr auto HW_REGS_SPAN = 0x04000000u;
constexpr auto HW_REGS_MASK = HW_REGS_SPAN - 1;
constexpr auto ALT_LWFPGALVS_OFST = 0xff200000u;
constexpr auto LED_PIO_BASE = 0x00003000u;
constexpr auto PAD_PIO_BASE = 0x00004000u;
constexpr auto GPIO_PIO_BASE = 0x00005000u;
constexpr auto LOG_PIO_BASE = 0x00040000u;

De10Compiler::De10Compiler() : CoreCompiler() { 
  fd_ = open("/dev/mem", (O_RDWR | O_SYNC));
  if (fd_ == -1) {
    virtual_base_ = reinterpret_cast<volatile uint8_t*>(MAP_FAILED);
  } else {
    virtual_base_ = reinterpret_cast<volatile uint8_t*>(mmap(nullptr, HW_REGS_SPAN, (PROT_READ|PROT_WRITE), MAP_SHARED, fd_, HW_REGS_BASE));
  }

  sequence_ = 0;
  slots_.resize(4, {0, State::FREE, ""});

  set_host("localhost"); 
  set_port(9900);
}

De10Compiler::~De10Compiler() {
  // Close the descriptor, and unmap the memory associated with the fpga
  if (fd_ != -1) {
    close(fd_);
  }
  if (virtual_base_ != MAP_FAILED) {
    munmap(reinterpret_cast<void*>(const_cast<uint8_t*>(virtual_base_)), HW_REGS_SPAN);
  }
}

De10Compiler& De10Compiler::set_host(const string& host) {
  host_ = host;
  return *this;
}

De10Compiler& De10Compiler::set_port(uint32_t port) {
  port_ = port;
  return *this;
}

void De10Compiler::release(size_t slot) {
  // Return this slot to the pool if necessary. No need to interrupt or abort
  // other compilations. 
  lock_guard<mutex> lg(lock_);
  slots_[slot].state = State::FREE;
}

void De10Compiler::stop_compile(Engine::Id id) {
  // Variables which cross critical sections
  size_t this_sequence = 0;

  // Record an error and return if we can't connect to the quartus server
  sockstream sock(host_.c_str(), port_);
  if (sock.error()) {
    get_compiler()->error("Unable to connect to quartus compilation server");
    return;
  }

  // Return this slot to the pool. If there are any other active slots, we'll
  // need to advance the sequence counter and initiate a compilation.
  { lock_guard<mutex> lg(lock_);
    auto any_active = false;
    for (auto& s : slots_) {
      if (s.id == id) {
        s.state = State::FREE;
      } 
      if (s.state == State::WAITING) {
        any_active = true;
      }
    }
    if (!any_active) {
      return;
    }
    this_sequence = ++sequence_;
    compile(&sock);
  }

  // Compilation can take a long time, so block on completion outside of
  // the critical section.
  const auto cache_id = block_on_compile(&sock);

  // If compilation returned failure or the sequence number has been advanced,
  // another thread has taken over compilation and we can return. Otherwise, we
  // can reporgram the device and alert any threads that are waiting.
  { lock_guard<mutex> lg(lock_);
    if ((cache_id != -1) && (this_sequence == sequence_)) {
      reprogram(&sock, cache_id);
    }
  }
}

void De10Compiler::stop_async() {
  // Does nothing. This class does not schedule any asynchronous tasks.
}

De10Gpio* De10Compiler::compile_gpio(Engine::Id id, ModuleDeclaration* md, Interface* interface) {
  (void) id;

  if (virtual_base_ == MAP_FAILED) {
    get_compiler()->error("De10 gpio compilation failed due to inability to memory map device");
    delete md;
    return nullptr;
  }
  if (!check_io(md, 8, 8)) {
    get_compiler()->error("Unable to compile a de10 gpio with more than 8 outputs");
    delete md;
    return nullptr;
  }

  volatile uint8_t* led_addr = virtual_base_+((ALT_LWFPGALVS_OFST + GPIO_PIO_BASE) & HW_REGS_MASK);
  if (!ModuleInfo(md).inputs().empty()) {
    const auto* in = *ModuleInfo(md).inputs().begin();
    const auto iid = to_vid(in);
    delete md;
    return new De10Gpio(interface, iid, led_addr);
  } else {
    delete md;
    return new De10Gpio(interface, nullid(), led_addr);
  }
}

De10Led* De10Compiler::compile_led(Engine::Id id, ModuleDeclaration* md, Interface* interface) {
  (void) id;

  if (virtual_base_ == MAP_FAILED) {
    get_compiler()->error("De10 led compilation failed due to inability to memory map device");
    delete md;
    return nullptr;
  }
  if (!check_io(md, 8, 8)) {
    get_compiler()->error("Unable to compile a de10 led with more than 8 outputs");
    delete md;
    return nullptr;
  }

  volatile uint8_t* led_addr = virtual_base_+((ALT_LWFPGALVS_OFST + LED_PIO_BASE) & HW_REGS_MASK);
  if (!ModuleInfo(md).inputs().empty()) {
    const auto* in = *ModuleInfo(md).inputs().begin();
    const auto iid = to_vid(in);
    delete md;
    return new De10Led(interface, iid, led_addr);
  } else {
    delete md;
    return new De10Led(interface, nullid(), led_addr);
  }
}

De10Logic* De10Compiler::compile_logic(Engine::Id id, ModuleDeclaration* md, Interface* interface) {
  // Variables which cross critical sections
  De10Logic* de = nullptr;
  size_t this_sequence = 0;
  int slot = -1;

  // Record an error and return if we can't connect to the quartus server
  sockstream sock(host_.c_str(), port_);
  if (sock.error()) {
    get_compiler()->error("Unable to connect to quartus compilation server");
    return nullptr;
  }

  // Find a new slot and generate code for this module. If either step fails,
  // return nullptr. Otherwise, we'll need to advance the sequence counter and
  // initiate a compilation.
  { lock_guard<mutex> lg(lock_);
    for (size_t i = 0, ie = slots_.size(); i < ie; ++i) {
      if (slots_[i].state == State::FREE) {
        slot = i;
        break;
      }
    }
    if (slot == -1) {
      get_compiler()->error("No remaining slots available on de10 fabric");
      return nullptr;
    }

    // Create a new core with address identity based on module id
    volatile uint8_t* addr = virtual_base_+((ALT_LWFPGALVS_OFST + LOG_PIO_BASE) & HW_REGS_MASK) + (slot << 14);
    de = new De10Logic(interface, md, addr, this, slot);

    // Register inputs, state, and outputs. Invoke these methods
    // lexicographically to ensure a deterministic variable table ordering. The
    // final invocation of index_tasks is lexicographic by construction, as
    // it's based on a recursive descent of the AST.
    ModuleInfo info(md);
    map<VId, const Identifier*> is;
    for (auto* i : info.inputs()) {
      is.insert(make_pair(to_vid(i), i));
    }
    for (const auto& i : is) {
      de->set_input(i.second, i.first);
    }
    map<VId, const Identifier*> ss;
    for (auto* s : info.stateful()) {
      ss.insert(make_pair(to_vid(s), s));
    }
    for (const auto& s : ss) {
      de->set_state(s.second, s.first);
    }
    map<VId, const Identifier*> os;
    for (auto* o : info.outputs()) {
      os.insert(make_pair(to_vid(o), o));
    }
    for (const auto& o : os) {
      de->set_output(o.second, o.first);
    }
    de->index_tasks();

    // Check table and index sizes. If this program uses too much state, we won't
    // be able to uniquely name its elements using our current addressing scheme.
    if (de->get_table().size() >= 0x1000) {
      get_compiler()->error("Unable to compile a module with more than 4096 entries in variable table");
      delete de;
      return nullptr;
    }

    // Update this core's slot state.
    slots_[slot].id = id;
    slots_[slot].state = State::WAITING;
    slots_[slot].text = De10Rewrite().run(md, de, slot);

    this_sequence = ++sequence_;
    compile(&sock);
  }

  // Compilation can take a long time, so block on completion outside of
  // the critical section.
  const auto cache_id = block_on_compile(&sock);

  // If compilation returned failure or the sequence number has been advanced,
  // another thread has taken over compilation and we'll have to wait .
  // Otherwise, we can reporgram the device and alert any threads that are
  // waiting.
  { unique_lock<mutex> ul(lock_);
    if ((cache_id != -1) && (this_sequence == sequence_)) {
      reprogram(&sock, cache_id);
    } else {
      while (slots_[slot].state == State::WAITING) {
        cv_.wait(ul);
      }
    }
  }
  return de;
}

De10Pad* De10Compiler::compile_pad(Engine::Id id, ModuleDeclaration* md, Interface* interface) {
  (void) id;

  if (virtual_base_ == MAP_FAILED) {
    get_compiler()->error("De10 pad compilation failed due to inability to memory map device");
    delete md;
    return nullptr;
  }
  if (!check_io(md, 0, 4)) {
    get_compiler()->error("Unable to compile a de10 pad with more than 4 inputs");
    delete md;
    return nullptr;
  }

  volatile uint8_t* pad_addr = virtual_base_+((ALT_LWFPGALVS_OFST + PAD_PIO_BASE) & HW_REGS_MASK);
  const auto* out = *ModuleInfo(md).outputs().begin();
  const auto oid = to_vid(out);
  const auto w = Evaluate().get_width(out);
  delete md;

  return new De10Pad(interface, oid, w, pad_addr);
}

void De10Compiler::compile(sockstream* sock) {
  (void) sock;
}

int De10Compiler::block_on_compile(sockstream* sock) {
  (void) sock;
  return -1;
}

void De10Compiler::reprogram(sockstream* sock, size_t id) {
  (void) sock;
  (void) id;
}

} // namespace cascade
