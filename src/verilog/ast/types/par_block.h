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

#ifndef CASCADE_SRC_VERILOG_AST_PAR_BLOCK_H
#define CASCADE_SRC_VERILOG_AST_PAR_BLOCK_H

#include "verilog/ast/types/block_statement.h"
#include "verilog/ast/types/declaration.h"
#include "verilog/ast/types/identifier.h"
#include "verilog/ast/types/macro.h"
#include "verilog/ast/types/scope.h"
#include "verilog/ast/types/statement.h"

namespace cascade {

class ParBlock : public BlockStatement {
  public:
    // Constructors:
    ParBlock();
    explicit ParBlock(Statement* stmt__);
    template <typename DeclsItr, typename StmtsItr>
    ParBlock(Identifier* id__, DeclsItr decls_begin__, DeclsItr decls_end__, StmtsItr stmts_begin__, StmtsItr stmts_end__);
    ~ParBlock() override;

    // Node Interface:
    NODE(ParBlock)
    ParBlock* clone() const override;

    // Get/Set:
    MANY_GET_SET(ParBlock, Declaration, decls)
    MANY_GET_SET(ParBlock, Statement, stmts)

  private:
    MANY_ATTR(Declaration, decls);
    MANY_ATTR(Statement, stmts);

    friend class Navigate;
    DECORATION(Scope, scope_idx);
};

inline ParBlock::ParBlock() : BlockStatement(Node::Tag::par_block) {
  MAYBE_DEFAULT_SETUP(id);
  MANY_DEFAULT_SETUP(decls);
  MANY_DEFAULT_SETUP(stmts);
  parent_ = nullptr;
  scope_idx_.next_supdate_ = 0;
}

inline ParBlock::ParBlock(Statement* stmt__) : ParBlock() {
  push_back_stmts(stmt__);
}

template <typename DeclsItr, typename StmtsItr>
inline ParBlock::ParBlock(Identifier* id__, DeclsItr decls_begin__, DeclsItr decls_end__, StmtsItr stmts_begin__, StmtsItr stmts_end__) : ParBlock() {
  MAYBE_SETUP(id);
  MANY_SETUP(decls);
  MANY_SETUP(stmts);
}

inline ParBlock::~ParBlock() {
  MAYBE_TEARDOWN(id);
  MANY_TEARDOWN(decls);
  MANY_TEARDOWN(stmts);
}

inline ParBlock* ParBlock::clone() const {
  auto* res = new ParBlock();
  MAYBE_CLONE(id);
  MANY_CLONE(decls);
  MANY_CLONE(stmts);
  return res;
}

} // namespace cascade 

#endif
