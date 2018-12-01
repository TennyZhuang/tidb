// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type evalLuaFunctionClass struct {
	baseFunctionClass
}

type builtinEvalLuaSig struct {
	baseBuiltinFunc
}

func (c *evalLuaFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := []types.EvalType{types.ETString, types.ETJson}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinEvalLuaSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_EvalLua)
	return sig, nil

}

func (c *builtinEvalLuaSig) Clone() builtinFunc {
	newSig := &builtinEvalLuaSig{}
	newSig.cloneFrom(&c.baseBuiltinFunc)
	return newSig
}

// func (b *builtinEvalLuaSig) evalReal(row chunk.Row) (float64, bool, error) {
// 	panic("Not implemented")
// }

func (b *builtinEvalLuaSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	panic("Should push down")
}
