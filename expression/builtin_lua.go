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
	// "fmt"
	"errors"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/yuin/gopher-lua"
)

type luaFunctionClass struct {
	baseFunctionClass
}

type Argument struct {
	Name string
	Tp   types.EvalType
}

type LuaFunc struct {
	Name  string
	Body  string
	Args  []Argument
	RetTp types.EvalType
}

type builtinLuaSig struct {
	baseBuiltinFunc
	l *LuaFunc
}

var LuaFunctionMap = make(map[string]*LuaFunc)

func (c *luaFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	funcName, isNull, err := args[0].EvalString(ctx, chunk.Row{}) // Constant will not use row
	if err != nil {
		return nil, errors.Trace(err)
	}
	var errFuncNotFound = errors.New("No such function")
	if isNull {
		return nil, errors.Trace(errFuncNotFound)
	}
	f, ok := LuaFunctionMap[funcName]
	if !ok {
		return nil, errors.Trace(errFuncNotFound)
	}
	if len(args) != len(f.Args)+1 {
		return nil, errors.Trace(errors.New("Invalid number of argument"))
	}
	argTps := []types.EvalType{types.ETString}
	for _, arg := range f.Args {
		argTps = append(argTps, arg.Tp)
	}
	// argTps := []types.EvalType{types.ETString, types.ETString}
	bf := newBaseBuiltinFuncWithTp(ctx, args, f.RetTp, argTps...)
	sig := &builtinLuaSig{bf, f}
	sig.setPbCode(tipb.ScalarFuncSig_Lua)
	return sig, nil
}

func (c *builtinLuaSig) Clone() builtinFunc {
	newSig := &builtinLuaSig{}
	newSig.cloneFrom(&c.baseBuiltinFunc)
	newSig.l = c.l
	return newSig
}

// func (b *builtinLuaSig) evalReal(row chunk.Row) (float64, bool, error) {
// 	panic("Not implemented")
// }

func (b *builtinLuaSig) evalLua(row chunk.Row) (res lua.LValue, isNull bool, err error) {
	L := lua.NewState()
	defer L.Close()

	for i, arg := range b.l.Args {
		switch arg.Tp {
		case types.ETString:
			s, subIsNull, err := b.args[i+1].EvalString(b.ctx, row)
			if err != nil {
				return nil, subIsNull, err
			}
			L.SetGlobal(arg.Name, lua.LString(s))

		case types.ETInt:
			itg, subIsNull, err := b.args[i+1].EvalInt(b.ctx, row)
			if err != nil {
				return nil, subIsNull, err
			}
			L.SetGlobal(arg.Name, lua.LNumber(float64(itg)))

		case types.ETReal:
			r, subIsNull, err := b.args[i+1].EvalReal(b.ctx, row)
			if err != nil {
				return nil, subIsNull, err
			}
			L.SetGlobal(arg.Name, lua.LNumber(r))

		case types.ETDecimal:
			d, subIsNull, err := b.args[i+1].EvalDecimal(b.ctx, row)
			if err != nil {
				return nil, subIsNull, err
			}
			f, err := d.ToFloat64()
			if err != nil {
				return nil, true, err
			}
			L.SetGlobal(arg.Name, lua.LNumber(f))
		}
	}

	if err := L.DoString(b.l.Body); err != nil {
		return nil, false, err
	}

	v := L.GetGlobal("result")
	if v == nil {
		return nil, false, errors.New("result not found")
	}
	return v, false, nil
}

func (b *builtinLuaSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	v, isNull, err := b.evalLua(row)
	return int64(v.(lua.LNumber)), isNull, err
}

func (b *builtinLuaSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	v, isNull, err := b.evalLua(row)
	return float64(v.(lua.LNumber)), isNull, err
}

func (b *builtinLuaSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	v, isNull, err := b.evalLua(row)
	return string(v.(lua.LString)), isNull, err
}

// func (b *builtinLuaSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
// 	v, isNull, err := b.evalLua(row)
// 	return types.float64(v.(lua.LNumber)), isNull, err
// }
