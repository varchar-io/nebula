/*
 * Copyright 2017-present Shawn Cao
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <glog/logging.h>

#include "common/Finally.h"
#include "common/Hash.h"
#include "surface/DataSurface.h"

#include <quickjs.h>
extern "C" {
#include <quickjs-libc.h>
}

/**
 * Define script evaluation module.
 * This interface will be used by compute engine directly.
 *
 * It's asked to execute a piece of script code (ES2015/ES6) through quickjs engine.
 * The input will be binding with a native row object through which we can read column value for computation.
 */
namespace nebula {
namespace surface {
namespace eval {

class ScriptContext final {
  using RowGetter = std::function<const nebula::surface::RowData&()>;
  using ColTyper = std::function<nebula::type::Kind(const std::string&)>;

  static JSValue eval_buf(JSContext* ctx, const std::string& buf, const std::string& file, int eval_flags) noexcept {
    JSValue val;

    if ((eval_flags & JS_EVAL_TYPE_MASK) == JS_EVAL_TYPE_MODULE) {
      // for the modules, we compile then run to be able to set import.meta
      val = JS_Eval(ctx, buf.c_str(), buf.size(), file.c_str(),
                    eval_flags | JS_EVAL_FLAG_COMPILE_ONLY);
      if (!JS_IsException(val)) {
        js_module_set_import_meta(ctx, val, 1, 1);
        val = JS_EvalFunction(ctx, val);
      }
    } else {
      val = JS_Eval(ctx, buf.data(), buf.size(), file.c_str(), eval_flags);
    }

    if (JS_IsException(val)) {
      // js_std_dump_error(ctx);
      return JS_EXCEPTION;
    }

    return val;
  }

  static inline JSValue eval_buf(JSContext* ctx, const std::string& buf) noexcept {
    return eval_buf(ctx, buf, "<script>", 0);
  }

  template <typename T>
  static T to(JSContext* ctx, const JSValue& val) noexcept {
    // the single parameter is the column name to extract value
    const char* str = JS_ToCString(ctx, val);
    if (!str) {
      return {};
    }

    T t(str);
    JS_FreeCString(ctx, str);
    return t;
  }

  static JSValue js_column(JSContext* ctx, JSValue, int, JSValue* args) {
    auto col = to<std::string>(ctx, args[0]);
    if (col.size() == 0) {
      return JS_NULL;
    }

    // let's use JS_SetContextOpaque/JS_GetContextOpaque to pass script context object
    // invoke native impl based on the column type
    // getter_().read
    ScriptContext* script = (ScriptContext*)JS_GetContextOpaque(ctx);
    const auto& r = script->row();
    if (r.isNull(col)) {
      return JS_NULL;
    }

    // read typed value based on the column type and set back to JS runtime
    auto kind = script->type(col);
    switch (kind) {
    case nebula::type::Kind::BOOLEAN: {
      auto value = r.readBool(col);
      return JS_NewBool(ctx, value ? 1 : 0);
    }
    case nebula::type::Kind::TINYINT: {
      auto value = r.readByte(col);
      return JS_NewInt32(ctx, value);
    }
    case nebula::type::Kind::SMALLINT: {
      auto value = r.readShort(col);
      return JS_NewInt32(ctx, value);
    }
    case nebula::type::Kind::INTEGER: {
      auto value = r.readInt(col);
      return JS_NewInt32(ctx, value);
    }
    case nebula::type::Kind::BIGINT: {
      auto value = r.readLong(col);
      return JS_NewInt64(ctx, value);
    }
    case nebula::type::Kind::REAL: {
      auto value = r.readFloat(col);
      return JS_NewFloat64(ctx, value);
    }
    case nebula::type::Kind::DOUBLE: {
      auto value = r.readDouble(col);
      return JS_NewFloat64(ctx, value);
    }
    case nebula::type::Kind::VARCHAR: {
      // TODO(cao): potential perf hit - we're using std::string copy here
      // to get a null-terminated c_str for JS_NewString API.
      auto value = r.readString(col);
      return JS_NewStringLen(ctx, value.data(), value.size());
    }

    default: {
      // other types are not supported at this moment.
      // Int128, Array, Map, Struct
      LOG(WARNING) << "Not supported type for column value: " << col;
      return JS_NULL;
    }
    }
  }

  // function list to register
  static inline const JSCFunctionListEntry* funcs(int* size) {
    static const JSCFunctionListEntry FUNCS[] = { JS_CFUNC_DEF("column", 1, js_column) };
    *size = ALEN(FUNCS);
    return FUNCS;
  };

  static inline int js_column_init(JSContext* ctx, JSModuleDef* m) {
    auto size = 0;
    auto array = funcs(&size);
    return JS_SetModuleExportList(ctx, m, array, size);
  }

  static inline JSModuleDef* js_init_module_column(JSContext* ctx, const char* name) {
    JSModuleDef* m;
    m = JS_NewCModule(ctx, name, js_column_init);
    if (!m)
      return NULL;

    auto size = 0;
    auto array = funcs(&size);
    JS_AddModuleExportList(ctx, m, array, size);
    return m;
  }

public:
  ScriptContext(RowGetter&& getter, ColTyper&& typer)
    : getter_{ std::move(getter) }, typer_{ std::move(typer) } {
    rt_ = JS_NewRuntime();
    js_std_init_handlers(rt_);
    ctx_ = JS_NewContext(rt_);

    // set current script context to JS context
    JS_SetContextOpaque(ctx_, this);

    // set module loader for ES6 modules
    JS_SetModuleLoaderFunc(rt_, NULL, js_module_loader, NULL);

    // register global values including console.log/print, or customized (count, string[])
    js_std_add_helpers(ctx_, 0, NULL);

    // init system modules
    js_init_module_std(ctx_, "std");
    js_init_module_os(ctx_, "os");

    // init custom module to access each column value of current row value through module nebula
    js_init_module_column(ctx_, "nebula");

    // evaluate the script, load the module nebula to be available to all scripts to be evaluated
    // all future script can call functions like "nebula.column('a') to get value of column 'a'"
    auto result = eval_buf(ctx_,
                           "import * as nebula from 'nebula';"
                           "globalThis.nebula = nebula;",
                           "<nebula-module>",
                           JS_EVAL_TYPE_MODULE);

    if (JS_IsException(result)) {
      js_std_dump_error(ctx_);
    }
  }

  ~ScriptContext() {
    // free the handlers
    js_std_free_handlers(rt_);
    JS_FreeContext(ctx_);
    JS_FreeRuntime(rt_);
  }

  // disable all copy and move
  ScriptContext(const ScriptContext&) = delete;                // copy constructor
  ScriptContext(ScriptContext&&) noexcept = delete;            // move constructor
  ScriptContext& operator=(const ScriptContext&) = delete;     // copy assignment
  ScriptContext& operator=(ScriptContext&&) noexcept = delete; // move assignment

public: // API
  const inline nebula::surface::RowData& row() const {
    return getter_();
  }

  inline nebula::type::Kind type(const std::string& col) const {
    return typer_(col);
  }

  template <typename T>
  T eval(const std::string& script, bool& valid, bool cache = false) noexcept {
    // if once is set, it means this script should be evaluated once
    // return instantly if this script evaluated before already
    constexpr auto dv = nebula::type::TypeDetect<T>::value;
    if (cache && flags_.count(script) > 0) {
      // for this type of operation, we assume caller doesn't care what return value is
      return dv;
    }

    JSValue val = eval_buf(ctx_, script);

    // release this value to avoid leak
    nebula::common::Finally onExit([ctx = ctx_, &val]() { JS_FreeValue(ctx, val); });

    if (JS_IsException(val)) {
      valid = false;
      return dv;
    }

    // record the evaluation if success
    if (cache) {
      flags_.emplace(script);
    }

    // mark this evaluated a valid value
    valid = true;

#define CONVERT_TYPE(DT, M)              \
  if constexpr (std::is_same_v<T, DT>) { \
    return (DT)M(val);                   \
  }

    // convert the val into desired type
    CONVERT_TYPE(bool, JS_VALUE_GET_BOOL)
    CONVERT_TYPE(int8_t, JS_VALUE_GET_INT)
    CONVERT_TYPE(int16_t, JS_VALUE_GET_INT)
    CONVERT_TYPE(int32_t, JS_VALUE_GET_INT)

    // NOTE: due JS limitation on int64, 8 bytes int is stored by floating number
    CONVERT_TYPE(int64_t, JS_VALUE_GET_FLOAT64)
    CONVERT_TYPE(float, JS_VALUE_GET_FLOAT64)
    CONVERT_TYPE(double, JS_VALUE_GET_FLOAT64)

    // for string type, we need to copy the value out
    if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
      return to<std::string_view>(ctx_, val);
    }

#undef CONVERT_TYPE

    // throw?
    LOG(INFO) << "do not support this type, return a default value";
    valid = false;
    return dv;
  }

private:
  RowGetter getter_;
  ColTyper typer_;
  JSRuntime* rt_;
  JSContext* ctx_;

  // record if a script is already evaluated
  nebula::common::unordered_set<std::string> flags_;
};

} // namespace eval
} // namespace surface
} // namespace nebula