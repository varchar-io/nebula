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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <string>

#include <quickjs.h>
extern "C" {
#include <quickjs-libc.h>
}

/**
 * Test namespace for testing external dependency APIs
 */
namespace nebula {
namespace common {
namespace test {

int eval_buf(JSContext* ctx, const std::string& buf, const std::string& file, int eval_flags) {
  JSValue val;
  int ret;

  if ((eval_flags & JS_EVAL_TYPE_MASK) == JS_EVAL_TYPE_MODULE) {
    // for the modules, we compile then run to be able to set import.meta
    val = JS_Eval(ctx, buf.data(), buf.size(), file.c_str(),
                  eval_flags | JS_EVAL_FLAG_COMPILE_ONLY);
    if (!JS_IsException(val)) {
      js_module_set_import_meta(ctx, val, 1, 1);
      val = JS_EvalFunction(ctx, val);
    }
  } else {
    val = JS_Eval(ctx, buf.data(), buf.size(), file.c_str(), eval_flags);
  }

  if (JS_IsException(val)) {
    js_std_dump_error(ctx);
    ret = -1;
  } else {
    ret = 0;
  }

  JS_FreeValue(ctx, val);
  return ret;
}

int eval_buf(JSContext* ctx, const std::string& buf) {
  return eval_buf(ctx, buf, "<script>", 0);
}

void run(const std::string& script) {
  // create a runtime (can have multiple runtime)
  JSRuntime* rt = JS_NewRuntime();
  JSContext* ctx = JS_NewContext(rt);

  // set module loader for ES6 modules
  JS_SetModuleLoaderFunc(rt, NULL, js_module_loader, NULL);

  // register global values including console.log/print, or customized (count, string[])
  js_std_add_helpers(ctx, 0, NULL);

  // system modules - file systems
  js_init_module_std(ctx, "std");
  js_init_module_os(ctx, "os");

  // expose std and os to user's script
  //   const char* str =
  //     "import * as std from 'std';\n"
  //     "import * as os from 'os';\n"
  //     "globalThis.std = std;\n"
  //     "globalThis.os = os;\n";
  //   eval_buf(ctx, str, strlen(str), "<input>", JS_EVAL_TYPE_MODULE);
  eval_buf(ctx, script);

  // main loop to finish all work in the script
  js_std_loop(ctx);

  // clean up
  js_std_free_handlers(rt);
  JS_FreeContext(ctx);
  JS_FreeRuntime(rt);
}

TEST(JSTest, TestBasicEval) {
  auto js = "var a=1; var b=1; var c = a+b; print(c);";
  run(js);
}

TEST(JSTest, TestFunction) {
  auto js = "var x=(a)=>a+1; print(x(5));";
  run(js);
}

// test interaction with C++ API
int fib(int n) {
  if (n > 1) {
    return fib(n - 1) + fib(n - 2);
  }

  return n / 2 + 1;
}

TEST(JSTest, TestNativeFib) {
  LOG(INFO) << "Native fib(10)=" << fib(10);
  EXPECT_EQ(89, fib(10));
}

// wrap by JS interface
// How to pass object rather than primitives? Through JSValue as well?
static JSValue js_fib(JSContext* ctx, JSValue, int, JSValue* args) {
  // convert first argument to input value
  int n;

  // should return 0 for converting correctly
  if (JS_ToInt32(ctx, &n, args[0])) {
    return JS_EXCEPTION;
  }

  // invoke native impl
  int res = fib(n);

  // adapt result back into JS context
  return JS_NewInt32(ctx, res);
}

// function list to register
static const JSCFunctionListEntry jsFuncs[] = {
  JS_CFUNC_DEF("fib", 1, js_fib)
};

// module init
static int js_fib_init(JSContext* ctx, JSModuleDef* m) {
  return JS_SetModuleExportList(ctx, m, jsFuncs, 1);
}

JSModuleDef* js_init_module_fib(JSContext* ctx, const char* module_name) {
  JSModuleDef* m;
  m = JS_NewCModule(ctx, module_name, js_fib_init);
  if (!m)
    return NULL;
  JS_AddModuleExportList(ctx, m, jsFuncs, 1);
  return m;
}

TEST(JSTest, TestCustomModule) {
  // create a runtime (can have multiple runtime)
  JSRuntime* rt = JS_NewRuntime();
  JSContext* ctx = JS_NewContext(rt);

  // set module loader for ES6 modules
  JS_SetModuleLoaderFunc(rt, NULL, js_module_loader, NULL);

  // register global values including console.log/print, or customized (count, string[])
  js_std_add_helpers(ctx, 0, NULL);

  // init system modules
  js_init_module_std(ctx, "std");
  js_init_module_os(ctx, "os");

  // init custom module
  js_init_module_fib(ctx, "fib");

  // evaluate the script, load script first
  eval_buf(ctx,
           "import * as fib from 'fib'; \n"
           "globalThis.fib = fib;",
           "<fib-module>",
           JS_EVAL_TYPE_MODULE);

  LOG(INFO) << "module loaded?";

  // evaluate the expression
  eval_buf(ctx, "print(fib.fib(10));");

  // main loop to finish all work in the script
  js_std_loop(ctx);

  // clean up
  js_std_free_handlers(rt);
  JS_FreeContext(ctx);
  JS_FreeRuntime(rt);
}

} // namespace test
} // namespace common
} // namespace nebula