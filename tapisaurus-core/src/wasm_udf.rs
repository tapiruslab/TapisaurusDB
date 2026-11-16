//! # Sandboxed WebAssembly (`\0asm`) UDF & In-Transaction Trigger Engine
//!
//! Replaces unsafe C shared-library extensions and legacy procedural languages (`PL/pgSQL`)
//! with a memory-safe, deterministic, fuel-metered WebAssembly (`\0asm\x01\0\0\0`) stack VM.
//!
//! ## Security & Isolation Guarantees
//! 1. **100% Memory-Safe & Sandboxed:** No host filesystem, network, or raw pointer access.
//! 2. **Strict Fuel Metering:** Every instruction decrements `fuel_remaining`; infinite loops
//!    or runaway UDFs trap deterministically once `max_fuel` is exhausted.
//! 3. **Bounded Stack & Call Depth:** Operand stack is capped at 256 slots and call depth at 16 frames.
//! 4. **In-Transaction Execution:** Runs directly during `INSERT` / `UPDATE` mutations to validate
//!    business invariants or compute derived fields atomically before WAL commit.

use parking_lot::RwLock;
use std::collections::HashMap;

use crate::document::{Document, Value};

/// Standard 8-byte WebAssembly v1 binary header (`\0asm\x01\0\0\0`)
pub const WASM_MAGIC_HEADER: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

/// Default instruction fuel limit per UDF invocation
pub const DEFAULT_WASM_FUEL: usize = 10_000;

const MAX_STACK_DEPTH: usize = 256;
const MAX_CALL_DEPTH: usize = 16;

#[derive(Debug, Clone)]
struct WasmFuncType {
    param_count: usize,
    result_count: usize,
}

#[derive(Debug, Clone)]
struct WasmFuncBody {
    type_idx: usize,
    local_count: usize,
    code: Vec<u8>,
}

/// Decoded and verified WebAssembly (`\0asm`) binary module ready for sandboxed execution
#[derive(Debug, Clone)]
pub struct WasmSandboxModule {
    types: Vec<WasmFuncType>,
    funcs: Vec<WasmFuncBody>,
    exports: HashMap<String, usize>,
}

impl WasmSandboxModule {
    /// Parse and validate a standard WebAssembly v1 binary module (`\0asm\x01\0\0\0`)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 || bytes[..8] != WASM_MAGIC_HEADER {
            return Err("Invalid WebAssembly binary header (expected \\0asm v1)".into());
        }

        let mut cursor = 8usize;
        let mut types = Vec::new();
        let mut func_type_indices = Vec::new();
        let mut exports = HashMap::new();
        let mut bodies = Vec::new();

        while cursor < bytes.len() {
            let section_id = bytes[cursor];
            cursor += 1;
            let section_len = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
            if cursor + section_len > bytes.len() {
                return Err("Truncated WebAssembly section".into());
            }
            let section_end = cursor + section_len;

            match section_id {
                1 => {
                    // Type section
                    let count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                    for _ in 0..count {
                        if cursor >= section_end || bytes[cursor] != 0x60 {
                            return Err("Expected 0x60 functype tag in Type section".into());
                        }
                        cursor += 1;
                        let param_count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        cursor += param_count; // skip value types (0x7e i64 / 0x7f i32)
                        let result_count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        cursor += result_count;
                        types.push(WasmFuncType {
                            param_count,
                            result_count,
                        });
                    }
                }
                3 => {
                    // Function section
                    let count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                    for _ in 0..count {
                        let type_idx = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        func_type_indices.push(type_idx);
                    }
                }
                7 => {
                    // Export section
                    let count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                    for _ in 0..count {
                        let name_len = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        if cursor + name_len > section_end {
                            return Err("Truncated export name in WASM module".into());
                        }
                        let name = String::from_utf8(bytes[cursor..cursor + name_len].to_vec())
                            .map_err(|e| format!("Invalid UTF-8 export name: {e}"))?;
                        cursor += name_len;
                        let export_kind = bytes[cursor];
                        cursor += 1;
                        let export_idx = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        if export_kind == 0x00 {
                            exports.insert(name, export_idx);
                        }
                    }
                }
                10 => {
                    // Code section
                    let count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                    for i in 0..count {
                        let body_size = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        if cursor + body_size > section_end {
                            return Err("Truncated function body in WASM Code section".into());
                        }
                        let body_end = cursor + body_size;
                        let local_decl_count = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                        let mut total_locals = 0usize;
                        for _ in 0..local_decl_count {
                            let n = Self::read_u32_leb128(bytes, &mut cursor)? as usize;
                            cursor += 1; // valtype byte
                            total_locals = total_locals.saturating_add(n);
                        }
                        let code = bytes[cursor..body_end].to_vec();
                        cursor = body_end;
                        let type_idx = func_type_indices.get(i).copied().unwrap_or(0);
                        bodies.push(WasmFuncBody {
                            type_idx,
                            local_count: total_locals,
                            code,
                        });
                    }
                }
                _ => {
                    // Skip custom or unused sections safely
                }
            }
            cursor = section_end;
        }

        Ok(Self {
            types,
            funcs: bodies,
            exports,
        })
    }

    /// Execute an exported WebAssembly function with strict instruction fuel metering
    pub fn invoke_i64(
        &self,
        export_name: &str,
        args: &[i64],
        max_fuel: usize,
    ) -> Result<i64, String> {
        let &func_idx = self
            .exports
            .get(export_name)
            .ok_or_else(|| format!("Exported WASM function '{export_name}' not found"))?;
        let mut fuel = max_fuel;
        self.exec_func(func_idx, args, &mut fuel, 0)
    }

    fn exec_func(
        &self,
        func_idx: usize,
        args: &[i64],
        fuel: &mut usize,
        call_depth: usize,
    ) -> Result<i64, String> {
        if call_depth > MAX_CALL_DEPTH {
            return Err("WASM call stack depth limit exceeded".into());
        }
        let body = self
            .funcs
            .get(func_idx)
            .ok_or_else(|| format!("WASM function index {func_idx} out of bounds"))?;
        let func_type = self
            .types
            .get(body.type_idx)
            .ok_or_else(|| format!("WASM type index {} out of bounds", body.type_idx))?;

        if args.len() != func_type.param_count {
            return Err(format!(
                "WASM argument count mismatch: expected {}, got {}",
                func_type.param_count,
                args.len()
            ));
        }

        let mut locals = Vec::with_capacity(args.len() + body.local_count);
        locals.extend_from_slice(args);
        locals.resize(args.len() + body.local_count, 0i64);

        let mut stack: Vec<i64> = Vec::with_capacity(16);
        let code = &body.code;
        let mut pc = 0usize;

        while pc < code.len() {
            if *fuel == 0 {
                return Err("WASM execution trapped: instruction fuel exhausted".into());
            }
            *fuel -= 1;

            let op = code[pc];
            pc += 1;

            match op {
                0x01 => {} // nop
                0x0b | 0x0f => {
                    // end or return
                    break;
                }
                0x10 => {
                    // call <func_idx>
                    let callee_idx = Self::read_u32_leb128(code, &mut pc)? as usize;
                    let callee_body = self
                        .funcs
                        .get(callee_idx)
                        .ok_or_else(|| format!("Invalid callee index {callee_idx}"))?;
                    let callee_type = self
                        .types
                        .get(callee_body.type_idx)
                        .ok_or_else(|| "Invalid callee type".to_string())?;
                    if stack.len() < callee_type.param_count {
                        return Err("WASM stack underflow on call".into());
                    }
                    let split_at = stack.len() - callee_type.param_count;
                    let call_args = stack.split_off(split_at);
                    let ret = self.exec_func(callee_idx, &call_args, fuel, call_depth + 1)?;
                    if callee_type.result_count > 0 {
                        Self::push_stack(&mut stack, ret)?;
                    }
                }
                0x1a => {
                    // drop
                    Self::pop_stack(&mut stack)?;
                }
                0x1b => {
                    // select: [val1, val2, cond] -> cond != 0 ? val1 : val2
                    let cond = Self::pop_stack(&mut stack)?;
                    let val2 = Self::pop_stack(&mut stack)?;
                    let val1 = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if cond != 0 { val1 } else { val2 })?;
                }
                0x20 => {
                    // local.get
                    let idx = Self::read_u32_leb128(code, &mut pc)? as usize;
                    let val = *locals
                        .get(idx)
                        .ok_or_else(|| format!("Invalid local index {idx}"))?;
                    Self::push_stack(&mut stack, val)?;
                }
                0x21 => {
                    // local.set
                    let idx = Self::read_u32_leb128(code, &mut pc)? as usize;
                    let val = Self::pop_stack(&mut stack)?;
                    let slot = locals
                        .get_mut(idx)
                        .ok_or_else(|| format!("Invalid local index {idx}"))?;
                    *slot = val;
                }
                0x22 => {
                    // local.tee
                    let idx = Self::read_u32_leb128(code, &mut pc)? as usize;
                    let val = *stack.last().ok_or("WASM stack underflow")?;
                    let slot = locals
                        .get_mut(idx)
                        .ok_or_else(|| format!("Invalid local index {idx}"))?;
                    *slot = val;
                }
                0x41 | 0x42 => {
                    // i32.const / i64.const
                    let val = Self::read_i64_leb128(code, &mut pc)?;
                    Self::push_stack(&mut stack, val)?;
                }
                0x50 => {
                    // i64.eqz
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a == 0 { 1 } else { 0 })?;
                }
                0x51 => {
                    // i64.eq
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a == b { 1 } else { 0 })?;
                }
                0x52 => {
                    // i64.ne
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a != b { 1 } else { 0 })?;
                }
                0x53 => {
                    // i64.lt_s
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a < b { 1 } else { 0 })?;
                }
                0x55 => {
                    // i64.gt_s
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a > b { 1 } else { 0 })?;
                }
                0x57 => {
                    // i64.le_s
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a <= b { 1 } else { 0 })?;
                }
                0x59 => {
                    // i64.ge_s
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, if a >= b { 1 } else { 0 })?;
                }
                0x7c => {
                    // i64.add
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, a.wrapping_add(b))?;
                }
                0x7d => {
                    // i64.sub
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, a.wrapping_sub(b))?;
                }
                0x7e => {
                    // i64.mul
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, a.wrapping_mul(b))?;
                }
                0x7f => {
                    // i64.div_s
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    if b == 0 {
                        return Err("WASM execution trapped: integer division by zero".into());
                    }
                    Self::push_stack(&mut stack, a.wrapping_div(b))?;
                }
                0x81 => {
                    // i64.rem_s
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    if b == 0 {
                        return Err("WASM execution trapped: remainder by zero".into());
                    }
                    Self::push_stack(&mut stack, a.wrapping_rem(b))?;
                }
                0x83 => {
                    // i64.and
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, a & b)?;
                }
                0x84 => {
                    // i64.or
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, a | b)?;
                }
                0x85 => {
                    // i64.xor
                    let b = Self::pop_stack(&mut stack)?;
                    let a = Self::pop_stack(&mut stack)?;
                    Self::push_stack(&mut stack, a ^ b)?;
                }
                other => {
                    return Err(format!("Unsupported WASM opcode 0x{other:02x}"));
                }
            }
        }

        if func_type.result_count > 0 {
            Self::pop_stack(&mut stack)
        } else {
            Ok(0)
        }
    }

    fn push_stack(stack: &mut Vec<i64>, val: i64) -> Result<(), String> {
        if stack.len() >= MAX_STACK_DEPTH {
            return Err("WASM operand stack overflow".into());
        }
        stack.push(val);
        Ok(())
    }

    fn pop_stack(stack: &mut Vec<i64>) -> Result<i64, String> {
        stack
            .pop()
            .ok_or_else(|| "WASM operand stack underflow".to_string())
    }

    fn read_u32_leb128(bytes: &[u8], cursor: &mut usize) -> Result<u32, String> {
        let mut result = 0u32;
        let mut shift = 0u32;
        loop {
            if *cursor >= bytes.len() {
                return Err("Unexpected EOF reading u32 LEB128".into());
            }
            let byte = bytes[*cursor];
            *cursor += 1;
            result |= ((byte & 0x7f) as u32) << shift;
            if (byte & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 35 {
                return Err("LEB128 u32 overflow".into());
            }
        }
    }

    fn read_i64_leb128(bytes: &[u8], cursor: &mut usize) -> Result<i64, String> {
        let mut result = 0i64;
        let mut shift = 0u32;
        let mut byte: u8;
        loop {
            if *cursor >= bytes.len() {
                return Err("Unexpected EOF reading i64 LEB128".into());
            }
            byte = bytes[*cursor];
            *cursor += 1;
            result |= ((byte & 0x7f) as i64) << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
            if shift >= 70 {
                return Err("LEB128 i64 overflow".into());
            }
        }
        if shift < 64 && (byte & 0x40) != 0 {
            result |= !0i64 << shift;
        }
        Ok(result)
    }

    /// Assemble a valid single-function `\0asm\x01\0\0\0` binary module from raw WASM bytecode instructions.
    /// Useful for compiling inline UDF expressions into standard WebAssembly binary format.
    pub fn assemble_single_fn_module(
        export_name: &str,
        param_count: usize,
        body_instructions: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::from(WASM_MAGIC_HEADER);

        // 1. Type Section (id=1): 1 type -> [0x60, param_count, 0x7e..., 1, 0x7e]
        let mut type_sec = vec![0x01, 0x60, param_count as u8];
        type_sec.resize(type_sec.len() + param_count, 0x7e); // 0x7e = i64
        type_sec.push(0x01);
        type_sec.push(0x7e);
        out.push(1);
        out.push(type_sec.len() as u8);
        out.extend_from_slice(&type_sec);

        // 2. Function Section (id=3): 1 func -> type 0
        out.extend_from_slice(&[3, 2, 1, 0]);

        // 3. Export Section (id=7): 1 export -> name, kind=0 (func), idx=0
        let name_bytes = export_name.as_bytes();
        let mut exp_sec = Vec::with_capacity(4 + name_bytes.len());
        exp_sec.push(1);
        exp_sec.push(name_bytes.len() as u8);
        exp_sec.extend_from_slice(name_bytes);
        exp_sec.push(0x00);
        exp_sec.push(0x00);
        out.push(7);
        out.push(exp_sec.len() as u8);
        out.extend_from_slice(&exp_sec);

        // 4. Code Section (id=10): 1 body -> [0 locals, instructions..., 0x0b (end)]
        let mut func_body = Vec::with_capacity(2 + body_instructions.len());
        func_body.push(0x00); // 0 local declarations
        func_body.extend_from_slice(body_instructions);
        if func_body.last().copied() != Some(0x0b) {
            func_body.push(0x0b);
        }
        let mut code_sec = Vec::with_capacity(2 + func_body.len());
        code_sec.push(1);
        code_sec.push(func_body.len() as u8);
        code_sec.extend_from_slice(&func_body);
        out.push(10);
        out.push(code_sec.len() as u8);
        out.extend_from_slice(&code_sec);

        out
    }
}

/// Action performed by an in-transaction WebAssembly trigger on a collection
#[derive(Debug, Clone)]
pub enum WasmTriggerAction {
    /// Compute `output_field = module.export_fn(doc[input_field])` before inserting/updating a document
    ComputeField {
        module_name: String,
        export_fn: String,
        input_field: String,
        output_field: String,
    },
    /// Validate `module.export_fn(doc[input_field]) != 0`; abort the transaction if the WASM validator returns 0
    ValidateField {
        module_name: String,
        export_fn: String,
        input_field: String,
        error_message: String,
    },
}

/// Thread-safe registry of sandboxed WebAssembly UDF modules and collection triggers
#[derive(Default)]
pub struct WasmUdfRegistry {
    modules: RwLock<HashMap<String, WasmSandboxModule>>,
    triggers: RwLock<HashMap<String, Vec<WasmTriggerAction>>>,
}

impl WasmUdfRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compile, validate, and register a `.wasm` binary module under `module_name`
    pub fn register_module(
        &self,
        module_name: impl Into<String>,
        wasm_bytes: &[u8],
    ) -> Result<(), String> {
        let module = WasmSandboxModule::from_bytes(wasm_bytes)?;
        self.modules.write().insert(module_name.into(), module);
        Ok(())
    }

    /// Attach an in-transaction WebAssembly trigger to a collection
    pub fn register_trigger(&self, collection: impl Into<String>, action: WasmTriggerAction) {
        self.triggers
            .write()
            .entry(collection.into())
            .or_default()
            .push(action);
    }

    /// Invoke a registered WASM UDF directly
    pub fn invoke_udf(
        &self,
        module_name: &str,
        export_fn: &str,
        args: &[i64],
    ) -> Result<i64, String> {
        let modules = self.modules.read();
        let module = modules
            .get(module_name)
            .ok_or_else(|| format!("WASM module '{module_name}' is not registered"))?;
        module.invoke_i64(export_fn, args, DEFAULT_WASM_FUEL)
    }

    /// Execute all registered in-transaction WASM triggers for `collection` on `doc`
    pub fn apply_triggers(&self, collection: &str, doc: &mut Document) -> Result<(), String> {
        let triggers_guard = self.triggers.read();
        let Some(actions) = triggers_guard.get(collection) else {
            return Ok(());
        };
        let modules_guard = self.modules.read();

        for action in actions {
            match action {
                WasmTriggerAction::ComputeField {
                    module_name,
                    export_fn,
                    input_field,
                    output_field,
                } => {
                    if let Some(in_val) = doc.get(input_field).and_then(|v| v.as_i64()) {
                        let module = modules_guard
                            .get(module_name)
                            .ok_or_else(|| format!("WASM module '{module_name}' not found"))?;
                        let computed =
                            module.invoke_i64(export_fn, &[in_val], DEFAULT_WASM_FUEL)?;
                        doc.set(output_field.clone(), Value::Integer(computed));
                    }
                }
                WasmTriggerAction::ValidateField {
                    module_name,
                    export_fn,
                    input_field,
                    error_message,
                } => {
                    let in_val =
                        doc.get(input_field)
                            .and_then(|v| v.as_i64())
                            .ok_or_else(|| {
                                format!("WASM trigger requires integer field '{input_field}'")
                            })?;
                    let module = modules_guard
                        .get(module_name)
                        .ok_or_else(|| format!("WASM module '{module_name}' not found"))?;
                    let valid = module.invoke_i64(export_fn, &[in_val], DEFAULT_WASM_FUEL)?;
                    if valid == 0 {
                        return Err(format!("WASM Trigger Validation Failed: {error_message}"));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_sandbox_udf_and_in_transaction_triggers() {
        // Build a standard \0asm module that computes loyalty_points = (amount * 15) / 100
        // Instructions: local.get 0 (0x20, 0x00), i64.const 15 (0x42, 15), i64.mul (0x7e), i64.const 100 (0x42, 0xe4, 0x00), i64.div_s (0x7f)
        let calc_points_wasm = WasmSandboxModule::assemble_single_fn_module(
            "calc_points",
            1,
            &[0x20, 0x00, 0x42, 15, 0x7e, 0x42, 0xe4, 0x00, 0x7f],
        );

        // Build a validator \0asm module that checks amount > 0:
        // local.get 0 (0x20, 0x00), i64.const 0 (0x42, 0x00), i64.gt_s (0x55)
        let validate_pos_wasm = WasmSandboxModule::assemble_single_fn_module(
            "is_positive",
            1,
            &[0x20, 0x00, 0x42, 0x00, 0x55],
        );

        let registry = WasmUdfRegistry::new();
        registry
            .register_module("rewards_mod", &calc_points_wasm)
            .unwrap();
        registry
            .register_module("guard_mod", &validate_pos_wasm)
            .unwrap();

        assert_eq!(
            registry
                .invoke_udf("rewards_mod", "calc_points", &[2000])
                .unwrap(),
            300
        );

        registry.register_trigger(
            "invoices",
            WasmTriggerAction::ValidateField {
                module_name: "guard_mod".into(),
                export_fn: "is_positive".into(),
                input_field: "amount".into(),
                error_message: "Invoice amount must be strictly positive".into(),
            },
        );
        registry.register_trigger(
            "invoices",
            WasmTriggerAction::ComputeField {
                module_name: "rewards_mod".into(),
                export_fn: "calc_points".into(),
                input_field: "amount".into(),
                output_field: "loyalty_points".into(),
            },
        );

        // 1. Valid document gets `loyalty_points` computed automatically by WASM trigger
        let mut valid_doc = Document::new();
        valid_doc.set("amount", 1000);
        registry.apply_triggers("invoices", &mut valid_doc).unwrap();
        assert_eq!(valid_doc.get("loyalty_points"), Some(&Value::Integer(150)));

        // 2. Invalid document (amount = -50) is rejected by WASM validator trigger
        let mut bad_doc = Document::new();
        bad_doc.set("amount", -50);
        let err = registry
            .apply_triggers("invoices", &mut bad_doc)
            .unwrap_err();
        assert!(err.contains("Invoice amount must be strictly positive"));
    }
}
