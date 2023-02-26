static FIB_SRC: &str = r#"
(module 
    (type (;0;) (func (param i64) (result i64))) 
    (func $fib (type 0) (param i64) (result i64) 
    (local i64) 
    i64.const 0 
    local.set 1 
    block ;; label = @1 
    local.get 0 
    i64.const 2 
    i64.lt_u 
    br_if 0 (;@1;) 
    i64.const 0 
    local.set 1 
    loop ;; label = @2 
    local.get 0 
    i64.const -1 
    i64.add 
    call $fib 
    local.get 1 
    i64.add 
    local.set 1 
    local.get 0 
    i64.const -2 
    i64.add 
    local.tee 0 
    i64.const 1 
    i64.gt_u 
    br_if 0 (;@2;) 
    end 
    end 
    local.get 0 
    local.get 1 
    i64.add) 
    (memory (;0;) 16) 
    (global $__stack_pointer (mut i32) (i32.const 1048576)) 
    (global (;1;) i32 (i32.const 1048576)) 
    (global (;2;) i32 (i32.const 1048576)) 
    (export "memory" (memory 0)) 
    (export "fib" (func $fib)))
"#;

static CONTAINS_SRC: &str = r#"

(module
    (type (;0;) (func (param i32 i32 i32 i32 i32 i32 i32)))
    (type (;1;) (func (param i32 i32)))
    (type (;2;) (func (param i32)))
    (type (;3;) (func (param i32 i32) (result i64)))
    (type (;4;) (func (param i32 i32 i32 i32)))
    (type (;5;) (func))
    (type (;6;) (func (param i32) (result i32)))
    (type (;7;) (func (param i32 i32 i32) (result i32)))
    (func $_ZN4core3str7pattern14TwoWaySearcher4next17hc1bfb3e180a449d2E (type 0) (param i32 i32 i32 i32 i32 i32 i32)
      (local i32 i32 i32 i32 i32 i32 i32 i64 i32 i32 i32 i32 i32)
      block  ;; label = @1
        block  ;; label = @2
          block  ;; label = @3
            local.get 1
            i32.load offset=20
            local.tee 7
            local.get 5
            i32.add
            i32.const -1
            i32.add
            local.tee 8
            local.get 3
            i32.ge_u
            br_if 0 (;@3;)
            i32.const 0
            local.get 1
            i32.load offset=8
            local.tee 9
            i32.sub
            local.set 10
            local.get 5
            local.get 1
            i32.load offset=16
            local.tee 11
            i32.sub
            local.set 12
            local.get 1
            i32.load offset=28
            local.set 13
            local.get 1
            i64.load
            local.set 14
            loop  ;; label = @4
              block  ;; label = @5
                block  ;; label = @6
                  block  ;; label = @7
                    block  ;; label = @8
                      local.get 14
                      local.get 2
                      local.get 8
                      i32.add
                      i64.load8_u
                      i64.shr_u
                      i64.const 1
                      i64.and
                      i64.eqz
                      br_if 0 (;@8;)
                      local.get 9
                      local.get 9
                      local.get 13
                      local.get 9
                      local.get 13
                      i32.gt_u
                      select
                      local.get 6
                      select
                      local.tee 15
                      local.get 5
                      local.get 15
                      local.get 5
                      i32.gt_u
                      select
                      local.set 16
                      local.get 2
                      local.get 7
                      i32.add
                      local.set 17
                      local.get 15
                      local.set 8
                      block  ;; label = @9
                        loop  ;; label = @10
                          block  ;; label = @11
                            local.get 16
                            local.get 8
                            i32.ne
                            br_if 0 (;@11;)
                            i32.const 0
                            local.get 13
                            local.get 6
                            select
                            local.set 18
                            local.get 9
                            local.set 8
                            block  ;; label = @12
                              block  ;; label = @13
                                block  ;; label = @14
                                  loop  ;; label = @15
                                    block  ;; label = @16
                                      local.get 18
                                      local.get 8
                                      i32.lt_u
                                      br_if 0 (;@16;)
                                      local.get 1
                                      local.get 7
                                      local.get 5
                                      i32.add
                                      local.tee 8
                                      i32.store offset=20
                                      local.get 6
                                      i32.eqz
                                      br_if 2 (;@14;)
                                      br 14 (;@2;)
                                    end
                                    local.get 8
                                    i32.const -1
                                    i32.add
                                    local.tee 8
                                    local.get 5
                                    i32.ge_u
                                    br_if 2 (;@13;)
                                    local.get 8
                                    local.get 7
                                    i32.add
                                    local.tee 19
                                    local.get 3
                                    i32.ge_u
                                    br_if 3 (;@12;)
                                    local.get 4
                                    local.get 8
                                    i32.add
                                    i32.load8_u
                                    local.get 2
                                    local.get 19
                                    i32.add
                                    i32.load8_u
                                    i32.eq
                                    br_if 0 (;@15;)
                                  end
                                  local.get 1
                                  local.get 11
                                  local.get 7
                                  i32.add
                                  local.tee 7
                                  i32.store offset=20
                                  local.get 12
                                  local.set 8
                                  local.get 6
                                  i32.eqz
                                  br_if 8 (;@6;)
                                  br 9 (;@5;)
                                end
                                local.get 1
                                i32.const 0
                                i32.store offset=28
                                br 11 (;@2;)
                              end
                              local.get 8
                              local.get 5
                              call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                              unreachable
                            end
                            local.get 19
                            local.get 3
                            call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                            unreachable
                          end
                          local.get 7
                          local.get 8
                          i32.add
                          local.get 3
                          i32.ge_u
                          br_if 1 (;@9;)
                          local.get 17
                          local.get 8
                          i32.add
                          local.set 19
                          local.get 4
                          local.get 8
                          i32.add
                          local.set 18
                          local.get 8
                          i32.const 1
                          i32.add
                          local.set 8
                          local.get 18
                          i32.load8_u
                          local.get 19
                          i32.load8_u
                          i32.eq
                          br_if 0 (;@10;)
                        end
                        local.get 10
                        local.get 7
                        i32.add
                        local.get 8
                        i32.add
                        local.set 7
                        br 2 (;@7;)
                      end
                      local.get 3
                      local.get 15
                      local.get 7
                      i32.add
                      local.tee 8
                      local.get 3
                      local.get 8
                      i32.gt_u
                      select
                      local.get 3
                      call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                      unreachable
                    end
                    local.get 1
                    local.get 7
                    local.get 5
                    i32.add
                    local.tee 7
                    i32.store offset=20
                  end
                  i32.const 0
                  local.set 8
                  local.get 6
                  br_if 1 (;@5;)
                end
                local.get 1
                local.get 8
                i32.store offset=28
                local.get 8
                local.set 13
              end
              local.get 7
              local.get 5
              i32.add
              i32.const -1
              i32.add
              local.tee 8
              local.get 3
              i32.lt_u
              br_if 0 (;@4;)
            end
          end
          local.get 1
          local.get 3
          i32.store offset=20
          i32.const 0
          local.set 8
          br 1 (;@1;)
        end
        local.get 0
        local.get 7
        i32.store offset=4
        local.get 0
        i32.const 8
        i32.add
        local.get 8
        i32.store
        i32.const 1
        local.set 8
      end
      local.get 0
      local.get 8
      i32.store)
    (func $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E (type 1) (param i32 i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E (type 1) (param i32 i32)
      (local i32 i32 i32 i32 i32 i32 i32 i32 i32)
      global.get $__stack_pointer
      i32.const 16
      i32.sub
      local.tee 2
      global.set $__stack_pointer
      i32.const 3
      local.set 3
      i32.const 1048576
      local.set 4
      block  ;; label = @1
        local.get 1
        i32.load8_u
        i32.const 3
        i32.ne
        br_if 0 (;@1;)
        local.get 1
        i32.const 1
        i32.add
        local.tee 4
        call $strlen
        local.tee 3
        i32.eqz
        br_if 0 (;@1;)
        i32.const 0
        local.get 3
        i32.const -7
        i32.add
        local.tee 5
        local.get 5
        local.get 3
        i32.gt_u
        select
        local.set 6
        local.get 1
        i32.const 4
        i32.add
        i32.const -4
        i32.and
        local.get 4
        i32.sub
        local.set 7
        i32.const 0
        local.set 5
        loop  ;; label = @2
          block  ;; label = @3
            block  ;; label = @4
              block  ;; label = @5
                block  ;; label = @6
                  block  ;; label = @7
                    block  ;; label = @8
                      local.get 4
                      local.get 5
                      i32.add
                      i32.load8_u
                      local.tee 8
                      i32.const 24
                      i32.shl
                      i32.const 24
                      i32.shr_s
                      local.tee 9
                      i32.const 0
                      i32.lt_s
                      br_if 0 (;@8;)
                      local.get 7
                      i32.const -1
                      i32.eq
                      br_if 1 (;@7;)
                      local.get 7
                      local.get 5
                      i32.sub
                      i32.const 3
                      i32.and
                      br_if 1 (;@7;)
                      block  ;; label = @9
                        local.get 5
                        local.get 6
                        i32.ge_u
                        br_if 0 (;@9;)
                        loop  ;; label = @10
                          local.get 1
                          local.get 5
                          i32.add
                          local.tee 8
                          i32.const 1
                          i32.add
                          i32.load
                          local.get 8
                          i32.const 5
                          i32.add
                          i32.load
                          i32.or
                          i32.const -2139062144
                          i32.and
                          br_if 1 (;@9;)
                          local.get 5
                          i32.const 8
                          i32.add
                          local.tee 5
                          local.get 6
                          i32.lt_u
                          br_if 0 (;@10;)
                        end
                      end
                      local.get 5
                      local.get 3
                      i32.ge_u
                      br_if 5 (;@3;)
                      loop  ;; label = @9
                        local.get 4
                        local.get 5
                        i32.add
                        i32.load8_s
                        i32.const 0
                        i32.lt_s
                        br_if 6 (;@3;)
                        local.get 3
                        local.get 5
                        i32.const 1
                        i32.add
                        local.tee 5
                        i32.ne
                        br_if 0 (;@9;)
                        br 8 (;@1;)
                      end
                    end
                    block  ;; label = @8
                      block  ;; label = @9
                        local.get 8
                        i32.const 1048579
                        i32.add
                        i32.load8_u
                        i32.const -2
                        i32.add
                        br_table 3 (;@6;) 1 (;@8;) 0 (;@9;) 4 (;@5;)
                      end
                      local.get 5
                      i32.const 1
                      i32.add
                      local.tee 10
                      local.get 3
                      i32.ge_u
                      br_if 3 (;@5;)
                      local.get 4
                      local.get 10
                      i32.add
                      i32.load8_s
                      local.set 10
                      block  ;; label = @9
                        block  ;; label = @10
                          block  ;; label = @11
                            block  ;; label = @12
                              local.get 8
                              i32.const -240
                              i32.add
                              br_table 1 (;@11;) 0 (;@12;) 0 (;@12;) 0 (;@12;) 2 (;@10;) 0 (;@12;)
                            end
                            local.get 9
                            i32.const 15
                            i32.add
                            i32.const 255
                            i32.and
                            i32.const 2
                            i32.gt_u
                            br_if 6 (;@5;)
                            local.get 10
                            i32.const -1
                            i32.gt_s
                            br_if 6 (;@5;)
                            local.get 10
                            i32.const -64
                            i32.lt_u
                            br_if 2 (;@9;)
                            br 6 (;@5;)
                          end
                          local.get 10
                          i32.const 112
                          i32.add
                          i32.const 255
                          i32.and
                          i32.const 48
                          i32.lt_u
                          br_if 1 (;@9;)
                          br 5 (;@5;)
                        end
                        local.get 10
                        i32.const -113
                        i32.gt_s
                        br_if 4 (;@5;)
                      end
                      local.get 5
                      i32.const 2
                      i32.add
                      local.tee 8
                      local.get 3
                      i32.ge_u
                      br_if 3 (;@5;)
                      local.get 4
                      local.get 8
                      i32.add
                      i32.load8_s
                      i32.const -65
                      i32.gt_s
                      br_if 3 (;@5;)
                      local.get 5
                      i32.const 3
                      i32.add
                      local.tee 5
                      local.get 3
                      i32.ge_u
                      br_if 3 (;@5;)
                      local.get 4
                      local.get 5
                      i32.add
                      i32.load8_s
                      i32.const -65
                      i32.gt_s
                      br_if 3 (;@5;)
                      br 4 (;@4;)
                    end
                    local.get 5
                    i32.const 1
                    i32.add
                    local.tee 10
                    local.get 3
                    i32.ge_u
                    br_if 2 (;@5;)
                    local.get 4
                    local.get 10
                    i32.add
                    i32.load8_s
                    local.set 10
                    block  ;; label = @8
                      block  ;; label = @9
                        block  ;; label = @10
                          block  ;; label = @11
                            local.get 8
                            i32.const 224
                            i32.eq
                            br_if 0 (;@11;)
                            local.get 8
                            i32.const 237
                            i32.eq
                            br_if 1 (;@10;)
                            local.get 9
                            i32.const 31
                            i32.add
                            i32.const 255
                            i32.and
                            i32.const 12
                            i32.lt_u
                            br_if 2 (;@9;)
                            local.get 9
                            i32.const -2
                            i32.and
                            i32.const -18
                            i32.ne
                            br_if 6 (;@5;)
                            local.get 10
                            i32.const -1
                            i32.gt_s
                            br_if 6 (;@5;)
                            local.get 10
                            i32.const -64
                            i32.lt_u
                            br_if 3 (;@8;)
                            br 6 (;@5;)
                          end
                          local.get 10
                          i32.const -32
                          i32.and
                          i32.const -96
                          i32.eq
                          br_if 2 (;@8;)
                          br 5 (;@5;)
                        end
                        local.get 10
                        i32.const -96
                        i32.lt_s
                        br_if 1 (;@8;)
                        br 4 (;@5;)
                      end
                      local.get 10
                      i32.const -65
                      i32.gt_s
                      br_if 3 (;@5;)
                    end
                    local.get 5
                    i32.const 2
                    i32.add
                    local.tee 5
                    local.get 3
                    i32.ge_u
                    br_if 2 (;@5;)
                    local.get 4
                    local.get 5
                    i32.add
                    i32.load8_s
                    i32.const -65
                    i32.le_s
                    br_if 3 (;@4;)
                    br 2 (;@5;)
                  end
                  local.get 5
                  i32.const 1
                  i32.add
                  local.set 5
                  br 3 (;@3;)
                end
                local.get 5
                i32.const 1
                i32.add
                local.tee 5
                local.get 3
                i32.ge_u
                br_if 0 (;@5;)
                local.get 4
                local.get 5
                i32.add
                i32.load8_s
                i32.const -65
                i32.le_s
                br_if 1 (;@4;)
              end
              local.get 2
              i32.const 8
              i32.add
              call $_ZN4core6result13unwrap_failed17h5da0ab182d2c24a1E
              unreachable
            end
            local.get 5
            i32.const 1
            i32.add
            local.set 5
          end
          local.get 5
          local.get 3
          i32.lt_u
          br_if 0 (;@2;)
        end
      end
      local.get 0
      local.get 3
      i32.store offset=4
      local.get 0
      local.get 4
      i32.store
      local.get 2
      i32.const 16
      i32.add
      global.set $__stack_pointer)
    (func $_ZN4core6result13unwrap_failed17h5da0ab182d2c24a1E (type 2) (param i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $contains (type 3) (param i32 i32) (result i64)
      (local i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i64)
      global.get $__stack_pointer
      i32.const 96
      i32.sub
      local.tee 2
      global.set $__stack_pointer
      local.get 2
      i32.const 8
      i32.add
      local.get 0
      call $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E
      local.get 2
      i32.load offset=12
      local.set 3
      local.get 2
      i32.load offset=8
      local.set 4
      local.get 2
      local.get 1
      call $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E
      local.get 2
      i32.load
      local.set 5
      block  ;; label = @1
        block  ;; label = @2
          block  ;; label = @3
            block  ;; label = @4
              block  ;; label = @5
                block  ;; label = @6
                  block  ;; label = @7
                    block  ;; label = @8
                      block  ;; label = @9
                        local.get 2
                        i32.load offset=4
                        local.tee 1
                        i32.eqz
                        br_if 0 (;@9;)
                        i32.const 1
                        local.set 6
                        i32.const 0
                        local.set 0
                        block  ;; label = @10
                          block  ;; label = @11
                            local.get 1
                            i32.const 1
                            i32.ne
                            br_if 0 (;@11;)
                            i32.const 1
                            local.set 7
                            i32.const 0
                            local.set 8
                            br 1 (;@10;)
                          end
                          i32.const 1
                          local.set 9
                          i32.const 0
                          local.set 10
                          i32.const 1
                          local.set 11
                          i32.const 0
                          local.set 0
                          i32.const 1
                          local.set 6
                          loop  ;; label = @11
                            local.get 11
                            local.set 12
                            block  ;; label = @12
                              block  ;; label = @13
                                block  ;; label = @14
                                  local.get 0
                                  local.get 10
                                  i32.add
                                  local.tee 11
                                  local.get 1
                                  i32.ge_u
                                  br_if 0 (;@14;)
                                  block  ;; label = @15
                                    local.get 5
                                    local.get 9
                                    i32.add
                                    i32.load8_u
                                    i32.const 255
                                    i32.and
                                    local.tee 9
                                    local.get 5
                                    local.get 11
                                    i32.add
                                    i32.load8_u
                                    local.tee 11
                                    i32.lt_u
                                    br_if 0 (;@15;)
                                    local.get 9
                                    local.get 11
                                    i32.eq
                                    br_if 2 (;@13;)
                                    i32.const 1
                                    local.set 6
                                    local.get 12
                                    i32.const 1
                                    i32.add
                                    local.set 11
                                    i32.const 0
                                    local.set 0
                                    local.get 12
                                    local.set 10
                                    br 3 (;@12;)
                                  end
                                  local.get 12
                                  local.get 0
                                  i32.add
                                  i32.const 1
                                  i32.add
                                  local.tee 11
                                  local.get 10
                                  i32.sub
                                  local.set 6
                                  i32.const 0
                                  local.set 0
                                  br 2 (;@12;)
                                end
                                local.get 11
                                local.get 1
                                call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                                unreachable
                              end
                              i32.const 0
                              local.get 0
                              i32.const 1
                              i32.add
                              local.tee 11
                              local.get 11
                              local.get 6
                              i32.eq
                              local.tee 9
                              select
                              local.set 0
                              local.get 11
                              i32.const 0
                              local.get 9
                              select
                              local.get 12
                              i32.add
                              local.set 11
                            end
                            local.get 11
                            local.get 0
                            i32.add
                            local.tee 9
                            local.get 1
                            i32.lt_u
                            br_if 0 (;@11;)
                          end
                          i32.const 1
                          local.set 9
                          i32.const 0
                          local.set 8
                          i32.const 1
                          local.set 11
                          i32.const 0
                          local.set 0
                          i32.const 1
                          local.set 7
                          loop  ;; label = @11
                            local.get 11
                            local.set 12
                            block  ;; label = @12
                              block  ;; label = @13
                                block  ;; label = @14
                                  local.get 0
                                  local.get 8
                                  i32.add
                                  local.tee 11
                                  local.get 1
                                  i32.ge_u
                                  br_if 0 (;@14;)
                                  block  ;; label = @15
                                    local.get 5
                                    local.get 9
                                    i32.add
                                    i32.load8_u
                                    i32.const 255
                                    i32.and
                                    local.tee 9
                                    local.get 5
                                    local.get 11
                                    i32.add
                                    i32.load8_u
                                    local.tee 11
                                    i32.gt_u
                                    br_if 0 (;@15;)
                                    local.get 9
                                    local.get 11
                                    i32.eq
                                    br_if 2 (;@13;)
                                    i32.const 1
                                    local.set 7
                                    local.get 12
                                    i32.const 1
                                    i32.add
                                    local.set 11
                                    i32.const 0
                                    local.set 0
                                    local.get 12
                                    local.set 8
                                    br 3 (;@12;)
                                  end
                                  local.get 12
                                  local.get 0
                                  i32.add
                                  i32.const 1
                                  i32.add
                                  local.tee 11
                                  local.get 8
                                  i32.sub
                                  local.set 7
                                  i32.const 0
                                  local.set 0
                                  br 2 (;@12;)
                                end
                                local.get 11
                                local.get 1
                                call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                                unreachable
                              end
                              i32.const 0
                              local.get 0
                              i32.const 1
                              i32.add
                              local.tee 11
                              local.get 11
                              local.get 7
                              i32.eq
                              local.tee 9
                              select
                              local.set 0
                              local.get 11
                              i32.const 0
                              local.get 9
                              select
                              local.get 12
                              i32.add
                              local.set 11
                            end
                            local.get 11
                            local.get 0
                            i32.add
                            local.tee 9
                            local.get 1
                            i32.lt_u
                            br_if 0 (;@11;)
                          end
                          local.get 10
                          local.set 0
                        end
                        block  ;; label = @10
                          local.get 1
                          local.get 0
                          local.get 8
                          local.get 0
                          local.get 8
                          i32.gt_u
                          local.tee 11
                          select
                          local.tee 13
                          i32.lt_u
                          br_if 0 (;@10;)
                          block  ;; label = @11
                            local.get 6
                            local.get 7
                            local.get 11
                            select
                            local.tee 11
                            local.get 13
                            i32.add
                            local.tee 0
                            local.get 11
                            i32.lt_u
                            br_if 0 (;@11;)
                            block  ;; label = @12
                              local.get 0
                              local.get 1
                              i32.gt_u
                              br_if 0 (;@12;)
                              local.get 5
                              local.get 5
                              local.get 11
                              i32.add
                              local.get 13
                              call $memcmp
                              br_if 5 (;@7;)
                              i32.const 1
                              local.set 8
                              i32.const 0
                              local.set 0
                              i32.const 1
                              local.set 9
                              i32.const 0
                              local.set 6
                              block  ;; label = @13
                                loop  ;; label = @14
                                  local.get 9
                                  local.tee 12
                                  local.get 0
                                  i32.add
                                  local.tee 7
                                  local.get 1
                                  i32.ge_u
                                  br_if 1 (;@13;)
                                  block  ;; label = @15
                                    block  ;; label = @16
                                      block  ;; label = @17
                                        block  ;; label = @18
                                          local.get 1
                                          local.get 0
                                          i32.sub
                                          local.get 12
                                          i32.const -1
                                          i32.xor
                                          i32.add
                                          local.tee 9
                                          local.get 1
                                          i32.ge_u
                                          br_if 0 (;@18;)
                                          local.get 1
                                          local.get 0
                                          i32.const -1
                                          i32.xor
                                          i32.add
                                          local.get 6
                                          i32.sub
                                          local.tee 10
                                          local.get 1
                                          i32.ge_u
                                          br_if 1 (;@17;)
                                          block  ;; label = @19
                                            local.get 5
                                            local.get 9
                                            i32.add
                                            i32.load8_u
                                            i32.const 255
                                            i32.and
                                            local.tee 9
                                            local.get 5
                                            local.get 10
                                            i32.add
                                            i32.load8_u
                                            local.tee 10
                                            i32.lt_u
                                            br_if 0 (;@19;)
                                            local.get 9
                                            local.get 10
                                            i32.eq
                                            br_if 3 (;@16;)
                                            local.get 12
                                            i32.const 1
                                            i32.add
                                            local.set 9
                                            i32.const 0
                                            local.set 0
                                            i32.const 1
                                            local.set 8
                                            local.get 12
                                            local.set 6
                                            br 4 (;@15;)
                                          end
                                          local.get 7
                                          i32.const 1
                                          i32.add
                                          local.tee 9
                                          local.get 6
                                          i32.sub
                                          local.set 8
                                          i32.const 0
                                          local.set 0
                                          br 3 (;@15;)
                                        end
                                        local.get 9
                                        local.get 1
                                        call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                                        unreachable
                                      end
                                      local.get 10
                                      local.get 1
                                      call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                                      unreachable
                                    end
                                    i32.const 0
                                    local.get 0
                                    i32.const 1
                                    i32.add
                                    local.tee 9
                                    local.get 9
                                    local.get 8
                                    i32.eq
                                    local.tee 10
                                    select
                                    local.set 0
                                    local.get 9
                                    i32.const 0
                                    local.get 10
                                    select
                                    local.get 12
                                    i32.add
                                    local.set 9
                                  end
                                  local.get 8
                                  local.get 11
                                  i32.ne
                                  br_if 0 (;@14;)
                                end
                              end
                              i32.const 1
                              local.set 8
                              i32.const 0
                              local.set 0
                              i32.const 1
                              local.set 9
                              i32.const 0
                              local.set 7
                              block  ;; label = @13
                                loop  ;; label = @14
                                  local.get 9
                                  local.tee 12
                                  local.get 0
                                  i32.add
                                  local.tee 14
                                  local.get 1
                                  i32.ge_u
                                  br_if 1 (;@13;)
                                  block  ;; label = @15
                                    block  ;; label = @16
                                      block  ;; label = @17
                                        block  ;; label = @18
                                          local.get 1
                                          local.get 0
                                          i32.sub
                                          local.get 12
                                          i32.const -1
                                          i32.xor
                                          i32.add
                                          local.tee 9
                                          local.get 1
                                          i32.ge_u
                                          br_if 0 (;@18;)
                                          local.get 1
                                          local.get 0
                                          i32.const -1
                                          i32.xor
                                          i32.add
                                          local.get 7
                                          i32.sub
                                          local.tee 10
                                          local.get 1
                                          i32.ge_u
                                          br_if 1 (;@17;)
                                          block  ;; label = @19
                                            local.get 5
                                            local.get 9
                                            i32.add
                                            i32.load8_u
                                            i32.const 255
                                            i32.and
                                            local.tee 9
                                            local.get 5
                                            local.get 10
                                            i32.add
                                            i32.load8_u
                                            local.tee 10
                                            i32.gt_u
                                            br_if 0 (;@19;)
                                            local.get 9
                                            local.get 10
                                            i32.eq
                                            br_if 3 (;@16;)
                                            local.get 12
                                            i32.const 1
                                            i32.add
                                            local.set 9
                                            i32.const 0
                                            local.set 0
                                            i32.const 1
                                            local.set 8
                                            local.get 12
                                            local.set 7
                                            br 4 (;@15;)
                                          end
                                          local.get 14
                                          i32.const 1
                                          i32.add
                                          local.tee 9
                                          local.get 7
                                          i32.sub
                                          local.set 8
                                          i32.const 0
                                          local.set 0
                                          br 3 (;@15;)
                                        end
                                        local.get 9
                                        local.get 1
                                        call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                                        unreachable
                                      end
                                      local.get 10
                                      local.get 1
                                      call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                                      unreachable
                                    end
                                    i32.const 0
                                    local.get 0
                                    i32.const 1
                                    i32.add
                                    local.tee 9
                                    local.get 9
                                    local.get 8
                                    i32.eq
                                    local.tee 10
                                    select
                                    local.set 0
                                    local.get 9
                                    i32.const 0
                                    local.get 10
                                    select
                                    local.get 12
                                    i32.add
                                    local.set 9
                                  end
                                  local.get 8
                                  local.get 11
                                  i32.ne
                                  br_if 0 (;@14;)
                                end
                              end
                              block  ;; label = @13
                                local.get 11
                                local.get 1
                                i32.gt_u
                                br_if 0 (;@13;)
                                local.get 1
                                local.get 6
                                local.get 7
                                local.get 6
                                local.get 7
                                i32.gt_u
                                select
                                i32.sub
                                local.set 10
                                i32.const 0
                                local.set 8
                                block  ;; label = @14
                                  local.get 11
                                  br_if 0 (;@14;)
                                  i64.const 0
                                  local.set 15
                                  i32.const 0
                                  local.set 11
                                  br 6 (;@8;)
                                end
                                local.get 11
                                i32.const 3
                                i32.and
                                local.set 12
                                block  ;; label = @14
                                  block  ;; label = @15
                                    local.get 11
                                    i32.const -1
                                    i32.add
                                    i32.const 3
                                    i32.ge_u
                                    br_if 0 (;@15;)
                                    i64.const 0
                                    local.set 15
                                    local.get 5
                                    local.set 0
                                    br 1 (;@14;)
                                  end
                                  local.get 11
                                  i32.const -4
                                  i32.and
                                  local.set 9
                                  i64.const 0
                                  local.set 15
                                  local.get 5
                                  local.set 0
                                  loop  ;; label = @15
                                    i64.const 1
                                    local.get 0
                                    i32.const 3
                                    i32.add
                                    i64.load8_u
                                    i64.shl
                                    i64.const 1
                                    local.get 0
                                    i32.const 2
                                    i32.add
                                    i64.load8_u
                                    i64.shl
                                    i64.const 1
                                    local.get 0
                                    i32.const 1
                                    i32.add
                                    i64.load8_u
                                    i64.shl
                                    i64.const 1
                                    local.get 0
                                    i64.load8_u
                                    i64.shl
                                    local.get 15
                                    i64.or
                                    i64.or
                                    i64.or
                                    i64.or
                                    local.set 15
                                    local.get 0
                                    i32.const 4
                                    i32.add
                                    local.set 0
                                    local.get 9
                                    i32.const -4
                                    i32.add
                                    local.tee 9
                                    br_if 0 (;@15;)
                                  end
                                end
                                local.get 12
                                i32.eqz
                                br_if 5 (;@8;)
                                loop  ;; label = @14
                                  i64.const 1
                                  local.get 0
                                  i64.load8_u
                                  i64.shl
                                  local.get 15
                                  i64.or
                                  local.set 15
                                  local.get 0
                                  i32.const 1
                                  i32.add
                                  local.set 0
                                  local.get 12
                                  i32.const -1
                                  i32.add
                                  local.tee 12
                                  br_if 0 (;@14;)
                                  br 6 (;@8;)
                                end
                              end
                              local.get 11
                              local.get 1
                              call $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E
                              unreachable
                            end
                            local.get 0
                            local.get 1
                            call $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E
                            unreachable
                          end
                          local.get 11
                          local.get 0
                          call $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE
                          unreachable
                        end
                        local.get 13
                        local.get 1
                        call $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E
                        unreachable
                      end
                      local.get 2
                      i32.const 92
                      i32.add
                      i32.const 0
                      i32.store
                      local.get 2
                      i32.const 84
                      i32.add
                      local.get 3
                      i32.store
                      local.get 2
                      i32.const 44
                      i32.add
                      i32.const 257
                      i32.store16
                      local.get 2
                      i32.const 40
                      i32.add
                      local.get 3
                      i32.store
                      local.get 2
                      local.get 5
                      i32.store offset=88
                      local.get 2
                      local.get 4
                      i32.store offset=80
                      local.get 2
                      i32.const 0
                      i32.store8 offset=46
                      local.get 2
                      i64.const 0
                      i64.store offset=32
                      i32.const 1
                      local.set 1
                      br 3 (;@5;)
                    end
                    local.get 1
                    local.set 0
                    br 1 (;@6;)
                  end
                  local.get 13
                  local.get 1
                  local.get 13
                  i32.sub
                  local.tee 9
                  i32.gt_u
                  local.set 8
                  local.get 1
                  i32.const 3
                  i32.and
                  local.set 11
                  block  ;; label = @7
                    block  ;; label = @8
                      local.get 1
                      i32.const -1
                      i32.add
                      i32.const 3
                      i32.ge_u
                      br_if 0 (;@8;)
                      i64.const 0
                      local.set 15
                      local.get 5
                      local.set 0
                      br 1 (;@7;)
                    end
                    local.get 1
                    i32.const -4
                    i32.and
                    local.set 12
                    i64.const 0
                    local.set 15
                    local.get 5
                    local.set 0
                    loop  ;; label = @8
                      i64.const 1
                      local.get 0
                      i32.const 3
                      i32.add
                      i64.load8_u
                      i64.shl
                      i64.const 1
                      local.get 0
                      i32.const 2
                      i32.add
                      i64.load8_u
                      i64.shl
                      i64.const 1
                      local.get 0
                      i32.const 1
                      i32.add
                      i64.load8_u
                      i64.shl
                      i64.const 1
                      local.get 0
                      i64.load8_u
                      i64.shl
                      local.get 15
                      i64.or
                      i64.or
                      i64.or
                      i64.or
                      local.set 15
                      local.get 0
                      i32.const 4
                      i32.add
                      local.set 0
                      local.get 12
                      i32.const -4
                      i32.add
                      local.tee 12
                      br_if 0 (;@8;)
                    end
                  end
                  local.get 13
                  local.get 9
                  local.get 8
                  select
                  local.set 12
                  block  ;; label = @7
                    local.get 11
                    i32.eqz
                    br_if 0 (;@7;)
                    loop  ;; label = @8
                      i64.const 1
                      local.get 0
                      i64.load8_u
                      i64.shl
                      local.get 15
                      i64.or
                      local.set 15
                      local.get 0
                      i32.const 1
                      i32.add
                      local.set 0
                      local.get 11
                      i32.const -1
                      i32.add
                      local.tee 11
                      br_if 0 (;@8;)
                    end
                  end
                  local.get 12
                  i32.const 1
                  i32.add
                  local.set 11
                  i32.const -1
                  local.set 8
                  local.get 13
                  local.set 10
                  i32.const -1
                  local.set 0
                end
                local.get 2
                i32.const 92
                i32.add
                local.get 1
                i32.store
                local.get 2
                i32.const 84
                i32.add
                local.get 3
                i32.store
                local.get 2
                i32.const 72
                i32.add
                local.get 0
                i32.store
                local.get 2
                i32.const 68
                i32.add
                local.get 8
                i32.store
                local.get 2
                i32.const 64
                i32.add
                local.get 3
                i32.store
                local.get 2
                i32.const 60
                i32.add
                i32.const 0
                i32.store
                local.get 2
                i32.const 56
                i32.add
                local.get 11
                i32.store
                local.get 2
                i32.const 52
                i32.add
                local.get 10
                i32.store
                local.get 2
                i32.const 48
                i32.add
                local.get 13
                i32.store
                local.get 2
                i32.const 40
                i32.add
                local.tee 0
                local.get 15
                i64.store
                local.get 2
                local.get 5
                i32.store offset=88
                local.get 2
                local.get 4
                i32.store offset=80
                local.get 2
                i32.const 1
                i32.store offset=32
                local.get 1
                br_if 2 (;@3;)
                local.get 15
                i64.const 48
                i64.shr_u
                i32.wrap_i64
                i32.const 255
                i32.and
                br_if 1 (;@4;)
                local.get 15
                i64.const 32
                i64.shr_u
                i32.wrap_i64
                local.set 1
              end
              local.get 1
              i32.const 255
              i32.and
              i32.eqz
              local.set 5
              i32.const 0
              local.set 1
              block  ;; label = @5
                block  ;; label = @6
                  loop  ;; label = @7
                    block  ;; label = @8
                      local.get 1
                      i32.eqz
                      br_if 0 (;@8;)
                      block  ;; label = @9
                        local.get 3
                        local.get 1
                        i32.gt_u
                        br_if 0 (;@9;)
                        local.get 3
                        local.get 1
                        i32.eq
                        br_if 1 (;@8;)
                        br 8 (;@1;)
                      end
                      local.get 4
                      local.get 1
                      i32.add
                      i32.load8_s
                      i32.const -65
                      i32.le_s
                      br_if 7 (;@1;)
                    end
                    block  ;; label = @8
                      local.get 1
                      local.get 3
                      i32.eq
                      br_if 0 (;@8;)
                      block  ;; label = @9
                        block  ;; label = @10
                          local.get 4
                          local.get 1
                          i32.add
                          local.tee 11
                          i32.load8_s
                          local.tee 0
                          i32.const -1
                          i32.le_s
                          br_if 0 (;@10;)
                          local.get 0
                          i32.const 255
                          i32.and
                          local.set 0
                          br 1 (;@9;)
                        end
                        local.get 11
                        i32.load8_u offset=1
                        i32.const 63
                        i32.and
                        local.set 12
                        local.get 0
                        i32.const 31
                        i32.and
                        local.set 9
                        block  ;; label = @10
                          local.get 0
                          i32.const -33
                          i32.gt_u
                          br_if 0 (;@10;)
                          local.get 9
                          i32.const 6
                          i32.shl
                          local.get 12
                          i32.or
                          local.set 0
                          br 1 (;@9;)
                        end
                        local.get 12
                        i32.const 6
                        i32.shl
                        local.get 11
                        i32.load8_u offset=2
                        i32.const 63
                        i32.and
                        i32.or
                        local.set 12
                        block  ;; label = @10
                          local.get 0
                          i32.const -16
                          i32.ge_u
                          br_if 0 (;@10;)
                          local.get 12
                          local.get 9
                          i32.const 12
                          i32.shl
                          i32.or
                          local.set 0
                          br 1 (;@9;)
                        end
                        local.get 12
                        i32.const 6
                        i32.shl
                        local.get 11
                        i32.load8_u offset=3
                        i32.const 63
                        i32.and
                        i32.or
                        local.get 9
                        i32.const 18
                        i32.shl
                        i32.const 1835008
                        i32.and
                        i32.or
                        local.set 0
                      end
                      block  ;; label = @9
                        local.get 5
                        i32.const 1
                        i32.and
                        br_if 0 (;@9;)
                        local.get 1
                        local.set 3
                        br 3 (;@6;)
                      end
                      local.get 0
                      i32.const 1114112
                      i32.eq
                      br_if 3 (;@5;)
                      i32.const 1
                      local.set 5
                      block  ;; label = @9
                        local.get 0
                        i32.const 128
                        i32.lt_u
                        br_if 0 (;@9;)
                        i32.const 2
                        local.set 5
                        local.get 0
                        i32.const 2048
                        i32.lt_u
                        br_if 0 (;@9;)
                        i32.const 3
                        i32.const 4
                        local.get 0
                        i32.const 65536
                        i32.lt_u
                        select
                        local.set 5
                      end
                      local.get 5
                      local.get 1
                      i32.add
                      local.set 1
                      i32.const 0
                      local.set 5
                      br 1 (;@7;)
                    end
                  end
                  local.get 5
                  i32.const 1
                  i32.and
                  br_if 1 (;@5;)
                end
                local.get 2
                i32.const 24
                i32.add
                local.get 3
                i32.store
                local.get 2
                local.get 3
                i32.store offset=20
                local.get 2
                i32.const 1
                i32.store offset=16
                br 3 (;@2;)
              end
              local.get 2
              i32.const 1
              i32.store8 offset=46
            end
            local.get 2
            i32.const 0
            i32.store offset=16
            br 1 (;@2;)
          end
          block  ;; label = @3
            local.get 8
            i32.const -1
            i32.eq
            br_if 0 (;@3;)
            local.get 2
            i32.const 16
            i32.add
            local.get 0
            local.get 4
            local.get 3
            local.get 5
            local.get 1
            i32.const 0
            call $_ZN4core3str7pattern14TwoWaySearcher4next17hc1bfb3e180a449d2E
            br 1 (;@2;)
          end
          local.get 2
          i32.const 16
          i32.add
          local.get 0
          local.get 4
          local.get 3
          local.get 5
          local.get 1
          i32.const 1
          call $_ZN4core3str7pattern14TwoWaySearcher4next17hc1bfb3e180a449d2E
        end
        local.get 2
        i64.load32_u offset=16
        local.set 15
        local.get 2
        i32.const 96
        i32.add
        global.set $__stack_pointer
        local.get 15
        return
      end
      local.get 4
      local.get 3
      local.get 1
      local.get 3
      call $_ZN4core3str16slice_error_fail17h08a4f4832f08c369E
      unreachable)
    (func $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E (type 1) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core10intrinsics17const_eval_select17h2cb6051202c964daE
      unreachable)
    (func $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE (type 1) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core10intrinsics17const_eval_select17hf41eeec4c1f94fc5E
      unreachable)
    (func $_ZN4core3str16slice_error_fail17h08a4f4832f08c369E (type 4) (param i32 i32 i32 i32)
      (local i32)
      global.get $__stack_pointer
      i32.const 16
      i32.sub
      local.tee 4
      global.set $__stack_pointer
      local.get 4
      local.get 3
      i32.store offset=12
      local.get 4
      local.get 2
      i32.store offset=8
      local.get 4
      local.get 1
      i32.store offset=4
      local.get 4
      local.get 0
      i32.store
      local.get 4
      call $_ZN4core10intrinsics17const_eval_select17h91adfe0d3c9924f7E
      unreachable)
    (func $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE (type 5)
      loop  ;; label = @1
        br 0 (;@1;)
      end)
    (func $_ZN4core10intrinsics17const_eval_select17h2cb6051202c964daE (type 1) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core3ops8function6FnOnce9call_once17hd203256c8930783eE
      unreachable)
    (func $_ZN4core3ops8function6FnOnce9call_once17hd203256c8930783eE (type 1) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core5slice5index27slice_end_index_len_fail_rt17h962df0d32abc7149E
      unreachable)
    (func $_ZN4core5slice5index27slice_end_index_len_fail_rt17h962df0d32abc7149E (type 1) (param i32 i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $_ZN4core9panicking5panic17h6f5024a57ca8da86E (type 5)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $_ZN4core10intrinsics17const_eval_select17hf41eeec4c1f94fc5E (type 1) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core3ops8function6FnOnce9call_once17hc0d1c496e6d46c21E
      unreachable)
    (func $_ZN4core3ops8function6FnOnce9call_once17hc0d1c496e6d46c21E (type 1) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core5slice5index25slice_index_order_fail_rt17h4242a308b2a8c792E
      unreachable)
    (func $_ZN4core5slice5index25slice_index_order_fail_rt17h4242a308b2a8c792E (type 1) (param i32 i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $_ZN4core10intrinsics17const_eval_select17h91adfe0d3c9924f7E (type 2) (param i32)
      local.get 0
      i32.load
      local.get 0
      i32.load offset=4
      local.get 0
      i32.load offset=8
      local.get 0
      i32.load offset=12
      call $_ZN4core3ops8function6FnOnce9call_once17h548679a6ecb90ce4E
      unreachable)
    (func $_ZN4core3ops8function6FnOnce9call_once17h548679a6ecb90ce4E (type 4) (param i32 i32 i32 i32)
      local.get 0
      local.get 1
      local.get 2
      local.get 3
      call $_ZN4core3str19slice_error_fail_rt17h6d55bbd538b77cbcE
      unreachable)
    (func $_ZN4core3str19slice_error_fail_rt17h6d55bbd538b77cbcE (type 4) (param i32 i32 i32 i32)
      (local i32 i32 i32 i32)
      block  ;; label = @1
        local.get 1
        i32.const 257
        i32.lt_u
        br_if 0 (;@1;)
        local.get 0
        i32.load8_s offset=256
        i32.const -65
        i32.gt_s
        br_if 0 (;@1;)
        local.get 0
        i32.load8_s offset=255
        i32.const -65
        i32.gt_s
        drop
      end
      block  ;; label = @1
        block  ;; label = @2
          block  ;; label = @3
            block  ;; label = @4
              local.get 2
              local.get 1
              i32.gt_u
              br_if 0 (;@4;)
              local.get 3
              local.get 1
              i32.gt_u
              br_if 0 (;@4;)
              local.get 2
              local.get 3
              i32.gt_u
              br_if 0 (;@4;)
              block  ;; label = @5
                block  ;; label = @6
                  local.get 2
                  i32.eqz
                  br_if 0 (;@6;)
                  block  ;; label = @7
                    local.get 2
                    local.get 1
                    i32.lt_u
                    br_if 0 (;@7;)
                    local.get 1
                    local.get 2
                    i32.eq
                    br_if 1 (;@6;)
                    br 2 (;@5;)
                  end
                  local.get 0
                  local.get 2
                  i32.add
                  i32.load8_s
                  i32.const -64
                  i32.lt_s
                  br_if 1 (;@5;)
                end
                local.get 3
                local.set 2
              end
              local.get 1
              local.set 3
              block  ;; label = @5
                local.get 2
                local.get 1
                i32.ge_u
                br_if 0 (;@5;)
                local.get 2
                i32.const 1
                i32.add
                local.tee 4
                i32.const 0
                local.get 2
                i32.const -3
                i32.add
                local.tee 3
                local.get 3
                local.get 2
                i32.gt_u
                select
                local.tee 3
                i32.lt_u
                br_if 2 (;@3;)
                block  ;; label = @6
                  local.get 3
                  local.get 4
                  i32.eq
                  br_if 0 (;@6;)
                  local.get 0
                  local.get 4
                  i32.add
                  local.get 0
                  local.get 3
                  i32.add
                  local.tee 5
                  i32.sub
                  local.set 4
                  block  ;; label = @7
                    local.get 0
                    local.get 2
                    i32.add
                    local.tee 6
                    i32.load8_s
                    i32.const -65
                    i32.le_s
                    br_if 0 (;@7;)
                    local.get 4
                    i32.const -1
                    i32.add
                    local.set 7
                    br 1 (;@6;)
                  end
                  local.get 3
                  local.get 2
                  i32.eq
                  br_if 0 (;@6;)
                  block  ;; label = @7
                    local.get 6
                    i32.const -1
                    i32.add
                    local.tee 2
                    i32.load8_s
                    i32.const -65
                    i32.le_s
                    br_if 0 (;@7;)
                    local.get 4
                    i32.const -2
                    i32.add
                    local.set 7
                    br 1 (;@6;)
                  end
                  local.get 5
                  local.get 2
                  i32.eq
                  br_if 0 (;@6;)
                  block  ;; label = @7
                    local.get 6
                    i32.const -2
                    i32.add
                    local.tee 2
                    i32.load8_s
                    i32.const -65
                    i32.le_s
                    br_if 0 (;@7;)
                    local.get 4
                    i32.const -3
                    i32.add
                    local.set 7
                    br 1 (;@6;)
                  end
                  local.get 5
                  local.get 2
                  i32.eq
                  br_if 0 (;@6;)
                  block  ;; label = @7
                    local.get 6
                    i32.const -3
                    i32.add
                    local.tee 2
                    i32.load8_s
                    i32.const -65
                    i32.le_s
                    br_if 0 (;@7;)
                    local.get 4
                    i32.const -4
                    i32.add
                    local.set 7
                    br 1 (;@6;)
                  end
                  local.get 5
                  local.get 2
                  i32.eq
                  br_if 0 (;@6;)
                  local.get 4
                  i32.const -5
                  i32.add
                  local.set 7
                end
                local.get 7
                local.get 3
                i32.add
                local.set 3
              end
              block  ;; label = @5
                local.get 3
                i32.eqz
                br_if 0 (;@5;)
                block  ;; label = @6
                  local.get 3
                  local.get 1
                  i32.lt_u
                  br_if 0 (;@6;)
                  local.get 3
                  local.get 1
                  i32.eq
                  br_if 1 (;@5;)
                  br 5 (;@1;)
                end
                local.get 0
                local.get 3
                i32.add
                i32.load8_s
                i32.const -65
                i32.le_s
                br_if 4 (;@1;)
              end
              local.get 3
              local.get 1
              i32.eq
              br_if 2 (;@2;)
              local.get 0
              local.get 3
              i32.add
              local.tee 2
              i32.load8_s
              local.tee 1
              i32.const -1
              i32.gt_s
              br_if 0 (;@4;)
              local.get 1
              i32.const -32
              i32.lt_u
              br_if 0 (;@4;)
              local.get 1
              i32.const -16
              i32.lt_u
              br_if 0 (;@4;)
              local.get 2
              i32.load8_u offset=1
              i32.const 63
              i32.and
              i32.const 12
              i32.shl
              local.get 2
              i32.load8_u offset=2
              i32.const 63
              i32.and
              i32.const 6
              i32.shl
              i32.or
              local.get 2
              i32.load8_u offset=3
              i32.const 63
              i32.and
              i32.or
              local.get 1
              i32.const 255
              i32.and
              i32.const 18
              i32.shl
              i32.const 1835008
              i32.and
              i32.or
              i32.const 1114112
              i32.eq
              br_if 2 (;@2;)
            end
            call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
            unreachable
          end
          local.get 3
          local.get 4
          call $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE
          unreachable
        end
        call $_ZN4core9panicking5panic17h6f5024a57ca8da86E
        unreachable
      end
      local.get 0
      local.get 1
      local.get 3
      local.get 1
      call $_ZN4core3str16slice_error_fail17h08a4f4832f08c369E
      unreachable)
    (func $strlen (type 6) (param i32) (result i32)
      local.get 0
      call $_ZN17compiler_builtins3mem6strlen17h43cca548dfe67c86E)
    (func $_ZN17compiler_builtins3mem6memcmp17hec639d9336f893c0E (type 7) (param i32 i32 i32) (result i32)
      (local i32 i32 i32)
      i32.const 0
      local.set 3
      block  ;; label = @1
        local.get 2
        i32.eqz
        br_if 0 (;@1;)
        block  ;; label = @2
          loop  ;; label = @3
            local.get 0
            i32.load8_u
            local.tee 4
            local.get 1
            i32.load8_u
            local.tee 5
            i32.ne
            br_if 1 (;@2;)
            local.get 0
            i32.const 1
            i32.add
            local.set 0
            local.get 1
            i32.const 1
            i32.add
            local.set 1
            local.get 2
            i32.const -1
            i32.add
            local.tee 2
            i32.eqz
            br_if 2 (;@1;)
            br 0 (;@3;)
          end
        end
        local.get 4
        local.get 5
        i32.sub
        local.set 3
      end
      local.get 3)
    (func $_ZN17compiler_builtins3mem6strlen17h43cca548dfe67c86E (type 6) (param i32) (result i32)
      (local i32 i32 i32)
      block  ;; label = @1
        block  ;; label = @2
          local.get 0
          i32.load8_u
          br_if 0 (;@2;)
          i32.const 0
          local.set 1
          br 1 (;@1;)
        end
        local.get 0
        i32.const 1
        i32.add
        local.set 2
        i32.const 0
        local.set 0
        loop  ;; label = @2
          local.get 2
          local.get 0
          i32.add
          local.set 3
          local.get 0
          i32.const 1
          i32.add
          local.tee 1
          local.set 0
          local.get 3
          i32.load8_u
          br_if 0 (;@2;)
        end
      end
      local.get 1)
    (func $memcmp (type 7) (param i32 i32 i32) (result i32)
      local.get 0
      local.get 1
      local.get 2
      call $_ZN17compiler_builtins3mem6memcmp17hec639d9336f893c0E)
    (memory (;0;) 17)
    (global $__stack_pointer (mut i32) (i32.const 1048576))
    (global (;1;) i32 (i32.const 1048835))
    (global (;2;) i32 (i32.const 1048848))
    (export "memory" (memory 0))
    (export "contains" (func $contains))
    (export "__data_end" (global 1))
    (export "__heap_base" (global 2))
    (data $.rodata (i32.const 1048576) "???\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\03\03\03\03\03\03\03\03\03\03\03\03\03\03\03\03\04\04\04\04\04\00\00\00\00\00\00\00\00\00\00\00"))
  
"#;

static CONCAT3_SRC: &str = r#"

(module
    (type (;0;) (func (param i32 i32)))
    (type (;1;) (func (param i32 i32 i32) (result i32)))
    (type (;2;) (func))
    (type (;3;) (func (param i32) (result i32)))
    (func $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E (type 0) (param i32 i32)
      (local i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
      i32.const 3
      local.set 2
      i32.const 1048576
      local.set 3
      block  ;; label = @1
        local.get 1
        i32.load8_u
        i32.const 3
        i32.ne
        br_if 0 (;@1;)
        block  ;; label = @2
          local.get 1
          i32.const 1
          i32.add
          local.tee 4
          call $strlen
          local.tee 5
          i32.eqz
          br_if 0 (;@2;)
          i32.const 0
          local.get 5
          i32.const -7
          i32.add
          local.tee 6
          local.get 6
          local.get 5
          i32.gt_u
          select
          local.set 7
          local.get 1
          i32.const 4
          i32.add
          i32.const -4
          i32.and
          local.get 4
          i32.sub
          local.set 8
          i32.const 0
          local.set 6
          loop  ;; label = @3
            block  ;; label = @4
              block  ;; label = @5
                block  ;; label = @6
                  block  ;; label = @7
                    local.get 4
                    local.get 6
                    i32.add
                    i32.load8_u
                    local.tee 9
                    i32.const 24
                    i32.shl
                    i32.const 24
                    i32.shr_s
                    local.tee 10
                    i32.const 0
                    i32.lt_s
                    br_if 0 (;@7;)
                    local.get 8
                    i32.const -1
                    i32.eq
                    br_if 1 (;@6;)
                    local.get 8
                    local.get 6
                    i32.sub
                    i32.const 3
                    i32.and
                    br_if 1 (;@6;)
                    block  ;; label = @8
                      local.get 6
                      local.get 7
                      i32.ge_u
                      br_if 0 (;@8;)
                      loop  ;; label = @9
                        local.get 1
                        local.get 6
                        i32.add
                        local.tee 9
                        i32.const 1
                        i32.add
                        i32.load
                        local.get 9
                        i32.const 5
                        i32.add
                        i32.load
                        i32.or
                        i32.const -2139062144
                        i32.and
                        br_if 1 (;@8;)
                        local.get 6
                        i32.const 8
                        i32.add
                        local.tee 6
                        local.get 7
                        i32.lt_u
                        br_if 0 (;@9;)
                      end
                    end
                    local.get 6
                    local.get 5
                    i32.ge_u
                    br_if 3 (;@4;)
                    loop  ;; label = @8
                      local.get 4
                      local.get 6
                      i32.add
                      i32.load8_s
                      i32.const 0
                      i32.lt_s
                      br_if 4 (;@4;)
                      local.get 5
                      local.get 6
                      i32.const 1
                      i32.add
                      local.tee 6
                      i32.ne
                      br_if 0 (;@8;)
                      br 6 (;@2;)
                    end
                  end
                  i32.const 1048579
                  local.set 3
                  i32.const 3
                  local.set 2
                  block  ;; label = @7
                    block  ;; label = @8
                      block  ;; label = @9
                        local.get 9
                        i32.const 1048582
                        i32.add
                        i32.load8_u
                        i32.const -2
                        i32.add
                        br_table 0 (;@9;) 2 (;@7;) 1 (;@8;) 8 (;@1;)
                      end
                      local.get 6
                      i32.const 1
                      i32.add
                      local.tee 6
                      local.get 5
                      i32.ge_u
                      br_if 7 (;@1;)
                      local.get 4
                      local.get 6
                      i32.add
                      i32.load8_s
                      i32.const -65
                      i32.le_s
                      br_if 3 (;@5;)
                      br 7 (;@1;)
                    end
                    local.get 6
                    i32.const 1
                    i32.add
                    local.tee 11
                    local.get 5
                    i32.ge_u
                    br_if 6 (;@1;)
                    local.get 4
                    local.get 11
                    i32.add
                    i32.load8_s
                    local.set 11
                    block  ;; label = @8
                      block  ;; label = @9
                        block  ;; label = @10
                          block  ;; label = @11
                            local.get 9
                            i32.const -240
                            i32.add
                            br_table 1 (;@10;) 0 (;@11;) 0 (;@11;) 0 (;@11;) 2 (;@9;) 0 (;@11;)
                          end
                          local.get 10
                          i32.const 15
                          i32.add
                          i32.const 255
                          i32.and
                          i32.const 2
                          i32.gt_u
                          br_if 9 (;@1;)
                          local.get 11
                          i32.const -1
                          i32.gt_s
                          br_if 9 (;@1;)
                          local.get 11
                          i32.const -64
                          i32.lt_u
                          br_if 2 (;@8;)
                          br 9 (;@1;)
                        end
                        local.get 11
                        i32.const 112
                        i32.add
                        i32.const 255
                        i32.and
                        i32.const 48
                        i32.lt_u
                        br_if 1 (;@8;)
                        br 8 (;@1;)
                      end
                      local.get 11
                      i32.const -113
                      i32.gt_s
                      br_if 7 (;@1;)
                    end
                    local.get 6
                    i32.const 2
                    i32.add
                    local.tee 9
                    local.get 5
                    i32.ge_u
                    br_if 6 (;@1;)
                    local.get 4
                    local.get 9
                    i32.add
                    i32.load8_s
                    i32.const -65
                    i32.gt_s
                    br_if 6 (;@1;)
                    i32.const 3
                    local.set 2
                    local.get 6
                    i32.const 3
                    i32.add
                    local.tee 6
                    local.get 5
                    i32.ge_u
                    br_if 6 (;@1;)
                    local.get 4
                    local.get 6
                    i32.add
                    i32.load8_s
                    i32.const -65
                    i32.gt_s
                    br_if 6 (;@1;)
                    br 2 (;@5;)
                  end
                  local.get 6
                  i32.const 1
                  i32.add
                  local.tee 11
                  local.get 5
                  i32.ge_u
                  br_if 5 (;@1;)
                  local.get 4
                  local.get 11
                  i32.add
                  i32.load8_s
                  local.set 11
                  block  ;; label = @7
                    block  ;; label = @8
                      block  ;; label = @9
                        block  ;; label = @10
                          local.get 9
                          i32.const 224
                          i32.eq
                          br_if 0 (;@10;)
                          local.get 9
                          i32.const 237
                          i32.eq
                          br_if 1 (;@9;)
                          local.get 10
                          i32.const 31
                          i32.add
                          i32.const 255
                          i32.and
                          i32.const 12
                          i32.lt_u
                          br_if 2 (;@8;)
                          local.get 10
                          i32.const -2
                          i32.and
                          i32.const -18
                          i32.ne
                          br_if 9 (;@1;)
                          local.get 11
                          i32.const -1
                          i32.gt_s
                          br_if 9 (;@1;)
                          local.get 11
                          i32.const -64
                          i32.lt_u
                          br_if 3 (;@7;)
                          br 9 (;@1;)
                        end
                        local.get 11
                        i32.const -32
                        i32.and
                        i32.const -96
                        i32.eq
                        br_if 2 (;@7;)
                        br 8 (;@1;)
                      end
                      local.get 11
                      i32.const -96
                      i32.lt_s
                      br_if 1 (;@7;)
                      br 7 (;@1;)
                    end
                    local.get 11
                    i32.const -65
                    i32.gt_s
                    br_if 6 (;@1;)
                  end
                  local.get 6
                  i32.const 2
                  i32.add
                  local.tee 6
                  local.get 5
                  i32.ge_u
                  br_if 5 (;@1;)
                  local.get 4
                  local.get 6
                  i32.add
                  i32.load8_s
                  i32.const -65
                  i32.le_s
                  br_if 1 (;@5;)
                  br 5 (;@1;)
                end
                local.get 6
                i32.const 1
                i32.add
                local.set 6
                br 1 (;@4;)
              end
              local.get 6
              i32.const 1
              i32.add
              local.set 6
            end
            local.get 6
            local.get 5
            i32.lt_u
            br_if 0 (;@3;)
          end
        end
        local.get 5
        local.set 2
        local.get 4
        local.set 3
      end
      local.get 0
      local.get 2
      i32.store offset=4
      local.get 0
      local.get 3
      i32.store)
    (func $concat3 (type 1) (param i32 i32 i32) (result i32)
      (local i32 i32 i32 i32 i32 i32 i32 i32 i32)
      global.get $__stack_pointer
      i32.const 32
      i32.sub
      local.tee 3
      global.set $__stack_pointer
      local.get 3
      i32.const 24
      i32.add
      local.get 0
      call $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E
      local.get 3
      i32.load offset=24
      local.set 4
      local.get 3
      i32.load offset=28
      local.set 0
      local.get 3
      i32.const 16
      i32.add
      local.get 1
      call $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E
      local.get 3
      i32.load offset=16
      local.set 5
      local.get 3
      i32.load offset=20
      local.set 6
      local.get 3
      i32.const 8
      i32.add
      local.get 2
      call $_ZN14libsql_bindgen6to_str17he3596e655b78fda2E
      local.get 3
      i32.load offset=8
      local.set 7
      local.get 6
      local.get 0
      i32.add
      local.tee 8
      local.get 3
      i32.load offset=12
      local.tee 9
      i32.add
      local.tee 10
      i32.const 65535
      i32.add
      i32.const 16
      i32.shr_u
      memory.grow
      local.set 2
      block  ;; label = @1
        block  ;; label = @2
          local.get 10
          i32.const 2
          i32.add
          local.tee 1
          i32.eqz
          br_if 0 (;@2;)
          local.get 2
          i32.const 16
          i32.shl
          local.tee 2
          i32.const 3
          i32.store8
          block  ;; label = @3
            block  ;; label = @4
              block  ;; label = @5
                block  ;; label = @6
                  block  ;; label = @7
                    block  ;; label = @8
                      local.get 0
                      i32.const 1
                      i32.add
                      local.tee 11
                      local.get 0
                      i32.lt_u
                      br_if 0 (;@8;)
                      local.get 11
                      local.get 1
                      i32.gt_u
                      br_if 1 (;@7;)
                      local.get 2
                      i32.const 1
                      i32.add
                      local.get 4
                      local.get 0
                      call $memcpy
                      drop
                      local.get 8
                      i32.const 1
                      i32.add
                      local.tee 0
                      local.get 11
                      i32.lt_u
                      br_if 2 (;@6;)
                      local.get 0
                      local.get 1
                      i32.gt_u
                      br_if 3 (;@5;)
                      local.get 2
                      local.get 11
                      i32.add
                      local.get 5
                      local.get 6
                      call $memcpy
                      drop
                      local.get 10
                      i32.const 1
                      i32.add
                      local.tee 11
                      local.get 0
                      i32.lt_u
                      br_if 4 (;@4;)
                      local.get 11
                      local.get 1
                      i32.gt_u
                      br_if 5 (;@3;)
                      local.get 2
                      local.get 0
                      i32.add
                      local.get 7
                      local.get 9
                      call $memcpy
                      drop
                      local.get 11
                      local.get 1
                      i32.lt_u
                      br_if 7 (;@1;)
                      local.get 11
                      local.get 1
                      call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
                      unreachable
                    end
                    i32.const 1
                    i32.const 0
                    call $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE
                    unreachable
                  end
                  local.get 11
                  local.get 1
                  call $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E
                  unreachable
                end
                local.get 11
                local.get 0
                call $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE
                unreachable
              end
              local.get 0
              local.get 1
              call $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E
              unreachable
            end
            local.get 0
            local.get 11
            call $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE
            unreachable
          end
          local.get 11
          local.get 1
          call $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E
          unreachable
        end
        i32.const 0
        i32.const 0
        call $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E
        unreachable
      end
      local.get 2
      local.get 11
      i32.add
      i32.const 0
      i32.store8
      local.get 3
      i32.const 32
      i32.add
      global.set $__stack_pointer
      local.get 2)
    (func $_ZN4core9panicking18panic_bounds_check17h07f8e486b16e6277E (type 0) (param i32 i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $_ZN4core5slice5index22slice_index_order_fail17hb053ab664d9d870bE (type 0) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core10intrinsics17const_eval_select17hf41eeec4c1f94fc5E
      unreachable)
    (func $_ZN4core5slice5index24slice_end_index_len_fail17h016f455fdd911dd6E (type 0) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core10intrinsics17const_eval_select17h2cb6051202c964daE
      unreachable)
    (func $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE (type 2)
      loop  ;; label = @1
        br 0 (;@1;)
      end)
    (func $_ZN4core10intrinsics17const_eval_select17h2cb6051202c964daE (type 0) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core3ops8function6FnOnce9call_once17hd203256c8930783eE
      unreachable)
    (func $_ZN4core3ops8function6FnOnce9call_once17hd203256c8930783eE (type 0) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core5slice5index27slice_end_index_len_fail_rt17h962df0d32abc7149E
      unreachable)
    (func $_ZN4core5slice5index27slice_end_index_len_fail_rt17h962df0d32abc7149E (type 0) (param i32 i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $_ZN4core10intrinsics17const_eval_select17hf41eeec4c1f94fc5E (type 0) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core3ops8function6FnOnce9call_once17hc0d1c496e6d46c21E
      unreachable)
    (func $_ZN4core3ops8function6FnOnce9call_once17hc0d1c496e6d46c21E (type 0) (param i32 i32)
      local.get 0
      local.get 1
      call $_ZN4core5slice5index25slice_index_order_fail_rt17h4242a308b2a8c792E
      unreachable)
    (func $_ZN4core5slice5index25slice_index_order_fail_rt17h4242a308b2a8c792E (type 0) (param i32 i32)
      call $_ZN4core9panicking9panic_fmt17h9e229748e3ae9f9dE
      unreachable)
    (func $strlen (type 3) (param i32) (result i32)
      local.get 0
      call $_ZN17compiler_builtins3mem6strlen17h43cca548dfe67c86E)
    (func $_ZN17compiler_builtins3mem6memcpy17hb4be5e98a8c97156E (type 1) (param i32 i32 i32) (result i32)
      (local i32 i32 i32 i32 i32 i32 i32 i32)
      block  ;; label = @1
        block  ;; label = @2
          local.get 2
          i32.const 15
          i32.gt_u
          br_if 0 (;@2;)
          local.get 0
          local.set 3
          br 1 (;@1;)
        end
        local.get 0
        i32.const 0
        local.get 0
        i32.sub
        i32.const 3
        i32.and
        local.tee 4
        i32.add
        local.set 5
        block  ;; label = @2
          local.get 4
          i32.eqz
          br_if 0 (;@2;)
          local.get 0
          local.set 3
          local.get 1
          local.set 6
          loop  ;; label = @3
            local.get 3
            local.get 6
            i32.load8_u
            i32.store8
            local.get 6
            i32.const 1
            i32.add
            local.set 6
            local.get 3
            i32.const 1
            i32.add
            local.tee 3
            local.get 5
            i32.lt_u
            br_if 0 (;@3;)
          end
        end
        local.get 5
        local.get 2
        local.get 4
        i32.sub
        local.tee 7
        i32.const -4
        i32.and
        local.tee 8
        i32.add
        local.set 3
        block  ;; label = @2
          block  ;; label = @3
            local.get 1
            local.get 4
            i32.add
            local.tee 9
            i32.const 3
            i32.and
            i32.eqz
            br_if 0 (;@3;)
            local.get 8
            i32.const 1
            i32.lt_s
            br_if 1 (;@2;)
            local.get 9
            i32.const 3
            i32.shl
            local.tee 6
            i32.const 24
            i32.and
            local.set 2
            local.get 9
            i32.const -4
            i32.and
            local.tee 10
            i32.const 4
            i32.add
            local.set 1
            i32.const 0
            local.get 6
            i32.sub
            i32.const 24
            i32.and
            local.set 4
            local.get 10
            i32.load
            local.set 6
            loop  ;; label = @4
              local.get 5
              local.get 6
              local.get 2
              i32.shr_u
              local.get 1
              i32.load
              local.tee 6
              local.get 4
              i32.shl
              i32.or
              i32.store
              local.get 1
              i32.const 4
              i32.add
              local.set 1
              local.get 5
              i32.const 4
              i32.add
              local.tee 5
              local.get 3
              i32.lt_u
              br_if 0 (;@4;)
              br 2 (;@2;)
            end
          end
          local.get 8
          i32.const 1
          i32.lt_s
          br_if 0 (;@2;)
          local.get 9
          local.set 1
          loop  ;; label = @3
            local.get 5
            local.get 1
            i32.load
            i32.store
            local.get 1
            i32.const 4
            i32.add
            local.set 1
            local.get 5
            i32.const 4
            i32.add
            local.tee 5
            local.get 3
            i32.lt_u
            br_if 0 (;@3;)
          end
        end
        local.get 7
        i32.const 3
        i32.and
        local.set 2
        local.get 9
        local.get 8
        i32.add
        local.set 1
      end
      block  ;; label = @1
        local.get 2
        i32.eqz
        br_if 0 (;@1;)
        local.get 3
        local.get 2
        i32.add
        local.set 5
        loop  ;; label = @2
          local.get 3
          local.get 1
          i32.load8_u
          i32.store8
          local.get 1
          i32.const 1
          i32.add
          local.set 1
          local.get 3
          i32.const 1
          i32.add
          local.tee 3
          local.get 5
          i32.lt_u
          br_if 0 (;@2;)
        end
      end
      local.get 0)
    (func $_ZN17compiler_builtins3mem6strlen17h43cca548dfe67c86E (type 3) (param i32) (result i32)
      (local i32 i32 i32)
      block  ;; label = @1
        block  ;; label = @2
          local.get 0
          i32.load8_u
          br_if 0 (;@2;)
          i32.const 0
          local.set 1
          br 1 (;@1;)
        end
        local.get 0
        i32.const 1
        i32.add
        local.set 2
        i32.const 0
        local.set 0
        loop  ;; label = @2
          local.get 2
          local.get 0
          i32.add
          local.set 3
          local.get 0
          i32.const 1
          i32.add
          local.tee 1
          local.set 0
          local.get 3
          i32.load8_u
          br_if 0 (;@2;)
        end
      end
      local.get 1)
    (func $memcpy (type 1) (param i32 i32 i32) (result i32)
      local.get 0
      local.get 1
      local.get 2
      call $_ZN17compiler_builtins3mem6memcpy17hb4be5e98a8c97156E)
    (memory (;0;) 17)
    (global $__stack_pointer (mut i32) (i32.const 1048576))
    (global (;1;) i32 (i32.const 1048838))
    (global (;2;) i32 (i32.const 1048848))
    (export "memory" (memory 0))
    (export "concat3" (func $concat3))
    (export "__data_end" (global 1))
    (export "__heap_base" (global 2))
    (data $.rodata (i32.const 1048576) "???!!!\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\02\03\03\03\03\03\03\03\03\03\03\03\03\03\03\03\03\04\04\04\04\04\00\00\00\00\00\00\00\00\00\00\00"))
  
"#;

static REVERSE_BLOB_SRC: &str = r#"

(module
  (type (;0;) (func (param i32) (result i32)))
  (func $reverse_blob (type 0) (param i32) (result i32)
    (local i32 i32 i32 i32)
    block  ;; label = @1
      local.get 0
      i32.load8_u
      i32.const 4
      i32.ne
      br_if 0 (;@1;)
      local.get 0
      i32.load offset=1 align=1
      local.tee 1
      i32.const 24
      i32.shl
      local.get 1
      i32.const 8
      i32.shl
      i32.const 16711680
      i32.and
      i32.or
      local.get 1
      i32.const 8
      i32.shr_u
      i32.const 65280
      i32.and
      local.get 1
      i32.const 24
      i32.shr_u
      i32.or
      i32.or
      local.tee 2
      i32.const 2
      i32.lt_u
      br_if 0 (;@1;)
      local.get 2
      i32.const 1
      i32.shr_u
      local.set 3
      local.get 0
      i32.const 5
      i32.add
      local.set 1
      local.get 2
      local.get 0
      i32.add
      i32.const 4
      i32.add
      local.set 2
      loop  ;; label = @2
        local.get 1
        i32.load8_u
        local.set 4
        local.get 1
        local.get 2
        i32.load8_u
        i32.store8
        local.get 2
        local.get 4
        i32.store8
        local.get 2
        i32.const -1
        i32.add
        local.set 2
        local.get 1
        i32.const 1
        i32.add
        local.set 1
        local.get 3
        i32.const -1
        i32.add
        local.tee 3
        br_if 0 (;@2;)
      end
    end
    local.get 0)
  (memory (;0;) 16)
  (global $__stack_pointer (mut i32) (i32.const 1048576))
  (global (;1;) i32 (i32.const 1048576))
  (global (;2;) i32 (i32.const 1048576))
  (export "memory" (memory 0))
  (export "reverse_blob" (func $reverse_blob)))
"#;

static GET_NULL_SRC: &str = r#"
(module
  (type (;0;) (func (result i32)))
  (func $get_null (type 0) (result i32)
    (local i32)
    i32.const 1
    memory.grow
    i32.const 16
    i32.shl
    local.tee 0
    i32.const 5
    i32.store8
    local.get 0)
  (memory (;0;) 16)
  (global $__stack_pointer (mut i32) (i32.const 1048576))
  (global (;1;) i32 (i32.const 1048576))
  (global (;2;) i32 (i32.const 1048576))
  (export "memory" (memory 0))
  (export "get_null" (func $get_null)))
"#;

pub fn fib_src() -> String {
    hex::encode(wabt::wat2wasm(FIB_SRC).unwrap())
}

pub fn contains_src() -> String {
    hex::encode(wabt::wat2wasm(CONTAINS_SRC).unwrap())
}

pub fn concat3_src() -> String {
    hex::encode(wabt::wat2wasm(CONCAT3_SRC).unwrap())
}

pub fn reverse_blob_src() -> String {
    hex::encode(wabt::wat2wasm(REVERSE_BLOB_SRC).unwrap())
}

pub fn get_null_src() -> String {
    hex::encode(wabt::wat2wasm(GET_NULL_SRC).unwrap())
}
