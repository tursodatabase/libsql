#ifndef OBJIFACE_H
#define OBJIFACE_H

/* This header defines macros to aid declaration of object interfaces
 * used for shell extension, and to support convenient implementation
 * of those interfaces.[a] These macros are suitable for either C or
 * C++ implementations. For C implementations, an extra struct is
 * defined, whose typename is <InterfaceName>_Vtable, which will need
 * to be instantiated and populated with function pointers having the
 * same order and signatures as those declared for the interface. For
 * C++ implementations, a purely abstract base class (with all public
 * methods) is declared from which a concrete class will need to be
 * derived and instantiated. (No *_Vtable is necessary for C++.)
 *
 * The macros defined for external use are:
 *   (for C or C++ implementations)
 *     INTERFACE_BEGIN( InterfaceName )
 *     PURE_VMETHOD( returnType, methodName, InterfaceName, argCount, args )
 *     INTERFACE_END( InterfaceName )
 *     IMPLEMENTING( returnType, methodName, ClassName, argCount, args ) [b]
 *   (for C implementations only)
 *     VTABLE_NAME( ClassName ) [c]
 *   (for C++ implementations only [d])
 *     CONCRETE_BEGIN( InterfaceName, DerivedName )
 *     CONCRETE_METHOD( returnType, methodName, ClassName, argCount, args )
 *     CONCRETE_END( DerivedName )
 * Notes on these macros:
 *   1. These macros should be used in the order shown. Many should be
 *      terminated with either ';' or a curly-braced construct (which
 *      helps auto-indentation tools to operate sanely.)
 *   2. The "args" parameter is a parenthesized list of the additional
 *     arguments, those beyond an explicit "InterfaceName *pThis" for C
 *     or the implicit "this" for C++.
 *   3. The argCount parameter must number the additional arguments.
 *   4. A leading method, named "destruct" without additional arguments
 *     and returning void, is declared for all interfaces. This is not
 *     the C++ destructor. (It might delegate to a destructor.)
 *   [a. The convenience is that the signatures from the interface may
 *     be reused for method implementations with a copy and paste. ]
 *   [b. This macro may be useful for function/method definitions which
 *     implement methods in an INTERFACE_{BEGIN,...,END} sequence. ]
 *   [c. This macro is useful for populating a C dispatch table whose
 *    layout is declared in the INTERFACE_{BEGIN,...,END} sequence. ]
 *   [d. These macros are useful for declaring instantiatable classes
 *    derived from an abstract base class via INTERFACE_{BEGIN,END}. ]
 */

#ifdef __cplusplus

# define VMETHOD_BEGIN(rType, mName) virtual rType mName(
# define PURE_VMETHOD_END )=0
#define ARG_FIRST_0(t)
#define ARG_FIRST_1(t)
#else
# define VMETHOD_BEGIN(rType, mName) rType (*mName)(
# define PURE_VMETHOD_END )
#define ARG_FIRST_0(t) t *pThis
#define ARG_FIRST_1(t) t *pThis,
#endif
#define ARG_FIRST_2 ARG_FIRST_1
#define ARG_FIRST_3 ARG_FIRST_1
#define ARG_FIRST_4 ARG_FIRST_1
#define ARG_FIRST_5 ARG_FIRST_1
#define ARGS_EXPAND(na) ARGS_EXPAND_ ## na
#define ARGS_EXPAND_0()
#define ARGS_EXPAND_1(a1) a1
#define ARGS_EXPAND_2(a1,a2) a1,a2
#define ARGS_EXPAND_3(a1,a2,a3) a1,a2,a3
#define ARGS_EXPAND_4(a1,a2,a3,a4) a1,a2,a3,a4
#define ARGS_EXPAND_5(a1,a2,a3,a4,a5) a1,a2,a3,a4,a5

#define PURE_VMETHOD(rt, mn, ot, na, args) VMETHOD_BEGIN(rt, mn) \
 ARG_FIRST_ ## na(ot) ARGS_EXPAND(na)args PURE_VMETHOD_END
#define CONCRETE_METHOD(rt, mn, ot, na, args) rt mn( \
 ARG_FIRST_ ## na(ot) ARGS_EXPAND(na)args )

#ifdef __cplusplus
# define INTERFACE_BEGIN(iname) struct iname { \
    PURE_VMETHOD(void, destruct, iname, 0, ())
# define INTERFACE_END(iname) }
# define CONCRETE_BEGIN(iname, derived) class derived : public iname { \
    CONCRETE_METHOD(void, destruct, derived, 0, ())
# define CONCRETE_END(derived) }
# define IMPLEMENTING(rt, mn, derived, na, args) rt derived::mn(  \
 ARG_FIRST_ ## na(derived) ARGS_EXPAND(na)args )
#else
# define VTABLE_NAME(name) name ## _Vtable
# define INTERFACE_BEGIN(iname) typedef struct iname { \
    struct VTABLE_NAME(iname) * pMethods;              \
  } iname; typedef struct VTABLE_NAME(iname) {         \
  PURE_VMETHOD(void, destruct, iname, 0, ())
# define INTERFACE_END(iname) } VTABLE_NAME(iname)
# define DECORATE_METHOD(ot, mn)  ot ## _ ## mn
# define IMPLEMENTING(rt, mn, ot, na, args) rt DECORATE_METHOD(ot, mn)(  \
 ARG_FIRST_ ## na(ot) ARGS_EXPAND(na)args )
#endif

#endif /* !defined(OBJIFACE_H) */
