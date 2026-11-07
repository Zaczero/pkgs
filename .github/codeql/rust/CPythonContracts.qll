/**
 * Reviewed CPython reference and result contracts, resolved through PyO3 declarations.
 *
 * Reference ownership describes one release obligation, not object identity.
 * Compatibility implementations retain their public C API's contract. Constructor
 * resolution also uses lexical declarations because generic call inference can be
 * incomplete at associated-function calls.
 */

import rust
import codeql.rust.internal.PathResolution as PathResolution
import codeql.rust.dataflow.DataFlow
import codeql.rust.internal.typeinference.TypeInference as TypeInference
import codeql.rust.internal.typeinference.Type as InferredTypes

/** The declaration called, including associated functions and imported aliases. */
Function resolvedFunction(Call call) {
  result = call.getStaticTarget()
  or
  result = PathResolution::resolvePath(call.(CallExpr).getFunction().(PathExpr).getPath())
}

/** A C API declaration or compatibility implementation in the PyO3 FFI crate. */
class CPythonFunction extends Function {
  CPythonFunction() { this.getCanonicalPath().matches("pyo3_ffi::%") }

  /** The public C API identifier, independent of its compatibility module. */
  string getApiName() { result = this.getName().getText() }
}

/**
 * A successful non-null return transfers one strong Python reference.
 * These facts do not describe out-parameters or allocation-domain handles.
 */
class NewReferenceFunction extends CPythonFunction {
  NewReferenceFunction() {
    this.getApiName() =
      [
        "Py_NewRef", "Py_XNewRef", "PyBool_FromLong", "PyByteArray_FromObject",
        "PyByteArray_FromStringAndSize", "PyBytes_FromObject", "PyBytes_FromString",
        "PyBytes_FromStringAndSize", "PyBytesWriter_Finish", "PyUnicodeWriter_Finish",
        "PyLongWriter_Finish", "PyCapsule_New", "PyComplex_FromDoubles", "PyDict_New",
        "PyDict_Copy", "PyDict_Items", "PyDict_Keys", "PyDict_Values", "PyErr_GetRaisedException",
        "PyFloat_FromDouble", "PyFrozenSet_New", "PyDictProxy_New", "PyMemoryView_FromObject",
        "PyOS_FSPath", "PyMapping_Keys", "PyMapping_Values", "PyMapping_Items",
        "PyImport_AddModuleRef", "PyImport_Import", "PyImport_ImportModule", "PyList_New",
        "PyList_AsTuple", "PyList_GetItemRef", "PyList_GetSlice", "PyLong_FromInt32",
        "PyLong_FromLong", "PyLong_FromLongLong", "PyLong_FromSsize_t", "PyLong_FromUInt32",
        "PyLong_FromUnsignedLong", "PyLong_FromUnsignedLongLong", "PyLong_FromSize_t",
        "PyModule_New", "PyModule_NewObject", "PyNumber_Index", "PyNumber_Long",
        "PyNumber_Absolute", "PyNumber_Divmod", "PyNumber_Add", "PyNumber_Subtract",
        "PyNumber_Multiply", "PyNumber_MatrixMultiply", "PyNumber_TrueDivide",
        "PyNumber_FloorDivide", "PyNumber_Remainder", "PyNumber_And", "PyNumber_Xor",
        "PyNumber_Invert", "PyNumber_Lshift", "PyNumber_Negative", "PyNumber_Or",
        "PyNumber_Positive", "PyNumber_Power", "PyNumber_Rshift", "PyObject_Call",
        "PyObject_CallNoArgs", "PyObject_CallOneArg", "PyObject_CallMethodNoArgs",
        "PyObject_CallMethodOneArg", "PyObject_GetAttr", "PyObject_GetAttrString",
        "PyObject_GetItem", "PyObject_GetIter", "PyObject_Dir", "PyObject_Repr",
        "PyObject_RichCompare", "PyObject_Str", "PyObject_Vectorcall", "PyObject_VectorcallDict",
        "PyObject_VectorcallMethod", "PySequence_GetItem", "PySequence_GetSlice", "PySequence_List",
        "PySequence_Tuple", "PySequence_Concat", "PySequence_Repeat", "PySequence_InPlaceConcat",
        "PySequence_InPlaceRepeat", "PySet_New", "PySet_Pop", "PySlice_New", "PyTuple_FromArray",
        "PyTuple_GetSlice", "PyTuple_New", "PyUnicode_AsEncodedString", "PyUnicode_AsUTF8String",
        "PyUnicode_DecodeFSDefaultAndSize", "PyUnicode_EncodeFSDefault",
        "PyUnicode_FromEncodedObject", "PyUnicode_FromKindAndData", "PyUnicode_FromOrdinal",
        "PyUnicode_FromString", "PyUnicode_FromStringAndSize", "PyUnicode_FromWideChar",
        "PyUnicode_InternFromString", "PyWeakref_NewProxy", "PyWeakref_NewRef"
      ]
  }
}

/** A returned Python object is borrowed; adopting it needs a distinct new reference. */
class BorrowedReferenceFunction extends CPythonFunction {
  BorrowedReferenceFunction() {
    this.getApiName() =
      [
        "PyDict_GetItem", "PyDict_GetItemWithError", "PyDict_GetItemString", "PyDict_SetDefault",
        "PyErr_Occurred", "PyEval_GetBuiltins", "PyImport_AddModule", "PyList_GetItem",
        "PyList_GET_ITEM", "PyModule_GetDict", "PyTuple_GetItem", "PyTuple_GET_ITEM", "Py_None",
        "Py_True", "Py_False", "Py_Ellipsis", "Py_NotImplemented"
      ]
  }
}

/** The value whose destructor is suppressed by this `ManuallyDrop` wrapper. */
Expr manuallyDroppedValue(Expr wrapper) {
  exists(Call suppressDrop |
    resolvedFunction(suppressDrop).getCanonicalPath() =
      "<core::mem::manually_drop::ManuallyDrop>::new" and
    localPointerFlow(suppressDrop, wrapper) and
    result = suppressDrop.getSyntacticPositionalArgument(0)
  )
}

/** Suppressing an owning value's destructor leaves its reference available for transfer. */
private predicate manuallyTransferredOwner(MethodCall pointer) {
  TypeInference::inferType(manuallyDroppedValue(pointer.getReceiver()))
      .(InferredTypes::DataType)
      .getTypeItem()
      .getCanonicalPath() =
    ["pyo3::instance::Bound", "pyo3::instance::Py", "pyo3::pycell::PyRef", "pyo3::pycell::PyRefMut"]
}

/** A raw Python pointer lent by a retained PyO3 owner; no reference is transferred. */
class BorrowedPointerSource extends Call {
  BorrowedPointerSource() {
    resolvedFunction(this) instanceof BorrowedReferenceFunction
    or
    not manuallyTransferredOwner(this.(MethodCall)) and
    resolvedFunction(this).getCanonicalPath() =
      [
        "<pyo3::instance::BoundObject>::as_ptr",
        "<pyo3::instance::Bound>::as_ptr",
        "<pyo3::instance::Bound as pyo3::instance::BoundObject>::as_ptr",
        "<pyo3::instance::Borrowed>::as_ptr",
        "<pyo3::instance::Borrowed as pyo3::instance::BoundObject>::as_ptr",
        "<pyo3::instance::Py>::as_ptr",
        "<pyo3::pycell::PyRef>::as_ptr",
        "<pyo3::pycell::PyRefMut>::as_ptr"
      ]
  }
}

/** An API whose negative error return must not be interpreted as Boolean true. */
class BooleanOrErrorFunction extends CPythonFunction {
  BooleanOrErrorFunction() {
    this.getApiName() =
      [
        "PyDict_Contains", "PyDict_ContainsString", "PyObject_HasAttrWithError",
        "PyObject_HasAttrStringWithError", "PyObject_IsInstance", "PyObject_IsSubclass",
        "PyObject_IsTrue", "PyObject_RichCompareBool", "PySequence_Contains", "PySet_Contains",
        "PySet_Discard"
      ]
  }
}

/** These successful object-producing APIs report failure as NULL plus an exception. */
class ErrorOnNullFunction extends NewReferenceFunction {
  ErrorOnNullFunction() {
    this.getApiName() =
      [
        "PyByteArray_FromObject", "PyByteArray_FromStringAndSize", "PyBytes_FromObject",
        "PyBytes_FromString", "PyBytes_FromStringAndSize", "PyCapsule_New", "PyComplex_FromDoubles",
        "PyBytesWriter_Finish", "PyUnicodeWriter_Finish", "PyLongWriter_Finish", "PyDict_New",
        "PyDict_Copy", "PyDict_Items", "PyDict_Keys", "PyDict_Values", "PyFloat_FromDouble",
        "PyImport_Import", "PyImport_ImportModule", "PyList_New", "PyList_AsTuple",
        "PyList_GetItemRef", "PyList_GetSlice", "PyNumber_Index", "PyNumber_Long", "PyObject_Call",
        "PyObject_CallNoArgs", "PyObject_CallOneArg", "PyObject_GetAttr", "PyObject_GetAttrString",
        "PyObject_GetItem", "PyObject_GetIter", "PyObject_Repr", "PyObject_RichCompare",
        "PyObject_Str", "PyObject_Vectorcall", "PyObject_VectorcallDict",
        "PyObject_VectorcallMethod", "PySequence_GetItem", "PySequence_GetSlice", "PySequence_List",
        "PySequence_Tuple", "PySet_New", "PySet_Pop", "PySlice_New", "PyTuple_FromArray",
        "PyTuple_GetSlice", "PyTuple_New", "PyUnicode_FromString", "PyUnicode_FromStringAndSize"
      ]
  }
}

/**
 * An API consumes an owned argument, with failure behavior recorded separately.
 */
private predicate stolenArgument(CPythonFunction function, int argument) {
  function.getApiName() =
    [
      "PyList_SetItem", "PyList_SET_ITEM", "PyTuple_SetItem", "PyTuple_SET_ITEM", "PyModule_Add",
      "PyModule_AddObject"
    ] and
  argument = 2
  or
  function.getApiName() = ["PyException_SetCause", "PyException_SetContext"] and argument = 1
  or
  function.getApiName() = "PyErr_SetRaisedException" and argument = 0
}

class StealingFunction extends CPythonFunction {
  StealingFunction() { stolenArgument(this, _) }

  /** Zero-based C argument carrying the consumed reference. */
  int getStolenArgument() { stolenArgument(this, result) }

  /** Legacy module insertion retains the caller's reference when insertion fails. */
  predicate consumesOnFailure() { this.getApiName() != "PyModule_AddObject" }
}

private predicate ffiPointerExtension(Call call) {
  resolvedFunction(call).getCanonicalPath().matches("<% as pyo3::ffi_ptr_ext::FfiPtrExt>::%")
  or
  resolvedFunction(call).getCanonicalPath().matches("<pyo3::ffi_ptr_ext::FfiPtrExt>::%")
}

/** A resolved PyO3 interpretation of a raw Python object's reference ownership. */
class PointerConversion extends Call {
  PointerConversion() {
    resolvedFunction(this).getCanonicalPath() =
      [
        "<pyo3::instance::Bound>::from_owned_ptr",
        "<pyo3::instance::Bound>::from_owned_ptr_or_err",
        "<pyo3::instance::Bound>::from_owned_ptr_or_opt",
        "<pyo3::instance::Bound>::from_owned_ptr_unchecked",
        "<pyo3::instance::Bound>::from_borrowed_ptr",
        "<pyo3::instance::Bound>::from_borrowed_ptr_or_err",
        "<pyo3::instance::Bound>::from_borrowed_ptr_or_opt",
        "<pyo3::instance::Borrowed>::from_ptr",
        "<pyo3::instance::Borrowed>::from_ptr_or_err",
        "<pyo3::instance::Borrowed>::from_ptr_or_opt"
      ]
    or
    ffiPointerExtension(this) and
    resolvedFunction(this).getName().getText() =
      [
        "assume_owned", "assume_owned_or_err", "assume_owned_or_opt", "assume_owned_unchecked",
        "assume_borrowed", "assume_borrowed_or_err", "assume_borrowed_or_opt",
        "assume_borrowed_unchecked"
      ]
  }

  /** The Python object pointer whose ownership is being interpreted. */
  Expr getPointer() {
    if ffiPointerExtension(this)
    then result = this.(MethodCall).getReceiver()
    else result = this.getSyntacticPositionalArgument(1)
  }

  /** This constructor consumes an existing strong reference. */
  predicate consumesReference() {
    resolvedFunction(this).getName().getText() =
      [
        "from_owned_ptr", "from_owned_ptr_or_err", "from_owned_ptr_or_opt",
        "from_owned_ptr_unchecked", "assume_owned", "assume_owned_or_err", "assume_owned_or_opt",
        "assume_owned_unchecked"
      ]
  }

  /** The conversion retains a borrowed view or acquires a separate reference. */
  predicate leavesReferenceUnconsumed() { not this.consumesReference() }
}

/**
 * Value-preserving pointer flow across helpers. Owning constructors terminate
 * the original raw obligation: later pointers borrowed from the managed owner
 * do not still count as unadopted factory results.
 */
module NewReferenceFlowConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) {
    resolvedFunction(source.asExpr().(Call)) instanceof NewReferenceFunction
  }

  predicate isSink(DataFlow::Node sink) {
    exists(PointerConversion constructor |
      constructor.leavesReferenceUnconsumed() and sink.asExpr() = constructor.getPointer()
    )
    or
    exists(Call release |
      releasesReference(release) and sink.asExpr() = release.getSyntacticPositionalArgument(0)
    )
  }

  predicate isBarrierIn(DataFlow::Node node) {
    exists(PointerConversion constructor |
      constructor.consumesReference() and node.asExpr() = constructor.getPointer()
    )
  }
}

module NewReferenceFlow = DataFlow::Global<NewReferenceFlowConfig>;

/** A release that can account for a new reference's independent obligation. */
predicate releasesReference(Call call) {
  resolvedFunction(call).(CPythonFunction).getApiName() =
    ["Py_DECREF", "Py_XDECREF", "Py_DecRef", "Py_XDecRef"]
}

/**
 * Exact local pointer flow, including locals but not integer/address taint.
 * Local flow preserves reference identity without merging unrelated call contexts.
 */
predicate localPointerFlow(Expr source, Expr sink) {
  exists(DataFlow::ExprNode sourceNode, DataFlow::ExprNode sinkNode |
    sourceNode.asExpr() = source and
    sinkNode.asExpr() = sink and
    DataFlow::localFlow(sourceNode, sinkNode)
  )
}
