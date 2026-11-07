/**
 * @name Consumed Python writer used again
 * @description Finishing or discarding a Python writer consumes its handle,
 *              including when finishing returns an error.
 * @kind problem
 * @problem.severity error
 * @precision high
 * @id pkgs/rust/consumed-python-writer
 * @tags correctness
 *       external/cwe/cwe-416
 */

import CPythonContracts
import codeql.rust.controlflow.BasicBlocks
import codeql.rust.dataflow.Ssa
import codeql.rust.internal.typeinference.TypeInference as TypeInference
import codeql.rust.internal.typeinference.Type as InferredTypes
import codeql.rust.internal.typeinference.TypeMention as TypeMentions

/**
 * Recover a field declaration through `ManuallyDrop` without guessing a getter
 * from its spelling. Generic dereference inference can lose this projection.
 */
private StructField suppressedField(FieldExpr access) {
  exists(Struct owner |
    owner =
      TypeInference::inferType(manuallyDroppedValue(access.getContainer()))
          .(InferredTypes::DataType)
          .getTypeItem() and
    result = owner.getFieldList().(StructFieldList).getAField() and
    result.getName().getText() = access.getIdentifier().getText()
  )
}

/** A successful Result binding extracts the field's declared `NonNull` handle. */
private predicate suppressedNonNullBinding(Ssa::Definition binding) {
  exists(
    MatchExpr match, MatchArm arm, TupleStructPat pattern, Call read, FieldExpr field,
    TypeMentions::TypeMention mention, InferredTypes::DataType resultType,
    InferredTypes::DataType handleType, Variant success
  |
    arm = match.getMatchArmList().getAnArm() and
    pattern = arm.getPat() and
    binding.(Ssa::WriteDefinition).getWriteAccess() = pattern.getField(0).(IdentPat).getName() and
    success = PathResolution::resolvePath(pattern.getPath()) and
    success.getEnum().getCanonicalPath() = "core::result::Result" and
    success.getName().getText() = "Ok" and
    localPointerFlow(read, match.getScrutinee()) and
    resolvedFunction(read).getCanonicalPath() = "core::ptr::read" and
    field = read.getSyntacticPositionalArgument(0).(RefExpr).getExpr() and
    mention = suppressedField(field).getTypeRepr() and
    resultType = mention.getType() and
    resultType.getTypeItem().getCanonicalPath() = "core::result::Result" and
    handleType =
      mention
          .getTypeAt(TypeInference::TypePath::singleton(resultType.getPositionalTypeParameter(0))) and
    handleType.getTypeItem().getCanonicalPath() = "core::ptr::non_null::NonNull"
  )
}

/** A native pointer getter, including a field handle whose declaration was recovered above. */
private predicate nonNullPointerRead(MethodCall pointer, Ssa::Definition owner) {
  owner.getARead() = pointer.getReceiver() and
  (
    resolvedFunction(pointer).getCanonicalPath() = "<core::ptr::non_null::NonNull>::as_ptr"
    or
    not exists(resolvedFunction(pointer)) and
    suppressedNonNullBinding(owner) and
    pointer.(MethodCallExpr).getIdentifier().getText() = "as_ptr"
  )
}

/** The reviewed writer APIs take their live handle as C argument zero. */
private predicate writerOperation(Call call, string family, string operation) {
  exists(CPythonFunction function |
    function = resolvedFunction(call) and
    family = ["PyBytesWriter", "PyUnicodeWriter", "PyLongWriter"] and
    operation =
      [
        "Finish", "Discard", "GetData", "GetSize", "Resize", "WriteBytes", "WriteUTF8",
        "WriteASCII", "WriteChar", "WriteStr"
      ] and
    function.getApiName() = family + "_" + operation
  )
}

/** This call is executed before the later use on every path reaching that use. */
private predicate executedBefore(Call first, Call last) {
  exists(BasicBlock firstBlock, BasicBlock lastBlock, int i, int j |
    firstBlock.getNode(i).getAstNode() = first and
    lastBlock.getNode(j).getAstNode() = last and
    (
      firstBlock = lastBlock and i < j
      or
      firstBlock.strictlyDominates(lastBlock)
    )
  )
}

/** A shared local definition retains handle identity across its separate reads. */
private predicate sameHandle(Expr first, Expr last) {
  exists(DataFlow::Node origin, DataFlow::ExprNode firstNode, DataFlow::ExprNode lastNode |
    firstNode.asExpr() = first and
    lastNode.asExpr() = last and
    (
      origin instanceof DataFlow::ParameterNode
      or
      exists(Call create, CPythonFunction function |
        origin.asExpr() = create and
        function = resolvedFunction(create) and
        function.getApiName() =
          ["PyBytesWriter_Create", "PyUnicodeWriter_Create", "PyLongWriter_Create"]
      )
    ) and
    DataFlow::localFlow(origin, firstNode) and
    DataFlow::localFlow(origin, lastNode)
  )
  or
  exists(MethodCall firstPointer, MethodCall lastPointer, Ssa::Definition owner |
    firstPointer = first and
    lastPointer = last and
    nonNullPointerRead(firstPointer, owner) and
    nonNullPointerRead(lastPointer, owner)
  )
}

from Call consume, Call use, string family, string operation
where
  writerOperation(consume, family, ["Finish", "Discard"]) and
  writerOperation(use, family, operation) and
  sameHandle(consume.getSyntacticPositionalArgument(0), use.getSyntacticPositionalArgument(0)) and
  executedBefore(consume, use)
select use,
  "This writer operation uses the handle already consumed by $@; an error from Finish does not retain ownership.",
  consume, "this consuming operation"
