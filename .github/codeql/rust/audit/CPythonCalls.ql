/**
 * @name Resolved CPython calls and reference contracts
 * @description Inventory called C APIs and the reference-return contracts known to this pack.
 * @kind table
 * @id pkgs/rust/audit-cpython-contracts
 */

import CPythonContracts

from Call call, CPythonFunction target, string contract
where
  target = resolvedFunction(call) and
  (
    target instanceof NewReferenceFunction and contract = "new reference"
    or
    target instanceof BorrowedReferenceFunction and contract = "borrowed reference"
    or
    not target instanceof NewReferenceFunction and
    not target instanceof BorrowedReferenceFunction and
    contract = "other or unmodeled return"
  )
select call, target.getApiName(), target.getCanonicalPath(), contract
