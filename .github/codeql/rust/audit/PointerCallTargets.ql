/**
 * @name Python pointer conversion declarations
 * @kind table
 * @id pkgs/rust/audit-pointer-declarations
 */

import CPythonContracts

from Function target
where
  target.getName().getText() in [
      "as_ptr", "into_ptr", "assume_owned", "assume_owned_or_err", "assume_owned_or_opt",
      "assume_owned_unchecked", "assume_borrowed", "assume_borrowed_or_err",
      "assume_borrowed_or_opt", "assume_borrowed_unchecked"
    ] and
  target.getLocation().getFile().getRelativePath().matches("%pyo3%")
select target, target.getCanonicalPath()
