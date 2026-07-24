macro_rules! frozen_pymethods {
    (impl $py_type:ty { $($body:tt)* }) => {
        #[pyo3::pymethods]
        impl $py_type {
            $($body)*

        fn __copy__(slf: &pyo3::Bound<'_, Self>) -> pyo3::Py<Self> {
            slf.clone().unbind()
        }

        #[pyo3(signature = (memo))]
        fn __deepcopy__(
            slf: &pyo3::Bound<'_, Self>,
            memo: &pyo3::Bound<'_, pyo3::PyAny>,
        ) -> pyo3::Py<Self> {
            let _ = memo;
            slf.clone().unbind()
        }
        }
    };
}

macro_rules! row_iter_pymethods {
    (
        impl $iter_type:ident {
            source: $source_type:ty $(,)?
        }
    ) => {
        #[pyo3::pymethods]
        impl $iter_type {
            const fn __iter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
                slf
            }

            /// Remaining rows — lets ``list(iter)`` preallocate.
            fn __length_hint__(&self) -> usize {
                self.state
                    .length_hint(<$source_type as crate::py::row::RowContainer>::row_count(
                        &self.source,
                    ))
            }

            /// ``sys.getsizeof`` support: the iterator plus the logical
            /// payload it keeps alive while iterating.
            fn __sizeof__(&self) -> usize {
                crate::HeapSize::total_size(self)
            }

            fn __next__(
                &self,
                py: pyo3::Python<'_>,
            ) -> pyo3::PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
                let len = <$source_type as crate::py::row::RowContainer>::row_count(&self.source);
                self.state
                    .next_row(len)
                    .map(|row| {
                        <$source_type as crate::py::row::RowContainer>::scalar_row(
                            &self.source,
                            py,
                            row,
                        )
                    })
                    .transpose()
            }
        }

        impl crate::HeapSize for $iter_type {
            fn heap_bytes(&self) -> usize {
                self.source.heap_bytes()
            }
        }
    };
}
