from htmlmin.parser import HTMLMinParser

class Minifier:
    def __init__(
        self,
        *,
        remove_comments: bool = ...,
        remove_empty_space: bool = ...,
        remove_all_empty_space: bool = ...,
        reduce_empty_attributes: bool = ...,
        reduce_boolean_attributes: bool = ...,
        remove_optional_attribute_quotes: bool = ...,
        convert_charrefs: bool = ...,
        keep_pre: bool = ...,
        pre_tags: object = ...,
        pre_attr: str = ...,
        cls: type[HTMLMinParser] = ...,
    ) -> None: ...
    def minify(self, *input: str) -> str: ...
