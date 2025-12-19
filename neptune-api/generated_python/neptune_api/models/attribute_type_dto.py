from enum import Enum


class AttributeTypeDTO(str, Enum):
    BOOL = "bool"
    COMPLEX = "complex"
    DATETIME = "datetime"
    EXPERIMENTSTATE = "experimentState"
    FILEREF = "fileRef"
    FILEREFSERIES = "fileRefSeries"
    FLOAT = "float"
    FLOATSERIES = "floatSeries"
    GITREF = "gitRef"
    HISTOGRAMSERIES = "histogramSeries"
    INT = "int"
    STRING = "string"
    STRINGSERIES = "stringSeries"
    STRINGSET = "stringSet"

    def __str__(self) -> str:
        return str(self.value)
