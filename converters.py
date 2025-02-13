#!/usr/bin/env python3
"""
Module that defines conversion tasks for DataTad using a direct factory function.
"""


from pathlib import Path
import duckdb


# === Input Classes ===


class BaseInputConnection:
    """
    Base class for input classes.
    """

    def __init__(self):
        self.conn = duckdb.connect()

    def import_file(self, path: Path):
        """
        Import a file into the duckdb.
        """
        pass


class CSVInput(BaseInputConnection):
    """
    Class for input classes that read CSV files.
    """

    def __init__(self):
        super().__init__()


class JSONInput(BaseInputConnection):
    """
    Class for input classes that read JSON files.
    """

    def __init__(self):
        super().__init__()
        pass


class ParquetInput(BaseInputConnection):
    """
    Class for input classes that read Parquet files.
    """

    def __init__(self):
        super().__init__()
        pass


class TsvInput(CSVInput):
    """
    Class for input classes that read TSV files.
    """

    def __init__(self):
        super().__init__()
        pass


class TxtInput(CSVInput):
    """
    Class for input classes that read TXT files.
    """

    def __init__(self):
        super().__init__()
        pass


class ExcelInputUntyped(BaseInputConnection):
    """
    Class for input classes that read Excel files.
    """

    def __init__(self):
        super().__init__()
        pass


class ExcelInputTyped(ExcelInputUntyped):
    """
    Base class for input classes that read Excel files.
    """

    def __init__(self):
        super().__init__()
        pass


# === Output Classes ===


class BaseOutputConnection:
    """
    Base class for output classes.
    """

    def __init__(self):
        pass


class ParquetOutput(BaseOutputConnection):
    """
    Class for output classes that write Parquet files.
    """

    def __init__(self):
        super().__init__()
        pass


class JSONOutput(BaseOutputConnection):
    """
    Class for output classes that write JSON files.
    """

    def __init__(self):
        super().__init__()
        pass


class CSVOutput(BaseOutputConnection):
    """
    Class for output classes that write CSV files.
    """

    def __init__(self):
        super().__init__()
        pass


class TsvOutput(CSVOutput):
    """
    Class for output classes that write TSV files.
    """

    def __init__(self):
        super().__init__()
        pass


class TxtOutput(CSVOutput):
    """
    Class for output classes that write TXT files.
    """

    def __init__(self):
        super().__init__()
        pass


class ExcelOutput(BaseOutputConnection):
    """
    Class for output classes that write Excel files.
    """

    def __init__(self):
        super().__init__()
        pass


#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# === Conversion Classes ===


# @dataclass(kw_only=True)
# class Conversion:
#     conn: duckdb.DuckDBPyConnection
#     src: Path
#     dst: Path
#     kwargs: Dict[str, Any] = field(default_factory=dict)

#     def __post_init__(self):
#         self.src = normalize_path(self.src)
#         self.dst = normalize_path(self.dst)

#     def run(self) -> None:
#         raise NotImplementedError("Subclasses must implement run().")


# @dataclass(kw_only=True)
# class StandardConversion(Conversion):
#     read_method: str
#     write_method: str
#     extra_params: Dict[str, Any] = field(default_factory=dict)

#     def run(self) -> None:
#         table = getattr(self.conn, self.read_method)(self.src)
#         getattr(table, self.write_method)(self.dst, **self.extra_params)


# @dataclass(kw_only=True)
# class DelimitedConversion(Conversion):
#     read_method: str
#     default_delimiter: str  # e.g. "," or "\t"
#     prompt_text: str

#     def run(self) -> None:
#         delimiter = self.kwargs.get("delimiter", self.default_delimiter)
#         if delimiter is None:
#             delimiter = get_delimiter(prompt_text=self.prompt_text or "")
#         table = getattr(self.conn, self.read_method)(self.src)
#         table.write_csv(str(self.dst), sep=delimiter)


# @dataclass(kw_only=True)
# class SQLConversion(Conversion):
#     query_template: str

#     def run(self) -> None:
#         query = self.query_template.format(src=self.src, dst=self.dst)
#         self.conn.sql(query)


# @dataclass(kw_only=True)
# class ExcelQueryConversion(Conversion):
#     sheet: Any = None
#     range_: Any = None
#     output_format: str = "csv"  # Options: "csv", "tsv", or "txt"
#     prompt_text: Optional[str] = None

#     def run(self) -> None:
#         if not ExcelUtils.load_extension(self.conn):
#             raise RuntimeError("Failed to load Excel extension.")
#         query = ExcelUtils.build_excel_query(self.src, self.sheet, self.range_)
#         result = self.conn.sql(query)
#         if self.output_format == "tsv":
#             sep = "\t"
#         elif self.output_format == "txt":
#             sep = self.kwargs.get("delimiter") or get_delimiter(
#                 prompt_text=self.prompt_text or ""
#             )
#         else:
#             sep = self.kwargs.get("delimiter", ",")
#         result.write_csv(str(self.dst), sep=sep)


# @dataclass(kw_only=True)
# class ExcelInferredTypeConversion(Conversion):
#     sheet: Any = None
#     range_: Any = None
#     fmt: str = "json"  # Can be "json" or "parquet"

#     def run(self) -> None:
#         if not ExcelUtils.load_extension(self.conn):
#             raise RuntimeError("Failed to load Excel extension.")
#         ExcelUtils.export_with_inferred_types(
#             self.conn,
#             self.src,
#             self.dst,
#             sheet=self.sheet,
#             range_=self.range_,
#             fmt=self.fmt,
#             **self.kwargs,
#         )


# @dataclass(kw_only=True)
# class ExportExcelConversion(Conversion):
#     query_template: str

#     def run(self) -> None:
#         base_query = self.query_template.format(src=self.src)
#         ExcelUtils.export_excel(self.conn, base_query, self.dst)


# # === Registry for Conversions ===

# # The registry maps (input_format, output_format) to a tuple:
# #   (ConversionClass, default_kwargs)
# CONVERSION_REGISTRY: Dict[
#     Tuple[str, str], Tuple[Callable[..., Conversion], Dict[str, Any]]
# ] = {}


# def register_conversion(
#     input_format: str,
#     output_format: str,
#     conversion_class: Callable[..., Conversion],
#     **conversion_kwargs,
# ):
#     """
#     Register a conversion class along with its default keyword arguments.
#     """
#     CONVERSION_REGISTRY[(input_format, output_format)] = (
#         conversion_class,
#         conversion_kwargs,
#     )


# # === Register Conversion Tasks ===

# # CSV Conversions
# register_conversion(
#     "csv",
#     "parquet",
#     StandardConversion,
#     read_method="read_csv",
#     write_method="to_parquet",
# )
# register_conversion(
#     "csv",
#     "json",
#     SQLConversion,
#     query_template="COPY (SELECT * FROM read_csv_auto('{src}')) TO '{dst}' (FORMAT JSON)",
# )
# register_conversion(
#     "csv",
#     "excel",
#     ExportExcelConversion,
#     query_template="SELECT * FROM read_csv_auto('{src}')",
# )
# register_conversion(
#     "csv",
#     "tsv",
#     DelimitedConversion,
#     read_method="read_csv",
#     default_delimiter="\t",
#     prompt_text="",
# )
# register_conversion(
#     "csv",
#     "txt",
#     DelimitedConversion,
#     read_method="read_csv",
#     default_delimiter=",",
#     prompt_text="For TXT export, choose t (tab) or c (comma): ",
# )

# # JSON Conversions
# register_conversion(
#     "json", "csv", StandardConversion, read_method="read_json", write_method="write_csv"
# )
# register_conversion(
#     "json",
#     "parquet",
#     StandardConversion,
#     read_method="read_json",
#     write_method="write_parquet",
# )
# register_conversion(
#     "json",
#     "excel",
#     ExportExcelConversion,
#     query_template="SELECT * FROM read_json('{src}')",
# )
# register_conversion(
#     "json",
#     "tsv",
#     DelimitedConversion,
#     read_method="read_json",
#     default_delimiter="\t",
#     prompt_text="",
# )
# register_conversion(
#     "json",
#     "txt",
#     DelimitedConversion,
#     read_method="read_json",
#     default_delimiter=",",
#     prompt_text="For TXT export, choose t (tab) or c (comma): ",
# )

# # Parquet Conversions
# register_conversion(
#     "parquet",
#     "csv",
#     StandardConversion,
#     read_method="from_parquet",
#     write_method="write_csv",
# )
# register_conversion(
#     "parquet",
#     "json",
#     SQLConversion,
#     query_template="COPY (SELECT * FROM read_parquet('{src}')) TO '{dst}'",
# )
# register_conversion(
#     "parquet",
#     "excel",
#     ExportExcelConversion,
#     query_template="SELECT * FROM read_parquet('{src}')",
# )
# register_conversion(
#     "parquet",
#     "tsv",
#     DelimitedConversion,
#     read_method="from_parquet",
#     default_delimiter="\t",
#     prompt_text="",
# )
# register_conversion(
#     "parquet",
#     "txt",
#     DelimitedConversion,
#     read_method="from_parquet",
#     default_delimiter=",",
#     prompt_text="For TXT export, choose t (tab) or c (comma): ",
# )

# # Excel Conversions
# register_conversion("excel", "csv", ExcelQueryConversion)
# register_conversion("excel", "tsv", ExcelQueryConversion)
# register_conversion("excel", "txt", ExcelQueryConversion)
# register_conversion("excel", "parquet", ExcelInferredTypeConversion, fmt="parquet")
# register_conversion("excel", "json", ExcelInferredTypeConversion, fmt="json")


# # === Factory Function ===


# def get_conversion_instance(
#     conn: duckdb.DuckDBPyConnection,
#     src: Path,
#     dst: Path,
#     input_format: str,
#     output_format: str,
#     **kwargs,
# ) -> Conversion:
#     """
#     Given the connection, source, destination, input format, and output format,
#     look up the registered conversion, merge any default parameters with any
#     additional keyword arguments, and return an instance of the appropriate
#     Conversion subclass.
#     """
#     key = (input_format, output_format)
#     if key not in CONVERSION_REGISTRY:
#         raise ValueError(
#             f"No conversion registered for {input_format} to {output_format}"
#         )

#     conversion_class, default_kwargs = CONVERSION_REGISTRY[key]
#     # Merge default kwargs with user-supplied kwargs (user kwargs take precedence)
#     final_kwargs = {**default_kwargs, **kwargs}
#     return conversion_class(conn=conn, src=src, dst=dst, **final_kwargs)
