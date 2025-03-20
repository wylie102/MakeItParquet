#!/usr/bin/env python3
"""
Module that contains dataclasses related to the conversion process in MakeItParquet!
"""

from dataclasses import dataclass
import re
from typing import NamedTuple
import uuid
from pathlib import Path

from .file_information import FileInfo


class _SQLReadStatementMapping(NamedTuple):
    csv: str = "read_csv"
    tsv: str = "read_csv"
    txt: str = "read_csv"
    json: str = "read_json"
    parquet: str = "read_parquet"
    xlsx: str = "read_xlsx"  # TODO: implement the method of writing a small amount to csv first to use to sniff column formats when writing to json or parquet.


class _DefaultArgumentMapping(NamedTuple):
    csv: str = ""
    tsv: str = r", delim = \t"
    txt: str = r", delim = \t"
    json: str = ""
    parquet: str = ""
    xlsx: str = ""


class _ExportArgumentMapping(NamedTuple):
    csv: str = ""
    tsv: str = r"(DELIMETER '\t')"
    txt: str = r"(DELIMETER '\t')"
    json: str = "(FORMAT json)"
    parquet: str = "(FORMAT parquet)"
    xlsx: str = "WITH (FORMAT xlsx)"


class ConversionInputAttributes(NamedTuple):
    input_ext: str
    file_path: Path
    read_function: str
    default_arguments: str
    table_name: str
    import_query: str


@dataclass
class ExportAttributes:
    output_ext: str
    output_directory_path: Path
    prepared_export_statement: str


class ConversionData:
    # Store mapping dataclasses as Class Attributes.
    read_statement_mapping: _SQLReadStatementMapping = _SQLReadStatementMapping()
    default_argument_mapping: _DefaultArgumentMapping = _DefaultArgumentMapping()
    export_argument_mapping: _ExportArgumentMapping = _ExportArgumentMapping()

    @staticmethod
    def generate_prepared_import_statement(input_ext: str) -> str:
        ext_key: str = ConversionData._generate_ext_key(input_ext)
        read_function: str = ConversionData._generate_read_function(ext_key)
        default_arguments: str = ConversionData._generate_default_arguments(ext_key)
        return f"PREPARE import_statement AS CREATE TABLE $table_name AS FROM {
            read_function
        }('$file_path'{default_arguments});"

    @staticmethod
    def _generate_ext_key(ext: str) -> str:
        return re.sub(r"^\.", "", ext)

    @staticmethod
    def _generate_read_function(ext_key: str) -> str:
        read_statement: str = getattr(ConversionData.read_statement_mapping, ext_key)
        return read_statement

    @staticmethod
    def _generate_default_arguments(ext_key: str) -> str:
        arguments: str = getattr(ConversionData.default_argument_mapping, ext_key)
        return arguments

    @staticmethod
    def generate_export_attributes(
        file_info: FileInfo, input_ext: str, output_ext: str
    ) -> ExportAttributes:
        """
        Generates ExportAttributes dataclass.

        Args:
            file_info: FileInfo
            output_ext: str

        Returns:
            ExportAttributes - contains output_ext, output_directory_path, prepared_export_statement
        """

        # get directory path, and export statement.
        input_ext_key: str = ConversionData._generate_ext_key(input_ext)
        output_ext_key: str = ConversionData._generate_ext_key(output_ext)
        output_directory_path: Path = ConversionData._get_parent_directory_path(
            file_info, input_ext_key, output_ext_key
        )
        prepared_export_statement: str = (
            ConversionData._generate_prepared_export_statement(output_ext_key)
        )

        # assign output_ext, output_directory_path, and prepared_export_statement to dataclass.
        export_attributes: ExportAttributes = ExportAttributes(
            output_ext, output_directory_path, prepared_export_statement
        )

        # return dataclass.
        return export_attributes

    @staticmethod
    def _get_parent_directory_path(
        file_info: FileInfo, input_ext_key: str, output_ext_key: str
    ) -> Path:
        if file_info.file_or_directory == "file":
            new_directory_path: Path = file_info.file_path.parent
        else:
            new_directory_path = ConversionData.generate_output_path(
                input_ext_key, output_ext_key, file_info.file_path
            )
        return new_directory_path

    @staticmethod
    def _generate_prepared_export_statement(ext_key: str) -> str:
        export_arguments: str = ConversionData._generate_export_arguments(ext_key)
        return f"PREPARE export_statement COPY $table_name TO '$file_path' {export_arguments};"

    @staticmethod
    def _generate_export_arguments(ext_key: str) -> str:
        arguments: str = getattr(ConversionData.default_argument_mapping, ext_key)
        return arguments

    @staticmethod
    def replacer(alias: str, match: re.Match[str]) -> str:
        """
        Adjust the replacement string (`alias`) to match the case of the original match.

        :param alias: The correct-case replacement string.
        :param match: The match object containing the original matched substring.
        :return: The alias string adjusted to match the case of the original.
        """
        orig: str = match.group()
        if orig.isupper():
            return alias.upper()
        elif orig.islower():
            return alias.lower()
        elif orig[0].isupper() and orig[1:].islower():
            return alias.capitalize()
        else:
            return alias

    @staticmethod
    def replace_alias_in_string(target_string: str, alias: str) -> str:
        """
        Replace occurrences of target_string in a case-insensitive manner with alias,
        preserving the original casing format.

        :param target_string: The string in which to search for the input format.
        :param alias: The replacement string (output format) with the correct case.
        :return: The modified string with replacements made.
        """
        pattern = re.compile(re.escape(target_string), re.IGNORECASE)
        result, count = pattern.subn(
            lambda match: ConversionData.replacer(alias, match), target_string
        )
        return result if count > 0 else target_string

    @staticmethod
    def generate_output_path(input_key: str, output_key: str, input_path: Path) -> Path:
        """
        Generate a new Path by replacing or appending the file format in the folder (or file) name.

        The function does the following:
        - Checks if the folder (or file) name contains the input format (input_key) in any case.
        - If it does, replaces it with the output format (output_key) while preserving the original case.
        - If not, appends an underscore and the output format to the original name.
        - Returns a new Path with the updated name in the same directory.

        :param input_key: The input file format to look for (e.g., "csv").
        :param output_key: The desired output file format (e.g., "parquet").
        :param input_path: The Path object for the original folder (or file).
        :return: A new Path with the modified name.
        """
        original_name = input_path.name  # Preserve the original case
        # Use lower-case for checking presence of the input format
        if input_key and input_key.lower() in original_name.lower():
            new_name = ConversionData.replace_alias_in_string(original_name, output_key)
        else:
            new_name = f"{original_name}_{output_key}"
        # Return a new Path using the same parent directory as the original
        return input_path.with_name(new_name)

    def __init__(self, input_ext: str, file_path: Path) -> None:
        """Initializes a ConversionData instance."""

        # Normalize the extension: remove a leading dot.
        ext_key: str = ConversionData._generate_ext_key(input_ext)
        read_function: str = ConversionData._generate_read_function(ext_key)
        default_arguments: str = ConversionData._generate_default_arguments(ext_key)
        table_name = self._create_unique_table_name(file_path)
        import_query = self.generate_import_query(table_name, file_path)

        self.import_attributes: ConversionInputAttributes = ConversionInputAttributes(
            input_ext=input_ext,
            file_path=file_path,
            read_function=read_function,
            default_arguments=default_arguments,
            table_name=table_name,
            import_query=import_query,
        )
        self.output_path: Path
        self.export_query: str

    def _create_unique_table_name(self, file_path: Path) -> str:
        name: str = file_path.name
        return f"{name}_{uuid.uuid4().hex[:8]}"

    def generate_import_query(self, table_name: str, file_path: Path) -> str:
        return f"EXECUTE import_statement(table_name := {table_name}, file_path := {file_path});"

    def generate_export_query(self, export_attributes: ExportAttributes) -> str:
        table_name = self.import_attributes.table_name
        # output_path constituents
        directory_path = export_attributes.output_directory_path
        file_name = self.import_attributes.file_path.stem
        output_ext = export_attributes.output_ext
        # concatenate output path
        self.output_path = directory_path / file_name / output_ext
        # construct query
        self.export_query = f"EXECUTE export_statement(table_name := {table_name}, output_path := {self.output_path} "
        return self.export_query
