"""custom exceptions"""


class BasePolygonException(Exception):
    """
    base exception class
    """


class WrongFileFormatException(BasePolygonException):
    """
    exception that can be raised when the format type
    given as parameter is not supported.
    """

    def __init__(self, file_format):
        self.file_format = file_format

    def __repr__(self):
        return f"file format {self.file_format} not supported"


class WrongMetaFileException(BasePolygonException):
    """
    exception that can be raised when the meta file format is not correct.
    """


class IncorrectColumns(Exception):
    """
    exception that can be raised if the target schema can't be applied to the dataframe being transformed
    """
