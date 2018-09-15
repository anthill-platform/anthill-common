
from tornado.options import OptionParser, define_logging_options
import os


class ServiceOptionsParser(OptionParser):
    def parse_env(self):
        for name, value in os.environ.items():
            name = self._normalize_name(name)

            if name not in self._options:
                continue

            self._options[name].parse(value)


options = ServiceOptionsParser()


def define(name, default=None, type=None, help=None, metavar=None,
           multiple=False, group=None, callback=None):
    """Defines an option in the global namespace.

    See `OptionParser.define`.
    """
    return options.define(name, default=default, type=type, help=help,
                          metavar=metavar, multiple=multiple, group=group,
                          callback=callback)


def parse_command_line(args=None, final=True):
    """Parses global options from the command line.

    See `OptionParser.parse_command_line`.
    """
    return options.parse_command_line(args, final=final)


def parse_env():
    """Parses global options from the os.environ.

    """
    return options.parse_env()


def parse_config_file(path, final=True):
    """Parses global options from a config file.

    See `OptionParser.parse_config_file`.
    """
    return options.parse_config_file(path, final=final)


def print_help(file=None):
    """Prints all the command line options to stderr (or another file).

    See `OptionParser.print_help`.
    """
    return options.print_help(file)


define_logging_options(options)
