from pathlib import Path

from jinja2 import (BaseLoader, Environment, FileSystemLoader,
                    contextfilter, contextfunction)

from schemaql.helpers.fileio import schemaql_path
from schemaql.helpers.logger import logger


INCLUDE_DIR = "include"
TEMPLATE_DIR = "templates"
MACRO_DIR = "macros"

CUSTOM_TESTS_DIR = "tests"
CUSTOM_MACRO_DIR = "macros"

ALL_EXT = "**/*.*"
MACRO_EXT = "**/*.sql"


class PrependingLoader(BaseLoader):
    """Class to automatically inject macro code into templates
    """

    def __init__(self, delegate, prepend_templates):
        self.delegate = delegate
        self.prepend_templates = prepend_templates

    def get_source(self, environment, template):
        """Overrides the base get_source function and
            injects/prepends all macro code
        """
        complete_prepend_source = ""
        if template not in self.prepend_templates:
            for prepend_template in self.prepend_templates:
                prepend_source, _, _ = self.delegate.get_source(environment, prepend_template)
                complete_prepend_source += prepend_source

        main_source, main_filename, main_uptodate = self.delegate.get_source(environment, template)

        def uptodate():
            return main_uptodate()

        complete_source = (complete_prepend_source + main_source)

        return complete_source, main_filename, uptodate

    def list_templates(self):
        return self.delegate.list_templates()


class JinjaConfig(object):

    def __init__(self, template_type, _connector):
        self._template_type = template_type
        self._connector = _connector
        self._environment = self._get_jinja_template_environment()

    @property
    def environment(self):
        return self._environment

    @contextfunction
    def get_context(self, context):
        return context

    @contextfunction
    def log(self, context, msg):
        logger.info(msg)
        return ""

    @contextfunction
    def set_func(self, context, itr):
        return set(itr)

    @contextfilter
    def difference(self, context, first, second):
        second = set(second)
        return [item for item in first if item not in second]

    @contextfunction
    def connector_macro(self, context, macro_name, *args, **kwargs):
        """Redirects a macro call based on the type of connector
            - If there is no matching macor name for the connector,
                redirects to default macro
        """
        original_name = macro_name
        if "." in macro_name:
            package_name, macro_name = macro_name.split(".", 1)
        else:
            package_name = None

        if not package_name:
            package_context = context
        elif package_name in context:
            package_context = context[package_name]
        else:
            logger.error(f"Could not find package {package_name}, called from {original_name}")

        separator = "__"
        default_macro_name = f"default{separator}{macro_name}"
        connector_macro_name = f"{self._connector.connector_type}{separator}{macro_name}"
        if connector_macro_name not in package_context.vars:
            connector_macro_name = default_macro_name

        return package_context.vars[connector_macro_name](*args, **kwargs)

    def _make_template_dir_list(self, path, ext=ALL_EXT):
        return list(set([str(t.parent) for t in path.glob(ext)]))

    def _get_template_paths(self):

        template_path = schemaql_path.joinpath(INCLUDE_DIR, TEMPLATE_DIR)
        custom_test_path = Path(Path(CUSTOM_TESTS_DIR).resolve())

        # We get all macro files we want to prepend to the templates
        macro_path = Path(schemaql_path.joinpath(INCLUDE_DIR, TEMPLATE_DIR, MACRO_DIR))
        custom_macro_path = Path(Path(CUSTOM_MACRO_DIR).resolve())

        # templates can have any extension
        template_dirs = self._make_template_dir_list(custom_test_path, ALL_EXT)
        # but custom macros can only have .sql extensions
        template_dirs += self._make_template_dir_list(custom_macro_path, MACRO_EXT)

        template_dirs += self._make_template_dir_list(template_path, ALL_EXT)

        return template_dirs, macro_path, custom_macro_path

    def _get_preload_macros(self, macro_paths):
        preload_macros = []

        for macro_path in macro_paths:
            for f in macro_path.glob(MACRO_EXT):
                macro_file_path = str(f.name)
                preload_macros.append(macro_file_path)

        return preload_macros

    def _get_jinja_template_environment(self):

        template_dirs, macro_path, custom_macro_path = self._get_template_paths()

        base_loader = FileSystemLoader(template_dirs)

        preload_macros = self._get_preload_macros([macro_path, custom_macro_path])

        loader = PrependingLoader(base_loader, preload_macros)

        env = Environment(loader=loader)

        env.globals["log"] = self.log
        env.globals["connector"] = self._connector
        env.globals["context"] = self.get_context
        env.globals["connector_macro"] = self.connector_macro

        env.globals["api.types.set"] = self.set_func

        env.filters["difference"] = self.difference

        return env
