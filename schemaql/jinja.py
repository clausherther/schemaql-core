from pathlib import Path

from jinja2 import (BaseLoader, Environment, FileSystemLoader, Template,
                    contextfilter, contextfunction)

from schemaql.helpers.fileio import schemaql_path
from schemaql.helpers.logger import logger


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
        uptodate = lambda: main_uptodate()
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
        
    def _get_jinja_template_environment(self):

        template_path = schemaql_path.joinpath("templates")
        custom_test_path = Path(Path("tests").resolve())

        # We get all macro files we want to prepend to the templates
        macro_path = Path(schemaql_path.joinpath("templates", "macros"))
        custom_macro_path = Path(Path("macros").resolve())

        # templates can have any extension
        template_dirs = list(set([str(t.parent) for t in template_path.glob("**/*.*")]))
        template_dirs += list(set([str(t.parent) for t in custom_test_path.glob("**/*.sql")]))

        template_dirs += list(set([str(t.parent) for t in custom_macro_path.glob("**/*.sql")]))
        
        base_loader = FileSystemLoader(template_dirs)
        
        preload_macros = []
        # you can only write macros in files using a .sql extensions
        for f in macro_path.glob("**/*.sql"):
            macro_file_path = str(f.name)
            preload_macros.append(macro_file_path)

        for f in custom_macro_path.glob("**/*.sql"):
            macro_file_path = str(f.name)
            preload_macros.append(macro_file_path)

        loader = PrependingLoader(base_loader, preload_macros)

        env = Environment(loader=loader)
        
        env.filters["difference"] = self.difference
        env.globals["log"] = self.log
        env.globals["connector"] = self._connector 
        env.globals["connector_macro"] = self.connector_macro

        return env

    @contextfunction
    def log(self, context, msg):
        logger.info(msg)

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
        default_macro_name = f"default__{macro_name}"
        connector_macro_name = f"{self._connector.connector_type}__{macro_name}"
        if connector_macro_name not in context.vars:
            connector_macro_name = default_macro_name 

        return context.vars[connector_macro_name](*args, **kwargs)
