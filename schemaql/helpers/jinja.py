from pathlib import Path
from jinja2 import Template, BaseLoader, FileSystemLoader, Environment, contextfilter, contextfunction
from schemaql.helpers.fileio import schemaql_path
from schemaql.helpers.logger import logger

class PrependingLoader(BaseLoader):
 
    def __init__(self, delegate, prepend_templates):
        self.delegate = delegate
        self.prepend_templates = prepend_templates
 
    def get_source(self, environment, template):

        complete_prepend_source = ""

        if template not in self.prepend_templates:
            for prepend_template in self.prepend_templates:
                prepend_source, _, _ = self.delegate.get_source(environment, prepend_template)
                # alias = Path(prepend_template).stem
                # prepend_source = f"{{% from '{prepend_template}' import {alias} with context %}}\n"
                # prepend_source = f"{{% from '{prepend_template}' import {alias} with context %}}\n"
                complete_prepend_source += prepend_source
        
        main_source, main_filename, main_uptodate = self.delegate.get_source(environment, template)
        # uptodate = lambda: prepend_uptodate() and main_uptodate()
        uptodate = lambda: main_uptodate()
        complete_source = (complete_prepend_source + main_source)
        # logger.info(complete_source)

        return complete_source, main_filename, uptodate
 
    def list_templates(self):
        return self.delegate.list_templates()

class JinjaConfig(object):

    def __init__(self, template_type, connector_type):
        self._template_type = template_type
        self._connector_type = connector_type
        self._environment = self._get_jinja_template_env()
 
    @property
    def environment(self):
        return self._environment
        
    def _get_jinja_template_env(self):

        template_path = schemaql_path.joinpath("templates", self._template_type).resolve()
        base_loader = FileSystemLoader(str(template_path))
    
        macro_path = schemaql_path.joinpath("templates", self._template_type, "macros")

        preload_macros = []
        for f in Path(macro_path).glob("**/*.sql"):
            preload_macros.append(str(f.relative_to(template_path)))

        loader = PrependingLoader(base_loader, preload_macros)

        env = Environment(loader=loader)
        env.globals["log"] = self.log
        env.globals["connector_type"] = self._connector_type 
        env.globals["connector_macro"] = self.connector_macro

        return env

    @contextfunction
    def log(self, context, msg):
        logger.info(msg)

    @contextfunction
    def connector_macro(self, context, macro_name, *args, **kwargs):
        default_macro_name = f"default__{macro_name}"
        connector_macro_name = f"{self._connector_type}__{macro_name}"
        if connector_macro_name not in context.vars:
            connector_macro_name = default_macro_name 

        return context.vars[connector_macro_name] (*args, **kwargs)