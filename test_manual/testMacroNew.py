from dbt.config import Profile
from dbt.config.renderer import ProfileRenderer

if __name__ == '__main__':
    import os

import os

os.environ["DBT_PROFILES_DIR"] = "/tmp/dbt_profiles/"

import dbt
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.model_config import NodeConfig
from dbt.config.project import PartialProject
from dbt.contracts.graph.parsed import ParsedMacro, ParsedModelNode
from dbt.node_types import NodeType


def createParsedMacro(macroName, macroContent, macroPath, packageName='prophecy_package'):
    compile_macro_root = f'{macroName}'
    customCompileMacro = ParsedMacro(
        name=compile_macro_root,
        resource_type=NodeType.Macro,
        unique_id=compile_macro_root,
        package_name=f'{packageName}',
        original_file_path=normalize(f'{macroPath}'),
        root_path=get_abs_os_path('/tmp/prophecy'),
        path=normalize(f'{macroPath}'),
        macro_sql=f'''{macroContent}''',
    )
    return customCompileMacro


from dbt.clients.jinja import get_template


# Kish - TODO : Figure out if we need to pass in arguments to this as well or not
def getTemplateAsModule(macroName, retVal):
    return get_template(f'''
         {{{{ {macroName}() }}}}
         ''', retVal, capture_macros=False).module


def inject_plugin(plugin):
    from dbt.adapters.factory import FACTORY
    key = plugin.adapter.type()
    FACTORY.plugins[key] = plugin


def inject_adapter(value, plugin):
    inject_plugin(plugin)
    from dbt.adapters.factory import FACTORY
    key = value.type()
    FACTORY.adapters[key] = value


def empty_profile_renderer():
    return dbt.config.renderer.ProfileRenderer({})


def normalize(path):
    return os.path.normcase(os.path.normpath(path))


def get_abs_os_path(unix_path):
    return normalize(os.path.abspath(unix_path))


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars='{}'):
    from dbt.context.target import generate_target_context
    from dbt.config import Project
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop('project-root', os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def profile_from_dict(profile, profile_name, cli_vars='{}'):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.context.base import generate_base_context
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


class Obj:
    which = 'blah'
    single_threaded = False


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars='{}'):
    from dbt.config import Project, Profile, RuntimeConfig
    from copy import deepcopy

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get('profile')

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = '/dev/null'
    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args=args
    )


model_config = NodeConfig.from_dict({
    'enabled': True,
    'materialized': 'view',
    'persist_docs': {},
    'post-hook': [],
    'pre-hook': [],
    'vars': {},
    'quoting': {},
    'column_types': {},
    'tags': []
})

project_cfg = {
    'name': 'prophecy_package',
    'version': '0.1',
    'profile': 'test',
    'project-root': '/Users/kishore/prophecy-git/dbt-core/dummy_profiles/dummy_dbt/',
    'config-version': 2,
    'vars': {},
}
profile_cfg = {
    'outputs': {
        'test': {
            'type': 'postgres',
            'dbname': 'postgres',
            'user': 'root',
            'host': 'localhost',
            'pass': 'password',
            'port': 5432,
            'schema': 'public'
        }
    },
    'target': 'test'
}
bigquery_profile_cfg = {
    'outputs': {
        'test': {
            'type': 'bigquery',
            'method': 'service-account',
            'project': 'prophecy-development',
            'keyfile': '/Users/kishore/Downloads/googlebigquery.json',
            'dataset': 'dataset',
            'threads': 3
        }
    },
    'target': 'test'
}
snowflake_profile_cfg = {
    'outputs': {
        'test': {
            'type': 'snowflake',
            'account': 'IO13013.ap-south-1.aws',
            'user': 'KISHOREPROPHECY',
            'password': 'BHA7RpsTjmhbph5',
            'authenticator': 'username_password_mfa',
            'role': 'PC_DBT_ROLE',
            'database': 'PC_DBT_DB',
            'warehouse': 'PC_DBT_WH',
            'schema': 'dbt_KBandi',
            'threads': 1,
            'client_session_keep_alive': False
        }
    },
    'target': 'test'
}

# Kish - TODO : This config can use uniqueID in future, when Program Builder comes
config = config_from_parts_or_dicts(project_cfg, profile_cfg)
bigquery_config = config_from_parts_or_dicts(project_cfg, bigquery_profile_cfg)
snowflake_config = config_from_parts_or_dicts(project_cfg, snowflake_profile_cfg)
from dbt.adapters.postgres import Plugin as PostgresPlugin
from dbt.adapters.snowflake import Plugin as SnowFlakePlugin
from dbt.adapters.bigquery import Plugin as BigQueryPlugin

inject_adapter(PostgresPlugin.adapter(config), PostgresPlugin)
inject_adapter(SnowFlakePlugin.adapter(snowflake_config), SnowFlakePlugin)
inject_adapter(BigQueryPlugin.adapter(bigquery_config), BigQueryPlugin)
from dbt.adapters.factory import get_adapter

adapter = get_adapter(config)
macro_hook = adapter.connections.set_query_header
from dbt.parser.manifest import ManifestLoader

loadedMacros = ManifestLoader.load_macros(config, macro_hook)

# We can add our custom macros in here
customMacros = {}

manifest = Manifest(
    macros={**loadedMacros.macros, **customMacros},
    nodes={
        'test': ParsedModelNode(
            name='test',
            database='dbt',
            schema='prophecy',
            alias='test',
            resource_type=NodeType.Model,
            unique_id='test',
            fqn=['test'],
            package_name='prophecy_package',
            root_path='/usr/kishore/app',
            config=model_config,
            path='view.sql',
            original_file_path='view.sql',
            language='sql',
            raw_code='''''',
            checksum=FileHash.from_contents(''),
        ),
    },
    sources={},
    docs={},
    disabled=[],
    files={},
    exposures={},
    metrics={},
    selectors={},
)

import copy

if '$manifestSpecificToActor' not in globals():
    print("not found")
    global manifest_1401344873
    manifest_1401344873 = copy.deepcopy(manifest)
    manifest_1401344873.nodes['test'].package_name = 'prophecy_package'
    global config_1401344873
    config_1401344873 = copy.deepcopy(config)
    config_1401344873.project_name = 'prophecy_package'
    renderer = ProfileRenderer(snowflake_profile_cfg)
    newProfile = Profile.from_raw_profile_info(
        copy.deepcopy(snowflake_profile_cfg),
        'test',
        renderer,
    )
    config_1401344873.credentials = newProfile
else:
    print("found manifestSpecificToActor, updating it")


def injectmacros_schema_analyzer_1401344873_6():
    from dbt.context.providers import generate_parser_model_context
    from dbt.context.context_config import ContextConfig

    macroName = "cents"
    macroContent = """{% macro cents(column_name='abc', decimal_places=123) %}
{{column_name}} + 123 + 1 + abc
{% endmacro %}
"""

    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'abc')
    compile_macro_root = f'abc.{macroName}'

    global manifest_1401344873
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    print(get_template('''
    {{ abc.cents() }}
    ''', ret, capture_macros=False).module)

    macroName = "my_call"
    macroContent = """{% macro my_call(column_name='abc', decimal_places=123) %}
{{abc.cents()}} + 123 + 3 + abc
{% endmacro %}
"""
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'abc')
    compile_macro_root = f'abc.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    print(get_template('''
    {{ abc.my_call() }}
    ''', ret, capture_macros=False).module)

    macroName = "get_table_types_sql"
    macroContent = """{%- macro get_table_types_sql() -%}
  {{ return(adapter.dispatch('get_table_types_sql', 'dbt_utils')()) }}
{%- endmacro -%}
"""
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'abc')
    compile_macro_root = f'abc.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    macroName = "default__get_table_types_sql"
    macroContent = """{% macro default__get_table_types_sql() %}
            case table_type
                when 'BASE TABLE' then 'table'
                when 'EXTERNAL TABLE' then 'external'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(table_type)
            end as {{ adapter.quote('table_type') }}
{% endmacro %}
"""
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'abc')
    compile_macro_root = f'abc.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    macroName = "postgres__get_table_types_sql"
    macroContent = """{% macro postgres__get_table_types_sql() %}
            case table_type
                when 'BASE TABLE' then 'table'
                when 'FOREIGN' then 'external'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(table_type)
            end as {{ adapter.quote('table_type') }}
{% endmacro %}
"""
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'abc')
    compile_macro_root = f'abc.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    macroName = "databricks__get_table_types_sql"
    macroContent = """{% macro databricks__get_table_types_sql() %}
            case table_type
                when 'MANAGED' then 'table'
                when 'BASE TABLE' then 'table'
                when 'MATERIALIZED VIEW' then 'materializedview'
                else lower(table_type)
            end as {{ adapter.quote('table_type') }}
{% endmacro %}
"""
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'abc')
    compile_macro_root = f'abc.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    macroName = "cents"
    macroContent = """{% macro cents(column_name='abc', decimal_places=123) %}
{{column_name}} + 123 + 2
{% endmacro %}
"""
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'defg')
    compile_macro_root = f'defg.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))

    print(get_template('''
    {{ defg.cents() }}
    ''', ret, capture_macros=False).module)

    macroContent = """{% macro cents(column_name='abc', decimal_places=123) %}
    {{column_name}} + 123 + 3
    {% endmacro %}
    """
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'prophecy_package')
    compile_macro_root = f'prophecy_package.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))
    print("calling prophecy_package cents")
    print(get_template('''
            {{ cents() }}
            ''', ret, capture_macros=False).module)

    macroName = "my_call"
    macroContent = """{% macro my_call(column_name='abc', decimal_places=123) %}
        {{cents()}} + 123 + 3 + prophecy
        {% endmacro %}
        """
    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/cents.sql", 'prophecy_package')
    compile_macro_root = f'prophecy_package.{macroName}'
    manifest_1401344873.macros[compile_macro_root] = parsedMacro

    ret = generate_parser_model_context(manifest_1401344873.nodes['test'], config_1401344873, manifest_1401344873,
                                        ContextConfig(
                                            config_1401344873,
                                            manifest_1401344873.nodes['test'].fqn,
                                            manifest_1401344873.nodes['test'].resource_type,
                                            "root",
                                        ))
    print("calling prophecy_package my_call")
    print(get_template('''
            {{ my_call() }}
            ''', ret, capture_macros=False).module)

    return 123


injectmacros_schema_analyzer_1401344873_6()


def macros_schema_analyzer_1401344873_41(query: str):
    from dbt.context.providers import generate_parser_model_context
    from dbt.context.context_config import ContextConfig

    macroName = "nested_cents"
    macroContent = """{% macro nested_cents(column_name) %}
{{abc.cents(column_name)}} + bla
{{defg.cents(column_name)}} + bla
{{abc.my_call()}} + bla
{% endmacro %}
"""

    parsedMacro = createParsedMacro(macroName, macroContent, "TestMacros/macros/nested_cents.sql", 'prophecy_package')
    compile_macro_root = f'prophecy_package.{macroName}'

    import copy
    global manifest_1401344873
    compiledMacroManifest = copy.deepcopy(manifest_1401344873)
    compiledMacroManifest.macros[compile_macro_root] = parsedMacro
    ret = generate_parser_model_context(compiledMacroManifest.nodes['test'], config_1401344873, compiledMacroManifest,
                                        ContextConfig(
                                            config_1401344873,
                                            compiledMacroManifest.nodes['test'].fqn,
                                            compiledMacroManifest.nodes['test'].resource_type,
                                            "root",
                                        ))

    compiledModules = {}
    compiledModules[macroName] = getTemplateAsModule(macroName, ret)

    print("Calling nestedCents")
    return get_template('''
    {{ abc.get_table_types_sql() }}
    ''', ret, capture_macros=False).module


print(macros_schema_analyzer_1401344873_41(""))
