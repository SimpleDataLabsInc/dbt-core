import os

from dbt.artifacts.resources.v1.config import NodeAndTestConfig
from dbt.artifacts.resources.v1.snapshot import SnapshotConfig
from dbt.parser import SnapshotParser

os.environ["DBT_PROFILES_DIR"] = "/Users/kishore/prophecy-git/dbt-core/dummy_profiles/dbt_profiles/"
projectPath = "/tmp/dummy_dbt/"

import os
from dbt.flags import set_from_args
from argparse import Namespace

os.environ["DBT_PROFILES_DIR"] = "/tmp/dbt_profiles/"
args = Namespace(
    profiles_dir="/tmp/dbt_profiles/",
    project_dir="/tmp/dummy-dbt/",
    target=None,
    profile=None,
    threads=None,
)
set_from_args(args, {})

import dbt
from dbt.contracts.files import FileHash, FilePath, SchemaSourceFile, SourceFile
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.model_config import NodeConfig
from dbt.config.project import PartialProject
from dbt.contracts.graph.nodes import Macro, ModelNode, SnapshotNode
from dbt.node_types import NodeType
from typing import Dict, Type


def createMacro(macroName, macroContent, macroPath, packageName='prophecy_package'):
    compile_macro_root = f'{macroName}'
    customCompileMacro = Macro(
        name=compile_macro_root,
        resource_type=NodeType.Macro,
        unique_id=compile_macro_root,
        package_name=f'{packageName}',
        original_file_path=normalize(f'{macroPath}'),
        path=normalize(f'{macroPath}'),
        macro_sql=f'''{macroContent}''',
    )
    return customCompileMacro


def createSnapshot(macroName, macroContent, macroPath, packageName='prophecy_package'):
    compile_macro_root = f'{macroName}'
    customCompileMacro = SnapshotNode(
        name=compile_macro_root,
        resource_type=NodeType.Macro,
        unique_id=compile_macro_root,
        package_name=f'{packageName}',
        original_file_path=normalize(f'{macroPath}'),
        path=normalize(f'{macroPath}'),
        macro_sql=f'''{macroContent}''',
    )
    return customCompileMacro


# Kish - TODO : Figure out if we need to pass in arguments to this as well or not
def getTemplateAsModule(macroName, retVal):
    return get_template(f'''
         {{{{ {macroName}() }}}}
         ''', retVal, capture_macros=False).make_module(dict())


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


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars=dict()):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, Dict):
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


def profile_from_dict(profile, profile_name, cli_vars=dict()):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars
    if not isinstance(cli_vars, Dict):
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


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars=dict()):
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
    args = Namespace(
        profiles_dir='/dev/null',
        project_dir='/dev/null',
        target=None,
        profile=None,
        threads=None,
        vars=cli_vars
    )
    rc = RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args=args
    )
    rc.load_dependencies()
    return rc


snowflake_profile_cfg = {
    'outputs': {
        'test': {
            'type': 'snowflake',
            'account': 'IO13013.ap-south-1.aws',
            'user': 'KISHOREPROPHECY',
            'password': 'BANDI123',
            'authenticator': 'username_password_mfa',
            'role': 'PC_DBT_ROLE',
            'database': 'PC_DBT_DB',
            'warehouse': 'PC_DBT_WH',
            'schema': 'dbt_KBandi',
            'threads': 3,
            'client_session_keep_alive': False
        }
    },
    'target': 'test'
}

bigquery_profile_cfg = {
    'outputs': {
        'test': {
        'method': 'service-account',
        'project': 'prophecy-development',
        'keyfile': '/Users/kishore/Downloads/google-bigquery-new.json',
        'dataset': 'dataset',
        'type': 'bigquery',
        'threads': 1
        }
    },
    'target': 'test'
}
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
    'project-root': projectPath,
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

# Kish - TODO : This config can use uniqueID in future, when Program Builder comes
config = config_from_parts_or_dicts(project_cfg, profile_cfg)
from dbt.adapters.postgres import Plugin as PostgresPlugin
from multiprocessing import get_context

inject_adapter(PostgresPlugin.adapter(config, get_context("spawn")), PostgresPlugin)

snowflake_config = config_from_parts_or_dicts(project_cfg, snowflake_profile_cfg)
from dbt.adapters.snowflake import Plugin as SnowFlakePlugin

inject_adapter(SnowFlakePlugin.adapter(snowflake_config, get_context("spawn")), SnowFlakePlugin)

bigquery_config = config_from_parts_or_dicts(project_cfg, bigquery_profile_cfg)
from dbt.adapters.bigquery import Plugin as BigQueryPlugin

inject_adapter(BigQueryPlugin.adapter(bigquery_config, get_context("spawn")), BigQueryPlugin)

from dbt.adapters.factory import get_adapter

adapter = get_adapter(config)
macro_hook = adapter.connections.set_query_header
from dbt.parser.manifest import ManifestLoader

loadedMacros = ManifestLoader.load_macros(config, macro_hook)
loadedMacros.macros.update(ManifestLoader.load_macros(snowflake_config, macro_hook).macros)
loadedMacros.macros.update(ManifestLoader.load_macros(bigquery_config, macro_hook).macros)

# We can add our custom macros in here
customMacros = {}

manifest = Manifest(
    macros={**loadedMacros.macros, **customMacros},
    nodes={
        'test': ModelNode(
            name='test',
            database='dbt',
            schema='prophecy',
            alias='test',
            resource_type=NodeType.Model,
            unique_id='test',
            fqn=['test'],
            package_name='prophecy_package',
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

if 'configSpecificToActor' not in globals():
    global configSpecificToActor
    configSpecificToActor = copy.deepcopy(bigquery_config)
    configSpecificToActor.project_name = '$packageName'
    global macrosSpecificToActor
    macrosSpecificToActor = {}
else:
    print("found configSpecificToActor configSpecificToActor, using it")
manifestSpecificToInvocation = copy.deepcopy(manifest)


def injectMacroFuncName():
    macroName = 'dbt_audit'
    macroContent = r'''{%- macro dbt_audit(cte_ref, created_by, updated_by, created_date, updated_date) -%}
    
        SELECT
          *,
          '{{ created_by }}'::VARCHAR       AS created_by,
          '{{ updated_by }}'::VARCHAR       AS updated_by,
          '{{ created_date }}'::DATE        AS model_created_date,
          '{{ updated_date }}'::DATE        AS model_updated_date,
          CURRENT_TIMESTAMP()               AS dbt_updated_at,
    
        {% if execute %}
    
            {% if not flags.FULL_REFRESH and config.get('materialized') == "incremental" %}
    
                {%- set source_relation = adapter.get_relation(
                    database=target.database,
                    schema=this.schema,
                    identifier=this.table,
                    ) -%}      
    
                {% if source_relation != None %}
    
                    {% set min_created_date %}
                        SELECT LEAST(MIN(dbt_created_at), CURRENT_TIMESTAMP()) AS min_ts 
                        FROM {{ this }}
                    {% endset %}
    
                    {% set results = run_query(min_created_date) %}
    
                    '{{results.columns[0].values()[0]}}'::TIMESTAMP AS dbt_created_at
    
                {% else %}
    
                    CURRENT_TIMESTAMP()               AS dbt_created_at
    
                {% endif %}
    
            {% else %}
    
                CURRENT_TIMESTAMP()               AS dbt_created_at
    
            {% endif %}
        
        {% endif %}
    
        FROM {{ cte_ref }}
    
    {%- endmacro -%}
    '''

    macro = createMacro(macroName, macroContent, "$macroSqlPath", "$packageName")
    compile_macro_root = f'$packageName.{macroName}'

    global macrosSpecificToActor
    macrosSpecificToActor[compile_macro_root] = macro

    return 123

if 'config_prClNeOf' not in globals():
    global config_prClNeOf
    config_prClNeOf = copy.deepcopy(bigquery_config)
    config_prClNeOf.project_name = 'jaffle_shop'
    global macros_prClNeOf
    macros_prClNeOf = {}
else:
    print("found configSpecificToActor config_prClNeOf, using it")

if __name__ == '__main__':
    # injectMacroFuncName()
#     query = """
#
#     {{ config(
#         target_database=database,
#         target_schema=schema,
#         unique_key='id',
#         strategy='timestamp',
#         updated_at='updated_at',
#         invalidate_hard_deletes=True,
#         materialization="snapshot",
#     ) }}
#     select * from {{ ref('fact') }}
#
# """
#
#     parser = SnapshotParser(
#         project=snowflake_config,
#         manifest=manifestSpecificToInvocation,
#         root_project=snowflake_config
#     )
#
#
#     def file_block_for(data: str, filename: str, searched: str):
#         root_dir = get_abs_os_path("./dbt_packages/snowplow")
#         filename = normalize(filename)
#         path = FilePath(
#             searched_path=searched,
#             relative_path=filename,
#             project_root=root_dir,
#             modification_time=0.0,
#         )
#         sf_cls = SchemaSourceFile if filename.endswith(".yml") else SourceFile
#         source_file = sf_cls(
#             path=path,
#             checksum=FileHash.from_contents(data),
#             project_name="snowplow",
#         )
#         source_file.contents = data
#         from dbt.parser.search import FileBlock
#         return FileBlock(file=source_file)
#
#
#     block = file_block_for(query, "nested/snap_1.sql", "models")
#     parser.manifest.files[block.file.file_id] = block.file
#     parser.parse_file(block)
#
#     node = list(parser.manifest.nodes.values())[0]
#     print(node)

    config = SnapshotConfig.from_dict({
        'enabled': True,
        'persist_docs': {},
        'post-hook': [],
        'pre-hook': [],
        'vars': {},
        'quoting': {},
        'column_types': {},
        'tags': [],
        "strategy": "timestamp",
        "target_schema": "kishore",
        "unique_key": "fn",
        "updated_at": "sal"
    })

    # The Jinja2 template string
    jinja2_template = '''{{ {"avx":"a" "b" "c" + 1 ~ "def" ~ true}}}'''

    def jinja_to_python_dict(jinja_dict_str):
        """Converts a Jinja2 dictionary string to a Python dictionary with appropriate types.

        Args:
            jinja_dict_str (str): The Jinja2 dictionary string.

        Returns:
            dict: The converted Python dictionary.
        """

        import jinja2
        # Create a Jinja2 environment for rendering
        env = jinja2.Environment()

        # Render the Jinja2 dictionary string
        rendered_dict_str = env.from_string(jinja_dict_str).render()

        # Load the rendered string as a Python dictionary
        python_dict = eval(rendered_dict_str)

        return python_dict

    # Example usage
    jinja_dict_str = """{{ {"avx":"a" "b" "c" + 1 ~ "def" ~ true, "mybal": true} }}"""

    python_dict = SnapshotConfig.from_dict(jinja_to_python_dict(jinja_dict_str))
    print(python_dict)

    # newVal = dbtResolverFunctionName(query)  # .make_module(vars={'test_var': 'test_var_value_wefe'})

    # print(f"This is a test message", flush=True)
    # Doing this deepcopy inside the func intermittently gives BrokenPipe error for some reason,
    # and sometimes it just gets stuck
    manifest_y5q4s8ew = copy.deepcopy(manifest)

    def dbt_resolver_y5q4s8ew(query: str, additionalConfig: Type[NodeAndTestConfig] = None):
        # print("Inside function resolver", flush=True)
        # print(query, flush=True)
        # print("Done printing additional configuration", flush=True)
        import json
        from dbt.context.providers import generate_parser_model_context
        from dbt.context.context_config import ContextConfig

        localConfig = copy.deepcopy(configSpecificToActor)
        localConfig.vars.vars = {}

        manifest_y5q4s8ew.nodes['test'].package_name = configSpecificToActor.project_name
        manifest_y5q4s8ew.refs = []
        manifest_y5q4s8ew.sources = []
        manifest_y5q4s8ew.macros.update(macrosSpecificToActor)
        newModelNode = copy.deepcopy(manifest_y5q4s8ew.nodes['test'])

        additionalConfig = SnapshotConfig.from_dict(jinja_to_python_dict(r"""{{{    
    "strategy": "timestamp",
    "target_database": "JAFFLE_SHOP",
    "target_schema": "SNAPSHOTS",
    "unique_key": "order_id",
    "updated_at": "undefined_updated_at"
  }}}"""))

        anotherAdditionalConfig = SnapshotConfig.from_dict(jinja_to_python_dict("""{{ {    
    "check_cols": ["STATUS"],
    "invalidate_hard_deletes": true,
    "strategy": 'check',
    "target_schema": "public",
    "unique_key": "ID"
  } }}"""))

        # Set the config as SnapshotConfig or any passed config
        if additionalConfig is not None:
            newModelNode.config = additionalConfig

        ret = generate_parser_model_context(newModelNode, localConfig, manifest_y5q4s8ew, ContextConfig(
            localConfig,
            manifest_y5q4s8ew.nodes['test'].fqn,
            manifest_y5q4s8ew.nodes['test'].resource_type,
            "root",
        ))

        from dbt.clients.jinja import get_template

        # We're explicitly replacing the ParseConfigObject with SnapshotConfig/User defined config as We're using a patched version of snapshot to get the SQL out.
        # Like {%- set config = model['config'] -%} this is not being done, so we're not truly running the original config.
        if additionalConfig is not None:
            tempDictConfig = additionalConfig.__dict__
            additionalConfig = {key: value for key, value in tempDictConfig.items() if value is not None}
            ret['config'] = additionalConfig

        return get_template(query, ret, capture_macros = False).make_module(dict())

    newVal = dbt_resolver_y5q4s8ew(r"""{# Code taken from dbt-adapters repo - dbt/include/global_project/macros/materializations/snapshots/snapshot.sql #}
{# {% set target_relation_exists, target_relation = get_or_create_relation(
    database = 'prophecy_db',
    schema = 'prophecy_schema',
    identifier = 'prophecy_identifier',
    type = 'table') -%} #}
{%- set strategy_name = config.get('strategy') -%}
{% set strategy_macro = strategy_dispatch(strategy_name) %}
{# We're settings target_relation_exists so that we don't have to query DB #}
{% set target_relation_exists = False %}
{% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}
{% set query = "\nSELECT *\n\nFROM raw_customers" %}
{% set build_sql = build_snapshot_table(strategy, query) %}
{#   {% set final_sql = create_table_as(False, target_relation, build_sql) %} #}
{#   {{ snapshot_staging_table(strategy, "test", target_relation) }}  #}
{{ build_sql}}""")
    print(newVal)
    print("value df")
    print(dbt_resolver_y5q4s8ew("{{config}}"))
    print("Done")