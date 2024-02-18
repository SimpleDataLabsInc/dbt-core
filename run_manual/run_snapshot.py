import os

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
from typing import Dict


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
    return RuntimeConfig.from_parts(
        project=project,
        profile=profile,
        args=args
    )


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

inject_adapter(PostgresPlugin.adapter(config), PostgresPlugin)

snowflake_config = config_from_parts_or_dicts(project_cfg, snowflake_profile_cfg)
from dbt.adapters.snowflake import Plugin as SnowFlakePlugin

inject_adapter(SnowFlakePlugin.adapter(snowflake_config), SnowFlakePlugin)

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
    configSpecificToActor = copy.deepcopy(snowflake_config)
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


def dbtResolverFunctionName(query: str):
    from dbt.context.providers import generate_parser_model_context
    from dbt.context.context_config import ContextConfig

    localConfig = copy.deepcopy(configSpecificToActor)
    resolvedConfig = {'test_var': 'test_var_value_wefefefrf',
                      'payment_methods': ['new_var_value_1', 'new_var_value_2']}
    localConfig.vars.vars = resolvedConfig

    manifestSpecificToInvocation.nodes['test'].package_name = configSpecificToActor.project_name
    manifestSpecificToInvocation.refs = []
    manifestSpecificToInvocation.sources = []
    manifestSpecificToInvocation.macros.update(macrosSpecificToActor)

    ret = generate_parser_model_context(manifestSpecificToInvocation.nodes['test'], localConfig,
                                        manifestSpecificToInvocation, ContextConfig(
            localConfig,
            manifestSpecificToInvocation.nodes['test'].fqn,
            manifestSpecificToInvocation.nodes['test'].resource_type,
            "root",
        ))

    from dbt.clients.jinja import get_template

    return get_template(query, ret, capture_macros=False).module


if __name__ == '__main__':
    # injectMacroFuncName()
    query = """
{% snapshot orders %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

    parser = SnapshotParser(
        project=snowflake_config,
        manifest=manifestSpecificToInvocation,
        root_project=snowflake_config
    )


    def file_block_for(data: str, filename: str, searched: str):
        root_dir = get_abs_os_path("./dbt_packages/snowplow")
        filename = normalize(filename)
        path = FilePath(
            searched_path=searched,
            relative_path=filename,
            project_root=root_dir,
            modification_time=0.0,
        )
        sf_cls = SchemaSourceFile if filename.endswith(".yml") else SourceFile
        source_file = sf_cls(
            path=path,
            checksum=FileHash.from_contents(data),
            project_name="snowplow",
        )
        source_file.contents = data
        from dbt.parser.search import FileBlock
        return FileBlock(file=source_file)

    block = file_block_for(query, "nested/snap_1.sql", "models")
    parser.manifest.files[block.file.file_id] = block.file
    parser.parse_file(block)

    node = list(parser.manifest.nodes.values())[0]
    print(node)

    print(dbtResolverFunctionName("select * from dual"))

    newVal = dbtResolverFunctionName(query)  # .make_module(vars={'test_var': 'test_var_value_wefe'})
    print(newVal)
